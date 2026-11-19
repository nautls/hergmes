//! The keeper: holds the keys named in the configuration tracked for the life of the
//! process.
//!
//! It loads each key and holds a subscription to it, so the engine never untracks it for
//! idleness. The subscription is balance-only because the keeper reads nothing but its
//! end: the kind only filters what reaches the keeper's sink, and the engine tracks the key's
//! boxes the same either way, for every other subscriber and query. A token key has no
//! balance, so its sink gets only `resync_required`.
//!
//! The engine still drops a key whose load block is orphaned, or every key on a fork deeper
//! than the window; the subscription then ends with `resync_required`, and the keeper loads
//! the key again. A subscription that ends without one (a slow drain) leaves the key
//! tracked, so the keeper subscribes again without a reload.
//!
//! One key's trouble never stops the relay, which serves every other key and client. A token
//! above the supply limit, or a key over the per-key cap, is logged and given up; a key the
//! total cap refuses, or a node that returns boxes not matching the key, is logged and asked
//! again later. Only a stopped engine ends every keeper.
//!
//! Loads that clients start wait for the configured keys ([`Settled`]), so at startup they
//! can't take the total cap first.

use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, watch},
    time::sleep,
};
use tracing::{debug, error, info, warn};

use crate::{
    adapters::{
        EngineGone, backoff,
        loader::{LoadError, Loader},
        tokens::{TokenChecker, TokenError},
    },
    clients::NodeSource,
    engine::{Output, TrackingKey},
    runtime::{Handle, Subscription},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// The wait before trying again after an unknown token or a node error the loader does
    /// not wait for itself.
    pub retry: Duration,
    /// The wait after a load returned a box that does not match the key. That is a node
    /// fault; each retry is a full reload, so it is spaced well apart.
    pub mismatch_retry: Duration,
    /// The wait after the total cap refused the key. Room is made only as keys go idle past
    /// the engine's `evict_after`, and a retry reads pages until it is past the room.
    pub capacity_retry: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            retry: Duration::from_secs(5),
            mismatch_retry: Duration::from_secs(60),
            capacity_retry: Duration::from_secs(60),
        }
    }
}

/// Whether every configured key has settled its first load: tracked, given up, or refused
/// for a reason other than the engine not being at the node's tip yet. Until then the relay
/// starts no load for a client, so clients can't fill the total cap before the configured
/// keys are in. A key that keeps failing still settles, so it never holds every client off.
#[derive(Debug, Clone)]
pub struct Settled(watch::Receiver<usize>);

impl Settled {
    /// Already settled: no configured key to wait for.
    pub fn now() -> Self {
        Self(watch::Sender::new(0).subscribe())
    }

    pub fn is_settled(&self) -> bool {
        *self.0.borrow() == 0
    }
}

/// One configured key's part in [`Settled`]. It counts as settled once settled or dropped,
/// so a keeper that panics never holds the clients off.
#[derive(Debug)]
pub struct Pending(Option<Arc<watch::Sender<usize>>>);

impl Pending {
    fn settle(&mut self) {
        if let Some(count) = self.0.take() {
            count.send_modify(|n| *n -= 1);
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.settle();
    }
}

/// A [`Pending`] for each of `keys` configured keys, and what settles when all of them have.
pub fn settling(keys: usize) -> (Settled, Vec<Pending>) {
    let count = Arc::new(watch::Sender::new(keys));
    let settled = Settled(count.subscribe());
    (settled, (0..keys).map(|_| Pending(Some(count.clone()))).collect())
}

/// Outputs a keeper's sink holds. It takes only `balance` messages (none for a token key)
/// and drains them as they come, so a few are plenty.
const SINK: usize = 16;

pub struct Keeper<S> {
    engine: Handle,
    /// Shared with the page service, so a client's first request and a keeper's load of the
    /// same key are one load.
    loader: Arc<Loader<S>>,
    tokens: Arc<TokenChecker<S>>,
    config: Config,
}

impl<S: NodeSource> Keeper<S> {
    pub fn new(
        engine: Handle,
        loader: Arc<Loader<S>>,
        tokens: Arc<TokenChecker<S>>,
        config: Config,
    ) -> Self {
        Self { engine, loader, tokens, config }
    }

    /// Keeps `key` tracked for good, settling `pending` once its first load has an answer.
    /// Returns `Ok` only after giving the key up (a token above the supply limit or a key over
    /// the per-key cap, logged at `error`), and `EngineGone` when the engine stops.
    pub async fn keep(&self, key: TrackingKey, mut pending: Pending) -> Result<(), EngineGone> {
        let key = &key;
        // Whether the engine may still track the key. Only a subscription that ended without
        // `resync_required` (a slow drain) leaves it tracked, and a reload would then page
        // through every box for the engine to ignore as a duplicate, so the key is subscribed
        // again first and loaded only if that answers `resync_required`.
        let mut tracked = false;
        loop {
            if !tracked && !self.load(key, &mut pending).await? {
                return Ok(());
            }
            let (sink, mut stream) = mpsc::channel(SINK);
            // A `resync_required`, here or in the stream, is logged by the engine task.
            match self.engine.subscribe(key.clone(), None, sink).await? {
                Subscription::Live { .. } => debug!(?key, "Holding configured key"),
                Subscription::Resync(reason) => {
                    debug!(?key, ?reason, "Configured key not tracked, loading it");
                    tracked = false;
                    continue;
                }
            }
            let mut resync = false;
            while let Some(output) = stream.recv().await {
                resync |= matches!(output.output, Output::ResyncRequired { .. });
            }
            tracked = !resync;
            if resync {
                debug!(?key, "Configured key dropped, loading it again");
            } else {
                warn!(?key, "Configured key's subscription ended, subscribing again");
            }
        }
    }

    /// Loads `key`, trying again until the engine tracks it. `false` if the key was given up.
    /// `pending` is settled by any answer but the engine not being at the node's tip yet,
    /// which every load waits through at startup.
    async fn load(&self, key: &TrackingKey, pending: &mut Pending) -> Result<bool, EngineGone> {
        loop {
            if let TrackingKey::Token(id) = key {
                match self.tokens.check(id).await {
                    Ok(()) => {}
                    Err(TokenError::Node(error)) => {
                        pending.settle();
                        let wait = backoff(&error, self.config.retry);
                        warn!(?key, %error, ?wait, "Token check failed, trying again");
                        sleep(wait).await;
                        continue;
                    }
                    Err(TokenError::UnknownToken) => {
                        // The indexer may not have reached the mint yet.
                        pending.settle();
                        warn!(?key, wait = ?self.config.retry, "Configured token is unknown, trying again");
                        sleep(self.config.retry).await;
                        continue;
                    }
                    Err(error @ TokenError::SupplyTooLarge { .. }) => {
                        // Its emission never changes, so asking again can't help.
                        error!(?key, %error, "Configured token can't be tracked, giving it up");
                        return Ok(false);
                    }
                }
            }

            let error = match self.loader.load(&self.engine, key).await {
                Ok((as_of, boxes)) => {
                    info!(?key, ?as_of, boxes, "Configured key loaded");
                    pending.settle();
                    return Ok(true);
                }
                Err(error) => error,
            };
            if !matches!(*error, LoadError::GaveUp { .. }) {
                pending.settle();
            }
            let wait = match &*error {
                LoadError::EngineGone(gone) => return Err(*gone),
                error @ LoadError::TooManyBoxes { .. } => {
                    // The operator sets both the keys and the caps.
                    error!(?key, %error, "Configured key can't be tracked, giving it up");
                    return Ok(false);
                }
                error @ LoadError::CapacityExceeded { .. } => {
                    // Room is made as keys go idle and can be evicted. An exemption from the
                    // cap would need a second path in the engine.
                    let wait = self.config.capacity_retry;
                    warn!(?key, %error, ?wait, "Configured key refused by the cap, trying again");
                    wait
                }
                LoadError::Mismatch { box_id } => {
                    let wait = self.config.mismatch_retry;
                    error!(
                        ?key,
                        %box_id,
                        ?wait,
                        "The indexer returned a box that does not match the key, trying again"
                    );
                    wait
                }
                error @ LoadError::GaveUp { wait, .. } => {
                    // The engine is still catching up with the node, the tip keeps moving,
                    // or the indexer is recovering. The loader skips the wait after its last
                    // attempt, so it is taken here, a `503`'s `Retry-After` included.
                    info!(?key, %error, ?wait, "Configured key not loaded yet, trying again");
                    *wait
                }
                LoadError::Node(error) => {
                    let wait = backoff(error, self.config.retry);
                    warn!(?key, %error, ?wait, "Load failed, trying again");
                    wait
                }
            };
            sleep(wait).await;
        }
    }
}

#[cfg(test)]
#[path = "keeper_tests.rs"]
mod tests;
