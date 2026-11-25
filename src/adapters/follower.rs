//! The follower: one loop that reads `/info` once per tick and serves it to the block consumer
//! and then to the mempool poller.
//!
//! One read serves both, and the order matters: the engine applies the blocks up to the tip
//! the read shows before the mempool is fetched at that tip, so a snapshot arrives at a tip
//! the engine already has.
//!
//! The mempool is polled only once the engine is at the node's tip. A snapshot taken while it
//! is behind would be held or ignored by the engine, but counted as taken by the poller, which
//! would then wait for its refresh period before fetching again at the tip the engine
//! reaches.
//!
//! A load that finds the engine behind the node wakes the follower for an early tick (see
//! [`Wake`]), so a block reaches the engine when a client is waiting for it, without reading
//! the node more often when none is.

use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{Notify, watch},
    time::{Instant, sleep, sleep_until},
};
use tracing::{info, warn};

use crate::{
    adapters::{
        AdapterError, EngineGone, EngineHandle, backoff, best_full_block,
        blocks::{BlockConsumer, Sync},
        headers::HeaderCache,
        mempool::{self, MempoolPoller, Poll},
    },
    clients::{NodeError, NodeSource},
    engine::Event,
    types::ergo::Parameters,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// How often `/info` is read.
    pub interval: Duration,
    /// The least time from the end of a tick to a tick a [`Wake`] asks for. A wake comes from
    /// a load that found the engine behind the node, and loads keep finding it so while the
    /// indexer lags after a block, so without it the follower would read the node back to back
    /// when the node is slowest. Counted from the end, so a slow `/info` doesn't shorten it.
    pub wake_gap: Duration,
    pub mempool: mempool::Config,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            wake_gap: Duration::from_millis(200),
            mempool: mempool::Config::default(),
        }
    }
}

/// Asks the follower for a tick before its interval is up.
///
/// The engine learns of a block only from the follower, while a load reads the node's tip
/// itself, so right after a block a load sees it first and would wait up to an interval for
/// the engine. It wakes the follower instead. Wakes made before the follower gets to them
/// count as one. A `Wake` made with `default` reaches no follower.
#[derive(Debug, Clone, Default)]
pub struct Wake(Arc<Notify>);

impl Wake {
    pub fn wake(&self) {
        self.0.notify_one();
    }

    /// Whether a wake is pending, taking it.
    #[cfg(test)]
    pub fn take(&self) -> bool {
        use std::future::Future;
        let notified = std::pin::pin!(self.0.notified());
        notified
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_ready()
    }
}

/// What one tick did.
#[derive(Debug)]
pub struct Tick {
    pub blocks: Result<Sync, NodeError>,
    /// `None` when the engine is not at the node's tip: the mempool was not polled.
    pub mempool: Option<Result<Poll, NodeError>>,
}

/// Follows the node's tip and mempool.
pub struct Follower<S> {
    source: S,
    blocks: BlockConsumer<S>,
    mempool: MempoolPoller<S>,
    config: Config,
    /// Whether a tick has left the engine at the node's tip, for `/v1/status`.
    started: watch::Sender<bool>,
    /// The chain's parameters from the last `/info` read, for `/v1/parameters`.
    parameters: watch::Sender<Option<Parameters>>,
    wake: Wake,
}

impl<S: NodeSource + Clone> Follower<S> {
    pub fn new(source: S, config: Config) -> Self {
        Self {
            blocks: BlockConsumer::new(source.clone()),
            mempool: MempoolPoller::new(source.clone(), config.mempool),
            source,
            config,
            started: watch::Sender::new(false),
            parameters: watch::Sender::new(None),
            wake: Wake::default(),
        }
    }

    /// Whether a tick has left the engine at the node's tip. Once true it stays true: after
    /// each block the engine is behind for a tick or two while the indexer catches up, and
    /// loads already wait that out, so readiness doesn't flicker with it. Read by the status
    /// route, which makes no node call of its own.
    pub fn started(&self) -> watch::Receiver<bool> {
        self.started.subscribe()
    }

    /// The chain's parameters as the node last reported them; `None` until the first `/info`
    /// read. They are the node's state, not the engine's, so a tick publishes them whether or
    /// not the engine reaches the node's tip. Read by the parameters route, which makes no node
    /// call of its own.
    pub fn parameters(&self) -> watch::Receiver<Option<Parameters>> {
        self.parameters.subscribe()
    }

    /// A handle that wakes this follower, for the readers that find the engine behind.
    pub fn waker(&self) -> Wake {
        self.wake.clone()
    }

    /// The headers of the blocks the follower applies, for the header reader.
    pub fn headers(&self) -> HeaderCache {
        self.blocks.headers()
    }

    /// One tick at `now`: read `/info`, bring the engine to its tip, then, if it got there,
    /// poll the mempool and send the snapshot taken. `Err` only if `/info` itself failed or
    /// the engine is gone.
    pub async fn tick(
        &mut self,
        engine: &impl EngineHandle,
        now: Instant,
    ) -> Result<Tick, AdapterError> {
        let info = self.source.info().await?;
        self.parameters.send_if_modified(|parameters| {
            parameters.replace(info.parameters) != Some(info.parameters)
        });
        let blocks = match best_full_block(&info) {
            Some(tip) => match self.blocks.sync(engine, tip).await {
                Ok(sync) => Ok(sync),
                Err(error) => Err(error.into_node()?),
            },
            None => Ok(Sync::Idle),
        };
        if !matches!(blocks, Ok(Sync::Idle | Sync::Synced { .. })) {
            return Ok(Tick { blocks, mempool: None });
        }
        self.started
            .send_if_modified(|started| !std::mem::replace(started, true));
        let mempool = self.mempool.poll(info, now).await;
        if let Ok(Poll::Snapshot { txs, observed_tip }) = &mempool {
            let event = Event::MempoolSnapshot { txs: txs.clone(), observed_tip: *observed_tip };
            // The engine never refuses a snapshot: one off its tip is held or ignored.
            let _ = engine.apply(event).await?;
        }
        Ok(Tick { blocks, mempool: Some(mempool) })
    }

    /// Ticks an interval after the last tick ended, or a `wake_gap` after it when woken, until
    /// the engine is gone. Counted from the end, so a node slower than the interval is still
    /// left an interval between reads. A failed call is logged, and a `503` waits for its
    /// `Retry-After` before the next tick, woken or not. A refused event is logged by the
    /// engine task; the next tick syncs again from the engine's chain.
    pub async fn run(mut self, engine: &impl EngineHandle) -> EngineGone {
        let wake = self.wake.0.clone();
        let mut ended: Option<Instant> = None;
        loop {
            let due = ended.map_or_else(Instant::now, |ended| ended + self.config.interval);
            tokio::select! {
                () = sleep_until(due) => {}
                () = wake.notified() => {
                    if let Some(ended) = ended {
                        sleep_until(ended + self.config.wake_gap).await;
                    }
                }
            }
            let result = self.tick(engine, Instant::now()).await;
            ended = Some(Instant::now());
            let wait = match result {
                Ok(tick) => {
                    if let Some(Err(error)) = &tick.mempool {
                        warn!(%error, "Mempool poll failed");
                    }
                    tick.blocks.err()
                }
                Err(AdapterError::Node(error)) => Some(error),
                Err(AdapterError::EngineGone(gone)) => return gone,
            };
            if let Some(error) = wait {
                let wait = backoff(&error, self.config.interval);
                match error {
                    NodeError::Unavailable { .. } => {
                        info!(?wait, "Node unavailable (503), backing off");
                    }
                    error => warn!(%error, "Block sync failed"),
                }
                sleep(wait).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "follower_tests.rs"]
mod tests;
