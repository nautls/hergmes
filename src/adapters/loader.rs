//! The first-load loader: a key's whole unspent set from the indexer, pinned to one block,
//! sent to the engine as `KeyLoaded`.
//!
//! The indexer can't pin a read to a block, so the loader pins it from outside: the node's
//! tip is read before and after the pages, and the indexer must be at that tip.
//!
//! The boxes of a load the engine takes seed the box cache, so the pages that follow (a
//! snapshot right after a subscribe, the common case) make no node call.
//!
//! Concurrent loads of one key are coalesced: one runs, and the other callers wait for its
//! outcome instead of paging the key from the node again. A key refused for having too many
//! boxes is remembered for a while, so asking again doesn't page it from the node again.

use std::{sync::Arc, time::Duration};

use moka::future::Cache;
use tokio::time::{Instant, sleep};
use tracing::{debug, info};

use crate::{
    adapters::{
        BoxCache, EngineGone, EngineHandle, backoff, best_full_block, engine_capacity, engine_tip,
    },
    clients::{NodeError, NodeSource, Page, source::MAX_PAGE_LIMIT},
    engine::{BlockRef, Capacity, Event, LoadedBox, Output, TrackingKey},
    types::ergo::{BoxId, IndexedBox, UTxO},
};

/// The default cap on a key's boxes: the most that leave room in one page, so a key over it
/// fills its first page and is refused there. The node reads every unspent box before a
/// page's offset, one database read each, and slices (`Segment.retrieveUtxos`, node 6.0.8),
/// so paging a key costs about `n² / (2 × page)` reads: on a mainnet node about 0.2 to 0.5 ms
/// a box, so a fourth page of 16,384 took over the client's 30 s timeout, and a load that
/// long often sees a block and starts over. About 2.2 MB of ids in the engine, and about 7 MB
/// of full boxes while it loads.
pub const MAX_BOXES: usize = MAX_PAGE_LIMIT as usize - 1;

/// How long a key over the per-key cap is answered `TooManyBoxes` without paging it again:
/// the default grace period. A key gets under the cap only by many consolidation txs,
/// so a wallet that just did waits at most this long, while a hot address over the cap costs
/// one reload per this long rather than one per request.
pub const REFUSAL_TTL: Duration = Duration::from_secs(10 * 60);

/// The most refused keys remembered. A key over the cap needs that many boxes on chain, so
/// there are few; the bound only keeps trees of up to 4 KB from adding up.
const REFUSALS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Boxes per page, at most [`MAX_PAGE_LIMIT`] (the node rejects more).
    pub page_limit: u32,
    /// The most boxes a key can have. Paging stops as soon as a key has more, so a huge
    /// address is never paged in full only to be refused.
    pub max_boxes: usize,
    /// How long a key over `max_boxes` is refused without paging it again.
    pub refusal_ttl: Duration,
    /// How long [`Loader::load`] keeps trying: no attempt starts past it, and one always
    /// runs.
    pub give_up_after: Duration,
    /// The wait after an attempt that paged the key and lost it ([`Attempt::TipMoved`],
    /// [`Attempt::Rejected`]): the next one is a full reload too, so it waits about a
    /// follower tick, which is what a moving tip needs to settle.
    pub retry: Duration,
    /// The wait after an attempt that stopped before the pages ([`Attempt::NoTip`],
    /// [`Attempt::EngineBehind`], [`Attempt::IndexerBehind`]). Those cost one or two cheap
    /// calls, and their cause usually clears within a block's first second: the engine trails
    /// the node by up to a follower tick, and a `retry` would answer up to that much late.
    pub poll: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            page_limit: MAX_PAGE_LIMIT,
            max_boxes: MAX_BOXES,
            refusal_ttl: REFUSAL_TTL,
            give_up_after: Duration::from_secs(10),
            retry: Duration::from_secs(1),
            poll: Duration::from_millis(200),
        }
    }
}

/// The outcome of one attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attempt {
    /// The engine accepted the load and tracks the key.
    Tracked { as_of: BlockRef, boxes: usize },
    /// The node has no full block yet.
    NoTip,
    /// The engine is not at the node's tip, so a load there would be rejected. Nothing was
    /// loaded; the block consumer catches up first.
    EngineBehind { engine: Option<BlockRef>, node: BlockRef },
    /// The indexer is not at the node's tip.
    IndexerBehind { indexed_height: u32, tip_height: u32 },
    /// The node's tip moved during the load. The boxes were dropped, never sent mixed.
    TipMoved,
    /// The engine moved between its tip read and the load (`LoadRejected`).
    Rejected,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error(transparent)]
    Node(#[from] NodeError),
    /// The indexer returned a box the key does not match. A node bug; the key is not tracked.
    #[error("the indexer returned box {box_id} that does not match the key")]
    Mismatch { box_id: BoxId },
    /// No attempt settled. `last` is the last attempt's outcome, `None` when the indexer
    /// answered `503`. `wait` is what the loader would have waited before the next attempt
    /// (a `503`'s `Retry-After`, or the retry): a caller that tries again waits it first.
    #[error("no load settled after {attempts} attempts, the last one {last:?}")]
    GaveUp { attempts: u32, last: Option<Attempt>, wait: Duration },
    /// The key has more than `max` boxes. Loading it again won't change that.
    #[error("the key has more than {max} boxes")]
    TooManyBoxes { max: usize },
    /// The key's boxes would pass the cap on tracked boxes: the engine refused them, or the
    /// loader stopped paging at the first page past the room the engine had left, idle keys'
    /// room counted. It frees up as keys go idle: once a key with no subscribers has gone
    /// unread for the engine's `evict_after`.
    #[error(
        "{boxes} more boxes would pass the cap on tracked boxes, with {tracked} tracked, \
         {evictable} of them by idle keys"
    )]
    CapacityExceeded { tracked: usize, evictable: usize, boxes: usize },
    #[error(transparent)]
    EngineGone(#[from] EngineGone),
}

/// The outcome of a load, as every caller waiting on it gets it.
pub type Loaded = Result<(BlockRef, usize), Arc<LoadError>>;

pub struct Loader<S> {
    source: S,
    config: Config,
    cache: BoxCache,
    /// The loads running, by key, so that a second caller waits for the first one's outcome.
    ///
    /// moka's `try_get_with` runs one `init` per key and gives its result to every caller
    /// waiting on it. It inserts only an `Ok`, and an outcome must never be kept: once the key
    /// is untracked it has to be loaded again. So the whole outcome travels as the `Err` side
    /// (shared as `Arc`), and the map holds nothing once a load returns, with no `invalidate`
    /// step a dropped caller could skip. If the caller running the load is dropped (a client
    /// that disconnects), a waiting caller runs it instead; moka panics after 200 such aborts
    /// in a row on one key, which only that caller sees (moka 0.12.16,
    /// `future/value_initializer.rs`, `try_init_or_read`).
    loads: Cache<TrackingKey, ()>,
    /// Keys refused for having more than `max_boxes`, until `refusal_ttl` passes. Expiring is
    /// the whole invalidation: the count changes only through blocks, and following it for
    /// untracked keys would mean engine state, rollbacks and a race at the refusal, to save a
    /// wallet a few minutes once after a long consolidation. The total cap is not
    /// remembered: it frees up as keys go idle.
    refused: Cache<TrackingKey, usize>,
}

impl<S: NodeSource> Loader<S> {
    pub fn new(source: S, config: Config) -> Self {
        assert!(config.page_limit >= 1 && config.page_limit <= MAX_PAGE_LIMIT);
        let refused = Cache::builder()
            .max_capacity(REFUSALS)
            .time_to_live(config.refusal_ttl)
            .build();
        Self {
            source,
            config,
            cache: BoxCache::disabled(),
            loads: Cache::builder().build(),
            refused,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Seeds `cache` with the boxes of every load the engine takes: one it accepts, and one
    /// it ignores because the key is already tracked. The loader can't tell those two apart,
    /// and a duplicate's boxes are the key's boxes at the tip all the same. A rejected load
    /// seeds nothing: the tip moved, some of its boxes may be spent, and a retry follows.
    pub fn seeding(mut self, cache: BoxCache) -> Self {
        self.cache = cache;
        self
    }

    /// Loads `key` and sends it to the engine, retrying until an attempt is accepted or the
    /// next would start past `give_up_after`. An attempt that paged waits `retry`, one that
    /// stopped before the pages waits `poll`, and a `503` its `Retry-After`; other node errors
    /// end the load, as do the caps. No wait follows the last attempt: a caller giving up
    /// should not sit through it, so `GaveUp` carries it instead.
    ///
    /// A call while another load of `key` runs waits for that one and gets its outcome. A key
    /// refused for too many boxes in the last `refusal_ttl` is refused again with no node
    /// call.
    pub async fn load(&self, engine: &impl EngineHandle, key: &TrackingKey) -> Loaded {
        if let Some(max) = self.refused.get(key).await {
            return Err(Arc::new(LoadError::TooManyBoxes { max }));
        }
        let run = async {
            let outcome = self.load_alone(engine, key).await;
            if let Err(LoadError::TooManyBoxes { max }) = outcome {
                self.refused.insert(key.clone(), max).await;
            }
            Err::<(), Loaded>(outcome.map_err(Arc::new))
        };
        match self.loads.try_get_with(key.clone(), run).await {
            Err(outcome) => (*outcome).clone(),
            Ok(()) => unreachable!("a load's outcome is always the `Err` side"),
        }
    }

    /// [`Loader::load`], not coalesced.
    async fn load_alone(
        &self,
        engine: &impl EngineHandle,
        key: &TrackingKey,
    ) -> Result<(BlockRef, usize), LoadError> {
        let started = Instant::now();
        for attempt in 1.. {
            let (last, wait) = match self.try_load(engine, key).await {
                Ok(Attempt::Tracked { as_of, boxes }) => return Ok((as_of, boxes)),
                Ok(outcome) => {
                    let wait = match outcome {
                        Attempt::TipMoved | Attempt::Rejected => self.config.retry,
                        _ => self.config.poll,
                    };
                    if outcome == Attempt::Rejected {
                        info!(?key, attempt, "Load rejected by the engine, retrying");
                    } else {
                        debug!(?key, attempt, ?outcome, "Load not settled, retrying");
                    }
                    (Some(outcome), wait)
                }
                Err(LoadError::Node(error @ NodeError::Unavailable { .. })) => {
                    let wait = backoff(&error, self.config.retry);
                    info!(?key, ?wait, "Indexer unavailable (503) during a load, backing off");
                    (None, wait)
                }
                Err(error) => return Err(error),
            };
            if started.elapsed() + wait >= self.config.give_up_after {
                return Err(LoadError::GaveUp { attempts: attempt, last, wait });
            }
            sleep(wait).await;
        }
        unreachable!("the attempts run until one returns")
    }

    /// One attempt: read the tip, check the engine and the indexer are at it, read the room
    /// the engine has left, read every page oldest first (stopping past a cap), read the tip
    /// again, check every box against the key, and send `KeyLoaded` pinned to that tip.
    pub async fn try_load(
        &self,
        engine: &impl EngineHandle,
        key: &TrackingKey,
    ) -> Result<Attempt, LoadError> {
        let Some(tip) = best_full_block(&self.source.info().await?) else {
            return Ok(Attempt::NoTip);
        };
        let engine_at = engine_tip(engine).await?;
        if engine_at != Some(tip) {
            return Ok(Attempt::EngineBehind { engine: engine_at, node: tip });
        }
        let indexed = self.source.indexed_height().await?;
        if indexed.indexed_height != tip.height {
            return Ok(Attempt::IndexerBehind {
                indexed_height: indexed.indexed_height,
                tip_height: tip.height,
            });
        }

        // Read before paging, so a load that can't fit stops early. The engine's check on
        // `KeyLoaded` is still the one that counts: the room can change while the pages are
        // read. A stop on room that grows meanwhile only costs a retry; a key another loader
        // tracks meanwhile is refused too, which a shared loader never does.
        // A load past `max_boxes` is refused before the room is looked at, so the engine
        // counts idle keys' room only when a load that large could need it.
        let capacity = engine_capacity(engine, key, self.config.max_boxes).await?;
        let boxes = self.pages(key, capacity).await?;
        if best_full_block(&self.source.info().await?) != Some(tip) {
            debug!(?key, ?tip, "Load dropped: the tip moved during it");
            return Ok(Attempt::TipMoved);
        }
        if let Some(b) = boxes.iter().find(|b| !key.matches(&b.utxo)) {
            return Err(LoadError::Mismatch { box_id: b.utxo.id });
        }

        let count = boxes.len();
        let boxes: Vec<LoadedBox> = boxes.into_iter().map(LoadedBox::from).collect();
        let seeds: Vec<Arc<UTxO>> = boxes.iter().map(|b| b.utxo.clone()).collect();
        let event = Event::KeyLoaded { key: key.clone(), as_of: tip, boxes };
        // `KeyLoaded` is never an `EngineError`: an off-tip load answers `LoadRejected`.
        let outputs = engine.apply(event).await?.unwrap_or_default();
        for output in outputs.iter().filter(|o| o.key() == key) {
            match output {
                Output::LoadRejected { .. } => return Ok(Attempt::Rejected),
                &Output::CapacityExceeded { tracked, evictable, boxes, .. } => {
                    return Err(LoadError::CapacityExceeded { tracked, evictable, boxes });
                }
                _ => {}
            }
        }
        for b in seeds {
            self.cache.insert(b);
        }
        Ok(Attempt::Tracked { as_of: tip, boxes: count })
    }

    /// Every page of the key's unspent boxes, oldest first, until a short page, or until the
    /// first page that takes the key over the per-key cap or past `capacity`. A full engine
    /// still costs a page: an empty key fits.
    async fn pages(
        &self,
        key: &TrackingKey,
        capacity: Option<Capacity>,
    ) -> Result<Vec<IndexedBox>, LoadError> {
        let (limit, max) = (self.config.page_limit, self.config.max_boxes);
        let mut boxes = Vec::new();
        loop {
            let page = Page { offset: boxes.len() as u32, limit };
            let mut read = match key {
                TrackingKey::Tree(tree) => self.source.unspent_by_ergo_tree(tree, page).await?,
                TrackingKey::Token(id) => self.source.unspent_by_token(id, page).await?,
            };
            let full = read.len() as u32 >= limit;
            boxes.append(&mut read);
            if boxes.len() > max {
                return Err(LoadError::TooManyBoxes { max });
            }
            if let Some(capacity) = capacity
                && !capacity.fits(boxes.len())
            {
                let Capacity { tracked, evictable, .. } = capacity;
                let boxes = boxes.len();
                return Err(LoadError::CapacityExceeded { tracked, evictable, boxes });
            }
            if !full {
                return Ok(boxes);
            }
        }
    }
}

#[cfg(test)]
#[path = "loader_tests.rs"]
mod tests;
