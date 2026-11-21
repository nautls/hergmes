use std::sync::Arc;

use serde::Serialize;

use crate::{
    engine::{
        BlockRef, BoxKey, ChainLink, Spend, StoredBox, Totals, TrackingKey,
        overlay::{PendingDelta, TxDelta},
    },
    types::ergo::{
        BoxId, ErgoTree, HeaderId, IndexedBlock, IndexedBox, TxId, UTxO, UnconfirmedTransaction,
    },
};

/// An input to the engine, sent by the adapters.
#[derive(Debug, Clone)]
pub enum Event {
    /// A block extending the current tip, with full input boxes.
    BlockApplied(IndexedBlock),
    /// The current tip was orphaned. The engine undoes it from its ring buffer.
    BlockReverted(BlockRef),
    /// The whole mempool, and the tip read alongside it.
    MempoolSnapshot { txs: Arc<[UnconfirmedTransaction]>, observed_tip: BlockRef },
    /// A key's first load. Accepted only if `as_of` is the current tip; it then starts tracking.
    /// A load of a balance-only key makes it a key with boxes.
    KeyLoaded { key: TrackingKey, as_of: BlockRef, boxes: Vec<LoadedBox> },
    /// An address's confirmed balance at `as_of`, read from the indexer, which starts tracking
    /// it balance-only: its totals, moved by each block, and no boxes, so it costs no paging
    /// and counts only its charge against the cap on tracked units. Accepted only if `as_of`
    /// is the current tip; ignored for a key already tracked either way.
    BalanceLoaded { tree: ErgoTree, as_of: BlockRef, confirmed: Totals },
    /// A client subscribed to a key. A key that is not tracked answers `ResyncRequired`: the
    /// transport loads it first.
    Subscribed(TrackingKey),
    /// A client unsubscribed from a key or disconnected. A key with no subscribers is
    /// untracked once it has been idle for the grace period, on the engine's clock.
    Unsubscribed(TrackingKey),
}

/// A box of a first load. The box is shared, not owned: the engine keeps only its id and
/// height, and the loader keeps the same allocation for the box cache, with no copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedBox {
    pub utxo: Arc<UTxO>,
    pub inclusion_height: u32,
}

impl From<IndexedBox> for LoadedBox {
    fn from(b: IndexedBox) -> Self {
        Self { utxo: Arc::new(b.utxo), inclusion_height: b.inclusion_height }
    }
}

/// A read of engine state, answered in order with the events. Reads of a tracked key count
/// as activity for the grace period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// Up to `limit` box keys of `key` as of block `at` (the tip when `None`), after the
    /// cursor `after`. A first page is pinned to the tip in this one message, with no
    /// `Query::Tip` before it that a block could follow.
    Page {
        key: TrackingKey,
        at: Option<BlockRef>,
        after: Option<BoxKey>,
        limit: usize,
    },
    /// A tracked key's boxes at the tip with its overlay applied, in one message so both
    /// halves are at one tip. At most `limit` confirmed boxes, the ones a node call may have
    /// to fill in; every pending box, which the snapshot holds in full.
    WithPool {
        key: TrackingKey,
        limit: usize,
    },
    /// A key's overlay, tracked or not.
    Overlay(TrackingKey),
    /// A key's pending delta, tracked or not: the signed pending side of a balance, with the
    /// block it was computed at.
    Pending(TrackingKey),
    /// The pending txs that touch an address, tracked or not, newest first, each with its
    /// net change for it, and the block they were decided at: the top of its history.
    PendingTxs(ErgoTree),
    /// A tracked address's balance at the tip, as the stream sends it. A token key has none.
    Balance(TrackingKey),
    Tip,
    /// The room a load of the key, of at most `up_to` boxes, may take under the cap on
    /// tracked units, or `None` if the key is tracked: a duplicate load is never refused. A
    /// balance-only key's load of its boxes is checked against the cap when it arrives. Not
    /// activity for the key.
    Capacity {
        key: TrackingKey,
        up_to: usize,
    },
    /// The blocks the engine can revert, oldest first, ending at the tip. The block consumer
    /// finds the fork point in it.
    Chain,
    /// Full boxes the ring buffer holds, for the box fetcher. Ids it does not hold are left
    /// out.
    Boxes(Vec<BoxId>),
    /// What the snapshot in use says of one box, for a read of a box by id: `None` if no tx
    /// of it spends or creates the box, or there is none. Its txs are the ones the overlay
    /// uses, valid at the tip. Not activity for any key.
    PoolBox(BoxId),
    MempoolSnapshot,
    /// The messages that bring a client from `from` to the tip: replayed blocks or
    /// rollbacks, then the overlay and the balance, or a resync. Without `from` (a
    /// balance-only subscription), only the balance. A token key gets no balance. A
    /// balance-only key has no deltas to replay, so a catch-up with `from` resyncs.
    CatchUp {
        key: TrackingKey,
        from: Option<BlockRef>,
    },
    /// The sizes an operator watches: counts only, never a key. Computed when asked, so
    /// nothing is kept up to date for it. Not activity for any key.
    Stats,
}

/// The answer to a [`Query`], in the matching variant. Where a reply is an `Option`, `None`
/// means the key is untracked (or, for a balance, a token key) or there is no tip; overlays
/// and pending deltas are answered for any key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Page(Result<PageIds, PageError>),
    WithPool(Option<WithPoolIds>),
    Overlay(Vec<PendingTx>),
    Pending(Option<PendingAt>),
    PendingTxs(Option<PendingTxsAt>),
    Balance(Option<Balance>),
    Tip(Option<BlockRef>),
    Capacity(Option<Capacity>),
    Chain(Vec<ChainLink>),
    Boxes(Vec<Arc<UTxO>>),
    PoolBox(Option<PoolBox>),
    MempoolSnapshot(Option<(Arc<[UnconfirmedTransaction]>, BlockRef)>),
    CatchUp(Vec<Output>),
    Stats(Stats),
}

/// A box the mempool snapshot spends or creates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolBox {
    /// A pool tx spends it, confirmed or created by another pool tx: it can't be spent now.
    Spent,
    /// A pool tx creates it and none spends it: spendable now, but pending.
    Created(Arc<UTxO>),
}

/// The engine's sizes, for the metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub tip: Option<BlockRef>,
    /// Addresses tracked with their boxes.
    pub address_keys: usize,
    /// Addresses tracked balance-only.
    pub balance_keys: usize,
    pub token_keys: usize,
    /// One per box and key. The cap counts these and the keys' charges, in `tracked_units`.
    pub tracked_boxes: usize,
    /// What the cap on tracked units counts: `tracked_boxes`, and every key's charge.
    pub tracked_units: usize,
    /// Blocks in the ring buffer.
    pub ring_blocks: usize,
    /// Full boxes the ring buffer's blocks hold.
    pub ring_boxes: usize,
    /// Boxes held by the keys a load may evict, one per box and key.
    pub evictable_boxes: usize,
    /// Keys evicted to make room for a load, since the engine started.
    pub evicted_keys: u64,
}

/// The units tracked (one per box and key, and each key's charge, the boxes its own memory is
/// worth), the part of them held by keys a load may evict, the cap on them, and the charge of
/// the key asked about, counted as holding no token. Its load of `n` boxes adds at least
/// `n + charge` and fits while `tracked - evictable + n + charge <= max`: it takes the room it
/// needs from idle keys, or is refused and evicts none. Idle keys are counted only until the
/// load asked about would fit, so `evictable` is all of their units only when it does not;
/// otherwise it is at least what the load needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    pub tracked: usize,
    pub evictable: usize,
    pub max: usize,
    pub charge: usize,
}

impl Capacity {
    pub fn fits(&self, boxes: usize) -> bool {
        self.tracked
            .saturating_sub(self.evictable)
            .saturating_add(boxes)
            .saturating_add(self.charge)
            <= self.max
    }
}

/// A page of a key's box keys, and the block it is as of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageIds {
    pub as_of: BlockRef,
    pub boxes: Vec<BoxKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PageError {
    #[error("key is not tracked")]
    NotTracked,
    #[error("block is no longer on the main chain or outside the retention window")]
    CursorExpired,
}

/// A key's view with the pool: its boxes at `as_of` that the overlay leaves unspent, in keyset
/// order and cut to the limit, then every box the overlay creates and leaves unspent, in
/// overlay order. Confirmed boxes are ids, filled in by the box fetcher; pending ones are in
/// full, from the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithPoolIds {
    pub as_of: BlockRef,
    pub confirmed: Vec<BoxKey>,
    pub pending: Vec<Arc<UTxO>>,
    /// The whole view's size, confirmed boxes left out by the limit included.
    pub total: usize,
}

/// An event the engine cannot apply: the block consumer's view of the chain is not the
/// engine's. Nothing changed; the consumer resynchronizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    #[error("block {block:?} does not extend the tip {tip:?}")]
    NotChild { block: BlockRef, tip: Option<BlockRef> },
    #[error("block {block:?} is not the tip {tip:?}")]
    NotTip { block: BlockRef, tip: Option<BlockRef> },
}

/// A key's pending delta and the engine's tip it was computed at. An untracked address's
/// balance pairs it with a confirmed balance read from the indexer only at the same height:
/// a block applied between the two reads would leave a tx it mined in neither half.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAt {
    pub as_of: BlockRef,
    pub delta: PendingDelta,
}

/// An address's pending txs and the engine's tip they were decided at. A history's first
/// page lists them above the indexer's page, which is read after and so is at or above that
/// tip: a tx confirmed in between is found by the heights on the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTxsAt {
    pub as_of: BlockRef,
    pub txs: Vec<TxDelta>,
}

/// An address's balance at a tip: its running totals, and the signed pending delta of its
/// overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    pub as_of: BlockRef,
    pub confirmed: Totals,
    pub pending: PendingDelta,
}

/// A message for the subscribers of one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// A block applied on top of `parent_id`. Every key tracked with its boxes gets one per
    /// block, empty or not, so its subscribers can check the parent chain. A balance-only key
    /// gets none, nor a `Rollback`: it has only balance subscribers.
    Block {
        key: TrackingKey,
        block: BlockRef,
        parent_id: HeaderId,
        created: Vec<Arc<UTxO>>,
        spent: Vec<Spend>,
    },
    /// The inverse of an orphaned block's delta: `created` are the boxes it had spent, with
    /// the heights that included them, and `spent` the boxes it had created (with the tx that
    /// created them). Afterwards the subscriber is at `parent_id`.
    Rollback {
        key: TrackingKey,
        block: BlockRef,
        parent_id: HeaderId,
        created: Vec<StoredBox>,
        spent: Vec<Spend>,
    },
    /// The key's whole pending state, replacing the previous one.
    Mempool { key: TrackingKey, tip_height: u32, pending: Vec<PendingTx> },
    /// An address's whole balance, replacing the previous one. Sent when its value changes;
    /// never for a token key.
    Balance { key: TrackingKey, balance: Balance },
    /// The subscription ended: the client redoes its snapshot, or for a balance-only one,
    /// subscribes again.
    ResyncRequired { key: TrackingKey, reason: ResyncReason },
    /// A first load pinned to a block other than the tip; the loader retries.
    LoadRejected { key: TrackingKey, as_of: BlockRef },
    /// A load refused because its `boxes`, and a new key's charge, on top of the `tracked`
    /// units would pass the cap on tracked units, even with the `evictable` ones (held by idle
    /// keys, counted in `tracked`) gone. Units are boxes and keys' charges (see [`Capacity`]);
    /// a balance-only load has no boxes. Nothing changed, no key was evicted. It frees up as
    /// subscribers leave and their keys go idle.
    CapacityExceeded { key: TrackingKey, tracked: usize, evictable: usize, boxes: usize },
}

impl Output {
    /// The key the message is about.
    pub fn key(&self) -> &TrackingKey {
        match self {
            Output::Block { key, .. }
            | Output::Rollback { key, .. }
            | Output::Mempool { key, .. }
            | Output::Balance { key, .. }
            | Output::ResyncRequired { key, .. }
            | Output::LoadRejected { key, .. }
            | Output::CapacityExceeded { key, .. } => key,
        }
    }
}

/// A pending transaction as it affects one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTx {
    pub tx_id: TxId,
    pub created: Vec<Arc<UTxO>>,
    pub spent: Vec<BoxId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncReason {
    /// The client's block is below the retention window.
    OutsideWindow,
    /// The client's block is not on the main chain, and the engine cannot roll it back: the
    /// fork is deeper than the window, or the block was never seen. Also sent when a block the
    /// key was loaded at is orphaned.
    ForkBeyondWindow,
    /// The key is not tracked, or was not tracked over the client's blocks.
    NotTracked,
    SlowConsumer,
    /// A balance-only key's totals could not take a block's change: the confirmed balance it
    /// started from was not the one at its load block. Subscribing again reads it anew.
    BalanceMismatch,
}
