//! The engine: per-key unspent id sets, running totals for addresses, the block delta ring
//! buffer, the mempool overlays and balances, driven by events and queries one at a time.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use tracing::debug;

use crate::{
    engine::{
        Balance, BlockDelta, BlockRef, BoxKey, Capacity, ChainLink, EngineError, Event, KeyDelta,
        KeyId, LoadedBox, Mempool, Output, PageError, PageIds, PendingAt, PendingTxsAt, Query,
        Reply, ResyncReason, Spend, Stats, StoredBox, Totals, TrackingKey, WithPoolIds,
        overlay::{self, PendingDelta},
    },
    types::ergo::{BoxId, ErgoTree, HeaderId, IndexedBlock, UTxO},
};

/// How far back the engine can serve, how long it keeps idle keys, and how many boxes it
/// tracks.
///
/// The window is counted in blocks, since what it bounds (replays, rollbacks, pinned pages)
/// is counted in blocks. The idle times are counted on the engine's clock, since what they
/// protect is a client's gap between requests, and block intervals vary too much for that:
/// two blocks can come seconds apart, or ten minutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// How many blocks of deltas the ring buffer keeps. It bounds replay on reconnect,
    /// rollbacks, and pages pinned to an earlier block.
    pub window: u32,
    /// How long a key with no subscribers is kept after its last activity. Checked at each
    /// block, so a key can stay up to a block longer. At least `evict_after` in a running
    /// relay (the config file refuses less), or a key could go between two requests that
    /// belong together; tests set less to see the grace period alone.
    pub grace: Duration,
    /// How long a key with no subscribers must be idle before a load may evict it, so a
    /// client between two requests that belong together (the pages of one walk) keeps its
    /// key.
    pub evict_after: Duration,
    /// The most units tracked over all keys. A unit is about the live memory of one tracked
    /// box: a box counts one, once per key that holds it (each is an entry of the reverse
    /// index), and a key counts the boxes its own memory is worth (see [`key_charge`]), so a
    /// key holding none (a balance-only key, an empty address) still takes room, and their
    /// number is bounded too. Checked when a key is loaded, never afterwards: a tracked key
    /// grows from blocks, and dropping it would break its subscribers. A load
    /// past it first evicts idle keys (see `Engine::evictable`), which would leave after
    /// the grace period anyway, so idle keys never stand in the way of active ones.
    pub max_tracked_units: usize,
    /// The most units a block untracks for keys idle past the grace period, a key counting
    /// its boxes and its charge as the cap does: the first key always goes, a later one only
    /// if it fits, and the rest wait for the next block. An untrack costs about 0.16 µs a box,
    /// and about 1.5 µs a key holding none, such as a balance-only key
    /// (`examples/untrackbench.rs`), so a mass disconnect would otherwise hold one block, and
    /// every request behind it, for seconds. Counted in units, a key with no box is charged
    /// less than its time (about 5 units for about 10 boxes' worth): a full budget of them
    /// still takes only about 15 ms. Keys left waiting stay
    /// idle, so a load that needs their room evicts them.
    pub untrack_units_per_block: usize,
}

impl Config {
    /// About a day of blocks, an hour of grace, a minute before eviction, about 290 MB
    /// of ids, and an untrack of at most about 15 ms a block: three keys at the loader's
    /// default per-key cap (about 8 ms), or about 10k balance-only keys.
    pub const DEFAULT: Self = Self {
        window: 720,
        grace: Duration::from_secs(60 * 60),
        evict_after: Duration::from_secs(60),
        max_tracked_units: 2_000_000,
        untrack_units_per_block: 50_000,
    };
}

impl Default for Config {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What a tracked box takes, in live bytes: its id in the key's set and in the reverse
/// index, with its share of the key (`examples/membench.rs`, `engine`, 50 boxes a key).
const BOX_BYTES: usize = 156;
/// What a key takes besides its boxes and its tree or token id: its entries in the engine's
/// and the mempool's maps, its state and its running totals with no token in them
/// (`examples/membench.rs`, `balance`, less the tree it measures).
const KEY_BYTES: usize = 680;
/// What a token in a key's running totals takes, and the least the first one takes: the
/// totals' map allocates a node holding up to eleven entries for it.
const TOKEN_BYTES: usize = 64;
const FIRST_TOKEN_BYTES: usize = 460;

/// What a key counts against the cap on tracked units besides its boxes: the boxes its own
/// memory is worth, holding `tokens` distinct tokens. Fixed when the key is loaded: tokens it
/// is paid later are not counted until it is loaded again. Its tree (a contract's can be
/// kilobytes) is stored once and counted once.
pub fn key_charge(key: &TrackingKey, tokens: usize) -> usize {
    let id = match key {
        TrackingKey::Tree(tree) => tree.0.len(),
        TrackingKey::Token(id) => id.0.len(),
    };
    let tokens = match tokens {
        0 => 0,
        n => FIRST_TOKEN_BYTES.max(TOKEN_BYTES * n),
    };
    (KEY_BYTES + id + tokens).div_ceil(BOX_BYTES)
}

/// A tracked key's state.
#[derive(Debug, Clone)]
struct KeyState {
    key: TrackingKey,
    /// The key's unspent boxes at the tip. Always empty for a balance-only key.
    boxes: BTreeSet<BoxKey>,
    /// An address's running totals. A token key has none: summed over its holders' boxes, a
    /// balance means nothing, so no block pays for keeping it.
    totals: Option<Totals>,
    /// An address tracked for its balance alone: its totals start from the indexer's balance
    /// and move with each block's change, with no boxes. Its subscribers are all
    /// balance-only, so it gets no `Block` or `Rollback`; a load of its boxes makes it a key
    /// with boxes.
    balance_only: bool,
    /// The block the key was loaded at. Ring entries up to it have no share for the key.
    since: BlockRef,
    /// The engine's block count when the key was loaded (see [`BlockDelta::seq`]).
    loaded_seq: u64,
    subscribers: u32,
    /// The engine's clock at the key's last activity, in ms: its load, a subscribe or
    /// unsubscribe, or a read.
    last_seen_ms: u64,
    /// The pending delta of the last `balance` sent. The confirmed half is `totals`. Always
    /// empty for a token key.
    pending_sent: PendingDelta,
    /// What the key counts against the cap besides its boxes, fixed at its load (see
    /// [`key_charge`]).
    charge: usize,
}

#[derive(Debug, Clone)]
pub struct Engine {
    config: Config,
    tip: Option<BlockRef>,
    /// Blocks applied so far.
    seq: u64,
    next_id: u32,
    /// Tracked keys, in the order messages go out in.
    ids: BTreeMap<TrackingKey, KeyId>,
    /// The balance-only keys, by tree: how a block's inputs find them, since they hold no
    /// boxes in the reverse index.
    balance_trees: HashMap<ErgoTree, KeyId>,
    /// The keys' charges, summed: with the reverse index's length, what the cap counts.
    charges: usize,
    keys: HashMap<KeyId, KeyState>,
    /// For each key tracked with its boxes, the block of its last change, or its load block
    /// before the first: what its next `Block` chains to, and a resume from it has nothing to
    /// replay however old it is. Kept apart from [`KeyState`] so a balance-only key, which
    /// has no boxes to chain, does not pay for it.
    last_changes: HashMap<KeyId, BlockRef>,
    /// The keys with no subscribers, least recently active first, with their box counts:
    /// the ones the idle sweep and eviction look at, in the order they go. Kept apart, counts
    /// included, so each walks only the idle keys it takes, not every key.
    idle: BTreeMap<(u64, KeyId), usize>,
    /// Every box a tracked key holds, once per key, with its inclusion height.
    reverse: BTreeMap<(BoxId, KeyId), u32>,
    /// The last blocks of the main chain, oldest first, ending at the tip.
    ring: VecDeque<BlockDelta>,
    /// Reverted blocks, kept to roll a client on their branch back to the fork point.
    orphans: HashMap<HeaderId, BlockDelta>,
    mempool: Mempool,
    /// Keys evicted to make room for a load, for the metrics.
    evicted_keys: u64,
    /// The engine's clock, in ms from an origin only the caller knows. The engine never reads
    /// a clock: the engine task sets it before each message (see [`Engine::set_now`]), so a
    /// run fed the same messages at the same times is replayed exactly.
    now_ms: u64,
}

impl Engine {
    pub fn new(config: Config) -> Self {
        assert!(config.window >= 1, "the ring buffer keeps at least the tip");
        Self {
            config,
            tip: None,
            seq: 0,
            next_id: 0,
            ids: BTreeMap::new(),
            balance_trees: HashMap::new(),
            charges: 0,
            keys: HashMap::new(),
            last_changes: HashMap::new(),
            idle: BTreeMap::new(),
            reverse: BTreeMap::new(),
            ring: VecDeque::new(),
            orphans: HashMap::new(),
            mempool: Mempool::default(),
            evicted_keys: 0,
            now_ms: 0,
        }
    }

    /// Sets the engine's clock, as time since an origin the caller keeps fixed. It never goes
    /// back: an earlier time is ignored, so a key is never seen as more recent than it was.
    pub fn set_now(&mut self, now: Duration) {
        self.now_ms = self.now_ms.max(millis(now));
    }

    pub fn tip(&self) -> Option<BlockRef> {
        self.tip
    }

    pub fn is_tracked(&self, key: &TrackingKey) -> bool {
        self.ids.contains_key(key)
    }

    /// Whether `key` is tracked balance-only: a key whose boxes a page or a load sees as not
    /// tracked.
    fn is_balance_only(&self, key: &TrackingKey) -> bool {
        matches!(key, TrackingKey::Tree(tree) if self.balance_trees.contains_key(tree))
    }

    /// The block a tracked key was loaded at: pages pinned below it expire.
    #[cfg(test)]
    pub fn tracked_since(&self, key: &TrackingKey) -> Option<BlockRef> {
        Some(self.keys[self.ids.get(key)?].since)
    }

    /// Applies one event. Within it, messages go out as `Block` / `Rollback` /
    /// `ResyncRequired` / `LoadRejected` / `CapacityExceeded`, then `Mempool`, then
    /// `Balance`, each in key order.
    pub fn apply(&mut self, event: Event) -> Result<Vec<Output>, EngineError> {
        let mut outputs = self.apply_event(event)?;
        self.stamp_last_changes(&mut outputs);
        Ok(outputs)
    }

    /// Sets `last_change` on each `Mempool` and `Balance` for a key tracked with its boxes.
    fn stamp_last_changes(&self, outputs: &mut [Output]) {
        for output in outputs {
            if let Output::Mempool { key, last_change, .. }
            | Output::Balance { key, last_change, .. } = output
            {
                *last_change = self.last_change(key);
            }
        }
    }

    /// Records a key loaded at the tip on the tip's ring entry, for the tip's revert to drop.
    /// A load is only ever at the tip, and the tip has an entry: with the ring empty, a revert
    /// drops every key anyway.
    fn loaded_at_tip(&mut self, key_id: KeyId) {
        debug_assert!(
            self.ring.back().is_none_or(|d| Some(d.block) == self.tip),
            "the ring ends at the tip"
        );
        if let Some(delta) = self.ring.back_mut() {
            delta.loaded.push(key_id);
        }
    }

    /// The block of a key's last change, if it is tracked with its boxes.
    fn last_change(&self, key: &TrackingKey) -> Option<BlockRef> {
        self.last_changes.get(self.ids.get(key)?).copied()
    }

    fn apply_event(&mut self, event: Event) -> Result<Vec<Output>, EngineError> {
        Ok(match event {
            Event::BlockApplied(block) => self.block_applied(&block)?,
            Event::BlockReverted(block) => self.block_reverted(block)?,
            Event::MempoolSnapshot { txs, observed_tip } => {
                let confirmed = Confirmed::new(&self.ids, &self.balance_trees, &self.reverse);
                let mut outputs = self
                    .mempool
                    .apply_snapshot(txs, observed_tip, self.tip, |b| confirmed.check(b));
                self.push_balances(&BTreeMap::new(), &mut outputs);
                outputs
            }
            Event::KeyLoaded { key, as_of, boxes } => self.key_loaded(key, as_of, boxes),
            Event::BalanceLoaded { tree, as_of, confirmed } => {
                self.balance_loaded(tree, as_of, confirmed)
            }
            Event::Subscribed(key) => match self.seen(&key, |n| n + 1) {
                Some(_) => Vec::new(),
                None => vec![Output::ResyncRequired { key, reason: ResyncReason::NotTracked }],
            },
            Event::Unsubscribed(key) => {
                if self.seen(&key, |n| n.saturating_sub(1)).is_none() {
                    debug!(?key, "Unsubscribe from an untracked key ignored");
                }
                Vec::new()
            }
        })
    }

    /// Answers one query. Reads of a tracked key count as its activity.
    pub fn query(&mut self, query: Query) -> Reply {
        match query {
            Query::Page { key, at, after, limit } => Reply::Page(self.page(&key, at, after, limit)),
            Query::WithPool { key, limit } => {
                if !self.is_balance_only(&key) {
                    self.touch(&key);
                }
                Reply::WithPool(self.with_pool(&key, limit))
            }
            Query::Overlay(key) => Reply::Overlay(self.mempool.overlay(&key)),
            Query::Pending(key) => Reply::Pending(
                self.tip
                    .map(|as_of| PendingAt { as_of, delta: self.mempool.pending_delta(&key) }),
            ),
            Query::PendingTxs(tree) => Reply::PendingTxs(
                self.tip
                    .map(|as_of| PendingTxsAt { as_of, txs: self.mempool.pending_txs(&tree) }),
            ),
            Query::Balance(key) => {
                self.touch(&key);
                Reply::Balance(self.balance(&key))
            }
            Query::Tip => Reply::Tip(self.tip),
            Query::Capacity { key, up_to } => {
                Reply::Capacity((!self.is_tracked(&key)).then(|| self.capacity(&key, up_to)))
            }
            Query::Chain => Reply::Chain(
                self.ring
                    .iter()
                    .map(|d| ChainLink { block: d.block, parent_id: d.parent_id })
                    .collect(),
            ),
            Query::Boxes(ids) => Reply::Boxes(self.ring_boxes(&ids)),
            Query::PoolBox(id) => Reply::PoolBox(self.mempool.pool_box(&id)),
            Query::MempoolSnapshot => Reply::MempoolSnapshot(self.mempool.latest()),
            Query::CatchUp { key, from } => {
                let mut outputs = self.catch_up(key, from);
                self.stamp_last_changes(&mut outputs);
                Reply::CatchUp(outputs)
            }
            Query::Stats => Reply::Stats(self.stats()),
        }
    }

    fn stats(&self) -> Stats {
        let addresses = self
            .ids
            .keys()
            .filter(|key| matches!(key, TrackingKey::Tree(_)))
            .count();
        let balance_keys = self.balance_trees.len();
        Stats {
            tip: self.tip,
            address_keys: addresses - balance_keys,
            balance_keys,
            token_keys: self.ids.len() - addresses,
            tracked_boxes: self.reverse.len(),
            tracked_units: self.units(),
            ring_blocks: self.ring.len(),
            ring_boxes: self.ring.iter().map(|d| d.boxes.len()).sum(),
            evictable_boxes: self.evictable_boxes().map(|(_, boxes)| boxes).sum(),
            evicted_keys: self.evicted_keys,
        }
    }

    fn block_applied(&mut self, block: &IndexedBlock) -> Result<Vec<Output>, EngineError> {
        let block_ref = BlockRef::from(&block.header);
        if let Some(tip) = self.tip
            && (block.header.parent_id != tip.header_id || block_ref.height != tip.height + 1)
        {
            return Err(EngineError::NotChild { block: block_ref, tip: self.tip });
        }
        self.seq += 1;
        let mut delta = BlockDelta {
            block: block_ref,
            parent_id: block.header.parent_id,
            seq: self.seq,
            boxes: HashMap::new(),
            keys: HashMap::new(),
            balances: HashMap::new(),
            loaded: Vec::new(),
        };
        let mut before: BTreeMap<KeyId, Totals> = BTreeMap::new();
        let mut mined = HashSet::new();
        let mut spent = HashSet::new();

        // Txs in order, so a box created and spent within the block nets out of each delta.
        for tx in &block.transactions {
            mined.insert(tx.id);
            for input in &tx.inputs {
                let id = input.utxo.id;
                spent.insert(id);
                for (key_id, height) in self.take_holders(&id) {
                    let state = self.keys.get_mut(&key_id).expect("a holder is tracked");
                    state
                        .boxes
                        .remove(&BoxKey { inclusion_height: height, box_id: id });
                    if let Some(totals) = &mut state.totals {
                        before.entry(key_id).or_insert_with(|| totals.clone());
                        totals.sub(&input.utxo);
                    }
                    let prev = self.last_changes[&key_id];
                    let key_delta = delta
                        .keys
                        .entry(key_id)
                        .or_insert_with(|| KeyDelta::new(prev));
                    match key_delta.created.iter().position(|c| *c == id) {
                        Some(i) => {
                            key_delta.created.remove(i);
                        }
                        None => {
                            key_delta.spent.push(Spend { box_id: id, tx_id: tx.id });
                            delta.boxes.entry(id).or_insert_with(|| StoredBox {
                                inclusion_height: height,
                                utxo: Arc::new(input.utxo.clone()),
                            });
                        }
                    }
                }
                // A balance-only key holds no boxes, so its spends are found by the input's
                // tree: the indexer sends a block's inputs in full, each looked up by its
                // number (node 6.0.8, `IndexedErgoTransaction.retrieveBody`). That lookup
                // drops an input it can't find rather than fail, which would leave the totals
                // too high, and nothing here notices: a mismatch is seen only when a change
                // would take them below zero. A load of the key's boxes replaces them.
                if let Some(&key_id) = self.balance_trees.get(&input.utxo.ergo_tree) {
                    delta
                        .balances
                        .entry(key_id)
                        .or_default()
                        .add(&input.utxo, -1);
                }
            }
            for output in &tx.outputs {
                let holders: Vec<KeyId> = overlay::keys_of(&output.utxo)
                    .filter_map(|k| self.ids.get(&k).copied())
                    .collect();
                let id = output.utxo.id;
                let mut held = false;
                for key_id in holders {
                    let state = self.keys.get_mut(&key_id).expect("an id is tracked");
                    if state.balance_only {
                        delta
                            .balances
                            .entry(key_id)
                            .or_default()
                            .add(&output.utxo, 1);
                        continue;
                    }
                    held = true;
                    state
                        .boxes
                        .insert(BoxKey { inclusion_height: block_ref.height, box_id: id });
                    if let Some(totals) = &mut state.totals {
                        before.entry(key_id).or_insert_with(|| totals.clone());
                        totals.add(&output.utxo);
                    }
                    self.reverse.insert((id, key_id), block_ref.height);
                    let prev = self.last_changes[&key_id];
                    let key_delta = delta
                        .keys
                        .entry(key_id)
                        .or_insert_with(|| KeyDelta::new(prev));
                    key_delta.created.push(id);
                }
                if held {
                    let utxo = Arc::new(output.utxo.clone());
                    delta
                        .boxes
                        .insert(id, StoredBox { inclusion_height: block_ref.height, utxo });
                }
            }
        }
        // Boxes created and spent within the block are in no delta.
        delta.keys.retain(|_, d| !d.is_empty());
        delta.balances.retain(|_, c| !c.is_empty());
        // A change a balance-only key's totals can't take: their start was wrong.
        let mut mismatched = BTreeSet::new();
        for (&key_id, change) in &delta.balances {
            let state = self.keys.get_mut(&key_id).expect("an id is tracked");
            let totals = state.totals.as_mut().expect("an address has totals");
            match totals.apply(change, 1) {
                Some(moved) => {
                    before.insert(key_id, std::mem::replace(totals, moved));
                }
                None => {
                    mismatched.insert(key_id);
                }
            }
        }
        delta.balances.retain(|id, _| !mismatched.contains(id));
        for &key_id in delta.keys.keys() {
            self.last_changes.insert(key_id, block_ref);
            self.recount(key_id);
        }
        let referenced: HashSet<BoxId> = delta
            .keys
            .values()
            .flat_map(|d| {
                d.created
                    .iter()
                    .copied()
                    .chain(d.spent.iter().map(|s| s.box_id))
            })
            .collect();
        delta.boxes.retain(|id, _| referenced.contains(id));

        let mut outputs = Vec::new();
        // A `Block` for every key whose boxes the block changed, and a resync for every
        // balance-only key whose totals the block could not take, merged in key order. A key
        // the block left alone gets nothing: its next change names its last one as `prev`.
        let dropped: Vec<TrackingKey> = mismatched
            .iter()
            .map(|id| self.keys[id].key.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let changed: BTreeMap<&TrackingKey, &KeyDelta> = delta
            .keys
            .iter()
            .map(|(id, d)| (&self.keys[id].key, d))
            .collect();
        let mut resyncs = dropped.iter().peekable();
        for (key, key_delta) in changed {
            while let Some(resync) = resyncs.next_if(|k| *k < key) {
                let reason = ResyncReason::BalanceMismatch;
                outputs.push(Output::ResyncRequired { key: resync.clone(), reason });
            }
            outputs.push(Output::Block {
                key: key.clone(),
                block: block_ref,
                prev: key_delta.prev,
                created: created_boxes(&delta, key_delta),
                spent: key_delta.spent.clone(),
            });
        }
        for resync in resyncs {
            let reason = ResyncReason::BalanceMismatch;
            outputs.push(Output::ResyncRequired { key: resync.clone(), reason });
        }

        self.orphans.remove(&block_ref.header_id);
        self.ring.push_back(delta);
        while self.ring.len() > self.config.window as usize {
            self.ring.pop_front();
        }
        self.tip = Some(block_ref);
        let floor = self.floor();
        self.orphans.retain(|_, o| o.block.height > floor);
        self.untrack(&dropped);

        // Untrack idle keys before deciding the overlays: their boxes are no longer checked.
        // Least recently active first, within the block's budget.
        let grace = millis(self.config.grace);
        let budget = self.config.untrack_units_per_block;
        let (mut keys, mut untracked) = (0, 0);
        let idle: Vec<TrackingKey> = self
            .idle
            .iter()
            .take_while(|&(&(last_seen_ms, _), _)| self.now_ms.saturating_sub(last_seen_ms) > grace)
            .take_while(|&((_, id), &boxes)| {
                keys += 1;
                untracked += boxes + self.keys[id].charge;
                keys == 1 || untracked <= budget
            })
            .map(|((_, id), _)| self.keys[id].key.clone())
            .collect();
        for key in &idle {
            let idle_ms = self.now_ms - self.keys[&self.ids[key]].last_seen_ms;
            debug!(?key, idle_ms, height = block_ref.height, "Untracking idle key");
        }
        // The sweep looked at one key past those it took: the budget stopped it there.
        if keys > idle.len() {
            debug!(
                height = block_ref.height,
                untracked_keys = idle.len(),
                budget,
                "Idle keys past the grace period held for the next block: the budget is spent"
            );
        }
        self.untrack(&idle);
        before.retain(|id, _| self.keys.contains_key(id));

        let confirmed = Confirmed::new(&self.ids, &self.balance_trees, &self.reverse);
        outputs.extend(
            self.mempool
                .block_applied(block_ref, &mined, &spent, |b| confirmed.check(b)),
        );
        self.push_balances(&before, &mut outputs);
        Ok(outputs)
    }

    fn block_reverted(&mut self, block: BlockRef) -> Result<Vec<Output>, EngineError> {
        if self.tip != Some(block) {
            return Err(EngineError::NotTip { block, tip: self.tip });
        }
        let Some(delta) = self.ring.pop_back() else {
            // The ring is exhausted: the fork is deeper than the window. Every key's state is
            // on the old chain and cannot be undone.
            debug!(?block, keys = self.ids.len(), "Revert beyond the ring, untracking every key");
            let mut outputs = Vec::new();
            for key in self.ids.keys() {
                let reason = ResyncReason::ForkBeyondWindow;
                outputs.push(Output::ResyncRequired { key: key.clone(), reason });
            }
            let keys: Vec<TrackingKey> = self.ids.keys().cloned().collect();
            self.untrack(&keys);
            self.tip = None;
            self.mempool.reset();
            self.orphans.clear();
            return Ok(outputs);
        };
        debug_assert_eq!(delta.block, block);

        let mut outputs = Vec::new();
        let (mut dropped, mut changed) = (Vec::new(), Vec::new());
        let mut before: BTreeMap<KeyId, Totals> = BTreeMap::new();
        // Only the keys the block concerns, in key order: those loaded while it was the tip,
        // and those it changed, so a revert costs the same however many keys are tracked. A
        // key loaded at it and upgraded at a later block has another `since`, and stays.
        let concerned: BTreeMap<TrackingKey, KeyId> = (delta.loaded.iter())
            .chain(delta.balances.keys())
            .chain(delta.keys.keys())
            .map(|&id| (self.keys[&id].key.clone(), id))
            .collect();
        for (key, &key_id) in &concerned {
            let state = self.keys.get_mut(&key_id).expect("an id is tracked");
            if state.since == block {
                // Loaded at this block: its share of the block was never recorded.
                debug!(?key, ?block, "Revert of the key's load block, untracking it");
                let reason = ResyncReason::ForkBeyondWindow;
                outputs.push(Output::ResyncRequired { key: key.clone(), reason });
                dropped.push(key.clone());
                continue;
            }
            if state.balance_only {
                let Some(change) = delta.balances.get(&key_id) else {
                    continue;
                };
                let totals = state.totals.as_mut().expect("an address has totals");
                let moved = totals
                    .apply(change, -1)
                    .expect("undoing a change gives back the totals it was applied to");
                before.insert(key_id, std::mem::replace(totals, moved));
                continue;
            }
            if let Some(d) = delta.keys.get(&key_id) {
                self.last_changes.insert(key_id, d.prev);
                changed.push(key_id);
                if let Some(totals) = &state.totals {
                    before.insert(key_id, totals.clone());
                }
                for id in &d.created {
                    let stored = &delta.boxes[id];
                    state
                        .boxes
                        .remove(&BoxKey { inclusion_height: block.height, box_id: *id });
                    if let Some(totals) = &mut state.totals {
                        totals.sub(&stored.utxo);
                    }
                    self.reverse.remove(&(*id, key_id));
                }
                for spend in &d.spent {
                    let stored = &delta.boxes[&spend.box_id];
                    let box_key =
                        BoxKey { inclusion_height: stored.inclusion_height, box_id: spend.box_id };
                    state.boxes.insert(box_key);
                    if let Some(totals) = &mut state.totals {
                        totals.add(&stored.utxo);
                    }
                    self.reverse
                        .insert((spend.box_id, key_id), stored.inclusion_height);
                }
                outputs.push(rollback(key, &delta, d));
            }
        }
        for key_id in changed {
            self.recount(key_id);
        }

        let parent = delta.parent();
        self.tip = Some(parent);
        self.orphans.insert(block.header_id, delta);
        // After the block is an orphan, so its share for a key upgraded at it goes too.
        self.untrack(&dropped);
        outputs.extend(self.mempool.block_reverted(parent));
        self.push_balances(&before, &mut outputs);
        Ok(outputs)
    }

    /// The boxes of `ids` the ring buffer holds, newest entry first, each once.
    fn ring_boxes(&self, ids: &[BoxId]) -> Vec<Arc<UTxO>> {
        let mut seen = HashSet::new();
        ids.iter()
            .filter(|id| seen.insert(**id))
            .filter_map(|id| {
                self.ring
                    .iter()
                    .rev()
                    .find_map(|d| d.boxes.get(id))
                    .map(|s| s.utxo.clone())
            })
            .collect()
    }

    fn key_loaded(
        &mut self,
        key: TrackingKey,
        as_of: BlockRef,
        boxes: Vec<LoadedBox>,
    ) -> Vec<Output> {
        if self.tip != Some(as_of) {
            return vec![Output::LoadRejected { key, as_of }];
        }
        // A balance-only key takes its boxes and keeps its subscribers.
        let upgraded = self.ids.get(&key).copied();
        if upgraded.is_some() && !self.is_balance_only(&key) {
            // Another load of a tracked key, from a racing loader: the engine's state is newer.
            self.touch(&key);
            debug!(?key, ?as_of, "Load of an already tracked key ignored");
            return Vec::new();
        }
        // The loader reads every page at one tip, so a box should come once; one that came
        // twice would be counted twice here, at most a refusal or an eviction too many. An
        // upgraded key already counts its charge from its balance, which its boxes replace.
        let tokens = match key {
            TrackingKey::Tree(_) => (boxes.iter())
                .flat_map(|b| b.utxo.tokens.iter().filter(|t| t.amount > 0).map(|t| t.id))
                .collect::<HashSet<_>>()
                .len(),
            TrackingKey::Token(_) => 0,
        };
        let charge = key_charge(&key, tokens);
        let was_charged = upgraded.map_or(0, |id| self.keys[&id].charge);
        let load = boxes.len() + charge.saturating_sub(was_charged);
        if let Some(refused) = self.make_room(&key, load, boxes.len(), upgraded) {
            return vec![refused];
        }
        debug!(?key, height = as_of.height, boxes = boxes.len(), "Tracking key");
        let (key_id, mut state, was) = match upgraded {
            Some(key_id) => {
                let mut state = self.keys.remove(&key_id).expect("an id is tracked");
                if state.subscribers == 0 {
                    self.idle.remove(&(state.last_seen_ms, key_id));
                }
                if let TrackingKey::Tree(tree) = &key {
                    self.balance_trees.remove(tree);
                }
                let was = state.totals.replace(Totals::default());
                self.charges -= state.charge;
                state.charge = charge;
                state.balance_only = false;
                state.since = as_of;
                state.loaded_seq = self.seq;
                state.last_seen_ms = self.now_ms;
                (key_id, state, was)
            }
            None => {
                let key_id = KeyId(self.next_id);
                self.next_id += 1;
                let state = KeyState {
                    key: key.clone(),
                    boxes: BTreeSet::new(),
                    totals: matches!(key, TrackingKey::Tree(_)).then(Totals::default),
                    balance_only: false,
                    since: as_of,
                    loaded_seq: self.seq,
                    subscribers: 0,
                    last_seen_ms: self.now_ms,
                    pending_sent: PendingDelta::default(),
                    charge,
                };
                (key_id, state, None)
            }
        };
        for b in &boxes {
            debug_assert!(key.matches(&b.utxo), "the loader checks every box");
            let box_key = BoxKey { inclusion_height: b.inclusion_height, box_id: b.utxo.id };
            if state.boxes.insert(box_key) {
                if let Some(totals) = &mut state.totals {
                    totals.add(&b.utxo);
                }
                self.reverse.insert((b.utxo.id, key_id), b.inclusion_height);
            }
        }
        if state.subscribers == 0 {
            self.idle.insert((self.now_ms, key_id), state.boxes.len());
        }
        self.charges += charge;
        self.ids.insert(key.clone(), key_id);
        self.keys.insert(key_id, state);
        self.last_changes.insert(key_id, as_of);
        self.loaded_at_tip(key_id);

        // The key's boxes are now checked against its set, which can leave txs out.
        let confirmed = Confirmed::new(&self.ids, &self.balance_trees, &self.reverse);
        let mut outputs = self.mempool.redecide(|b| confirmed.check(b));
        if upgraded.is_none() {
            self.mempool.track(key.clone());
            let delta = self
                .mempool
                .tracked_delta(&key)
                .cloned()
                .unwrap_or_default();
            self.keys
                .get_mut(&key_id)
                .expect("just inserted")
                .pending_sent = delta;
        }
        // Only the keys whose overlay the re-decision changed can have a new balance: a
        // pending delta moves only with its overlay, and the loaded key's was just set. A
        // walk of every key here made each load cost as much as there are keys, and a
        // restart's reload of all of them quadratic. An upgraded key's subscribers have its
        // balance from its totals, which its boxes replaced.
        let mut changed: BTreeSet<TrackingKey> = outputs.iter().map(|o| o.key().clone()).collect();
        if was.is_some() {
            changed.insert(key.clone());
        }
        for k in &changed {
            let before = if *k == key { was.as_ref() } else { None };
            self.push_balance(k, before, &mut outputs);
        }
        outputs
    }

    fn balance_loaded(
        &mut self,
        tree: ErgoTree,
        as_of: BlockRef,
        confirmed: Totals,
    ) -> Vec<Output> {
        let key = TrackingKey::Tree(tree.clone());
        if self.tip != Some(as_of) {
            return vec![Output::LoadRejected { key, as_of }];
        }
        if self.touch(&key).is_some() {
            debug!(?key, ?as_of, "Balance load of an already tracked key ignored");
            return Vec::new();
        }
        let charge = key_charge(&key, confirmed.tokens.len());
        if let Some(refused) = self.make_room(&key, charge, 0, None) {
            return vec![refused];
        }
        debug!(?key, height = as_of.height, "Tracking key balance-only");
        let key_id = KeyId(self.next_id);
        self.next_id += 1;
        let state = KeyState {
            key: key.clone(),
            boxes: BTreeSet::new(),
            totals: Some(confirmed),
            balance_only: true,
            since: as_of,
            loaded_seq: self.seq,
            subscribers: 0,
            last_seen_ms: self.now_ms,
            pending_sent: PendingDelta::default(),
            charge,
        };
        self.charges += charge;
        self.idle.insert((self.now_ms, key_id), 0);
        self.ids.insert(key.clone(), key_id);
        self.balance_trees.insert(tree, key_id);
        self.keys.insert(key_id, state);
        self.loaded_at_tip(key_id);
        // No overlay is decided again: a balance-only key's boxes are not checked against a
        // set, as for an untracked key.
        self.mempool.track(key.clone());
        let delta = self
            .mempool
            .tracked_delta(&key)
            .cloned()
            .unwrap_or_default();
        self.keys
            .get_mut(&key_id)
            .expect("just inserted")
            .pending_sent = delta;
        Vec::new()
    }

    /// What the cap on tracked units counts: every tracked box, once per key that holds it,
    /// and every key's charge.
    fn units(&self) -> usize {
        self.reverse.len() + self.charges
    }

    /// Makes room for a load that adds `load` units (its `boxes`, and a new key's charge) by
    /// evicting idle keys, least recently active first, never `keeping`. Idle keys are looked
    /// for only past the cap, and only until there is room. A refusal if even all of them
    /// would not make room, and then none is evicted.
    fn make_room(
        &mut self,
        key: &TrackingKey,
        load: usize,
        boxes: usize,
        keeping: Option<KeyId>,
    ) -> Option<Output> {
        let (tracked, max) = (self.units(), self.config.max_tracked_units);
        let needed = tracked.saturating_add(load).saturating_sub(max);
        if needed == 0 {
            return None;
        }
        let (mut evictable, mut chosen) = (0, Vec::new());
        for (last_seen_ms, key_id, units) in self.evictable() {
            if evictable >= needed {
                break;
            }
            if Some(key_id) == keeping {
                continue;
            }
            evictable += units;
            chosen.push((last_seen_ms, key_id));
        }
        if evictable < needed {
            let key = key.clone();
            return Some(Output::CapacityExceeded { key, tracked, evictable, boxes });
        }
        let evicted: Vec<TrackingKey> = chosen
            .into_iter()
            .map(|(last_seen_ms, key_id)| {
                let evicted = self.keys[&key_id].key.clone();
                let idle_ms = self.now_ms - last_seen_ms;
                debug!(?evicted, idle_ms, for_key = ?key, "Evicting idle key for a load");
                evicted
            })
            .collect();
        self.untrack(&evicted);
        self.evicted_keys += evicted.len() as u64;
        None
    }

    /// The room for a load of `key`, new, of at most `up_to` boxes, charged as holding no
    /// token: the loader can't know its tokens before its pages, and the check on its load
    /// counts them. Idle keys' room is counted only if such a load could pass the cap, and
    /// only until it would fit: past that it changes no answer. So `evictable` is exact only
    /// when the load does not fit; otherwise it is at least the room the load needs. With the
    /// defaults, idle keys are counted only past 1.95M tracked units.
    fn capacity(&self, key: &TrackingKey, up_to: usize) -> Capacity {
        let (tracked, max) = (self.units(), self.config.max_tracked_units);
        let charge = key_charge(key, 0);
        let needed = tracked
            .saturating_add(up_to.saturating_add(charge))
            .saturating_sub(max);
        let mut evictable = 0;
        if needed > 0 {
            for (_, _, units) in self.evictable() {
                if evictable >= needed {
                    break;
                }
                evictable += units;
            }
        }
        Capacity { tracked, evictable, max, charge }
    }

    /// The keys a load may evict, in the order it evicts them, with their last activity and
    /// the units they hold (their boxes and their charge): no subscribers, so no stream breaks, and
    /// idle for `evict_after`, so a client between two requests that belong together keeps
    /// its key. Configured keys always have their keeper's subscription. Least recently
    /// active first, then oldest load first.
    fn evictable(&self) -> impl Iterator<Item = (u64, KeyId, usize)> {
        self.evictable_boxes()
            .map(|((last_seen_ms, id), boxes)| (last_seen_ms, id, boxes + self.keys[&id].charge))
    }

    /// The keys a load may evict, as [`Self::evictable`] has them, with their box counts
    /// alone: read from the idle index, so the metrics' sum over every idle key looks up no
    /// key state.
    fn evictable_boxes(&self) -> impl Iterator<Item = ((u64, KeyId), usize)> {
        let after = millis(self.config.evict_after);
        self.idle
            .iter()
            .take_while(move |&(&(last_seen_ms, _), _)| {
                self.now_ms.saturating_sub(last_seen_ms) >= after
            })
            .map(|(&key, &boxes)| (key, boxes))
    }

    /// Records activity on a tracked key, and returns its state.
    fn touch(&mut self, key: &TrackingKey) -> Option<&mut KeyState> {
        self.seen(key, |n| n)
    }

    /// Records activity on a tracked key that changes its subscriber count to
    /// `subscribers(count)`, keeping the idle index in step, and returns its state.
    fn seen(
        &mut self,
        key: &TrackingKey,
        subscribers: impl FnOnce(u32) -> u32,
    ) -> Option<&mut KeyState> {
        let key_id = *self.ids.get(key)?;
        let state = self.keys.get_mut(&key_id).expect("an id is tracked");
        if state.subscribers == 0 {
            self.idle.remove(&(state.last_seen_ms, key_id));
        }
        state.subscribers = subscribers(state.subscribers);
        state.last_seen_ms = self.now_ms;
        if state.subscribers == 0 {
            self.idle.insert((self.now_ms, key_id), state.boxes.len());
        }
        Some(state)
    }

    /// Brings an idle key's box count in the idle index up to its set, after a block or a
    /// rollback changed it.
    fn recount(&mut self, key_id: KeyId) {
        let state = &self.keys[&key_id];
        if let Some(boxes) = self.idle.get_mut(&(state.last_seen_ms, key_id)) {
            *boxes = state.boxes.len();
        }
    }

    /// Stops tracking `keys`, with their boxes and their deltas. All at once, so the ring is
    /// walked once however many keys go: an eviction or an idle sweep can drop thousands.
    fn untrack(&mut self, keys: &[TrackingKey]) {
        let mut gone = HashSet::new();
        for key in keys {
            let Some(key_id) = self.ids.remove(key) else {
                continue;
            };
            let state = self.keys.remove(&key_id).expect("an id is tracked");
            self.last_changes.remove(&key_id);
            self.idle.remove(&(state.last_seen_ms, key_id));
            self.charges -= state.charge;
            if let (true, TrackingKey::Tree(tree)) = (state.balance_only, key) {
                self.balance_trees.remove(tree);
            }
            for b in &state.boxes {
                self.reverse.remove(&(b.box_id, key_id));
            }
            self.mempool.untrack(key);
            gone.insert(key_id);
        }
        if gone.is_empty() {
            return;
        }
        for delta in self.ring.iter_mut().chain(self.orphans.values_mut()) {
            if !delta.loaded.is_empty() {
                delta.loaded.retain(|key_id| !gone.contains(key_id));
            }
            if !delta.balances.is_empty() {
                delta.balances.retain(|key_id, _| !gone.contains(key_id));
            }
            let held = delta.keys.len();
            // Looked up from the smaller side: a block touches few keys, a sweep may drop few.
            if gone.len() < held {
                for key_id in &gone {
                    delta.keys.remove(key_id);
                }
            } else {
                delta.keys.retain(|key_id, _| !gone.contains(key_id));
            }
            if delta.keys.len() < held {
                let referenced: HashSet<BoxId> = delta
                    .keys
                    .values()
                    .flat_map(|d| {
                        d.created
                            .iter()
                            .copied()
                            .chain(d.spent.iter().map(|s| s.box_id))
                    })
                    .collect();
                delta.boxes.retain(|id, _| referenced.contains(id));
            }
        }
    }

    /// Removes and returns the keys holding box `id`, with its inclusion height.
    fn take_holders(&mut self, id: &BoxId) -> Vec<(KeyId, u32)> {
        let holders: Vec<(KeyId, u32)> = self
            .reverse
            .range((*id, KeyId::MIN)..=(*id, KeyId::MAX))
            .map(|(&(_, k), &h)| (k, h))
            .collect();
        for (key_id, _) in &holders {
            self.reverse.remove(&(*id, *key_id));
        }
        holders
    }

    /// A tracked address's balance at the tip. `None` for a token key.
    fn balance(&self, key: &TrackingKey) -> Option<Balance> {
        let state = &self.keys[self.ids.get(key)?];
        Some(Balance {
            as_of: self.tip?,
            confirmed: state.totals.clone()?,
            pending: self.mempool.tracked_delta(key).cloned().unwrap_or_default(),
        })
    }

    /// Appends a `Balance`, in key order, for each tracked address whose totals changed in this
    /// step (`before` holds the totals of the keys the step touched, as they were) or whose
    /// pending delta differs from the one sent. Token keys get none.
    ///
    /// Only those keys are looked at: totals change only for the keys in `before`, and a
    /// pending delta only with its overlay, so only for a key with a `Mempool` message in
    /// `outputs`. A walk of every key here cost each block and mempool snapshot about a
    /// quarter of a microsecond per tracked key, growing faster than the keys as their maps
    /// outgrew the CPU's caches (`examples/eventbench.rs`).
    fn push_balances(&mut self, before: &BTreeMap<KeyId, Totals>, outputs: &mut Vec<Output>) {
        let mut changed: BTreeSet<TrackingKey> = before
            .keys()
            .filter_map(|id| self.keys.get(id))
            .map(|state| state.key.clone())
            .collect();
        changed.extend(outputs.iter().filter_map(|o| match o {
            Output::Mempool { key, .. } => Some(key.clone()),
            _ => None,
        }));
        for key in &changed {
            let totals = self.ids.get(key).and_then(|id| before.get(id));
            self.push_balance(key, totals, outputs);
        }
    }

    /// [`Engine::push_balances`] for one key, for an event that names the keys it changed.
    /// Nothing for a key that is not tracked.
    fn push_balance(
        &mut self,
        key: &TrackingKey,
        before: Option<&Totals>,
        outputs: &mut Vec<Output>,
    ) {
        let (Some(tip), Some(key_id)) = (self.tip, self.ids.get(key)) else {
            return;
        };
        let state = self.keys.get_mut(key_id).expect("an id is tracked");
        let pending = self.mempool.tracked_delta(key);
        if let Some(balance) = changed_balance(state, tip, before, pending) {
            outputs.push(Output::Balance { key: key.clone(), balance, last_change: None });
        }
    }

    /// The lowest height whose state the engine can still serve: the parent of the oldest
    /// block in the ring, or the tip when the ring is empty.
    fn floor(&self) -> u32 {
        match self.ring.front() {
            Some(oldest) => oldest.block.height - 1,
            None => self.tip.map_or(0, |t| t.height),
        }
    }

    /// Whether `block` is on the main chain within the window.
    fn on_main(&self, block: BlockRef) -> bool {
        if self.tip == Some(block) {
            return true;
        }
        let Some(oldest) = self.ring.front() else {
            return false;
        };
        if block.height + 1 == oldest.block.height {
            return oldest.parent_id == block.header_id;
        }
        block
            .height
            .checked_sub(oldest.block.height)
            .and_then(|i| self.ring.get(i as usize))
            .is_some_and(|d| d.block == block)
    }

    /// Ring entries above `height`, oldest first.
    fn ring_above(&self, height: u32) -> impl Iterator<Item = &BlockDelta> {
        self.ring.iter().filter(move |d| d.block.height > height)
    }

    fn page(
        &mut self,
        key: &TrackingKey,
        at: Option<BlockRef>,
        after: Option<BoxKey>,
        limit: usize,
    ) -> Result<PageIds, PageError> {
        if self.is_balance_only(key) {
            return Err(PageError::NotTracked);
        }
        let since = self.touch(key).ok_or(PageError::NotTracked)?.since;
        // A tracked key implies a tip: dropping the tip untracks every key.
        let at = at.or(self.tip).ok_or(PageError::NotTracked)?;
        let key_id = self.ids[key];
        let state = &self.keys[&key_id];
        if at.height < since.height || !self.on_main(at) {
            return Err(PageError::CursorExpired);
        }
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);

        // The set at `at` is the tip's boxes that existed then (included at or below it), and
        // the boxes spent since that existed then.
        let mut restored = BTreeSet::new();
        for delta in self.ring_above(at.height) {
            for spend in delta.keys.get(&key_id).into_iter().flat_map(|d| &d.spent) {
                let height = delta.boxes[&spend.box_id].inclusion_height;
                if height <= at.height {
                    restored.insert(BoxKey { inclusion_height: height, box_id: spend.box_id });
                }
            }
        }
        let current = state
            .boxes
            .range((lower, Bound::Unbounded))
            .take_while(|b| b.inclusion_height <= at.height);
        let restored = restored.range((lower, Bound::Unbounded));
        Ok(PageIds { as_of: at, boxes: merge(current, restored).take(limit).collect() })
    }

    /// A tracked key's boxes at the tip with its overlay applied, with at most `limit`
    /// confirmed ones. The limit is taken after the pending spends, so the boxes cut are ones
    /// a client could spend.
    pub(crate) fn with_pool(&self, key: &TrackingKey, limit: usize) -> Option<WithPoolIds> {
        if self.is_balance_only(key) {
            return None;
        }
        let key_id = *self.ids.get(key)?;
        // A tracked key implies a tip: dropping the tip untracks every key.
        let as_of = self.tip?;
        let state = &self.keys[&key_id];
        let overlay = self.mempool.overlay(key);
        let spent: HashSet<BoxId> = overlay
            .iter()
            .flat_map(|p| p.spent.iter().copied())
            .collect();
        let pending: Vec<Arc<UTxO>> = (overlay.into_iter().flat_map(|p| p.created))
            .filter(|b| !spent.contains(&b.id))
            .collect();
        // A spent box is either the key's (confirmed) or created by an earlier pending tx.
        let spent_held = spent
            .iter()
            .filter(|id| self.reverse.contains_key(&(**id, key_id)))
            .count();
        let total = state.boxes.len() - spent_held + pending.len();
        let confirmed = (state.boxes.iter())
            .filter(|b| !spent.contains(&b.box_id))
            .take(limit)
            .copied()
            .collect();
        Some(WithPoolIds { as_of, confirmed, pending, total })
    }

    fn catch_up(&mut self, key: TrackingKey, from: Option<BlockRef>) -> Vec<Output> {
        let Some(state) = self.touch(&key) else {
            return vec![Output::ResyncRequired { key, reason: ResyncReason::NotTracked }];
        };
        let (since, loaded_seq) = (state.since, state.loaded_seq);
        if from.is_some() && state.balance_only {
            return vec![Output::ResyncRequired { key, reason: ResyncReason::NotTracked }];
        }
        let key_id = self.ids[&key];
        let mut outputs = Vec::new();
        // From the key's last change there is nothing to replay, however old it is: the key's
        // boxes are the same at every block since.
        let last_change = self.last_changes.get(&key_id).copied();
        if let Some(from) = from.filter(|&from| Some(from) != last_change) {
            // Roll back through orphaned blocks to the main chain, then replay. Only the
            // blocks that changed the key are sent.
            let mut cursor = from;
            let mut rollbacks = Vec::new();
            // Below the floor, `from` is still on the key's chain if the key's oldest change
            // in the ring chains to it: every change since is in the ring. So a quiet key
            // whose client was away while it changed replays instead of resyncing.
            let chained_below = from.height < self.floor()
                && (self.ring.iter())
                    .find_map(|d| d.keys.get(&key_id))
                    .is_some_and(|d| d.prev == from);
            while !chained_below && !self.on_main(cursor) {
                let reason = if cursor.height < self.floor() {
                    ResyncReason::OutsideWindow
                } else {
                    match self.orphans.get(&cursor.header_id) {
                        Some(orphan) if orphan.seq > loaded_seq => {
                            rollbacks.push(orphan);
                            cursor = orphan.parent();
                            continue;
                        }
                        Some(_) => ResyncReason::NotTracked,
                        None => ResyncReason::ForkBeyondWindow,
                    }
                };
                return vec![Output::ResyncRequired { key, reason }];
            }
            if cursor.height < since.height {
                return vec![Output::ResyncRequired { key, reason: ResyncReason::NotTracked }];
            }
            for orphan in rollbacks {
                if let Some(key_delta) = orphan.keys.get(&key_id) {
                    outputs.push(rollback(&key, orphan, key_delta));
                }
            }
            for delta in self.ring_above(cursor.height) {
                if let Some(key_delta) = delta.keys.get(&key_id) {
                    outputs.push(Output::Block {
                        key: key.clone(),
                        block: delta.block,
                        prev: key_delta.prev,
                        created: created_boxes(delta, key_delta),
                        spent: key_delta.spent.clone(),
                    });
                }
            }
        }
        if from.is_some() {
            let pending = self.mempool.overlay(&key);
            outputs.push(Output::Mempool { key: key.clone(), pending, last_change: None });
        }
        if let Some(balance) = self.balance(&key) {
            outputs.push(Output::Balance { key, balance, last_change: None });
        }
        outputs
    }
}

/// Decides whether a pending tx's input counts as confirmed at the engine's tip: a box a key
/// tracked with its boxes would hold counts only if one does; any other box counts, on the
/// source's word (see `overlay::pending_order`), a balance-only key's included, since it has
/// no boxes to check against.
struct Confirmed<'a> {
    ids: &'a BTreeMap<TrackingKey, KeyId>,
    balance_trees: &'a HashMap<ErgoTree, KeyId>,
    reverse: &'a BTreeMap<(BoxId, KeyId), u32>,
}

impl<'a> Confirmed<'a> {
    fn new(
        ids: &'a BTreeMap<TrackingKey, KeyId>,
        balance_trees: &'a HashMap<ErgoTree, KeyId>,
        reverse: &'a BTreeMap<(BoxId, KeyId), u32>,
    ) -> Self {
        Self { ids, balance_trees, reverse }
    }

    fn check(&self, b: &UTxO) -> bool {
        let held = self
            .reverse
            .range((b.id, KeyId::MIN)..=(b.id, KeyId::MAX))
            .next()
            .is_some();
        let with_boxes = |k: &TrackingKey| match k {
            TrackingKey::Tree(tree) => !self.balance_trees.contains_key(tree),
            TrackingKey::Token(_) => true,
        };
        held || !overlay::keys_of(b).any(|k| self.ids.contains_key(&k) && with_boxes(&k))
    }
}

fn created_boxes(delta: &BlockDelta, key_delta: &KeyDelta) -> Vec<Arc<UTxO>> {
    key_delta
        .created
        .iter()
        .map(|id| delta.boxes[id].utxo.clone())
        .collect()
}

/// The `Rollback` of `delta` for `key`, whose share of it is `key_delta`: its sides swapped.
fn rollback(key: &TrackingKey, delta: &BlockDelta, key_delta: &KeyDelta) -> Output {
    Output::Rollback {
        key: key.clone(),
        block: delta.block,
        prev: key_delta.prev,
        created: key_delta
            .spent
            .iter()
            .map(|s| delta.boxes[&s.box_id].clone())
            .collect(),
        spent: key_delta
            .created
            .iter()
            .map(|id| Spend { box_id: *id, tx_id: delta.boxes[id].utxo.transaction_id })
            .collect(),
    }
}

/// Merges two ascending iterators of distinct keys.
fn merge<'a>(
    a: impl Iterator<Item = &'a BoxKey>,
    b: impl Iterator<Item = &'a BoxKey>,
) -> impl Iterator<Item = BoxKey> {
    let (mut a, mut b) = (a.peekable(), b.peekable());
    std::iter::from_fn(move || match (a.peek(), b.peek()) {
        (Some(x), Some(y)) if x <= y => a.next().copied(),
        (Some(_), Some(_)) => b.next().copied(),
        (Some(_), None) => a.next().copied(),
        (None, _) => b.next().copied(),
    })
}

/// The balance to send for a key, if it differs from the last one sent: its totals changed
/// from `before`, or its pending delta from the one sent. Records the delta as sent. `None`
/// for a token key, which has no balance.
fn changed_balance(
    state: &mut KeyState,
    tip: BlockRef,
    before: Option<&Totals>,
    pending: Option<&PendingDelta>,
) -> Option<Balance> {
    let totals = state.totals.as_ref()?;
    let pending = pending.cloned().unwrap_or_default();
    let totals_changed = before.is_some_and(|t| t != totals);
    if !totals_changed && pending == state.pending_sent {
        return None;
    }
    state.pending_sent = pending.clone();
    Some(Balance { as_of: tip, confirmed: totals.clone(), pending })
}

/// A duration in whole ms, the unit of the engine's clock. Past `u64::MAX` ms (584 million
/// years) it saturates.
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
