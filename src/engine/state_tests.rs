//! The engine against the oracle over random histories, driven the way the adapters
//! and the transport drive it, with simulated clients that apply what they are sent.

use std::collections::{BTreeSet, HashSet};

use proptest::{prelude::*, sample::Index};

use super::*;
use crate::{
    engine::{PendingTx, PoolBox, overlay::TxDelta},
    sim::{self, Chain, Oracle, SimConfig, SimOp, reference_delta, strategy},
    types::{
        Digest,
        ergo::{ErgoTree, TxId, UnconfirmedTransaction},
    },
};

/// Small enough that random histories reach the window, idle, cap and untrack budget edges:
/// reorgs go up to 5 blocks deep, `Advance` moves the clock up to [`MAX_ADVANCE_SECS`] at a
/// time, and two or three of the simulator's keys fill the cap, each counting its charge
/// (five to eight boxes) and two to four boxes.
const CONFIG: Config = Config {
    window: 4,
    grace: Duration::from_secs(3),
    evict_after: Duration::from_secs(2),
    max_tracked_units: 20,
    untrack_units_per_block: 15,
};
const MAX_ADVANCE_SECS: u8 = 2;
const MAX_REORG: u8 = 5;
const CASES: u32 = 5_000;

#[derive(Debug, Clone)]
enum Op {
    /// The node moves on; the engine does not see it yet.
    Chain(SimOp),
    /// The block consumer brings the engine to the node's tip.
    Sync,
    /// One step of it: a revert or an apply.
    SyncOne,
    /// The mempool poller sends the node's mempool, observed at the node's tip.
    Snapshot,
    /// The loader loads a key at the node's tip.
    Load(Index),
    /// The balance loader tracks an address balance-only, from its confirmed balance at the
    /// node's tip.
    LoadBalance(Index),
    /// A new client: a snapshot through pages (or none, for a balance-only one), a
    /// subscription, and its catch-up. A balance-only client of an untracked address loads
    /// its balance, a client with boxes of a key without them loads them.
    Subscribe {
        key: Index,
        balance_only: bool,
    },
    Unsubscribe(Index),
    /// A client disconnects, keeping its state to resume.
    Disconnect(Index),
    /// A disconnected client subscribes again from the last block it applied.
    Resume(Index),
    /// A REST read of a key's pages at a block the engine has seen, main or orphaned.
    Page {
        key: Index,
        at: Index,
        limit: u8,
    },
    /// A REST read of a key's boxes with the pool, with at most `limit` confirmed ones.
    Spendable {
        key: Index,
        limit: u8,
    },
    /// The engine's clock moves this many seconds.
    Advance(u8),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => strategy::op(MAX_REORG).prop_map(Op::Chain),
        4 => Just(Op::Sync),
        2 => Just(Op::SyncOne),
        4 => Just(Op::Snapshot),
        2 => any::<Index>().prop_map(Op::Load),
        2 => any::<Index>().prop_map(Op::LoadBalance),
        4 => (any::<Index>(), any::<bool>())
            .prop_map(|(key, balance_only)| Op::Subscribe { key, balance_only }),
        1 => any::<Index>().prop_map(Op::Unsubscribe),
        2 => any::<Index>().prop_map(Op::Disconnect),
        2 => any::<Index>().prop_map(Op::Resume),
        2 => (any::<Index>(), any::<Index>(), 1..5u8)
            .prop_map(|(key, at, limit)| Op::Page { key, at, limit }),
        2 => (any::<Index>(), 0..6u8).prop_map(|(key, limit)| Op::Spendable { key, limit }),
        4 => (0..=MAX_ADVANCE_SECS).prop_map(Op::Advance),
    ]
}

/// What the test expects of a tracked key, kept apart from the engine.
#[derive(Debug, Clone)]
struct KeyModel {
    /// Counts loads, so a client can tell whether the key stayed tracked since its snapshot.
    epoch: u64,
    since: BlockRef,
    /// The blocks that changed the key's boxes, oldest first, after `since`, which starts it:
    /// the chain each `prev` must follow. Kept from the oracle, not from the engine's messages.
    changes: Vec<BlockRef>,
    /// Tracked for its balance alone, with no boxes.
    balance_only: bool,
    /// What it counts against the cap besides its boxes, from the tokens it held at its load.
    charge: usize,
    /// The clock at the key's last activity, in ms.
    last_seen: u64,
    subscribers: u32,
    /// The last balance sent (or delivered with a catch-up), and the last overlay. A token
    /// key has no balance.
    balance: Option<(Totals, PendingDelta)>,
    overlay: Vec<PendingTx>,
}

impl KeyModel {
    fn last_change(&self) -> BlockRef {
        *self.changes.last().expect("starts at the load block")
    }
}

#[derive(Debug, Clone)]
struct Client {
    key: TrackingKey,
    balance_only: bool,
    /// Subscribed and receiving messages.
    live: bool,
    /// Disconnected with its state, and may resume.
    resumable: bool,
    epoch: u64,
    /// The block of the key's last change, once a message names it.
    last: Option<BlockRef>,
    /// Until then, the block the client's boxes are known at (its snapshot's, or the one it
    /// resumed from): the key's last change is at or below it.
    known_at: BlockRef,
    /// The boxes held, with the heights that included them.
    boxes: BTreeMap<BoxId, (u32, Arc<UTxO>)>,
    overlay: Vec<PendingTx>,
    balance: Option<Balance>,
}

impl Client {
    /// Applies one message, checking what a client SDK checks.
    fn receive(
        &mut self,
        output: &Output,
        engine_tip: Option<BlockRef>,
        seen: &mut BTreeSet<&'static str>,
    ) {
        match output {
            Output::Block { block, prev, created, spent, .. } if !self.balance_only => {
                self.check_chain(*prev, "block", seen);
                assert!(prev.height < block.height, "block {block:?} after its prev {prev:?}");
                assert!(!created.is_empty() || !spent.is_empty(), "an empty block {block:?}");
                for s in spent {
                    assert!(self.boxes.remove(&s.box_id).is_some(), "spends unknown {}", s.box_id);
                }
                for b in created {
                    let held = self.boxes.insert(b.id, (block.height, b.clone()));
                    assert!(held.is_none(), "creates held {}", b.id);
                }
                self.last = Some(*block);
            }
            Output::Rollback { block, prev, created, spent, .. } if !self.balance_only => {
                self.check_chain(*block, "rollback", seen);
                assert!(prev.height < block.height, "rollback {block:?} to {prev:?}");
                assert!(!created.is_empty() || !spent.is_empty(), "an empty rollback {block:?}");
                for s in spent {
                    assert!(self.boxes.remove(&s.box_id).is_some(), "undoes unknown {}", s.box_id);
                }
                for b in created {
                    let id = b.utxo.id;
                    let held = self.boxes.insert(id, (b.inclusion_height, b.utxo.clone()));
                    assert!(held.is_none(), "restores held {id}");
                }
                self.last = Some(*prev);
            }
            Output::Mempool { pending, last_change, .. } if !self.balance_only => {
                let last_change = last_change.expect("a key with boxes names its last change");
                self.check_chain(last_change, "mempool", seen);
                // Every pending spend refers to a box the client has, or to one an earlier tx
                // of the overlay creates.
                let mut created = HashSet::new();
                for tx in pending {
                    for id in &tx.spent {
                        assert!(
                            self.boxes.contains_key(id) || created.contains(id),
                            "{} spends {id}, which the client does not have",
                            tx.tx_id
                        );
                    }
                    created.extend(tx.created.iter().map(|b| b.id));
                }
                self.overlay = pending.clone();
            }
            Output::Balance { key, balance, last_change } => {
                assert!(matches!(key, TrackingKey::Tree(_)), "a balance sent for {key:?}");
                let engine_tip = engine_tip.expect("a balance goes out with a tip");
                assert_eq!(balance.as_of, engine_tip, "a balance for another block");
                if !self.balance_only {
                    let last_change = last_change.expect("a key with boxes names its last change");
                    self.check_chain(last_change, "balance", seen);
                }
                self.balance = Some(balance.clone());
            }
            Output::ResyncRequired { .. } => {
                self.live = false;
                self.resumable = false;
            }
            Output::Block { .. } | Output::Rollback { .. } | Output::Mempool { .. } => {}
            Output::LoadRejected { .. } | Output::CapacityExceeded { .. } => {
                panic!("load results go to the loader")
            }
        }
    }

    /// The check of a message naming the key's last change as the client has it: a block's
    /// `prev`, a rollback's block, an overlay's or balance's `last_change`. Before any
    /// message named it, it is at or below the block the client's boxes are known at, and
    /// then the message names it.
    fn check_chain(&mut self, named: BlockRef, kind: &str, seen: &mut BTreeSet<&'static str>) {
        match self.last {
            Some(last) => assert_eq!(named, last, "{kind} off the key's chain of changes"),
            None => {
                assert!(named.height <= self.known_at.height, "{kind} above {:?}", self.known_at);
                if named.height == self.known_at.height {
                    assert_eq!(named, self.known_at, "{kind} on another fork");
                }
                if named.height < self.known_at.height {
                    seen.insert("client learns a last change below where its boxes are known");
                }
                self.last = Some(named);
            }
        }
    }
}

struct Harness {
    chain: Chain,
    engine: Engine,
    /// The blocks fed to the engine and not reverted, oldest first, ending at its tip.
    fed: Vec<BlockRef>,
    /// Blocks the engine reverted.
    reverted: Vec<BlockRef>,
    /// The model of the ring buffer's length.
    ring_len: u32,
    keys: BTreeMap<TrackingKey, KeyModel>,
    next_epoch: u64,
    /// The engine's clock, in ms.
    now: u64,
    /// Keys evicted for a load so far.
    evicted: u64,
    /// The snapshot the engine uses, as received, and the one it holds for a later block.
    held: Option<Arc<[UnconfirmedTransaction]>>,
    ahead: Option<(Arc<[UnconfirmedTransaction]>, BlockRef)>,
    clients: Vec<Client>,
    /// The keys with boxes that a revert since the last applied block left alone.
    unchanged_reverts: BTreeSet<TrackingKey>,
    seen: BTreeSet<&'static str>,
}

impl Harness {
    fn new() -> Self {
        let mut harness = Self {
            chain: Chain::new(SimConfig::default()),
            engine: Engine::new(CONFIG),
            fed: Vec::new(),
            reverted: Vec::new(),
            ring_len: 0,
            keys: BTreeMap::new(),
            next_epoch: 0,
            now: 0,
            evicted: 0,
            held: None,
            ahead: None,
            clients: Vec::new(),
            unchanged_reverts: BTreeSet::new(),
            seen: BTreeSet::new(),
        };
        harness.sync(usize::MAX);
        harness
    }

    fn tip(&self) -> BlockRef {
        self.engine.tip().expect("the engine has a tip")
    }

    /// The oracle over the engine's chain.
    fn oracle(&self) -> Oracle {
        self.chain.oracle_at(&self.tip().header_id)
    }

    /// The lowest height the engine can serve, by the model of its ring.
    fn floor(&self) -> u32 {
        self.tip().height - self.ring_len
    }

    fn run(&mut self, op: &Op) {
        match op {
            Op::Chain(op) => self.chain.apply(op),
            Op::Sync => self.sync(usize::MAX),
            Op::SyncOne => self.sync(1),
            Op::Snapshot => self.snapshot(),
            Op::Load(key) => {
                let key = self.pick_key(key);
                self.load(&key);
            }
            Op::LoadBalance(key) => {
                if let TrackingKey::Tree(tree) = self.pick_key(key) {
                    self.load_balance(tree);
                }
            }
            Op::Subscribe { key, balance_only } => {
                let key = self.pick_key(key);
                self.subscribe(key, *balance_only);
            }
            Op::Unsubscribe(pick) => {
                if let Some(i) = self.pick_client(pick, |c| c.live) {
                    self.unsubscribe(i, false);
                }
            }
            Op::Disconnect(pick) => {
                if let Some(i) = self.pick_client(pick, |c| c.live) {
                    self.unsubscribe(i, true);
                }
            }
            Op::Resume(pick) => {
                if let Some(i) = self.pick_client(pick, |c| c.resumable) {
                    self.resume(i);
                }
            }
            Op::Page { key, at, limit } => {
                let key = self.pick_key(key);
                let blocks: Vec<BlockRef> =
                    self.fed.iter().chain(&self.reverted).copied().collect();
                let at = blocks[at.index(blocks.len())];
                self.page(&key, at, usize::from(*limit));
            }
            Op::Spendable { key, limit } => {
                let key = self.pick_key(key);
                self.spendable(&key, usize::from(*limit));
            }
            Op::Advance(secs) => {
                self.now += u64::from(*secs) * 1000;
                self.engine.set_now(Duration::from_millis(self.now));
            }
        }
        self.check();
    }

    /// The boxes `key` holds in the engine at `height` by the model: none if it is
    /// balance-only.
    fn held(&self, oracle: &Oracle, key: &TrackingKey, height: u32) -> usize {
        match self.keys.get(key) {
            Some(m) if m.balance_only => 0,
            _ => oracle.unspent(key, height).len(),
        }
    }

    /// `key`'s charge against the cap if loaded at `height`, on the node's chain: from the
    /// distinct tokens an address holds there, none for a token key.
    fn charge_at(&self, key: &TrackingKey, height: u32) -> usize {
        let tokens = match key {
            TrackingKey::Tree(_) => self.chain.oracle().totals(key, height).tokens.len(),
            TrackingKey::Token(_) | TrackingKey::Template(_) => 0,
        };
        key_charge(key, tokens)
    }

    /// The keys tracked with their boxes, in key order.
    fn box_keys(&self) -> Vec<TrackingKey> {
        let keys = self.keys.iter().filter(|(_, m)| !m.balance_only);
        keys.map(|(k, _)| k.clone()).collect()
    }

    fn pick_key(&self, index: &Index) -> TrackingKey {
        let keys = self.chain.keys();
        keys[index.index(keys.len())].clone()
    }

    fn pick_client(&self, index: &Index, filter: impl Fn(&Client) -> bool) -> Option<usize> {
        let candidates: Vec<usize> = (0..self.clients.len())
            .filter(|&i| filter(&self.clients[i]))
            .collect();
        (!candidates.is_empty()).then(|| candidates[index.index(candidates.len())])
    }

    /// Moves the engine up to `steps` blocks towards the node's tip, as the block consumer
    /// does: reverts its blocks off the node's chain, tip first, then applies the node's
    /// blocks, oldest first. With no tip (a fresh start, or after the ring ran out) it starts
    /// at the node's tip.
    fn sync(&mut self, steps: usize) {
        for _ in 0..steps {
            let Some(&tip) = self.fed.last() else {
                let start = self.chain.tip();
                self.apply_block(start);
                continue;
            };
            if self.chain.header_at(tip.height) != Some(tip) {
                self.revert_block(tip);
            } else if let Some(next) = self.chain.header_at(tip.height + 1) {
                self.apply_block(next);
            } else {
                return;
            }
        }
    }

    fn apply_block(&mut self, block: BlockRef) {
        let indexed = self
            .chain
            .oracle_at(&block.header_id)
            .indexed_block(block.height);
        let before = self.expected_states();
        // The keys tracked with their boxes whose boxes the block changes, by the oracle: a
        // box created and spent within the block changes nothing.
        let oracle = self.chain.oracle_at(&block.header_id);
        let changed: Vec<TrackingKey> = self
            .box_keys()
            .into_iter()
            .filter(|k| oracle.unspent(k, block.height - 1) != oracle.unspent(k, block.height))
            .collect();
        let in_block = in_block_spends(&indexed);
        let moves_balance_only = self.keys.iter().any(|(k, m)| {
            m.balance_only
                && indexed.transactions.iter().any(|tx| {
                    tx.inputs
                        .iter()
                        .chain(&tx.outputs)
                        .any(|b| k.matches(&b.utxo))
                })
        });
        let outputs = self.feed(Event::BlockApplied(indexed));
        if moves_balance_only {
            self.seen.insert("block moves a balance-only key's totals");
        }
        self.fed.push(block);
        self.ring_len = (self.ring_len + 1).min(CONFIG.window);

        // Exactly the keys tracked with their boxes that the block changes get it, in key
        // order, each chained to its last change, and nothing else at rank 0: a key the block
        // leaves alone gets nothing, and a balance-only key gets none.
        let blocks: Vec<&TrackingKey> = outputs
            .iter()
            .filter_map(|o| match o {
                Output::Block { key, block: b, prev, .. } => {
                    assert_eq!(*b, block);
                    let model = &self.keys[key];
                    assert_eq!(*prev, model.last_change(), "{key:?}'s block skips a change");
                    Some(key)
                }
                Output::Rollback { .. } | Output::ResyncRequired { .. } => {
                    panic!("an applied block sends {o:?}")
                }
                _ => None,
            })
            .collect();
        assert_eq!(blocks, changed.iter().collect::<Vec<_>>(), "the keys the block changes");
        if changed.len() < self.box_keys().len() {
            self.seen.insert("block leaves a key with boxes unchanged");
        }
        for key in &changed {
            let alone = !outputs.iter().any(|o| {
                matches!(o, Output::Mempool { .. } | Output::Balance { .. }) && o.key() == key
            });
            if alone {
                self.seen
                    .insert("block changes a key with no overlay or balance after it");
            }
            let model = self.keys.get_mut(key).expect("tracked");
            if model.changes.len() > 1 && model.last_change().height + 1 < block.height {
                self.seen.insert("change chained across unchanged blocks");
            }
            model.changes.push(block);
        }
        self.unchanged_reverts.clear();
        if outputs.iter().any(|o| {
            matches!(o, Output::Block { created, spent, .. }
            if created.iter().any(|b| in_block.contains(&b.id))
                || spent.iter().any(|s| in_block.contains(&s.box_id)))
        }) {
            panic!("a box created and spent within the block is in a delta");
        }
        if self
            .keys
            .keys()
            .any(|k| in_block_matches(&in_block, &self.chain, k))
        {
            self.seen
                .insert("tracked box created and spent within a block");
        }

        // The snapshot held for this block replaces the one in use.
        match self.ahead.take() {
            Some((txs, observed)) if observed == block => {
                self.held = Some(txs);
                self.seen.insert("snapshot held for a block used");
            }
            Some(ahead) if ahead.1.height > block.height => self.ahead = Some(ahead),
            _ => {}
        }

        // Idle keys go, least recently active and then oldest load first, until the block's
        // budget of units is spent, a key counting its boxes and its charge: the first always
        // goes, a later one only if it fits, and none after one that does not. Keys with
        // subscribers or recent activity stay.
        let grace = CONFIG.grace.as_millis() as u64;
        let now = self.now;
        let oracle = self.oracle();
        let mut idle: Vec<(u64, u64, TrackingKey, usize)> = self
            .keys
            .iter()
            .filter(|(_, m)| m.subscribers == 0 && now - m.last_seen > grace)
            .map(|(k, m)| {
                let units = self.held(&oracle, k, block.height) + m.charge;
                (m.last_seen, m.epoch, k.clone(), units)
            })
            .collect();
        idle.sort();
        let mut untracked = 0;
        for (i, (_, _, key, units)) in idle.into_iter().enumerate() {
            if i > 0 && untracked + units > CONFIG.untrack_units_per_block {
                self.seen
                    .insert("idle key kept past grace: the block's budget was spent");
                break;
            }
            untracked += units;
            self.keys.remove(&key);
            self.seen.insert("idle key untracked");
            if i > 0 {
                self.seen
                    .insert("idle keys untracked together in one block");
            }
            if units > CONFIG.untrack_units_per_block {
                self.seen
                    .insert("idle key over the block's budget untracked alone");
            }
        }
        if self
            .keys
            .values()
            .any(|m| m.subscribers > 0 && now - m.last_seen > grace)
        {
            self.seen.insert("key kept by a subscriber");
        }
        self.check_emissions(&outputs, &before);
    }

    fn revert_block(&mut self, block: BlockRef) {
        let before = self.expected_states();
        let undoes_balance_only = self.keys.iter().any(|(k, m)| {
            m.balance_only
                && m.since != block
                && self
                    .engine
                    .ring
                    .back()
                    .is_some_and(|d| d.balances.contains_key(&self.engine.ids[k]))
        });
        let outputs = self.feed(Event::BlockReverted(block));
        self.fed.pop();
        self.reverted.push(block);
        self.held = None;

        let resyncs: BTreeSet<&TrackingKey> = outputs
            .iter()
            .filter_map(|o| match o {
                Output::ResyncRequired { key, reason } => {
                    assert_eq!(*reason, ResyncReason::ForkBeyondWindow);
                    Some(key)
                }
                _ => None,
            })
            .collect();
        if self.ring_len == 0 {
            // The ring ran out: every key goes, and the engine starts again from a tip.
            assert_eq!(resyncs, before.keys().collect());
            assert_eq!(self.engine.tip(), None);
            self.keys.clear();
            self.fed.clear();
            self.ahead = None;
            self.ring_len = 0;
            self.seen.insert("ring ran out");
            return;
        }
        self.ring_len -= 1;
        if self.fed.is_empty() {
            // The engine is at the reverted block's parent, which it never applied.
            self.fed.push(self.tip());
        }
        let dropped: BTreeSet<&TrackingKey> = self
            .keys
            .iter()
            .filter(|(_, m)| m.since == block)
            .map(|(k, _)| k)
            .collect();
        assert_eq!(resyncs, dropped, "exactly the keys loaded at the block are dropped");
        if !dropped.is_empty() {
            self.seen.insert("key dropped with its load block");
        }
        self.keys.retain(|_, m| m.since != block);

        // Exactly the keys tracked with their boxes whose last change was the block get its
        // rollback, which moves their last change back to the one before.
        let changed: Vec<TrackingKey> = self
            .box_keys()
            .into_iter()
            .filter(|k| self.keys[k].last_change() == block)
            .collect();
        for key in &changed {
            self.keys.get_mut(key).expect("tracked").changes.pop();
        }
        let rollbacks: Vec<&TrackingKey> = outputs
            .iter()
            .filter_map(|o| match o {
                Output::Rollback { key, block: b, prev, .. } => {
                    assert_eq!(*b, block);
                    assert_eq!(*prev, self.keys[key].last_change(), "{key:?}'s rollback");
                    Some(key)
                }
                _ => None,
            })
            .collect();
        assert_eq!(rollbacks, changed.iter().collect::<Vec<_>>(), "the keys the block changed");
        for key in &changed {
            if self.unchanged_reverts.contains(key) {
                self.seen
                    .insert("rollback of a change below an orphan that left the key alone");
            }
        }
        let unchanged = self.box_keys().into_iter().filter(|k| !changed.contains(k));
        self.unchanged_reverts.extend(unchanged);
        if undoes_balance_only {
            self.seen
                .insert("rollback undoes a balance-only key's change");
        }
        self.check_emissions(&outputs, &before);
    }

    fn snapshot(&mut self) {
        let txs: Arc<[UnconfirmedTransaction]> = self.chain.mempool().into();
        let observed = self.chain.tip();
        let before = self.expected_states();
        let outputs =
            self.feed(Event::MempoolSnapshot { txs: txs.clone(), observed_tip: observed });
        // A reorg past the ring leaves the engine with no tip until the next sync: a snapshot
        // then has nothing to apply to, and is ignored.
        let Some(tip) = self.engine.tip() else {
            assert!(outputs.is_empty());
            return;
        };
        if observed == tip {
            // Txs of orphaned blocks back in the pool reach overlays only from a snapshot
            // taken after the rollback.
            let returned = self.reverted.iter().any(|b| {
                let block = self.chain.block(&b.header_id).expect("mined");
                block.txs.iter().any(|t| txs.iter().any(|p| p.id == t.id))
            });
            if returned {
                self.seen
                    .insert("reorg-returned tx in a snapshot after the rollback");
            }
            self.held = Some(txs);
        } else if observed.height > tip.height {
            self.ahead = Some((txs, observed));
            assert!(outputs.is_empty());
        } else {
            assert!(outputs.is_empty());
        }
        self.check_emissions(&outputs, &before);
    }

    /// The loader: the key's boxes at the node's tip.
    fn load(&mut self, key: &TrackingKey) {
        let as_of = self.chain.tip();
        let boxes = self
            .chain
            .oracle()
            .load(key, as_of.height)
            .into_iter()
            .map(crate::engine::LoadedBox::from)
            .collect::<Vec<_>>();
        let count = boxes.len();
        let charge = self.charge_at(key, as_of.height);
        let before = self.expected_states();
        let tracked = self.keys.get(key).map(|m| m.balance_only);
        let outputs = self.feed(Event::KeyLoaded { key: key.clone(), as_of, boxes });
        if self.engine.tip() != Some(as_of) {
            assert_eq!(outputs, vec![Output::LoadRejected { key: key.clone(), as_of }]);
            self.seen.insert("load rejected");
            return;
        }
        if tracked == Some(false) {
            self.keys.get_mut(key).expect("tracked").last_seen = self.now;
        } else if tracked == Some(true) {
            // A balance-only key takes its boxes, keeping its subscribers, last balance and
            // overlay; its totals are the oracle's either way, so no balance goes out. It is
            // charged again, from its boxes' tokens, and already counts its old charge.
            let was_charged = self.keys[key].charge;
            let load = count + charge.saturating_sub(was_charged);
            if !self.make_room(key, load, count, as_of.height, &outputs) {
                self.check_emissions(&outputs, &before);
                return;
            }
            // The same key, so the same epoch: its id, which orders idle keys, stays too, and
            // no client has its boxes from before.
            let model = self.keys.get_mut(key).expect("tracked");
            model.since = as_of;
            model.changes = vec![as_of];
            model.charge = charge;
            model.balance_only = false;
            model.last_seen = self.now;
            self.seen.insert("balance-only key upgraded to boxes");
            if model.subscribers > 0 {
                self.seen
                    .insert("balance-only key with subscribers upgraded to boxes");
            }
        } else {
            if !self.make_room(key, count + charge, count, as_of.height, &outputs) {
                self.check_emissions(&outputs, &before);
                return;
            }
            let (balance, overlay) = self.expected(key);
            self.next_epoch += 1;
            let model = KeyModel {
                epoch: self.next_epoch,
                since: as_of,
                changes: vec![as_of],
                balance_only: false,
                charge,
                last_seen: self.now,
                subscribers: 0,
                balance,
                overlay,
            };
            self.keys.insert(key.clone(), model);
        }
        self.check_emissions(&outputs, &before);
    }

    /// The balance loader: the address's confirmed balance at the node's tip.
    fn load_balance(&mut self, tree: ErgoTree) {
        let key = TrackingKey::Tree(tree.clone());
        let as_of = self.chain.tip();
        let confirmed = self.chain.oracle().totals(&key, as_of.height);
        let before = self.expected_states();
        let event = Event::BalanceLoaded { tree, as_of, confirmed };
        let outputs = self.feed(event);
        if self.engine.tip() != Some(as_of) {
            assert_eq!(outputs, vec![Output::LoadRejected { key, as_of }]);
            self.seen.insert("balance load rejected");
            return;
        }
        if let Some(model) = self.keys.get_mut(&key) {
            assert_eq!(outputs, vec![]);
            model.last_seen = self.now;
            self.seen.insert("balance load of a tracked key ignored");
        } else {
            let charge = self.charge_at(&key, as_of.height);
            if !self.make_room(&key, charge, 0, as_of.height, &outputs) {
                self.seen.insert("balance load refused at the cap");
                self.check_emissions(&outputs, &before);
                return;
            }
            // Nothing is decided again: a balance-only key's boxes are taken on the source's
            // word.
            assert_eq!(outputs, vec![]);
            let (balance, overlay) = self.expected(&key);
            self.next_epoch += 1;
            let model = KeyModel {
                epoch: self.next_epoch,
                since: as_of,
                changes: vec![as_of],
                balance_only: true,
                charge,
                last_seen: self.now,
                subscribers: 0,
                balance,
                overlay,
            };
            self.keys.insert(key, model);
        }
        self.check_emissions(&outputs, &before);
    }

    /// The cap on tracked boxes, for a load adding `load` units (its `boxes`, and one for a
    /// new key) at `height`. Every key counts its boxes and one more. Past the cap, the idle
    /// keys (no subscribers, idle for `evict_after`) go, least recently active and then
    /// oldest load first, until the load fits. If even all of them would not make room, the
    /// load is refused and none goes. Returns whether the load is taken.
    fn make_room(
        &mut self,
        key: &TrackingKey,
        load: usize,
        boxes: usize,
        height: u32,
        outputs: &[Output],
    ) -> bool {
        let oracle = self.oracle();
        let held = |k: &TrackingKey| self.held(&oracle, k, height) + self.keys[k].charge;
        let tracked: usize = self.keys.keys().map(held).sum();
        if tracked + load <= CONFIG.max_tracked_units {
            return true;
        }
        let evict_after = CONFIG.evict_after.as_millis() as u64;
        let mut idle: Vec<(u64, u64, TrackingKey, usize)> = self
            .keys
            .iter()
            .filter(|(k, m)| {
                m.subscribers == 0 && self.now - m.last_seen >= evict_after && *k != key
            })
            .map(|(k, m)| (m.last_seen, m.epoch, k.clone(), held(k)))
            .collect();
        let room: usize = idle.iter().map(|i| i.3).sum();
        if tracked - room + load > CONFIG.max_tracked_units {
            let refused =
                Output::CapacityExceeded { key: key.clone(), tracked, evictable: room, boxes };
            assert_eq!(outputs, [refused], "refused, and nothing else changed");
            self.seen.insert("load refused at the cap");
            if room > 0 {
                self.seen.insert("load refused though some keys were idle");
            }
            return false;
        }
        idle.sort();
        let mut left = tracked;
        for (_, _, evicted, units) in idle {
            if left + load <= CONFIG.max_tracked_units {
                self.seen.insert("idle key kept: the load fit without it");
                break;
            }
            self.keys.remove(&evicted);
            self.evicted += 1;
            left -= units;
            self.seen.insert("idle key evicted for a load");
        }
        true
    }

    /// Feeds an event, checks the order of its messages and delivers them.
    fn feed(&mut self, event: Event) -> Vec<Output> {
        let outputs = self
            .engine
            .apply(event)
            .expect("the harness follows the engine's tip");
        let rank = |o: &Output| match o {
            Output::Mempool { .. } => 1,
            Output::Balance { .. } => 2,
            _ => 0,
        };
        let key = |o: &Output| match o {
            Output::Block { key, .. }
            | Output::Rollback { key, .. }
            | Output::Mempool { key, .. }
            | Output::Balance { key, .. }
            | Output::ResyncRequired { key, .. }
            | Output::LoadRejected { key, .. }
            | Output::CapacityExceeded { key, .. } => key.clone(),
        };
        for pair in outputs.windows(2) {
            let (a, b) = ((rank(&pair[0]), key(&pair[0])), (rank(&pair[1]), key(&pair[1])));
            assert!(a < b, "messages out of order: {:?} then {:?}", pair[0], pair[1]);
        }
        let tip = self.engine.tip();
        for output in outputs
            .iter()
            .filter(|o| !matches!(o, Output::LoadRejected { .. } | Output::CapacityExceeded { .. }))
        {
            for client in self
                .clients
                .iter_mut()
                .filter(|c| c.live && c.key == key(output))
            {
                client.receive(output, tip, &mut self.seen);
            }
        }
        outputs
    }

    fn subscribe(&mut self, key: TrackingKey, balance_only: bool) {
        let with_boxes = |h: &Self| h.keys.get(&key).is_some_and(|m| !m.balance_only);
        match (&key, balance_only) {
            (TrackingKey::Tree(tree), true) if !self.keys.contains_key(&key) => {
                self.load_balance(tree.clone());
            }
            (_, true) if !self.keys.contains_key(&key) => self.load(&key),
            (_, false) if !with_boxes(self) => self.load(&key),
            _ => {}
        }
        let tracked = self.engine.is_tracked(&key);
        if !tracked || (!balance_only && !with_boxes(self)) {
            return;
        }
        let tip = self.tip();
        let mut client = Client {
            key: key.clone(),
            balance_only,
            live: true,
            resumable: false,
            epoch: self.keys[&key].epoch,
            last: None,
            known_at: tip,
            boxes: BTreeMap::new(),
            overlay: Vec::new(),
            balance: None,
        };
        if !balance_only {
            let ids = self.pages(&key, tip, 3).expect("the tip is always served");
            client.boxes = ids
                .iter()
                .map(|k| {
                    let utxo = self.chain.find_box(&k.box_id).expect("a built box");
                    (k.box_id, (k.inclusion_height, utxo))
                })
                .collect();
        }
        let outputs = self.feed(Event::Subscribed(key.clone()));
        assert!(outputs.is_empty());
        let model = self.keys.get_mut(&key).expect("tracked");
        model.subscribers += 1;
        model.last_seen = self.now;

        let from = (!balance_only).then_some(tip);
        let query = Query::CatchUp { key: key.clone(), from };
        let Reply::CatchUp(outputs) = self.engine.query(query) else {
            panic!("a catch-up reply");
        };
        // From the tip: nothing to replay, then the overlay (box subscriptions) and the
        // balance (addresses).
        let balance = usize::from(matches!(key, TrackingKey::Tree(_)));
        assert_eq!(outputs.len(), usize::from(!balance_only) + balance);
        for output in &outputs {
            client.receive(output, Some(tip), &mut self.seen);
        }
        if !balance_only {
            // The overlay names the key's last change, so the client knows it from here on.
            assert_eq!(client.last, Some(self.keys[&key].last_change()));
        }
        self.clients.push(client);
    }

    fn unsubscribe(&mut self, i: usize, keep: bool) {
        let key = self.clients[i].key.clone();
        self.clients[i].live = false;
        self.clients[i].resumable = keep;
        assert!(self.feed(Event::Unsubscribed(key.clone())).is_empty());
        let now = self.now;
        let model = self
            .keys
            .get_mut(&key)
            .expect("a key with a subscriber is tracked");
        model.subscribers -= 1;
        model.last_seen = now;
    }

    fn resume(&mut self, i: usize) {
        let key = self.clients[i].key.clone();
        self.clients[i].resumable = false;
        let outputs = self.feed(Event::Subscribed(key.clone()));
        if !self.keys.contains_key(&key) {
            assert_eq!(
                outputs,
                vec![Output::ResyncRequired { key, reason: ResyncReason::NotTracked }]
            );
            self.seen.insert("resync: not tracked");
            return;
        }
        assert!(outputs.is_empty());
        let tip = self.tip();
        let model = self.keys.get_mut(&key).expect("tracked");
        model.subscribers += 1;
        model.last_seen = self.now;

        let client = &self.clients[i];
        // A client with boxes resumes from its last change, which its first message named.
        let from = (!client.balance_only).then(|| client.last.expect("named at the subscribe"));
        let model = &self.keys[&key];
        // From the key's last change, there is nothing to replay, however old it is.
        let current = !model.balance_only && from == Some(model.last_change());
        // From the change before the key's oldest one the ring still holds, every change
        // since is in the ring, however old `from` is: a quiet key whose last change is
        // below the floor, changed while its client was away.
        let floor = self.floor();
        let chained_below = from.is_some_and(|from| {
            !model.balance_only
                && model
                    .changes
                    .windows(2)
                    .find(|pair| pair[1].height > floor)
                    .is_some_and(|pair| pair[0] == from)
        });
        // A key tracked again since the client left replays too, if the client's block is
        // on the main chain at or above the new load: the key's boxes there are the same
        // whichever load found them. Eviction can untrack and reload a key within a block.
        let reloaded_below = from.is_some_and(|from| {
            !model.balance_only
                && client.epoch != model.epoch
                && self.fed.contains(&from)
                && from.height >= model.since.height.max(self.floor())
        });
        let expect_replay = client.balance_only
            || current
            || chained_below
            || reloaded_below
            || from.is_some_and(|from| {
                client.epoch == model.epoch
                    && !model.balance_only
                    && self
                        .fork_point(from)
                        .is_some_and(|f| f.height >= self.floor())
            });
        if chained_below && from.is_some_and(|from| from.height < floor) {
            self.seen
                .insert("resume from below the floor replays the key's changes since");
        }
        if current && from.is_some_and(|from| from.height < self.floor()) {
            self.seen
                .insert("resume from a last change below the floor");
        }
        if reloaded_below && !client.balance_only {
            self.seen
                .insert("replay to a client across a reload at or below its block");
        }
        let Reply::CatchUp(outputs) = self.engine.query(Query::CatchUp { key: key.clone(), from })
        else {
            panic!("a catch-up reply");
        };
        if let [Output::ResyncRequired { reason, .. }] = outputs[..] {
            assert!(!expect_replay, "resync ({reason:?}) where a replay was possible");
            if model.balance_only {
                self.seen
                    .insert("resync: a client with boxes of a balance-only key");
            }
            self.seen.insert(match reason {
                ResyncReason::OutsideWindow => "resync: outside window",
                ResyncReason::ForkBeyondWindow => "resync: fork beyond window",
                ResyncReason::NotTracked => "resync: not tracked",
                ResyncReason::SlowConsumer => unreachable!("the engine has no consumers"),
                ResyncReason::BalanceMismatch => unreachable!("the oracle's balances are right"),
            });
            // The transport ends the subscription.
            assert!(self.feed(Event::Unsubscribed(key.clone())).is_empty());
            self.keys.get_mut(&key).expect("tracked").subscribers -= 1;
            return;
        }
        assert!(expect_replay, "a replay where the client's blocks are gone: {outputs:?}");
        if outputs.iter().any(|o| matches!(o, Output::Rollback { .. })) {
            self.seen.insert("catch-up rolls back an orphaned branch");
        }
        let client = &mut self.clients[i];
        client.live = true;
        if let Some(from) = from {
            // The replay names the key's last change again: after a reload it may chain from
            // a change below `from` rather than from `from` itself.
            client.last = None;
            client.known_at = from;
        }
        for output in &outputs {
            client.receive(output, Some(tip), &mut self.seen);
        }
        if !client.balance_only {
            let last = self.keys[&key].last_change();
            assert_eq!(client.last, Some(last), "the catch-up reaches the key's last change");
        }
    }

    /// Where `block`'s branch meets the engine's chain, if it does above the first fed block.
    fn fork_point(&self, block: BlockRef) -> Option<BlockRef> {
        let first = self.fed.first()?;
        let mut cursor = block;
        loop {
            if cursor.height < first.height {
                return None;
            }
            if self.fed.get((cursor.height - first.height) as usize) == Some(&cursor) {
                return Some(cursor);
            }
            let parent = self.chain.block(&cursor.header_id)?.header.parent_id;
            cursor = BlockRef { height: cursor.height - 1, header_id: parent };
        }
    }

    /// Pages through `key` at `at`, `limit` ids at a time.
    fn pages(
        &mut self,
        key: &TrackingKey,
        at: BlockRef,
        limit: usize,
    ) -> Result<Vec<BoxKey>, PageError> {
        let mut ids = Vec::new();
        loop {
            let after = ids.last().copied();
            let query = Query::Page { key: key.clone(), at: Some(at), after, limit };
            let Reply::Page(page) = self.engine.query(query) else {
                panic!("a page reply");
            };
            let page = page?;
            assert_eq!(page.as_of, at, "a page is as of the block asked for");
            let page = page.boxes;
            assert!(page.len() <= limit);
            let done = page.len() < limit;
            ids.extend(page);
            if done {
                return Ok(ids);
            }
        }
    }

    fn page(&mut self, key: &TrackingKey, at: BlockRef, limit: usize) {
        let result = self.pages(key, at, limit);
        let Some(model) = self.keys.get_mut(key).filter(|m| !m.balance_only) else {
            // A balance-only key's boxes are not tracked, and a read of them is no activity.
            assert_eq!(result, Err(PageError::NotTracked));
            return;
        };
        model.last_seen = self.now;
        let since = model.since;
        let on_main = self.fed.contains(&at);
        if !on_main || at.height < self.floor() || at.height < since.height {
            assert_eq!(result, Err(PageError::CursorExpired));
            self.seen.insert("page cursor expired");
            return;
        }
        let expected: Vec<BoxKey> = self.oracle().unspent(key, at.height).into_keys().collect();
        let ids = result.expect("a page on the main chain in the window");
        assert_eq!(ids, expected);
        if at != self.tip()
            && expected
                .iter()
                .any(|k| !self.engine.keys[&self.engine.ids[key]].boxes.contains(k))
        {
            self.seen
                .insert("page at an earlier block restores spent boxes");
        }
    }

    fn spendable(&mut self, key: &TrackingKey, limit: usize) {
        let query = Query::Spendable { key: key.clone(), limit };
        let Reply::Spendable(reply) = self.engine.query(query) else {
            panic!("a spendable reply");
        };
        let (tip, now) = (self.tip(), self.now);
        let Some(model) = self.keys.get_mut(key).filter(|m| !m.balance_only) else {
            assert_eq!(reply, None);
            return;
        };
        model.last_seen = now;
        let (mut confirmed, pending) = self.reference_spendable(key);
        let total = confirmed.len() + pending.len();
        let held = self.engine.keys[&self.engine.ids[key]].boxes.len();
        if confirmed.len() > limit {
            // Only confirmed boxes are cut, the first ones in keyset order kept.
            confirmed.truncate(limit);
            self.seen.insert("spendable view cut to the limit");
            if !pending.is_empty() {
                self.seen
                    .insert("spendable view cut to the limit lists every pending box");
            }
        } else if held > limit {
            self.seen
                .insert("spendable view fits the limit only after pending spends");
        }
        assert_eq!(reply, Some(SpendableIds { as_of: tip, confirmed, pending, total }));
    }

    /// `key`'s spendable view at the engine's tip: the oracle's unspent boxes the reference
    /// overlay leaves unspent, then the boxes it creates and leaves unspent, in its order.
    fn reference_spendable(&self, key: &TrackingKey) -> (Vec<BoxKey>, Vec<Arc<UTxO>>) {
        let oracle = self.oracle();
        let height = self.tip().height;
        let ids: HashSet<BoxId> = oracle.utxo(height).values().map(|b| b.id).collect();
        let overlay = self.reference_overlay(key, &ids);
        let spent: HashSet<BoxId> = overlay
            .iter()
            .flat_map(|p| p.spent.iter().copied())
            .collect();
        let confirmed = (oracle.unspent(key, height).into_keys())
            .filter(|k| !spent.contains(&k.box_id))
            .collect();
        let pending = (overlay.into_iter().flat_map(|p| p.created))
            .filter(|b| !spent.contains(&b.id))
            .collect();
        (confirmed, pending)
    }

    /// The overlay the engine should hold for `key`: the oracle's over the snapshot in use,
    /// against the confirmed state at the engine's tip.
    fn reference_overlay(&self, key: &TrackingKey, utxo: &HashSet<BoxId>) -> Vec<PendingTx> {
        match &self.held {
            Some(txs) => sim::overlay(utxo, txs, key),
            None => Vec::new(),
        }
    }

    /// `key`'s pending txs as a history lists them: the reference overlay newest first, each
    /// with what it pays to the address minus what it takes from it, from the snapshot's full
    /// txs and summed with the oracle's totals.
    fn reference_pending_txs(&self, key: &TrackingKey, utxo: &HashSet<BoxId>) -> Vec<TxDelta> {
        let mut overlay = self.reference_overlay(key, utxo);
        overlay.reverse();
        overlay
            .into_iter()
            .map(|p| {
                let tx = self
                    .held_tx(&p.tx_id)
                    .expect("an overlay tx is in the snapshot");
                let spent: Vec<&UTxO> = tx.inputs.iter().filter(|i| key.matches(i)).collect();
                let created = sim::totals(p.created.iter().map(|b| &**b));
                let spent = sim::totals(spent);
                let mut delta = PendingDelta {
                    nano_ergs: i128::from(created.nano_ergs) - i128::from(spent.nano_ergs),
                    ..PendingDelta::default()
                };
                for (id, a) in created.tokens {
                    *delta.tokens.entry(id).or_default() += i128::from(a);
                }
                for (id, a) in spent.tokens {
                    *delta.tokens.entry(id).or_default() -= i128::from(a);
                }
                delta.tokens.retain(|_, a| *a != 0);
                TxDelta { tx_id: p.tx_id, delta }
            })
            .collect()
    }

    fn held_tx(&self, id: &TxId) -> Option<&UnconfirmedTransaction> {
        self.held.as_deref()?.iter().find(|tx| tx.id == *id)
    }

    /// `key`'s balance values and overlay at the engine's tip.
    fn expected(&self, key: &TrackingKey) -> Expected {
        let oracle = self.oracle();
        let utxo = oracle.utxo(self.tip().height);
        let ids: HashSet<BoxId> = utxo.values().map(|b| b.id).collect();
        expected_for(self, key, &utxo, &ids)
    }

    fn expected_states(&self) -> BTreeMap<TrackingKey, Expected> {
        let Some(tip) = self.engine.tip() else {
            return BTreeMap::new();
        };
        let utxo = self.chain.oracle_at(&tip.header_id).utxo(tip.height);
        let ids: HashSet<BoxId> = utxo.values().map(|b| b.id).collect();
        self.keys
            .keys()
            .map(|k| (k.clone(), expected_for(self, k, &utxo, &ids)))
            .collect()
    }

    /// Each tracked key gets a `Mempool` message if and only if its overlay changed in the
    /// step, and an address a `Balance` if and only if its value did; a token key never gets
    /// one. `before` is from before the step.
    fn check_emissions(&mut self, outputs: &[Output], before: &BTreeMap<TrackingKey, Expected>) {
        let after = self.expected_states();
        for (key, model) in &mut self.keys {
            // Overlays and balances name the key's last change if it has boxes.
            let last_change = (!model.balance_only).then(|| model.last_change());
            for output in outputs.iter().filter(|o| o.key() == key) {
                if let Output::Mempool { last_change: named, .. }
                | Output::Balance { last_change: named, .. } = output
                {
                    assert_eq!(*named, last_change, "{key:?}'s last change");
                }
            }
            let (balance, overlay) = &after[key];
            let mempool = outputs.iter().find_map(|o| match o {
                Output::Mempool { key: k, pending, .. } if k == key => Some(pending),
                _ => None,
            });
            match mempool {
                Some(pending) => {
                    assert_eq!(pending, overlay);
                    assert_ne!(&model.overlay, overlay, "an unchanged overlay sent");
                }
                None => assert_eq!(&model.overlay, overlay, "a changed overlay not sent"),
            }
            model.overlay = overlay.clone();

            let sent = outputs.iter().find_map(|o| match o {
                Output::Balance { key: k, balance, .. } if k == key => Some(balance),
                _ => None,
            });
            match (sent, balance) {
                (Some(b), Some(balance)) => {
                    assert_eq!((&b.confirmed, &b.pending), (&balance.0, &balance.1));
                    assert_ne!(model.balance.as_ref(), Some(balance), "an unchanged balance sent");
                }
                (Some(_), None) => panic!("a balance sent for {key:?}"),
                (None, _) => assert_eq!(&model.balance, balance, "a changed balance not sent"),
            }
            if let Some((Some((was, was_pending)), _)) = before.get(key)
                && let Some(balance) = balance
                && was != &balance.0
                && sum(was, was_pending) == sum(&balance.0, &balance.1)
            {
                self.seen
                    .insert("balance sent though confirmed + pending stayed");
            }
            model.balance = balance.clone();
        }
    }

    fn check(&mut self) {
        let tracked: Vec<&TrackingKey> = self.engine.ids.keys().collect();
        assert_eq!(tracked, self.keys.keys().collect::<Vec<_>>(), "tracked keys");
        let Reply::Stats(stats) = self.engine.query(Query::Stats) else { unreachable!() };
        let Some(tip) = self.engine.tip() else {
            assert!(self.keys.is_empty());
            assert_eq!(stats, Stats::default());
            return;
        };
        let oracle = self.chain.oracle_at(&tip.header_id);
        let utxo = oracle.utxo(tip.height);

        // The sizes the metrics report are the model's: its keys by kind, the oracle's boxes
        // of each, and the ring's length.
        let addresses = self
            .keys
            .keys()
            .filter(|key| matches!(key, TrackingKey::Tree(_)))
            .count();
        let balance_keys = self.keys.values().filter(|m| m.balance_only).count();
        let boxes: usize = self
            .keys
            .keys()
            .map(|key| self.held(&oracle, key, tip.height))
            .sum();
        let ring_boxes: usize = self.engine.ring.iter().map(|d| d.boxes.len()).sum();
        let evictable_boxes: usize = self
            .keys
            .iter()
            .filter(|(_, m)| {
                m.subscribers == 0
                    && self.now - m.last_seen >= CONFIG.evict_after.as_millis() as u64
            })
            .map(|(key, _)| self.held(&oracle, key, tip.height))
            .sum();
        let expected = Stats {
            tip: Some(tip),
            address_keys: addresses - balance_keys,
            balance_keys,
            token_keys: (self.keys.keys())
                .filter(|k| matches!(k, TrackingKey::Token(_)))
                .count(),
            template_keys: (self.keys.keys())
                .filter(|k| matches!(k, TrackingKey::Template(_)))
                .count(),
            tracked_boxes: boxes,
            tracked_units: boxes + self.keys.values().map(|m| m.charge).sum::<usize>(),
            ring_blocks: self.ring_len as usize,
            ring_boxes,
            evictable_boxes,
            evicted_keys: self.evicted,
        };
        assert_eq!(stats, expected, "stats");
        let ids: HashSet<BoxId> = utxo.values().map(|b| b.id).collect();

        // Each tracked key's set and running totals equal the oracle's, and the reverse index
        // holds exactly their boxes.
        let mut reverse = BTreeMap::new();
        for (key, &key_id) in &self.engine.ids {
            let state = &self.engine.keys[&key_id];
            let balance_only = self.keys[key].balance_only;
            assert_eq!(state.balance_only, balance_only, "{key:?} balance-only");
            let expected: BTreeSet<BoxKey> = if balance_only {
                BTreeSet::new()
            } else {
                oracle.unspent(key, tip.height).into_keys().collect()
            };
            assert_eq!(state.boxes, expected, "{key:?}'s set");
            let totals =
                matches!(key, TrackingKey::Tree(_)).then(|| oracle.totals(key, tip.height));
            assert_eq!(state.totals, totals, "{key:?}'s totals");
            // A REST read of a tracked key's balance is the last one streamed. Read without
            // `Query::Balance`, which would count as activity.
            let streamed = self.keys[key].balance.as_ref();
            let read = self.engine.balance(key);
            assert_eq!(
                read.as_ref().map(|b| (&b.confirmed, &b.pending)),
                streamed.map(|(c, p)| (c, p))
            );
            for b in &state.boxes {
                reverse.insert((b.box_id, key_id), b.inclusion_height);
            }
            if matches!(key, TrackingKey::Template(_))
                && (state.boxes.iter())
                    .map(|b| &utxo[b].ergo_tree)
                    .collect::<HashSet<_>>()
                    .len()
                    >= 2
            {
                self.seen
                    .insert("template key holds the boxes of two trees");
            }
            if state.boxes.iter().any(|b| {
                let held = &utxo[b];
                overlay::keys_of(held, true)
                    .filter(|k| self.engine.ids.contains_key(k))
                    .count()
                    >= 2
            }) {
                self.seen.insert("box held by two tracked keys");
            }
        }
        assert_eq!(self.engine.reverse, reverse, "reverse index");

        // The idle index holds exactly the model's keys with no subscribers, at their last
        // activity, with the oracle's box counts.
        let idle: BTreeMap<(u64, KeyId), usize> = self
            .keys
            .iter()
            .filter(|(_, m)| m.subscribers == 0)
            .map(|(key, m)| {
                let boxes = self.held(&oracle, key, tip.height);
                ((m.last_seen, self.engine.ids[key]), boxes)
            })
            .collect();
        assert_eq!(self.engine.idle, idle, "idle index");

        // Nothing is kept for a key no longer tracked: the mempool sends overlays for the
        // tracked keys alone, and each ring or orphaned block holds shares of tracked keys
        // only, with just the boxes those shares name.
        let sent: Vec<&TrackingKey> = self.engine.mempool.tracked().collect();
        assert_eq!(sent, self.keys.keys().collect::<Vec<_>>(), "keys the mempool sends for");
        // `emit` compares only these and the keys a snapshot touches, so a key missing here
        // would never get the message that empties its overlay.
        let with_overlay: Vec<&TrackingKey> = self
            .keys
            .iter()
            .filter(|(_, m)| !m.overlay.is_empty())
            .map(|(key, _)| key)
            .collect();
        assert_eq!(
            self.engine.mempool.with_overlay().collect::<Vec<_>>(),
            with_overlay,
            "keys with an overlay"
        );
        // The balance-only keys are found by their trees, and only they.
        let trees: BTreeMap<&ErgoTree, KeyId> = self
            .engine
            .balance_trees
            .iter()
            .map(|(t, id)| (t, *id))
            .collect();
        let expected_trees: BTreeMap<&ErgoTree, KeyId> = (self.keys.iter())
            .filter(|(_, m)| m.balance_only)
            .map(|(k, _)| match k {
                TrackingKey::Tree(t) => (t, self.engine.ids[k]),
                TrackingKey::Token(_) | TrackingKey::Template(_) => {
                    panic!("a balance-only {k:?}")
                }
            })
            .collect();
        assert_eq!(trees, expected_trees, "balance-only trees");
        // The engine's last change of each key with boxes is the model's, and only those keys
        // have one.
        let last_changes: BTreeMap<&TrackingKey, BlockRef> = self
            .engine
            .last_changes
            .iter()
            .map(|(id, block)| (&self.engine.keys[id].key, *block))
            .collect();
        let expected: BTreeMap<&TrackingKey, BlockRef> = (self.keys.iter())
            .filter(|(_, m)| !m.balance_only)
            .map(|(k, m)| (k, m.last_change()))
            .collect();
        assert_eq!(last_changes, expected, "the keys' last changes");
        for delta in self.engine.ring.iter().chain(self.engine.orphans.values()) {
            for key_id in delta.loaded.iter() {
                assert!(self.engine.keys.contains_key(key_id), "a load of an untracked key");
            }
            for key_id in delta.keys.keys().chain(delta.balances.keys()) {
                assert!(self.engine.keys.contains_key(key_id), "a share of an untracked key");
            }
            let named: HashSet<BoxId> = delta
                .keys
                .values()
                .flat_map(|d| {
                    d.created
                        .iter()
                        .copied()
                        .chain(d.spent.iter().map(|s| s.box_id))
                })
                .collect();
            let held: HashSet<BoxId> = delta.boxes.keys().copied().collect();
            assert_eq!(held, named, "the boxes of block {:?}", delta.block);
        }

        // The room read for any load size gives the model's answer, and the whole idle room
        // when the load can't fit. Counting stops once it would, so the room is then at
        // least what the load needs.
        let untracked = TrackingKey::Token(Digest([0xff; 32]));
        let max = CONFIG.max_tracked_units;
        for key in self.keys.keys() {
            let query = Query::Capacity { key: key.clone(), up_to: 0 };
            let Reply::Capacity(capacity) = self.engine.query(query) else { unreachable!() };
            assert_eq!(capacity, None, "room for {key:?}, tracked");
        }
        // The cap counts every key's charge besides its boxes.
        let units = boxes + self.keys.values().map(|m| m.charge).sum::<usize>();
        let evictable_charges: usize = (self.keys.values())
            .filter(|m| {
                m.subscribers == 0
                    && self.now - m.last_seen >= CONFIG.evict_after.as_millis() as u64
            })
            .map(|m| m.charge)
            .sum();
        let evictable_units = evictable_boxes + evictable_charges;
        let charge = key_charge(&untracked, 0);
        for up_to in 0..=2 * max {
            let query = Query::Capacity { key: untracked.clone(), up_to };
            let Reply::Capacity(Some(capacity)) = self.engine.query(query) else {
                panic!("room for an untracked key");
            };
            assert_eq!((capacity.tracked, capacity.max, capacity.charge), (units, max, charge));
            let fits = units - evictable_units + up_to + charge <= max;
            assert_eq!(capacity.fits(up_to), fits, "room for {up_to}");
            if fits {
                assert!(capacity.evictable <= evictable_units);
                assert!(units - capacity.evictable + up_to + charge <= max);
            } else {
                assert_eq!(capacity.evictable, evictable_units, "the whole room, for {up_to}");
            }
        }

        // Every key's overlay, tracked or not, is the oracle's over the snapshot in use.
        for key in self.chain.keys() {
            let reference = self.reference_overlay(&key, &ids);
            assert_eq!(self.engine.mempool.overlay(&key), reference, "{key:?}'s overlay");
            if matches!(key, TrackingKey::Template(_))
                && self.engine.ids.contains_key(&key)
                && !reference.is_empty()
            {
                self.seen.insert("tracked template key with an overlay");
            }
        }

        // Every address's pending txs, tracked or not, are the reference overlay's, newest
        // first, each with its own change; with no conflict, they add up to its pending delta.
        for key in self.chain.keys() {
            let TrackingKey::Tree(tree) = &key else { continue };
            let reference = self.reference_pending_txs(&key, &ids);
            let txs = self.engine.mempool.pending_txs(tree);
            assert_eq!(txs, reference, "{key:?}'s pending txs");
            let spends: Vec<BoxId> = self
                .reference_overlay(&key, &ids)
                .iter()
                .flat_map(|p| p.spent.iter().copied())
                .collect();
            let conflict = spends.len() > spends.iter().collect::<HashSet<_>>().len();
            let mut summed = PendingDelta::default();
            for tx in &txs {
                summed.nano_ergs += tx.delta.nano_ergs;
                for (id, amount) in &tx.delta.tokens {
                    *summed.tokens.entry(*id).or_default() += amount;
                }
            }
            summed.tokens.retain(|_, a| *a != 0);
            if conflict {
                self.seen.insert("pending history of a conflict");
            } else {
                assert_eq!(summed, self.engine.mempool.pending_delta(&key), "{key:?}'s sum");
            }
            if txs.len() >= 2 {
                let created: HashSet<TxId> = self
                    .reference_overlay(&key, &ids)
                    .iter()
                    .flat_map(|p| p.created.iter().map(|b| b.transaction_id))
                    .collect();
                // A child listed before a parent whose output it spends.
                let child_first = txs.iter().enumerate().any(|(i, child)| {
                    self.held_tx(&child.tx_id).is_some_and(|tx| {
                        tx.inputs.iter().any(|input| {
                            created.contains(&input.transaction_id)
                                && txs[i + 1..].iter().any(|t| t.tx_id == input.transaction_id)
                        })
                    })
                });
                if child_first {
                    self.seen
                        .insert("pending history lists a child above its parent");
                }
            }
        }

        // Every tracked key's spendable view is the reference one, and an address's sums to its
        // balance: the overlay applied to the boxes, or to the totals, gives the same assets.
        // Read without `Query::Spendable`, which would count as activity.
        for key in self
            .keys
            .iter()
            .filter(|(_, m)| m.balance_only)
            .map(|(k, _)| k)
        {
            assert_eq!(self.engine.spendable(key, usize::MAX), None, "{key:?} has no view");
        }
        for key in &self.box_keys() {
            let view = self
                .engine
                .spendable(key, usize::MAX)
                .expect("a tracked key has a view");
            let (confirmed, pending) = self.reference_spendable(key);
            let total = confirmed.len() + pending.len();
            let expected = SpendableIds { as_of: tip, confirmed, pending, total };
            assert_eq!(view, expected, "{key:?}'s view");
            let held = &self.engine.keys[&self.engine.ids[key]].boxes;
            if view.confirmed.len() < held.len() {
                self.seen.insert("spendable view drops a pending-spent box");
            }
            if !view.pending.is_empty() {
                self.seen.insert("spendable view lists a pending box");
            }
            let overlay = self.engine.mempool.overlay(key);
            let created = overlay.iter().map(|p| p.created.len()).sum::<usize>();
            if created > view.pending.len() {
                self.seen
                    .insert("spendable view hides a box created and spent in the pool");
            }
            if let Some(balance) = self.engine.balance(key) {
                let boxes = view
                    .confirmed
                    .iter()
                    .map(|k| &*utxo[k])
                    .chain(view.pending.iter().map(|b| &**b));
                assert_eq!(
                    sum(&sim::totals(boxes), &PendingDelta::default()),
                    sum(&balance.confirmed, &balance.pending),
                    "{key:?}'s view does not sum to its balance"
                );
            }
        }

        // What the snapshot says of a box is the reference order's: spent if a tx spends it,
        // else created if one creates it. Asked of every box the snapshot names, every
        // confirmed one and an unknown one.
        let order = match &self.held {
            Some(txs) => sim::pending_order(&ids, txs),
            None => Vec::new(),
        };
        let named: BTreeSet<BoxId> = (self.held.iter().flat_map(|txs| txs.iter()))
            .flat_map(|tx| tx.inputs.iter().chain(&tx.outputs).map(|b| b.id))
            .chain(ids.iter().copied())
            .chain([Digest([0xee; 32])])
            .collect();
        for id in named {
            let Reply::PoolBox(answer) = self.engine.query(Query::PoolBox(id)) else {
                panic!("a pool box reply");
            };
            let spent = order.iter().any(|tx| tx.inputs.iter().any(|b| b.id == id));
            let created = order.iter().flat_map(|tx| &tx.outputs).find(|b| b.id == id);
            let expected = match (spent, created) {
                (true, _) => Some(PoolBox::Spent),
                (false, Some(b)) => Some(PoolBox::Created(Arc::new(b.clone()))),
                (false, None) => None,
            };
            assert_eq!(answer, expected, "pool box {id}");
            if spent && created.is_some() {
                self.seen.insert("pool box created and spent in the pool");
            } else if spent {
                self.seen.insert("pool box spends a confirmed box");
            } else if created.is_some() {
                self.seen.insert("pool box created");
            }
        }

        // Every live client holds the oracle's state at the tip.
        for client in self.clients.iter().filter(|c| c.live) {
            let (balance, overlay) = expected_for(self, &client.key, &utxo, &ids);
            if !client.balance_only {
                let model = &self.keys[&client.key];
                assert_eq!(client.last, Some(model.last_change()), "the key's last change");
                let expected: BTreeMap<BoxId, (u32, Arc<UTxO>)> = oracle
                    .unspent(&client.key, tip.height)
                    .into_iter()
                    .map(|(k, b)| (k.box_id, (k.inclusion_height, b)))
                    .collect();
                assert_eq!(client.boxes, expected, "a client of {:?} diverged", client.key);
                assert_eq!(client.overlay, overlay);
            }
            let got = client.balance.as_ref().map(|b| (&b.confirmed, &b.pending));
            let expected = balance.as_ref().map(|(c, p)| (c, p));
            assert_eq!(
                got, expected,
                "an address's balance came with the catch-up, a token's never"
            );
        }
        self.check_partial_confirmation(&ids);
    }

    fn check_partial_confirmation(&mut self, utxo: &HashSet<BoxId>) {
        let Some(txs) = &self.held else {
            return;
        };
        // A pending tx in the overlay spending an output of a snapshot tx a block mined.
        let order = sim::pending_order(utxo, txs);
        let listed: HashSet<TxId> = order.iter().map(|tx| tx.id).collect();
        let mined_outputs: HashSet<BoxId> = txs
            .iter()
            .filter(|tx| {
                !listed.contains(&tx.id) && tx.outputs.iter().any(|o| utxo.contains(&o.id))
            })
            .flat_map(|tx| tx.outputs.iter().map(|o| o.id))
            .collect();
        if order
            .iter()
            .any(|tx| tx.inputs.iter().any(|i| mined_outputs.contains(&i.id)))
        {
            self.seen.insert("partial confirmation");
        }
    }
}

fn sum(
    totals: &Totals,
    pending: &PendingDelta,
) -> (i128, BTreeMap<crate::types::ergo::TokenId, i128>) {
    let mut tokens: BTreeMap<_, i128> = totals
        .tokens
        .iter()
        .map(|(id, &a)| (*id, i128::from(a)))
        .collect();
    for (id, a) in &pending.tokens {
        *tokens.entry(*id).or_default() += a;
    }
    tokens.retain(|_, a| *a != 0);
    (i128::from(totals.nano_ergs) + pending.nano_ergs, tokens)
}

/// A key's expected balance values (none for a token key) and overlay.
type Expected = (Option<(Totals, PendingDelta)>, Vec<PendingTx>);

fn expected_for(
    harness: &Harness,
    key: &TrackingKey,
    utxo: &BTreeMap<BoxKey, Arc<UTxO>>,
    ids: &HashSet<BoxId>,
) -> Expected {
    let held: Vec<&UTxO> = utxo
        .values()
        .map(|b| &**b)
        .filter(|b| key.matches(b))
        .collect();
    let overlay = harness.reference_overlay(key, ids);
    let totals = sim::totals(held.iter().copied());
    let delta = reference_delta(&held, &overlay);
    let balance = matches!(key, TrackingKey::Tree(_)).then_some((totals, delta));
    (balance, overlay)
}

/// Boxes a block both creates and spends.
fn in_block_spends(block: &IndexedBlock) -> HashSet<BoxId> {
    let created: HashSet<BoxId> = block
        .transactions
        .iter()
        .flat_map(|tx| tx.outputs.iter().map(|o| o.utxo.id))
        .collect();
    block
        .transactions
        .iter()
        .flat_map(|tx| tx.inputs.iter().map(|i| i.utxo.id))
        .filter(|id| created.contains(id))
        .collect()
}

fn in_block_matches(boxes: &HashSet<BoxId>, chain: &Chain, key: &TrackingKey) -> bool {
    boxes
        .iter()
        .any(|id| chain.find_box(id).is_some_and(|b| key.matches(&b)))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// After every op of a random history, the engine's state and every client's equal the
    /// oracle's, and the engine sends exactly the messages the step called for.
    #[test]
    fn the_engine_and_its_clients_follow_the_oracle(ops in prop::collection::vec(op(), 0..=60)) {
        let mut harness = Harness::new();
        for op in &ops {
            harness.run(op);
        }
    }
}

/// The property is only useful if random histories reach the cases that break engines.
#[test]
fn random_histories_reach_the_hard_engine_cases() {
    use proptest::{strategy::ValueTree, test_runner::TestRunner};
    let mut runner = TestRunner::deterministic();
    let mut seen = BTreeSet::new();
    for _ in 0..500 {
        let ops = prop::collection::vec(op(), 100)
            .new_tree(&mut runner)
            .expect("strategy")
            .current();
        let mut harness = Harness::new();
        for op in &ops {
            harness.run(op);
        }
        seen.extend(harness.seen);
    }
    for feature in [
        "tracked box created and spent within a block",
        "snapshot held for a block used",
        "idle key untracked",
        "idle key kept past grace: the block's budget was spent",
        "idle key over the block's budget untracked alone",
        "idle keys untracked together in one block",
        "idle key evicted for a load",
        "idle key kept: the load fit without it",
        "load refused at the cap",
        "load refused though some keys were idle",
        "ring ran out",
        "key dropped with its load block",
        "load rejected",
        "balance load rejected",
        "balance load of a tracked key ignored",
        "balance load refused at the cap",
        "block moves a balance-only key's totals",
        "rollback undoes a balance-only key's change",
        "balance-only key upgraded to boxes",
        "balance-only key with subscribers upgraded to boxes",
        "resync: a client with boxes of a balance-only key",
        "resync: not tracked",
        "resync: outside window",
        "resync: fork beyond window",
        "catch-up rolls back an orphaned branch",
        "page cursor expired",
        "page at an earlier block restores spent boxes",
        "box held by two tracked keys",
        "template key holds the boxes of two trees",
        "tracked template key with an overlay",
        "key kept by a subscriber",
        "partial confirmation",
        "balance sent though confirmed + pending stayed",
        "reorg-returned tx in a snapshot after the rollback",
        "spendable view drops a pending-spent box",
        "spendable view lists a pending box",
        "spendable view hides a box created and spent in the pool",
        "spendable view cut to the limit",
        "spendable view cut to the limit lists every pending box",
        "spendable view fits the limit only after pending spends",
        "pending history of a conflict",
        "pending history lists a child above its parent",
        "pool box created and spent in the pool",
        "pool box spends a confirmed box",
        "pool box created",
        "block leaves a key with boxes unchanged",
        "block changes a key with no overlay or balance after it",
        "change chained across unchanged blocks",
        "rollback of a change below an orphan that left the key alone",
        "resume from a last change below the floor",
        "resume from below the floor replays the key's changes since",
        "client learns a last change below where its boxes are known",
    ] {
        assert!(seen.contains(feature), "no random history reached: {feature}");
    }
}

/// An engine following a `Chain` by hand.
struct Fixture {
    chain: Chain,
    engine: Engine,
    /// The engine's clock: [`Fixture::mine`] moves it a block's time.
    now: Duration,
}

impl Fixture {
    fn new(config: Config) -> Self {
        let chain = sim::testing::new_chain();
        let mut engine = Engine::new(config);
        let genesis = chain.oracle().indexed_block(chain.height());
        engine
            .apply(Event::BlockApplied(genesis))
            .expect("the first block");
        Self { chain, engine, now: Duration::ZERO }
    }

    fn key(&self, tree: usize) -> TrackingKey {
        TrackingKey::Tree(self.chain.trees()[tree].clone())
    }

    /// Mines a block of `txs`, a block's time after the last, and applies it.
    fn mine(&mut self, txs: Vec<Arc<sim::SimTx>>) -> Vec<Output> {
        self.now += sim::testing::BLOCK_TIME;
        self.engine.set_now(self.now);
        let block = self.chain.mine_txs(txs);
        let indexed = self.chain.oracle().indexed_block(block.height);
        self.engine
            .apply(Event::BlockApplied(indexed))
            .expect("extends the tip")
    }

    /// Orphans the tip on both sides.
    fn revert(&mut self) -> Vec<Output> {
        self.chain.orphan(1);
        let tip = self.engine.tip().expect("a tip");
        self.engine
            .apply(Event::BlockReverted(tip))
            .expect("the tip")
    }

    fn load(&mut self, key: &TrackingKey) -> Vec<Output> {
        let as_of = self.chain.tip();
        let boxes = self
            .chain
            .oracle()
            .load(key, as_of.height)
            .into_iter()
            .map(crate::engine::LoadedBox::from)
            .collect();
        self.engine
            .apply(Event::KeyLoaded { key: key.clone(), as_of, boxes })
            .expect("no error")
    }

    fn snapshot(&mut self) -> Vec<Output> {
        let txs = self.chain.mempool().into();
        let observed_tip = self.chain.tip();
        self.engine
            .apply(Event::MempoolSnapshot { txs, observed_tip })
            .expect("no error")
    }

    fn catch_up(&mut self, key: &TrackingKey, from: Option<BlockRef>) -> Vec<Output> {
        let Reply::CatchUp(outputs) = self.engine.query(Query::CatchUp { key: key.clone(), from })
        else {
            panic!("a catch-up reply");
        };
        outputs
    }

    fn state(&self, key: &TrackingKey) -> (BTreeSet<BoxKey>, Option<Totals>) {
        let state = &self.engine.keys[&self.engine.ids[key]];
        (state.boxes.clone(), state.totals.clone())
    }
}

/// The message of kind `kind` for `key`.
fn find<'a>(outputs: &'a [Output], kind: &str, key: &TrackingKey) -> &'a Output {
    let at = kinds(outputs)
        .iter()
        .position(|(k, o)| *k == kind && o == key)
        .unwrap_or_else(|| panic!("no {kind} for {key:?} in {:?}", kinds(outputs)));
    &outputs[at]
}

fn kinds(outputs: &[Output]) -> Vec<(&'static str, TrackingKey)> {
    outputs
        .iter()
        .map(|o| match o {
            Output::Block { key, .. } => ("block", key.clone()),
            Output::Rollback { key, .. } => ("rollback", key.clone()),
            Output::Mempool { key, .. } => ("mempool", key.clone()),
            Output::Balance { key, .. } => ("balance", key.clone()),
            Output::ResyncRequired { key, .. } => ("resync", key.clone()),
            Output::LoadRejected { key, .. } => ("load_rejected", key.clone()),
            Output::CapacityExceeded { key, .. } => ("capacity_exceeded", key.clone()),
        })
        .collect()
}

#[test]
fn a_block_moves_a_box_between_keys_and_its_rollback_moves_it_back() {
    let mut f = Fixture::new(Config::default());
    let (zero, one) = (f.key(0), f.key(1));
    f.load(&zero);
    f.load(&one);
    let loaded = f.engine.tip().expect("a tip");
    let before = (f.state(&zero), f.state(&one), f.engine.reverse.clone());

    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    let output = tx.outputs[0].clone();
    let outputs = f.mine(vec![tx.clone()]);
    let block = f.engine.tip().expect("a tip");
    assert_eq!(outputs.len(), 4, "two blocks then two balances");
    assert_eq!(
        find(&outputs, "block", &zero),
        &Output::Block {
            key: zero.clone(),
            block,
            prev: loaded,
            created: vec![],
            spent: vec![Spend { box_id: source, tx_id: tx.id }],
        }
    );
    assert_eq!(
        find(&outputs, "block", &one),
        &Output::Block {
            key: one.clone(),
            block,
            prev: loaded,
            created: vec![output.clone()],
            spent: vec![]
        }
    );

    // The rollback is the delta with its sides swapped, and restores the state exactly.
    let outputs = f.revert();
    let spent_box = StoredBox {
        inclusion_height: before
            .0
            .0
            .iter()
            .find(|k| k.box_id == source)
            .expect("held")
            .inclusion_height,
        utxo: f.chain.find_box(&source).expect("built"),
    };
    assert_eq!(
        find(&outputs, "rollback", &zero),
        &Output::Rollback {
            key: zero.clone(),
            block,
            prev: loaded,
            created: vec![spent_box],
            spent: vec![],
        }
    );
    assert_eq!(
        find(&outputs, "rollback", &one),
        &Output::Rollback {
            key: one.clone(),
            block,
            prev: loaded,
            created: vec![],
            spent: vec![Spend { box_id: output.id, tx_id: tx.id }],
        }
    );
    assert_eq!((f.state(&zero), f.state(&one), f.engine.reverse.clone()), before);
}

impl Fixture {
    /// Tracks `key` balance-only from `confirmed`, at the tip.
    fn load_balance(&mut self, key: &TrackingKey, confirmed: Totals) -> Vec<Output> {
        let TrackingKey::Tree(tree) = key else { panic!("an address") };
        let as_of = self.chain.tip();
        self.engine
            .apply(Event::BalanceLoaded { tree: tree.clone(), as_of, confirmed })
            .expect("no error")
    }

    /// `key`'s balance at the tip, by the oracle.
    fn totals(&self, key: &TrackingKey) -> Totals {
        self.chain.oracle().totals(key, self.chain.height())
    }
}

/// A balance-only key follows a block's change and its rollback with no boxes at all: no
/// `Block` or `Rollback` for it, nothing in the reverse index or kept in the ring.
#[test]
fn a_balance_only_key_follows_blocks_and_rollbacks_without_boxes() {
    let mut f = Fixture::new(Config::default());
    let (zero, one) = (f.key(0), f.key(1));
    let (was_zero, was_one) = (f.totals(&zero), f.totals(&one));
    assert!(f.load_balance(&zero, was_zero.clone()).is_empty());
    assert!(f.load_balance(&one, was_one.clone()).is_empty());

    // Both balances, in key order.
    let mut balances = [("balance", zero.clone()), ("balance", one.clone())];
    balances.sort();
    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    let outputs = f.mine(vec![tx]);
    assert_eq!(kinds(&outputs), balances);
    assert_eq!(f.state(&zero), (BTreeSet::new(), Some(f.totals(&zero))));
    assert_eq!(f.state(&one), (BTreeSet::new(), Some(f.totals(&one))));
    assert_ne!(f.totals(&zero), was_zero);
    assert!(f.engine.reverse.is_empty());
    let tip = f.engine.ring.back().expect("the block");
    assert!(tip.boxes.is_empty() && tip.keys.is_empty() && tip.balances.len() == 2);

    let outputs = f.revert();
    assert_eq!(kinds(&outputs), balances);
    assert_eq!(f.state(&zero), (BTreeSet::new(), Some(was_zero)));
    assert_eq!(f.state(&one), (BTreeSet::new(), Some(was_one)));
}

/// A balance-only key has no deltas, so a client with boxes that asks to catch up from a
/// block is sent to its snapshot, even from the block the key was loaded at, where a key
/// with boxes would replay nothing.
#[test]
fn a_catch_up_from_a_block_resyncs_a_balance_only_key() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    let tip = f.engine.tip().expect("a tip");
    f.load_balance(&zero, f.totals(&zero));
    assert_eq!(
        f.catch_up(&zero, Some(tip)),
        [Output::ResyncRequired { key: zero.clone(), reason: ResyncReason::NotTracked }]
    );
    assert!(matches!(&f.catch_up(&zero, None)[..], [Output::Balance { .. }]));
    let page = Query::Page { key: zero.clone(), at: None, after: None, limit: 10 };
    assert_eq!(f.engine.query(page), Reply::Page(Err(PageError::NotTracked)));
    let spendable = Query::Spendable { key: zero.clone(), limit: 10 };
    assert_eq!(f.engine.query(spendable), Reply::Spendable(None));
}

/// A balance-only key that started from a wrong balance resyncs when a block spends more
/// than it holds, and is untracked. A block walks only the keys with boxes, so the resyncs
/// are merged among their blocks in key order, the first key's and the last's included.
#[test]
fn a_wrong_starting_balance_resyncs_at_an_overspend_in_key_order() {
    let mut f = Fixture::new(Config::default());
    // The first and the last address in key order start from nothing and are overspent; the
    // ones between are tracked with their boxes, and the one paid gets its block between the
    // resyncs.
    let mut trees: Vec<usize> = (0..f.chain.trees().len()).collect();
    trees.sort_by_key(|&t| f.key(t));
    let (first, last) = (trees[0], *trees.last().expect("trees"));
    let middle = &trees[1..trees.len() - 1];
    assert!(middle.len() >= 2);
    let (low, high) = (f.key(first), f.key(last));
    f.load_balance(&low, Totals::default());
    f.load_balance(&high, Totals::default());
    for &t in middle {
        let key = f.key(t);
        f.load(&key);
    }
    let spend = |f: &mut Fixture, t: usize| {
        let source = sim::testing::box_of(&f.chain, t);
        let to = f.chain.trees()[middle[0]].clone();
        f.chain.transfer(&[source], &to)
    };
    let txs = vec![spend(&mut f, first), spend(&mut f, last)];
    let outputs = f.mine(txs);
    let mut expected = vec![("block", f.key(middle[0]))];
    expected.push(("resync", low.clone()));
    expected.push(("resync", high.clone()));
    expected.sort_by(|a, b| a.1.cmp(&b.1));
    let rank0: Vec<_> = kinds(&outputs)
        .into_iter()
        .filter(|(k, _)| matches!(*k, "block" | "resync"))
        .collect();
    assert_eq!(rank0, expected, "in key order");
    let reason = ResyncReason::BalanceMismatch;
    for key in [&low, &high] {
        assert_eq!(
            find(&outputs, "resync", key),
            &Output::ResyncRequired { key: key.clone(), reason }
        );
        assert!(!f.engine.is_tracked(key));
    }
    assert!(f.engine.ring.iter().all(|d| d.balances.is_empty()));
}

/// A balance-only key that started a little high is never overspent, so it keeps the error;
/// a load of its boxes corrects it, and sends the balance its boxes give.
#[test]
fn a_load_of_its_boxes_corrects_a_balance_only_key() {
    let mut f = Fixture::new(Config::default());
    let one = f.key(1);
    let high = Totals { nano_ergs: f.totals(&one).nano_ergs + 1, ..f.totals(&one) };
    f.load_balance(&one, high.clone());
    assert_eq!(f.state(&one).1, Some(high));

    let outputs = f.load(&one);
    let Output::Balance { balance, .. } = find(&outputs, "balance", &one) else { unreachable!() };
    assert_eq!(balance.confirmed, f.totals(&one));
    assert_eq!(f.state(&one).1, Some(f.totals(&one)));
    assert!(!f.engine.keys[&f.engine.ids[&one]].balance_only);
}

/// The sizes the metrics report, on a small case: the ring's boxes are the full boxes its
/// blocks hold, each once however many keys it touched, and none for a block no key is in.
#[test]
fn stats_count_keys_by_kind_boxes_and_the_ring() {
    let mut f = Fixture::new(Config::default());
    let (zero, one) = (f.key(0), f.key(1));
    let stats = |f: &mut Fixture| match f.engine.query(Query::Stats) {
        Reply::Stats(stats) => stats,
        other => panic!("{other:?}"),
    };
    let tip = f.engine.tip();
    assert_eq!(stats(&mut f), Stats { tip, ring_blocks: 1, ..Stats::default() });

    f.load(&zero);
    f.load(&one);
    let boxes = f.engine.reverse.len();
    assert!(boxes > 0);
    let units = cost(&f, &zero).1 + cost(&f, &one).1;
    let loaded = Stats {
        tip,
        address_keys: 2,
        tracked_boxes: boxes,
        tracked_units: units,
        ring_blocks: 1,
        ..Stats::default()
    };
    assert_eq!(stats(&mut f), loaded, "a load adds nothing to the ring");

    // One box moves from key 0 to key 1: the spent box and the created one, each stored once.
    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    f.mine(vec![tx]);
    // Neither key has been active since: a load may evict either.
    let moved = Stats {
        tip: f.engine.tip(),
        ring_blocks: 2,
        ring_boxes: 2,
        evictable_boxes: boxes,
        ..loaded
    };
    assert_eq!(stats(&mut f), moved);
}

#[test]
fn a_box_created_and_spent_in_one_block_is_in_no_delta() {
    let mut f = Fixture::new(Config::default());
    let (zero, one) = (f.key(0), f.key(1));
    f.load(&zero);
    f.load(&one);
    let source = sim::testing::box_of(&f.chain, 0);
    let first = f.chain.transfer(&[source], &f.chain.trees()[0].clone());
    let second = f
        .chain
        .transfer(&[first.outputs[0].id], &f.chain.trees()[1].clone());
    let outputs = f.mine(vec![first.clone(), second.clone()]);
    let Output::Block { created, spent, .. } = find(&outputs, "block", &zero) else {
        panic!("a block")
    };
    assert_eq!((created, spent), (&vec![], &vec![Spend { box_id: source, tx_id: first.id }]));
    let Output::Block { created, spent, .. } = find(&outputs, "block", &one) else {
        panic!("a block")
    };
    assert_eq!((created, spent), (&vec![second.outputs[0].clone()], &vec![]));
    let delta = f.engine.ring.back().expect("the tip's entry");
    assert!(!delta.boxes.contains_key(&first.outputs[0].id), "not stored");
}

/// The parent is mined, the child stays pending: after the block, the child spends the box
/// the block delivered, and the balance moves the parent's output from pending to confirmed.
#[test]
fn a_mined_parent_leaves_its_pending_child_in_the_overlay() {
    let mut f = Fixture::new(Config::default());
    let one = f.key(1);
    f.load(&one);
    let source = sim::testing::box_of(&f.chain, 0);
    let to_one = f.chain.trees()[1].clone();
    let parent = f.chain.transfer(&[source], &to_one);
    let child = f.chain.transfer(&[parent.outputs[0].id], &to_one);
    f.chain.submit(parent.clone());
    f.chain.submit(child.clone());
    let outputs = f.snapshot();
    assert_eq!(kinds(&outputs), [("mempool", one.clone()), ("balance", one.clone())]);
    let Output::Balance { balance: before, .. } = &outputs[1] else { panic!("a balance") };

    let outputs = f.mine(vec![parent.clone()]);
    assert_eq!(
        kinds(&outputs),
        [("block", one.clone()), ("mempool", one.clone()), ("balance", one.clone())]
    );
    let Output::Mempool { pending, .. } = &outputs[1] else { panic!("a mempool message") };
    let x = parent.outputs[0].id;
    assert_eq!(pending.len(), 1);
    assert_eq!((pending[0].tx_id, &pending[0].spent), (child.id, &vec![x]));
    let Output::Balance { balance: after, .. } = &outputs[2] else { panic!("a balance") };
    assert_eq!(after.confirmed.nano_ergs, before.confirmed.nano_ergs + parent.outputs[0].value);
    // The child keeps the value with the key, so the spendable total stays.
    assert_eq!(
        i128::from(after.confirmed.nano_ergs) + after.pending.nano_ergs,
        i128::from(before.confirmed.nano_ergs) + before.pending.nano_ergs
    );
}

/// The node's state can run ahead of its pool, so a snapshot can list a tx spending a box a
/// newly tracked key does not hold. Loading the key leaves that tx out, and every key whose
/// overlay changes is told.
#[test]
fn loading_a_key_decides_the_snapshot_again() {
    let mut f = Fixture::new(Config::default());
    let (zero, one) = (f.key(0), f.key(1));
    f.load(&one);
    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    f.chain.submit(tx);
    f.snapshot();
    assert_eq!(f.engine.mempool.overlay(&one).len(), 1);

    let as_of = f.chain.tip();
    let boxes = f
        .chain
        .oracle()
        .load(&zero, as_of.height)
        .into_iter()
        .filter(|b| b.utxo.id != source)
        .map(crate::engine::LoadedBox::from)
        .collect();
    let outputs = f
        .engine
        .apply(Event::KeyLoaded { key: zero, as_of, boxes })
        .expect("no error");
    assert_eq!(kinds(&outputs), [("mempool", one.clone()), ("balance", one.clone())]);
    assert!(f.engine.mempool.overlay(&one).is_empty());
}

#[test]
fn events_off_the_tip_are_refused_and_change_nothing() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    f.load(&zero);
    let genesis = f.engine.tip().expect("a tip");
    f.chain.mine(1, 1, 7);
    let next = f.chain.oracle().indexed_block(genesis.height + 1);
    f.chain.mine(1, 1, 8);
    let skipping = f.chain.oracle().indexed_block(genesis.height + 2);
    let state = f.state(&zero);

    let error = f
        .engine
        .apply(Event::BlockApplied(skipping))
        .expect_err("skips a block");
    assert!(matches!(error, EngineError::NotChild { .. }));
    let not_tip = BlockRef::from(&next.header);
    let error = f
        .engine
        .apply(Event::BlockReverted(not_tip))
        .expect_err("not the tip");
    assert!(matches!(error, EngineError::NotTip { .. }));
    assert_eq!((f.engine.tip(), f.state(&zero)), (Some(genesis), state));

    // A load pinned elsewhere is rejected.
    let one = f.key(1);
    let outputs = f.load(&one);
    let as_of = f.chain.tip();
    assert_eq!(outputs, [Output::LoadRejected { key: one.clone(), as_of }]);
    assert!(!f.engine.is_tracked(&one));
}

#[test]
fn an_untracked_key_is_refused_and_a_balance_only_catch_up_is_the_balance() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    let resync =
        vec![Output::ResyncRequired { key: zero.clone(), reason: ResyncReason::NotTracked }];
    assert_eq!(
        f.engine
            .apply(Event::Subscribed(zero.clone()))
            .expect("no error"),
        resync
    );
    let tip = f.engine.tip();
    assert_eq!(f.catch_up(&zero, tip), resync);
    assert_eq!(f.catch_up(&zero, None), resync);
    let Reply::Balance(None) = f.engine.query(Query::Balance(zero.clone())) else {
        panic!("no balance for an untracked key");
    };

    f.load(&zero);
    let loaded = f.engine.tip();
    let outputs = f.catch_up(&zero, None);
    assert_eq!(kinds(&outputs), [("balance", zero.clone())]);
    let Reply::Balance(Some(balance)) = f.engine.query(Query::Balance(zero.clone())) else {
        panic!("a balance");
    };
    assert_eq!(outputs, [Output::Balance { key: zero, balance, last_change: loaded }]);
}

/// A token key gets its blocks and overlays but no balance: not on a snapshot, a block, a
/// catch-up or a read. The address holding the token gets one each time.
#[test]
fn a_token_key_has_no_balance() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    let input = sim::testing::box_of(&f.chain, 0);
    let to = f.chain.trees()[0].clone();
    let mint = f.chain.mint(&input, 5, &to);
    f.mine(vec![mint]);
    let token = TrackingKey::Token(input);
    f.load(&zero);
    f.load(&token);
    assert_eq!(f.engine.keys[&f.engine.ids[&token]].totals, None, "no totals for a token");
    assert_eq!(f.catch_up(&token, None), []);
    let Reply::Balance(None) = f.engine.query(Query::Balance(token.clone())) else {
        panic!("no balance for a token key");
    };

    // A pending tx moving the token out of the address.
    let held = f
        .chain
        .utxo()
        .values()
        .find(|b| token.matches(b))
        .expect("minted")
        .id;
    let to = f.chain.trees()[1].clone();
    let tx = f.chain.transfer(&[held], &to);
    f.chain.submit(tx.clone());
    let outputs = f.snapshot();
    assert_eq!(
        kinds(&outputs),
        [("mempool", zero.clone()), ("mempool", token.clone()), ("balance", zero.clone())]
    );
    // A pending delta is still answered for any key, with the tip it was computed at. The
    // token only moves between holders, so the token key's nets out.
    let Reply::Pending(Some(pending)) = f.engine.query(Query::Pending(token.clone())) else {
        panic!("a pending delta");
    };
    assert_eq!(pending.as_of, f.engine.tip().expect("a tip"));
    assert_eq!(pending.delta, PendingDelta::default());

    let outputs = f.mine(vec![tx]);
    let kinds = kinds(&outputs);
    assert!(kinds.contains(&("balance", zero.clone())));
    assert!(
        !kinds
            .iter()
            .any(|(kind, key)| *kind == "balance" && *key != zero),
        "{kinds:?}"
    );
}

/// A client left on a branch that was orphaned after the key stopped being tracked: the
/// orphaned block's delta for the key was never kept, so the engine cannot roll it back,
/// even though the key is tracked again from the fork point.
#[test]
fn a_client_from_an_earlier_tracking_period_resyncs() {
    let mut f = Fixture::new(Config { window: 10, grace: Duration::ZERO, ..Config::DEFAULT });
    let zero = f.key(0);
    f.load(&zero);
    f.engine
        .apply(Event::Subscribed(zero.clone()))
        .expect("no error");
    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    f.mine(vec![tx]);
    let client_at = f.engine.tip().expect("a tip");
    f.engine
        .apply(Event::Unsubscribed(zero.clone()))
        .expect("no error");
    f.mine(vec![]);
    assert!(!f.engine.is_tracked(&zero), "idle past the grace period");

    f.revert();
    f.revert();
    f.load(&zero);
    let outputs = f.catch_up(&zero, Some(client_at));
    assert_eq!(outputs, [Output::ResyncRequired { key: zero, reason: ResyncReason::NotTracked }]);
}

/// A key untracked and loaded again at the client's last change replays to it: its boxes
/// there are the same whichever load found them. Random histories rarely reach it, since a
/// client's last change is usually older than the load.
#[test]
fn a_client_resumes_across_a_reload_at_its_last_change() {
    let mut f = Fixture::new(Config { grace: Duration::ZERO, ..Config::DEFAULT });
    let zero = f.key(0);
    f.load(&zero);
    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    f.mine(vec![tx]);
    let last = f.engine.tip().expect("a tip");
    f.mine(vec![]);
    assert!(!f.engine.is_tracked(&zero), "idle past the grace period");
    f.revert();
    f.load(&zero);
    assert_eq!(f.engine.tracked_since(&zero), Some(last));
    f.engine
        .apply(Event::Subscribed(zero.clone()))
        .expect("tracked");

    let paid = f
        .chain
        .transfer(&[sim::testing::box_of(&f.chain, 1)], &f.chain.trees()[0].clone());
    f.mine(vec![paid]);
    let outputs = f.catch_up(&zero, Some(last));
    let Output::Block { prev, .. } = find(&outputs, "block", &zero) else { unreachable!() };
    assert_eq!(*prev, last, "{outputs:?}");
}

/// Pages pinned to an earlier block keep their ids while blocks land between page reads.
#[test]
fn pages_pinned_to_a_block_survive_later_blocks() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    f.load(&zero);
    let at = f.engine.tip().expect("a tip");
    let expected: Vec<BoxKey> = f
        .chain
        .oracle()
        .unspent(&zero, at.height)
        .into_keys()
        .collect();
    assert!(expected.len() >= 2);

    let page = |f: &mut Fixture, after| {
        let query = Query::Page { key: zero.clone(), at: Some(at), after, limit: 1 };
        let Reply::Page(Ok(ids)) = f.engine.query(query) else { panic!("a page") };
        ids.boxes
    };
    let first = page(&mut f, None);
    // Spend the box the next page would return, and add one to the key.
    let next = expected[1].box_id;
    let tx = f.chain.transfer(&[next], &f.chain.trees()[0].clone());
    f.mine(vec![tx]);
    let second = page(&mut f, first.last().copied());
    assert_eq!([first, second].concat(), expected[..2]);
}

/// A key dropped with its load block and loaded again at the parent: the orphaned block was
/// the last applied before the new load, so its `seq` equals the key's `loaded_seq`, and it
/// still predates the load.
#[test]
fn a_client_at_an_orphaned_load_block_resyncs() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    f.mine(vec![tx]);
    f.load(&zero);
    let client_at = f.engine.tip().expect("a tip");

    let outputs = f.revert();
    let dropped =
        Output::ResyncRequired { key: zero.clone(), reason: ResyncReason::ForkBeyondWindow };
    assert_eq!(outputs, [dropped]);
    f.load(&zero);
    let outputs = f.catch_up(&zero, Some(client_at));
    assert_eq!(outputs, [Output::ResyncRequired { key: zero, reason: ResyncReason::NotTracked }]);
}

/// A snapshot can list a tx spending a box a block already spent, if the node's pool lags its
/// state. For a box a tracked key held, the engine checks its own set, so the tx is in no
/// overlay, whether the snapshot arrives at the tip or was held for the block.
#[test]
fn a_pending_spend_of_a_box_a_block_spent_is_left_out() {
    for held in [false, true] {
        let mut f = Fixture::new(Config::default());
        let (zero, one) = (f.key(0), f.key(1));
        f.load(&zero);
        f.load(&one);
        let source = sim::testing::box_of(&f.chain, 0);
        let to_one = f.chain.trees()[1].clone();
        let mined = f.chain.transfer(&[source], &to_one);
        let stale = f.chain.transfer(&[source], &to_one);
        let txs: Arc<[UnconfirmedTransaction]> = vec![stale.to_unconfirmed()].into();

        let block = f.chain.mine_txs(vec![mined]);
        let snapshot = Event::MempoolSnapshot { txs, observed_tip: block };
        let indexed = Event::BlockApplied(f.chain.oracle().indexed_block(block.height));
        let outputs = if held {
            assert!(f.engine.apply(snapshot).expect("no error").is_empty(), "held");
            f.engine.apply(indexed).expect("extends the tip")
        } else {
            f.engine.apply(indexed).expect("extends the tip");
            f.engine.apply(snapshot).expect("no error")
        };
        assert!(
            !outputs.iter().any(|o| matches!(o, Output::Mempool { .. })),
            "held: {held}, {:?}",
            kinds(&outputs)
        );
        assert!(f.engine.mempool.overlay(&zero).is_empty());
        assert!(f.engine.mempool.overlay(&one).is_empty());
    }
}

/// As for an address, a pending spend of a box a tracked template key held, which a block
/// already spent, is in no overlay: the box is found to be a template key's by its template,
/// with neither of its trees tracked.
#[test]
fn a_pending_spend_of_a_template_box_a_block_spent_is_left_out() {
    let mut f = Fixture::new(Config::default());
    let template = TrackingKey::Template(sim::script_template());
    f.load(&template);
    let scripts = f.chain.trees().len() - 2;
    let source = sim::testing::box_of(&f.chain, scripts);
    let to = f.chain.trees()[scripts + 1].clone();
    let mined = f.chain.transfer(&[source], &to);
    let stale = f.chain.transfer(&[source], &to);
    let txs: Arc<[UnconfirmedTransaction]> = vec![stale.to_unconfirmed()].into();

    let block = f.chain.mine_txs(vec![mined]);
    f.engine
        .apply(Event::BlockApplied(f.chain.oracle().indexed_block(block.height)))
        .expect("extends the tip");
    let outputs = f
        .engine
        .apply(Event::MempoolSnapshot { txs, observed_tip: block })
        .expect("no error");
    assert!(!outputs.iter().any(|o| matches!(o, Output::Mempool { .. })), "{:?}", kinds(&outputs));
    assert!(f.engine.mempool.overlay(&template).is_empty());
}

#[test]
fn the_chain_query_lists_the_ring_oldest_first() {
    let mut f =
        Fixture::new(Config { window: 2, grace: sim::testing::blocks(5), ..Config::DEFAULT });
    let genesis = f.engine.tip().expect("a tip");
    let chain = |f: &mut Fixture| match f.engine.query(Query::Chain) {
        Reply::Chain(links) => links,
        other => panic!("expected a chain, got {other:?}"),
    };
    assert_eq!(chain(&mut f).iter().map(|l| l.block).collect::<Vec<_>>(), [genesis]);

    f.mine(vec![]);
    let second = f.engine.tip().expect("a tip");
    let links = chain(&mut f);
    assert_eq!(links.iter().map(|l| l.block).collect::<Vec<_>>(), [genesis, second]);
    assert_eq!(links[1].parent(), genesis);

    // The window drops the oldest block; a revert drops the tip.
    f.mine(vec![]);
    let third = f.engine.tip().expect("a tip");
    assert_eq!(chain(&mut f).iter().map(|l| l.block).collect::<Vec<_>>(), [second, third]);
    f.revert();
    assert_eq!(chain(&mut f).iter().map(|l| l.block).collect::<Vec<_>>(), [second]);
}

#[test]
fn the_boxes_query_answers_from_the_ring_only() {
    let mut f =
        Fixture::new(Config { window: 2, grace: sim::testing::blocks(5), ..Config::DEFAULT });
    let (zero, one) = (f.key(0), f.key(1));
    f.load(&zero);
    f.load(&one);
    let source = sim::testing::box_of(&f.chain, 0);
    let untracked = sim::testing::box_of(&f.chain, 3);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    let created = tx.outputs[0].clone();
    f.mine(vec![tx]);

    let boxes = |f: &mut Fixture, ids: Vec<BoxId>| match f.engine.query(Query::Boxes(ids)) {
        Reply::Boxes(boxes) => boxes,
        other => panic!("expected boxes, got {other:?}"),
    };
    let spent = f.chain.find_box(&source).expect("built");
    // Both sides of a tracked key's delta, each once; an untracked box is not held.
    assert_eq!(
        boxes(&mut f, vec![created.id, source, untracked, created.id]),
        vec![created.clone(), spent]
    );

    // An older entry still answers; once the block leaves the window, its boxes are gone.
    f.mine(vec![]);
    assert_eq!(boxes(&mut f, vec![created.id]), vec![created.clone()]);
    f.mine(vec![]);
    assert!(boxes(&mut f, vec![created.id, source]).is_empty());
}

/// What loading `key` at the chain's tip counts against the cap: its boxes, and its charge
/// from the tokens it holds there. And the charge the room a loader reads counts for it, with
/// no token, since the loader reads the room before the pages.
fn cost(probe: &Fixture, key: &TrackingKey) -> (usize, usize, usize) {
    let height = probe.chain.height();
    let boxes = probe.chain.oracle().load(key, height).len();
    // A token or a template key has no totals: only an address is charged for its tokens.
    let tokens = match key {
        TrackingKey::Tree(_) => probe.chain.oracle().totals(key, height).tokens.len(),
        TrackingKey::Token(_) | TrackingKey::Template(_) => 0,
    };
    (boxes, boxes + key_charge(key, tokens), key_charge(key, 0))
}

/// The cap on tracked boxes: a load that reaches it exactly is taken, one unit more is
/// refused and changes nothing. A duplicate load is ignored even at the cap, and a load off
/// the tip is rejected before the cap is looked at.
#[test]
fn a_load_past_the_cap_on_tracked_boxes_is_refused() {
    let probe = Fixture::new(Config::default());
    let [(b0, c0, _), (b1, c1, _), (b2, _, r2)] = [0, 1, 2].map(|t| cost(&probe, &probe.key(t)));
    assert!(b0 > 0 && b1 > 0 && b2 > 0);
    let cap = c0 + c1;

    let mut f = Fixture::new(Config { max_tracked_units: cap - 1, ..Config::default() });
    let [zero, one] = [0, 1].map(|t| f.key(t));
    f.load(&zero);
    let refused =
        Output::CapacityExceeded { key: one.clone(), tracked: c0, evictable: 0, boxes: b1 };
    assert_eq!(f.load(&one), [refused]);
    assert!(!f.engine.is_tracked(&one));
    assert_eq!(f.engine.reverse.len(), b0, "nothing changed");

    let mut f = Fixture::new(Config { max_tracked_units: cap, ..Config::default() });
    let [zero, one, two] = [0, 1, 2].map(|t| f.key(t));
    f.load(&zero);
    let outputs = f.load(&one);
    assert!(f.engine.is_tracked(&one), "exactly at the cap: {outputs:?}");
    assert_eq!(f.load(&zero), [], "a duplicate is ignored");
    // The room a loader reads before paging: none for a tracked key, which is never refused.
    let full = Capacity { tracked: cap, evictable: 0, max: cap, charge: r2 };
    let room = |key: &TrackingKey| Query::Capacity { key: key.clone(), up_to: b2 };
    assert_eq!(f.engine.query(room(&two)), Reply::Capacity(Some(full)));
    assert_eq!(f.engine.query(room(&zero)), Reply::Capacity(None));
    assert!(!full.fits(0), "even a key with no boxes takes room");
    let left = Capacity { tracked: cap - r2, ..full };
    assert!(left.fits(0) && !left.fits(1));
    let refused =
        Output::CapacityExceeded { key: two.clone(), tracked: cap, evictable: 0, boxes: b2 };
    assert_eq!(f.load(&two), [refused]);

    f.chain.mine(1, 1, 3);
    let as_of = f.chain.tip();
    assert_eq!(f.load(&two), [Output::LoadRejected { key: two, as_of }]);
}

/// Past the cap, a load evicts idle keys, least recently active first, and only as many as
/// it needs. A key with a subscriber or idle for less than `evict_after` is never evicted
/// (each block here is two minutes, past the default minute), and a load
/// that all the idle keys together can't make room for is refused with none evicted. The
/// room a loader reads before paging agrees with the load each time.
#[test]
fn a_load_past_the_cap_evicts_idle_keys_least_recently_active_first() {
    let probe = Fixture::new(Config::default());
    // Key 0 the cheapest key, and `big` the costliest: too large for the room key 3 leaves
    // and key 0's together, even counted with no token.
    let mut keys = probe.chain.keys();
    keys.sort_by_key(|k| cost(&probe, k).1);
    let zero = keys[0].clone();
    let big = keys.last().expect("keys").clone();
    let [one, two, three] = [1, 2, 3].map(|t| probe.key(t));
    let [(_, c0, _), (_, c1, _), (_, c2, _), (b3, c3, r3)] =
        [&zero, &one, &two, &three].map(|k| cost(&probe, k));
    assert!(c3 <= c1, "key 3 fits in key 1's room");
    let (b, _, r) = cost(&probe, &big);
    assert!(b + r > c1 - c3 + c0, "big fits in neither");
    let (big_boxes, _, big_charge) = cost(&probe, &big);
    let cap = c0 + c1 + c2;

    let mut f = Fixture::new(Config { max_tracked_units: cap, ..Config::default() });
    let room = |f: &mut Fixture, key: &TrackingKey| {
        let up_to = cost(&probe, key).0;
        match f.engine.query(Query::Capacity { key: key.clone(), up_to }) {
            Reply::Capacity(Some(capacity)) => capacity,
            other => panic!("{other:?}"),
        }
    };
    let tracked = |f: &Fixture| [&zero, &one, &two, &three].map(|k| f.engine.is_tracked(k));
    let evicted = |f: &mut Fixture| match f.engine.query(Query::Stats) {
        Reply::Stats(stats) => stats.evicted_keys,
        other => panic!("{other:?}"),
    };
    for key in [&zero, &one, &two] {
        f.load(key);
    }
    // Just loaded: none has been idle for `evict_after` yet.
    let full = Capacity { tracked: cap, evictable: 0, max: cap, charge: r3 };
    assert_eq!(room(&mut f, &three), full);
    let refused =
        Output::CapacityExceeded { key: three.clone(), tracked: cap, evictable: 0, boxes: b3 };
    assert_eq!(f.load(&three), [refused]);

    // Key 0 read a block later, key 2 subscribed two blocks later: only key 1, idle since its
    // load, and key 0 are evictable, key 1 first. The room is counted only until the load
    // fits, and key 1 alone makes it.
    f.mine(vec![]);
    let _ = f.engine.query(Query::Balance(zero.clone()));
    f.mine(vec![]);
    f.engine
        .apply(Event::Subscribed(two.clone()))
        .expect("tracked");
    let capacity = room(&mut f, &three);
    assert_eq!(capacity, Capacity { evictable: c1, ..full });
    assert!(capacity.fits(b3));
    let outputs = f.load(&three);
    assert!(f.engine.is_tracked(&three), "{outputs:?}");
    assert_eq!(tracked(&f), [true, false, true, true], "key 1 alone made room");
    assert_eq!(evicted(&mut f), 1);

    // Key 0 is the only idle key left, and too small for `big`: refused, key 0 kept.
    let capacity = room(&mut f, &big);
    assert_eq!(capacity.evictable, c0, "key 3 just loaded, key 2 subscribed");
    assert_eq!(capacity.charge, big_charge);
    assert!(!capacity.fits(big_boxes));
    let held = f.engine.units();
    let refused = Output::CapacityExceeded {
        key: big.clone(),
        tracked: held,
        evictable: c0,
        boxes: big_boxes,
    };
    assert_eq!(f.load(&big), [refused]);
    assert_eq!(tracked(&f), [true, false, true, true], "nothing evicted");
    assert_eq!(f.engine.units(), held);
    assert_eq!(evicted(&mut f), 1);
}

/// The room a loader reads counts idle keys only when a load of the size it asks about could
/// pass the cap, and only until that load would fit: past either, their room changes no
/// answer. A load that can't fit gets the whole room, for its refusal.
#[test]
fn the_room_counts_idle_keys_only_when_a_load_could_need_them() {
    let probe = Fixture::new(Config::default());
    let [zero, one, two] = [0, 1, 2].map(|t| probe.key(t));
    let [(_, c0, _), (_, c1, _), (_, c2, r2)] = [&zero, &one, &two].map(|k| cost(&probe, k));
    let cap = c0 + c1 + c2;

    let mut f = Fixture::new(Config { max_tracked_units: cap, ..Config::default() });
    f.load(&zero);
    f.load(&one);
    f.mine(vec![]);
    let room = |f: &mut Fixture, up_to| match f
        .engine
        .query(Query::Capacity { key: two.clone(), up_to })
    {
        Reply::Capacity(Some(capacity)) => capacity,
        other => panic!("{other:?}"),
    };
    let tracked = c0 + c1;
    // The most boxes key 2, counted with no token, fits without evicting.
    let fits = cap - tracked - r2;
    let at = Capacity { tracked, evictable: 0, max: cap, charge: r2 };
    assert_eq!(room(&mut f, fits), at, "fits as it is");
    let near = Capacity { evictable: c0, ..at };
    assert_eq!(room(&mut f, fits + 1), near, "key 0, loaded first, makes the one box of room");
    let past = Capacity { evictable: c0 + c1, ..at };
    assert_eq!(room(&mut f, cap + 1), past, "both keys, and still no room");
}

/// The engine's clock never goes back: an earlier time leaves it where it was, so a key read
/// late is not seen as idle for longer than it was.
#[test]
fn the_clock_never_goes_back() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    f.load(&zero);
    let grace = Config::default().grace;
    f.engine.set_now(grace);
    f.engine.set_now(Duration::ZERO);
    let _ = f.engine.query(Query::Balance(zero.clone()));
    // The next block comes half a grace period after the read.
    f.now = grace + grace / 2 - sim::testing::BLOCK_TIME;
    f.mine(vec![]);
    assert!(f.engine.is_tracked(&zero), "read at the grace period, not at zero");
}

#[test]
fn a_page_without_a_block_is_at_the_tip() {
    let mut f = Fixture::new(Config::default());
    let zero = f.key(0);
    let query = |at| Query::Page { key: zero.clone(), at, after: None, limit: usize::MAX };
    let Reply::Page(Err(PageError::NotTracked)) = f.engine.query(query(None)) else {
        panic!("an untracked key has no page");
    };
    f.load(&zero);
    let tx = f
        .chain
        .transfer(&[sim::testing::box_of(&f.chain, 0)], &f.chain.trees()[1].clone());
    f.mine(vec![tx]);
    let tip = f.engine.tip().expect("a tip");
    let Reply::Page(Ok(page)) = f.engine.query(query(None)) else { panic!("a page") };
    assert_eq!(page.as_of, tip);
    let Reply::Page(Ok(pinned)) = f.engine.query(query(Some(tip))) else { panic!("a page") };
    assert_eq!(page, pinned);
    let expected: Vec<BoxKey> = f
        .chain
        .oracle()
        .unspent(&zero, tip.height)
        .into_keys()
        .collect();
    assert_eq!(page.boxes, expected);
}
