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
        ergo::{TxId, UnconfirmedTransaction},
    },
};

/// Small enough that random histories reach the window, idle and cap edges: reorgs go up to
/// 5 blocks deep, `Advance` moves the clock up to [`MAX_ADVANCE_SECS`] at a time, and the
/// simulator's keys hold about twice the cap.
const CONFIG: Config = Config {
    window: 4,
    grace: Duration::from_secs(3),
    evict_after: Duration::from_secs(2),
    max_tracked_boxes: 12,
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
    /// A new client: a snapshot through pages (or none, for a balance-only one), a
    /// subscription, and its catch-up.
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
    WithPool {
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
        4 => (any::<Index>(), any::<bool>())
            .prop_map(|(key, balance_only)| Op::Subscribe { key, balance_only }),
        1 => any::<Index>().prop_map(Op::Unsubscribe),
        2 => any::<Index>().prop_map(Op::Disconnect),
        2 => any::<Index>().prop_map(Op::Resume),
        2 => (any::<Index>(), any::<Index>(), 1..5u8)
            .prop_map(|(key, at, limit)| Op::Page { key, at, limit }),
        2 => (any::<Index>(), 0..6u8).prop_map(|(key, limit)| Op::WithPool { key, limit }),
        4 => (0..=MAX_ADVANCE_SECS).prop_map(Op::Advance),
    ]
}

/// What the test expects of a tracked key, kept apart from the engine.
#[derive(Debug, Clone)]
struct KeyModel {
    /// Counts loads, so a client can tell whether the key stayed tracked since its snapshot.
    epoch: u64,
    since: BlockRef,
    /// The clock at the key's last activity, in ms.
    last_seen: u64,
    subscribers: u32,
    /// The last balance sent (or delivered with a catch-up), and the last overlay. A token
    /// key has no balance.
    balance: Option<(Totals, PendingDelta)>,
    overlay: Vec<PendingTx>,
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
    tip: BlockRef,
    /// The boxes held, with the heights that included them.
    boxes: BTreeMap<BoxId, (u32, Arc<UTxO>)>,
    overlay: Vec<PendingTx>,
    balance: Option<Balance>,
}

impl Client {
    /// Applies one message, checking what a client SDK checks.
    fn receive(&mut self, output: &Output, engine_tip: Option<BlockRef>) {
        match output {
            Output::Block { block, parent_id, created, spent, .. } if !self.balance_only => {
                assert_eq!(*parent_id, self.tip.header_id, "block {block:?} skips a block");
                assert_eq!(block.height, self.tip.height + 1);
                for s in spent {
                    assert!(self.boxes.remove(&s.box_id).is_some(), "spends unknown {}", s.box_id);
                }
                for b in created {
                    let held = self.boxes.insert(b.id, (block.height, b.clone()));
                    assert!(held.is_none(), "creates held {}", b.id);
                }
                self.tip = *block;
            }
            Output::Rollback { block, parent_id, created, spent, .. } if !self.balance_only => {
                assert_eq!(*block, self.tip, "rolls back a block the client is not at");
                for s in spent {
                    assert!(self.boxes.remove(&s.box_id).is_some(), "undoes unknown {}", s.box_id);
                }
                for b in created {
                    let id = b.utxo.id;
                    let held = self.boxes.insert(id, (b.inclusion_height, b.utxo.clone()));
                    assert!(held.is_none(), "restores held {id}");
                }
                self.tip = BlockRef { height: block.height - 1, header_id: *parent_id };
            }
            Output::Mempool { tip_height, pending, .. } if !self.balance_only => {
                assert_eq!(*tip_height, self.tip.height);
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
            Output::Balance { key, balance } => {
                assert!(matches!(key, TrackingKey::Tree(_)), "a balance sent for {key:?}");
                let engine_tip = engine_tip.expect("a balance goes out with a tip");
                let tip = if self.balance_only { engine_tip } else { self.tip };
                assert_eq!(balance.as_of, tip, "a balance for another block");
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
            Op::WithPool { key, limit } => {
                let key = self.pick_key(key);
                self.with_pool(&key, usize::from(*limit));
            }
            Op::Advance(secs) => {
                self.now += u64::from(*secs) * 1000;
                self.engine.set_now(Duration::from_millis(self.now));
            }
        }
        self.check();
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

    // --- Adapters ---

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
        let in_block = in_block_spends(&indexed);
        let outputs = self.feed(Event::BlockApplied(indexed));
        self.fed.push(block);
        self.ring_len = (self.ring_len + 1).min(CONFIG.window);

        // Every tracked key gets the block, in key order, and nothing else at rank 0.
        let blocks: Vec<&TrackingKey> = outputs
            .iter()
            .filter_map(|o| match o {
                Output::Block { key, block: b, .. } => {
                    assert_eq!(*b, block);
                    Some(key)
                }
                Output::Rollback { .. } | Output::ResyncRequired { .. } => {
                    panic!("an applied block sends {o:?}")
                }
                _ => None,
            })
            .collect();
        assert_eq!(blocks, before.keys().collect::<Vec<_>>());
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

        // Idle keys go; keys with subscribers or recent activity stay.
        let grace = CONFIG.grace.as_millis() as u64;
        let now = self.now;
        let idle: Vec<TrackingKey> = self
            .keys
            .iter()
            .filter(|(_, m)| m.subscribers == 0 && now - m.last_seen > grace)
            .map(|(k, _)| k.clone())
            .collect();
        for key in idle {
            self.keys.remove(&key);
            self.seen.insert("idle key untracked");
        }
        if self.keys.values().any(|m| now - m.last_seen > grace) {
            self.seen.insert("key kept by a subscriber");
        }
        self.check_emissions(&outputs, &before);
    }

    fn revert_block(&mut self, block: BlockRef) {
        let before = self.expected_states();
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

        let rollbacks: Vec<&TrackingKey> = outputs
            .iter()
            .filter_map(|o| match o {
                Output::Rollback { key, block: b, .. } => {
                    assert_eq!(*b, block);
                    Some(key)
                }
                _ => None,
            })
            .collect();
        assert_eq!(rollbacks, self.keys.keys().collect::<Vec<_>>());
        self.check_emissions(&outputs, &before);
    }

    fn snapshot(&mut self) {
        let txs: Arc<[UnconfirmedTransaction]> = self.chain.mempool().into();
        let observed = self.chain.tip();
        let before = self.expected_states();
        let outputs =
            self.feed(Event::MempoolSnapshot { txs: txs.clone(), observed_tip: observed });
        let tip = self.tip();
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
        let before = self.expected_states();
        let tracked = self.keys.contains_key(key);
        let outputs = self.feed(Event::KeyLoaded { key: key.clone(), as_of, boxes });
        if self.engine.tip() != Some(as_of) {
            assert_eq!(outputs, vec![Output::LoadRejected { key: key.clone(), as_of }]);
            self.seen.insert("load rejected");
            return;
        }
        if tracked {
            self.keys.get_mut(key).expect("tracked").last_seen = self.now;
        } else {
            if !self.make_room(key, count, as_of.height, &outputs) {
                self.check_emissions(&outputs, &before);
                return;
            }
            let (balance, overlay) = self.expected(key);
            self.next_epoch += 1;
            let model = KeyModel {
                epoch: self.next_epoch,
                since: as_of,
                last_seen: self.now,
                subscribers: 0,
                balance,
                overlay,
            };
            self.keys.insert(key.clone(), model);
        }
        self.check_emissions(&outputs, &before);
    }

    /// The cap on tracked boxes, for a load of `count` boxes at `height`: past it, the idle
    /// keys (no subscribers, idle for `evict_after`) go, least recently active and then
    /// oldest load first, until the load fits. If even all of them would not make room, the
    /// load is refused and none goes. Returns whether the load is taken.
    fn make_room(
        &mut self,
        key: &TrackingKey,
        count: usize,
        height: u32,
        outputs: &[Output],
    ) -> bool {
        let oracle = self.oracle();
        let held = |k: &TrackingKey| oracle.unspent(k, height).len();
        let tracked: usize = self.keys.keys().map(held).sum();
        if tracked + count <= CONFIG.max_tracked_boxes {
            return true;
        }
        let evict_after = CONFIG.evict_after.as_millis() as u64;
        let mut idle: Vec<(u64, u64, TrackingKey, usize)> = self
            .keys
            .iter()
            .filter(|(_, m)| m.subscribers == 0 && self.now - m.last_seen >= evict_after)
            .map(|(k, m)| (m.last_seen, m.epoch, k.clone(), held(k)))
            .collect();
        let room: usize = idle.iter().map(|i| i.3).sum();
        if tracked - room + count > CONFIG.max_tracked_boxes {
            let refused = Output::CapacityExceeded {
                key: key.clone(),
                tracked,
                evictable: room,
                boxes: count,
            };
            assert_eq!(outputs, [refused], "refused, and nothing else changed");
            self.seen.insert("load refused at the cap");
            if room > 0 {
                self.seen.insert("load refused though some keys were idle");
            }
            return false;
        }
        idle.sort();
        let mut left = tracked;
        for (_, _, evicted, boxes) in idle {
            if left + count <= CONFIG.max_tracked_boxes {
                self.seen.insert("idle key kept: the load fit without it");
                break;
            }
            self.keys.remove(&evicted);
            self.evicted += 1;
            left -= boxes;
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
            .filter(|o| !matches!(o, Output::LoadRejected { .. }))
        {
            for client in self
                .clients
                .iter_mut()
                .filter(|c| c.live && c.key == key(output))
            {
                client.receive(output, tip);
            }
        }
        outputs
    }

    // --- The transport ---

    fn subscribe(&mut self, key: TrackingKey, balance_only: bool) {
        if !self.engine.is_tracked(&key) {
            self.load(&key);
            if !self.engine.is_tracked(&key) {
                return;
            }
        }
        let tip = self.tip();
        let mut client = Client {
            key: key.clone(),
            balance_only,
            live: true,
            resumable: false,
            epoch: self.keys[&key].epoch,
            tip,
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
            client.receive(output, Some(tip));
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
        let from = (!client.balance_only).then_some(client.tip);
        // A key tracked again since the client left replays too, if the client's block is
        // on the main chain at or above the new load: the key's boxes there are the same
        // whichever load found them. Eviction can untrack and reload a key within a block.
        let model = &self.keys[&key];
        let reloaded_below = client.epoch != model.epoch
            && self.fed.contains(&client.tip)
            && client.tip.height >= model.since.height.max(self.floor());
        let expect_replay = client.balance_only
            || reloaded_below
            || (client.epoch == model.epoch
                && self
                    .fork_point(client.tip)
                    .is_some_and(|f| f.height >= self.floor()));
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
            self.seen.insert(match reason {
                ResyncReason::OutsideWindow => "resync: outside window",
                ResyncReason::ForkBeyondWindow => "resync: fork beyond window",
                ResyncReason::NotTracked => "resync: not tracked",
                ResyncReason::SlowConsumer => unreachable!("the engine has no consumers"),
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
        for output in &outputs {
            client.receive(output, Some(tip));
        }
        if !client.balance_only {
            assert_eq!(client.tip, tip, "the catch-up reaches the tip");
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
        let Some(model) = self.keys.get_mut(key) else {
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

    fn with_pool(&mut self, key: &TrackingKey, limit: usize) {
        let query = Query::WithPool { key: key.clone(), limit };
        let Reply::WithPool(reply) = self.engine.query(query) else {
            panic!("a with-pool reply");
        };
        let (tip, now) = (self.tip(), self.now);
        let Some(model) = self.keys.get_mut(key) else {
            assert_eq!(reply, None);
            return;
        };
        model.last_seen = now;
        let (mut confirmed, pending) = self.reference_with_pool(key);
        let total = confirmed.len() + pending.len();
        let held = self.engine.keys[&self.engine.ids[key]].boxes.len();
        if confirmed.len() > limit {
            // Only confirmed boxes are cut, the first ones in keyset order kept.
            confirmed.truncate(limit);
            self.seen.insert("with-pool view cut to the limit");
            if !pending.is_empty() {
                self.seen
                    .insert("with-pool view cut to the limit lists every pending box");
            }
        } else if held > limit {
            self.seen
                .insert("with-pool view fits the limit only after pending spends");
        }
        assert_eq!(reply, Some(WithPoolIds { as_of: tip, confirmed, pending, total }));
    }

    // --- Expectations ---

    /// `key`'s view with the pool at the engine's tip: the oracle's unspent boxes the reference
    /// overlay leaves unspent, then the boxes it creates and leaves unspent, in its order.
    fn reference_with_pool(&self, key: &TrackingKey) -> (Vec<BoxKey>, Vec<Arc<UTxO>>) {
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
                Output::Balance { key: k, balance } if k == key => Some(balance),
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

    // --- Invariants after every op ---

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
        let boxes: usize = self
            .keys
            .keys()
            .map(|key| oracle.unspent(key, tip.height).len())
            .sum();
        let ring_boxes: usize = self.engine.ring.iter().map(|d| d.boxes.len()).sum();
        let evictable_boxes: usize = self
            .keys
            .iter()
            .filter(|(_, m)| {
                m.subscribers == 0
                    && self.now - m.last_seen >= CONFIG.evict_after.as_millis() as u64
            })
            .map(|(key, _)| oracle.unspent(key, tip.height).len())
            .sum();
        let expected = Stats {
            tip: Some(tip),
            address_keys: addresses,
            token_keys: self.keys.len() - addresses,
            tracked_boxes: boxes,
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
            let expected: BTreeSet<BoxKey> = oracle.unspent(key, tip.height).into_keys().collect();
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
            if state.boxes.iter().any(|b| {
                let held = &utxo[b];
                overlay::keys_of(held)
                    .filter(|k| self.engine.ids.contains_key(k))
                    .count()
                    >= 2
            }) {
                self.seen.insert("box held by two tracked keys");
            }
        }
        assert_eq!(self.engine.reverse, reverse, "reverse index");

        // Every key's overlay, tracked or not, is the oracle's over the snapshot in use.
        for key in self.chain.keys() {
            let reference = self.reference_overlay(&key, &ids);
            assert_eq!(self.engine.mempool.overlay(&key), reference, "{key:?}'s overlay");
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

        // Every tracked key's view with the pool is the reference one, and an address's sums to its
        // balance: the overlay applied to the boxes, or to the totals, gives the same assets.
        // Read without `Query::WithPool`, which would count as activity.
        let keys: Vec<TrackingKey> = self.keys.keys().cloned().collect();
        for key in &keys {
            let view = self
                .engine
                .with_pool(key, usize::MAX)
                .expect("a tracked key has a view");
            let (confirmed, pending) = self.reference_with_pool(key);
            let total = confirmed.len() + pending.len();
            let expected = WithPoolIds { as_of: tip, confirmed, pending, total };
            assert_eq!(view, expected, "{key:?}'s view");
            let held = &self.engine.keys[&self.engine.ids[key]].boxes;
            if view.confirmed.len() < held.len() {
                self.seen.insert("with-pool view drops a pending-spent box");
            }
            if !view.pending.is_empty() {
                self.seen.insert("with-pool view lists a pending box");
            }
            let overlay = self.engine.mempool.overlay(key);
            let created = overlay.iter().map(|p| p.created.len()).sum::<usize>();
            if created > view.pending.len() {
                self.seen
                    .insert("with-pool view hides a box created and spent in the pool");
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
                assert_eq!(client.tip, tip);
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
    for _ in 0..400 {
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
        "idle key evicted for a load",
        "idle key kept: the load fit without it",
        "load refused at the cap",
        "load refused though some keys were idle",
        "ring ran out",
        "key dropped with its load block",
        "load rejected",
        "resync: not tracked",
        "resync: outside window",
        "resync: fork beyond window",
        "catch-up rolls back an orphaned branch",
        "replay to a client across a reload at or below its block",
        "page cursor expired",
        "page at an earlier block restores spent boxes",
        "box held by two tracked keys",
        "key kept by a subscriber",
        "partial confirmation",
        "balance sent though confirmed + pending stayed",
        "reorg-returned tx in a snapshot after the rollback",
        "with-pool view drops a pending-spent box",
        "with-pool view lists a pending box",
        "with-pool view hides a box created and spent in the pool",
        "with-pool view cut to the limit",
        "with-pool view cut to the limit lists every pending box",
        "with-pool view fits the limit only after pending spends",
        "pending history of a conflict",
        "pending history lists a child above its parent",
        "pool box created and spent in the pool",
        "pool box spends a confirmed box",
        "pool box created",
    ] {
        assert!(seen.contains(feature), "no random history reached: {feature}");
    }
}

// --- Hand-driven cases ---

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
    let before = (f.state(&zero), f.state(&one), f.engine.reverse.clone());

    let source = sim::testing::box_of(&f.chain, 0);
    let tx = f.chain.transfer(&[source], &f.chain.trees()[1].clone());
    let output = tx.outputs[0].clone();
    let outputs = f.mine(vec![tx.clone()]);
    let block = f.engine.tip().expect("a tip");
    let parent_id = f
        .chain
        .block(&block.header_id)
        .expect("mined")
        .header
        .parent_id;
    assert_eq!(outputs.len(), 4, "two blocks then two balances");
    assert_eq!(
        find(&outputs, "block", &zero),
        &Output::Block {
            key: zero.clone(),
            block,
            parent_id,
            created: vec![],
            spent: vec![Spend { box_id: source, tx_id: tx.id }],
        }
    );
    assert_eq!(
        find(&outputs, "block", &one),
        &Output::Block {
            key: one.clone(),
            block,
            parent_id,
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
            parent_id,
            created: vec![spent_box],
            spent: vec![],
        }
    );
    assert_eq!(
        find(&outputs, "rollback", &one),
        &Output::Rollback {
            key: one.clone(),
            block,
            parent_id,
            created: vec![],
            spent: vec![Spend { box_id: output.id, tx_id: tx.id }],
        }
    );
    assert_eq!((f.state(&zero), f.state(&one), f.engine.reverse.clone()), before);
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
    let loaded =
        Stats { tip, address_keys: 2, tracked_boxes: boxes, ring_blocks: 1, ..Stats::default() };
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
    let outputs = f.catch_up(&zero, None);
    assert_eq!(kinds(&outputs), [("balance", zero.clone())]);
    let Reply::Balance(Some(balance)) = f.engine.query(Query::Balance(zero.clone())) else {
        panic!("a balance");
    };
    assert_eq!(outputs, [Output::Balance { key: zero, balance }]);
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

/// The cap on tracked boxes: a load that reaches it exactly is taken, one box more is
/// refused and changes nothing. A duplicate load is ignored even at the cap, and a load off
/// the tip is rejected before the cap is looked at.
#[test]
fn a_load_past_the_cap_on_tracked_boxes_is_refused() {
    let probe = Fixture::new(Config::default());
    let sizes: Vec<usize> = (0..3)
        .map(|t| {
            probe
                .chain
                .oracle()
                .load(&probe.key(t), probe.chain.height())
                .len()
        })
        .collect();
    assert!(sizes.iter().all(|&n| n > 0), "{sizes:?}");
    let cap = sizes[0] + sizes[1];

    let mut f = Fixture::new(Config { max_tracked_boxes: cap - 1, ..Config::default() });
    let [zero, one] = [0, 1].map(|t| f.key(t));
    f.load(&zero);
    let refused = Output::CapacityExceeded {
        key: one.clone(),
        tracked: sizes[0],
        evictable: 0,
        boxes: sizes[1],
    };
    assert_eq!(f.load(&one), [refused]);
    assert!(!f.engine.is_tracked(&one));
    assert_eq!(f.engine.reverse.len(), sizes[0], "nothing changed");

    let mut f = Fixture::new(Config { max_tracked_boxes: cap, ..Config::default() });
    let [zero, one, two] = [0, 1, 2].map(|t| f.key(t));
    f.load(&zero);
    let outputs = f.load(&one);
    assert!(f.engine.is_tracked(&one), "exactly at the cap: {outputs:?}");
    assert_eq!(f.load(&zero), [], "a duplicate is ignored");
    // The room a loader reads before paging: none for a tracked key, which is never refused.
    let full = Capacity { tracked: cap, evictable: 0, max: cap };
    let room = |key: &TrackingKey| Query::Capacity { key: key.clone(), up_to: sizes[2] };
    assert_eq!(f.engine.query(room(&two)), Reply::Capacity(Some(full)));
    assert_eq!(f.engine.query(room(&zero)), Reply::Capacity(None));
    assert!(full.fits(0) && !full.fits(1));
    let refused =
        Output::CapacityExceeded { key: two.clone(), tracked: cap, evictable: 0, boxes: sizes[2] };
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
    let height = probe.chain.height();
    let size = |key: &TrackingKey| probe.chain.oracle().load(key, height).len();
    let [zero, one, two, three] = [0, 1, 2, 3].map(|t| probe.key(t));
    let [s0, s1, s2, s3] = [&zero, &one, &two, &three].map(size);
    assert!(s3 <= s1, "key 3 fits in key 1's room");
    let big = probe
        .chain
        .keys()
        .into_iter()
        .find(|k| size(k) > s0)
        .expect("a key larger than key 0");
    let cap = s0 + s1 + s2;

    let mut f = Fixture::new(Config { max_tracked_boxes: cap, ..Config::default() });
    let room = |f: &mut Fixture, key: &TrackingKey| {
        let up_to = size(key);
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
    assert_eq!(room(&mut f, &three), Capacity { tracked: cap, evictable: 0, max: cap });
    let refused =
        Output::CapacityExceeded { key: three.clone(), tracked: cap, evictable: 0, boxes: s3 };
    assert_eq!(f.load(&three), [refused]);

    // Key 0 read a block later, key 2 subscribed two blocks later: only key 1, idle since its
    // load, and key 0 are evictable, key 1 first.
    f.mine(vec![]);
    let _ = f.engine.query(Query::Balance(zero.clone()));
    f.mine(vec![]);
    f.engine
        .apply(Event::Subscribed(two.clone()))
        .expect("tracked");
    let capacity = room(&mut f, &three);
    assert_eq!(capacity, Capacity { tracked: cap, evictable: s0 + s1, max: cap });
    assert!(capacity.fits(s3));
    let outputs = f.load(&three);
    assert!(f.engine.is_tracked(&three), "{outputs:?}");
    assert_eq!(tracked(&f), [true, false, true, true], "key 1 alone made room");
    assert_eq!(evicted(&mut f), 1);

    // Key 0 is the only idle key left, and too small for `big`: refused, key 0 kept.
    let capacity = room(&mut f, &big);
    assert_eq!(capacity.evictable, s0, "key 3 just loaded, key 2 subscribed");
    assert!(!capacity.fits(size(&big)));
    let held = f.engine.reverse.len();
    let refused = Output::CapacityExceeded {
        key: big.clone(),
        tracked: held,
        evictable: s0,
        boxes: size(&big),
    };
    assert_eq!(f.load(&big), [refused]);
    assert_eq!(tracked(&f), [true, false, true, true], "nothing evicted");
    assert_eq!(f.engine.reverse.len(), held);
    assert_eq!(evicted(&mut f), 1);
}

/// The room a loader reads counts idle keys only when a load of the size it asks about could
/// pass the cap: below that, their room changes no answer and counting it walks every key.
#[test]
fn the_room_counts_idle_keys_only_when_a_load_could_need_them() {
    let probe = Fixture::new(Config::default());
    let height = probe.chain.height();
    let [zero, one, two] = [0, 1, 2].map(|t| probe.key(t));
    let [s0, s1, s2] = [&zero, &one, &two].map(|k| probe.chain.oracle().load(k, height).len());
    let cap = s0 + s1 + s2;

    let mut f = Fixture::new(Config { max_tracked_boxes: cap, ..Config::default() });
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
    let tracked = s0 + s1;
    assert_eq!(room(&mut f, s2), Capacity { tracked, evictable: 0, max: cap }, "fits as it is");
    let near = Capacity { tracked, evictable: s0 + s1, max: cap };
    assert_eq!(room(&mut f, s2 + 1), near, "both keys idle since the block");
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
