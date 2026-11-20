//! The engine's mempool state: the snapshot in use, pruned to the txs that can be valid at
//! the engine's tip, and per tracked key the overlay hash and pending delta last sent.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use tracing::debug;

use crate::{
    engine::{
        BlockRef, Output, PendingTx, PoolBox, TrackingKey,
        overlay::{self, DeltaSides, OverlayHash, PendingDelta, TxDelta},
    },
    types::ergo::{BoxId, ErgoTree, TxId, UTxO, UnconfirmedTransaction},
};

/// A snapshot the engine accepted.
#[derive(Debug, Clone)]
struct Snapshot {
    /// As received.
    txs: Arc<[UnconfirmedTransaction]>,
    /// The block it was observed at.
    observed: BlockRef,
    /// The engine's tip its txs were last decided at.
    tip: BlockRef,
    /// The txs that can be valid at `tip`, in canonical order. Only shrinks until the next
    /// snapshot: a tx left out once never comes back.
    order: Vec<usize>,
}

/// What a tracked key's subscribers have.
#[derive(Debug, Clone)]
struct Sent {
    hash: OverlayHash,
    /// The pending delta of that overlay, for an address; empty for a token key, which has
    /// no balance. Fixed by the overlay, so it changes only with the hash.
    delta: PendingDelta,
}

#[derive(Debug, Clone, Default)]
pub struct Mempool {
    snapshot: Option<Snapshot>,
    /// A snapshot observed above the engine's tip, used if the engine reaches its block.
    ahead: Option<(Arc<[UnconfirmedTransaction]>, BlockRef)>,
    sent: BTreeMap<TrackingKey, Sent>,
    /// The tracked keys whose sent overlay is not empty. A new snapshot can change only these
    /// and the keys its txs touch, so `emit` compares those alone: every other key's overlay
    /// was empty and stays so.
    with_overlay: BTreeSet<TrackingKey>,
}

impl Mempool {
    /// Takes a snapshot observed at `observed_tip` while the engine is at `tip`, and returns a
    /// `Mempool` message for each tracked key whose overlay changed, in key order.
    ///
    /// A snapshot observed at any other block changes nothing and emits nothing: its confirmed
    /// state is not the engine's, so an overlay from it could spend boxes clients do not have
    /// yet, or list txs a block already confirmed. One observed above the tip is held, and
    /// used if the next block the engine applies is the one it was observed at.
    ///
    /// `is_confirmed` is asked about the inputs no listed tx creates (see
    /// [`overlay::pending_order`]). The source drops txs whose parent it could not resolve, so
    /// such an input was in the node's UTXO set when it answered; for a box a tracked key holds,
    /// the engine checks its own set instead, since the node's state can run ahead of its pool.
    pub fn apply_snapshot(
        &mut self,
        txs: Arc<[UnconfirmedTransaction]>,
        observed_tip: BlockRef,
        tip: Option<BlockRef>,
        is_confirmed: impl Fn(&UTxO) -> bool,
    ) -> Vec<Output> {
        match tip {
            Some(tip) if tip == observed_tip => {
                self.install(txs, tip, is_confirmed);
                self.emit(tip.height)
            }
            Some(tip) if observed_tip.height > tip.height => {
                debug!(
                    ?observed_tip,
                    ?tip,
                    txs = txs.len(),
                    "Mempool snapshot ahead of the tip held"
                );
                self.ahead = Some((txs, observed_tip));
                Vec::new()
            }
            _ => {
                debug!(
                    ?observed_tip,
                    ?tip,
                    txs = txs.len(),
                    "Mempool snapshot off the tip ignored"
                );
                Vec::new()
            }
        }
    }

    fn install(
        &mut self,
        txs: Arc<[UnconfirmedTransaction]>,
        tip: BlockRef,
        is_confirmed: impl Fn(&UTxO) -> bool,
    ) {
        let order = overlay::pending_order(&txs, is_confirmed);
        debug!(
            height = tip.height,
            txs = txs.len(),
            left_out = txs.len() - order.len(),
            "Mempool snapshot installed"
        );
        self.snapshot = Some(Snapshot { txs, observed: tip, tip, order });
    }

    /// The engine applied a block and is now at `tip`. `mined` are the block's txs and `spent`
    /// every box its txs spent, tracked or not.
    ///
    /// A snapshot held for this block replaces the one in use. Otherwise the one in use is
    /// decided again: the block's own txs leave (so a child of a mined tx now spends a
    /// confirmed box, and stays), and so does every tx spending a box the block spent, with
    /// its descendants.
    pub fn block_applied(
        &mut self,
        tip: BlockRef,
        mined: &HashSet<TxId>,
        spent: &HashSet<BoxId>,
        is_confirmed: impl Fn(&UTxO) -> bool,
    ) -> Vec<Output> {
        match self.ahead.take() {
            Some((txs, observed)) if observed == tip => {
                self.install(txs, tip, is_confirmed);
                return self.emit(tip.height);
            }
            Some(ahead) if ahead.1.height > tip.height => self.ahead = Some(ahead),
            Some((txs, observed)) => {
                debug!(
                    ?observed,
                    ?tip,
                    txs = txs.len(),
                    "Held mempool snapshot off the chain dropped"
                );
            }
            None => {}
        }
        if let Some(s) = &mut self.snapshot {
            let candidates = s
                .order
                .iter()
                .copied()
                .filter(|&i| !mined.contains(&s.txs[i].id));
            let before = s.order.len();
            s.order = overlay::pending_order_among(&s.txs, candidates, |b| {
                !spent.contains(&b.id) && is_confirmed(b)
            });
            s.tip = tip;
            debug!(
                height = tip.height,
                left = before - s.order.len(),
                kept = s.order.len(),
                "Mempool snapshot pruned by block"
            );
        }
        self.emit(tip.height)
    }

    /// The engine reverted its tip and is now at `tip`. The snapshot in use is dropped: it
    /// was decided against boxes the orphaned block created, and does not hold the block's
    /// txs, which return to the node's pool. Overlays are empty until a snapshot observed at
    /// the new tip arrives, so returned txs appear only after the rollback.
    pub fn block_reverted(&mut self, tip: BlockRef) -> Vec<Output> {
        if self.snapshot.take().is_some() {
            debug!(height = tip.height, "Mempool snapshot dropped by revert");
        }
        self.emit(tip.height)
    }

    /// Forgets every snapshot, as when the engine loses its tip.
    pub fn reset(&mut self) {
        debug!(
            snapshot = self.snapshot.is_some(),
            ahead = self.ahead.is_some(),
            "Mempool snapshots reset"
        );
        self.snapshot = None;
        self.ahead = None;
    }

    /// Decides the snapshot in use again after `is_confirmed` became stricter (a key started
    /// tracking, so its boxes are checked against its set).
    pub fn redecide(&mut self, is_confirmed: impl Fn(&UTxO) -> bool) -> Vec<Output> {
        let Some(s) = &mut self.snapshot else {
            return Vec::new();
        };
        let candidates = std::mem::take(&mut s.order);
        let before = candidates.len();
        s.order = overlay::pending_order_among(&s.txs, candidates, is_confirmed);
        if s.order.len() < before {
            debug!(
                left = before - s.order.len(),
                kept = s.order.len(),
                "Mempool snapshot re-decided"
            );
        }
        if s.order.len() == before {
            // The same txs in the same order, so every overlay is as it was: nothing to
            // compare, where `emit` would walk every tracked key on each load.
            return Vec::new();
        }
        let height = s.tip.height;
        self.emit(height)
    }

    /// The snapshot in use as received, and the block it was observed at.
    pub fn latest(&self) -> Option<(Arc<[UnconfirmedTransaction]>, BlockRef)> {
        self.snapshot.as_ref().map(|s| (s.txs.clone(), s.observed))
    }

    /// What the snapshot in use says of box `id`, over the txs valid at the tip: spent if one
    /// spends it, which wins over a tx creating it. A scan of the snapshot, as the node's own
    /// read of a pool box is (`UtxoStateReader.withUnconfirmedTransactions`).
    pub fn pool_box(&self, id: &BoxId) -> Option<PoolBox> {
        let s = self.snapshot.as_ref()?;
        let mut created = None;
        for tx in s.order.iter().map(|&i| &s.txs[i]) {
            if tx.inputs.iter().any(|b| b.id == *id) {
                return Some(PoolBox::Spent);
            }
            if created.is_none() {
                created = tx.outputs.iter().find(|b| b.id == *id);
            }
        }
        created.map(|b| PoolBox::Created(Arc::new(b.clone())))
    }

    /// `key`'s overlay, tracked or not. Empty without a snapshot.
    pub fn overlay(&self, key: &TrackingKey) -> Vec<PendingTx> {
        match &self.snapshot {
            Some(s) => overlay::overlay(&s.txs, &s.order, key),
            None => Vec::new(),
        }
    }

    /// `key`'s pending delta over its overlay, tracked or not.
    pub fn pending_delta(&self, key: &TrackingKey) -> PendingDelta {
        match &self.snapshot {
            Some(s) => overlay::pending_delta(&s.txs, &s.order, key),
            None => PendingDelta::default(),
        }
    }

    /// The pending txs that touch the address guarded by `tree`, tracked or not, newest
    /// first, each with its net change for the address. Empty without a snapshot.
    pub fn pending_txs(&self, tree: &ErgoTree) -> Vec<TxDelta> {
        match &self.snapshot {
            Some(s) => overlay::pending_txs(&s.txs, &s.order, tree),
            None => Vec::new(),
        }
    }

    /// A tracked key's pending delta, as of the last message sent for it. Empty for a token
    /// key.
    pub fn tracked_delta(&self, key: &TrackingKey) -> Option<&PendingDelta> {
        self.sent.get(key).map(|s| &s.delta)
    }

    /// Starts sending `key`'s overlay when it changes. Its subscribers get the current one
    /// with their catch-up, so it counts as sent.
    pub fn track(&mut self, key: TrackingKey) {
        if !self.sent.contains_key(&key) {
            let delta = match key {
                TrackingKey::Tree(_) => self.pending_delta(&key),
                TrackingKey::Token(_) => PendingDelta::default(),
            };
            let overlay = self.overlay(&key);
            if !overlay.is_empty() {
                self.with_overlay.insert(key.clone());
            }
            let sent = Sent { hash: overlay::hash(&overlay), delta };
            self.sent.insert(key, sent);
        }
    }

    pub fn untrack(&mut self, key: &TrackingKey) {
        self.sent.remove(key);
        self.with_overlay.remove(key);
    }

    pub fn is_tracked(&self, key: &TrackingKey) -> bool {
        self.sent.contains_key(key)
    }

    /// The tracked keys whose sent overlay is not empty, in key order.
    #[cfg(test)]
    pub fn with_overlay(&self) -> impl Iterator<Item = &TrackingKey> {
        self.with_overlay.iter()
    }

    /// The keys whose overlays are sent, in key order.
    #[cfg(test)]
    pub fn tracked(&self) -> impl Iterator<Item = &TrackingKey> {
        self.sent.keys()
    }

    /// A message, in key order, for each tracked key whose overlay hash differs from the one
    /// sent. Only the keys with an overlay now or at the last message can differ, so only
    /// those are compared: a walk of every key cost each snapshot and block about a tenth of
    /// a microsecond per tracked key (`examples/eventbench.rs`).
    fn emit(&mut self, tip_height: u32) -> Vec<Output> {
        let mut overlays = self.tracked_overlays();
        let candidates: BTreeSet<TrackingKey> =
            overlays.keys().chain(&self.with_overlay).cloned().collect();
        let mut outputs = Vec::new();
        for key in candidates {
            let sent = self.sent.get_mut(&key).expect("a candidate is tracked");
            let (overlay, delta) = overlays.remove(&key).unwrap_or_default();
            let hash = overlay::hash(&overlay);
            if hash != sent.hash {
                *sent = Sent { hash, delta };
                if overlay.is_empty() {
                    self.with_overlay.remove(&key);
                } else {
                    self.with_overlay.insert(key.clone());
                }
                outputs.push(Output::Mempool { key, tip_height, pending: overlay });
            }
        }
        outputs
    }

    /// Every tracked key's overlay and pending delta (empty for a token key), in one pass over
    /// the snapshot. A key no pending tx touches is absent.
    fn tracked_overlays(&self) -> BTreeMap<TrackingKey, (Vec<PendingTx>, PendingDelta)> {
        let Some(snapshot) = &self.snapshot else {
            return BTreeMap::new();
        };
        let mut overlays: BTreeMap<TrackingKey, (Vec<PendingTx>, DeltaSides)> = BTreeMap::new();
        for tx in snapshot.order.iter().map(|&i| &snapshot.txs[i]) {
            let mut touched: BTreeMap<TrackingKey, PendingTx> = BTreeMap::new();
            for output in &tx.outputs {
                let mut shared: Option<Arc<UTxO>> = None;
                for key in overlay::keys_of(output).filter(|k| self.sent.contains_key(k)) {
                    let utxo = shared
                        .get_or_insert_with(|| Arc::new(output.clone()))
                        .clone();
                    let entry = overlays.entry(key.clone()).or_default();
                    if matches!(key, TrackingKey::Tree(_)) {
                        entry.1.created(output);
                    }
                    pending_entry(&mut touched, key, tx.id).created.push(utxo);
                }
            }
            for input in &tx.inputs {
                for key in overlay::keys_of(input).filter(|k| self.sent.contains_key(k)) {
                    let entry = overlays.entry(key.clone()).or_default();
                    if matches!(key, TrackingKey::Tree(_)) {
                        entry.1.spent(input);
                    }
                    pending_entry(&mut touched, key, tx.id).spent.push(input.id);
                }
            }
            for (key, pending) in touched {
                overlays.entry(key).or_default().0.push(pending);
            }
        }
        overlays
            .into_iter()
            .map(|(key, (overlay, sides))| (key, (overlay, sides.delta())))
            .collect()
    }
}

fn pending_entry(
    touched: &mut BTreeMap<TrackingKey, PendingTx>,
    key: TrackingKey,
    tx_id: TxId,
) -> &mut PendingTx {
    touched.entry(key).or_insert_with(|| PendingTx {
        tx_id,
        created: Vec::new(),
        spent: Vec::new(),
    })
}

#[cfg(test)]
#[path = "mempool_tests.rs"]
mod tests;
