use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::types::ergo::{BlockHeader, BoxId, HeaderId, TokenId, TxId, UTxO};

/// A block's position and identity. Deserialized strictly, since clients send it as a
/// subscription's `from`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockRef {
    pub height: u32,
    pub header_id: HeaderId,
}

impl From<&BlockHeader> for BlockRef {
    fn from(header: &BlockHeader) -> Self {
        Self { height: header.height, header_id: header.id }
    }
}

/// A box's position in a key's set, which is also the keyset pagination order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BoxKey {
    pub inclusion_height: u32,
    pub box_id: BoxId,
}

/// A box leaving a key's set, and the transaction that spent it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Spend {
    pub box_id: BoxId,
    pub tx_id: TxId,
}

/// ERG and token totals over a set of boxes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Totals {
    pub nano_ergs: u64,
    pub tokens: BTreeMap<TokenId, u64>,
}

impl Totals {
    /// Adds a box's ERG and tokens. A token with no units is left out, so two totals over the
    /// same boxes are equal as values.
    pub fn add(&mut self, utxo: &UTxO) {
        self.nano_ergs += utxo.value;
        for token in utxo.tokens.iter().filter(|t| t.amount > 0) {
            *self.tokens.entry(token.id).or_default() += token.amount;
        }
    }

    /// Removes a box added before. A token whose units drop to zero is left out.
    ///
    /// Panics if the box's amounts were never added, which would be a bookkeeping bug.
    pub fn sub(&mut self, utxo: &UTxO) {
        self.nano_ergs = self
            .nano_ergs
            .checked_sub(utxo.value)
            .expect("the box's ERG was added");
        for token in utxo.tokens.iter().filter(|t| t.amount > 0) {
            let amount = self
                .tokens
                .get_mut(&token.id)
                .expect("the box's token was added");
            *amount = amount
                .checked_sub(token.amount)
                .expect("the box's token units were added");
            if *amount == 0 {
                self.tokens.remove(&token.id);
            }
        }
    }

    /// Adds `sign` times `change`, or `None` if an amount would go below zero, which leaves
    /// the totals unchanged. A token whose units drop to zero is left out.
    ///
    /// For a balance-only key, whose totals start from the indexer's balance rather than from
    /// boxes the engine saw: a spend larger than what it holds means that start was wrong.
    pub fn apply(&self, change: &Change, sign: i128) -> Option<Totals> {
        let moved = |amount: u64, by: i128| u64::try_from(i128::from(amount) + sign * by).ok();
        let mut totals = Totals {
            nano_ergs: moved(self.nano_ergs, change.nano_ergs)?,
            tokens: self.tokens.clone(),
        };
        for (id, by) in &change.tokens {
            let amount = moved(totals.tokens.get(id).copied().unwrap_or(0), *by)?;
            if amount == 0 {
                totals.tokens.remove(id);
            } else {
                totals.tokens.insert(*id, amount);
            }
        }
        Some(totals)
    }
}

/// A signed change to an address's totals: what a block paid it minus what it spent from it.
/// A token whose change nets to zero is left out, so an empty change is the default value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Change {
    pub nano_ergs: i128,
    pub tokens: BTreeMap<TokenId, i128>,
}

impl Change {
    /// Adds `sign` times a box's ERG and tokens.
    pub fn add(&mut self, utxo: &UTxO, sign: i128) {
        self.nano_ergs += sign * i128::from(utxo.value);
        for token in utxo.tokens.iter().filter(|t| t.amount > 0) {
            let by = self.tokens.entry(token.id).or_default();
            *by += sign * i128::from(token.amount);
            if *by == 0 {
                self.tokens.remove(&token.id);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// A small id standing for a tracked key in per-box structures (the reverse index and the
/// ring buffer), so they do not copy an ErgoTree per box. Never reused: an untracked key's
/// deltas are dropped, and a key tracked again gets a new id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyId(pub(super) u32);

impl KeyId {
    pub(super) const MIN: KeyId = KeyId(u32::MIN);
    pub(super) const MAX: KeyId = KeyId(u32::MAX);
}

/// A block of the engine's chain, with its parent: what the block consumer walks back to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainLink {
    pub block: BlockRef,
    pub parent_id: HeaderId,
}

impl ChainLink {
    /// The block before this one.
    pub fn parent(&self) -> BlockRef {
        BlockRef { height: self.block.height - 1, header_id: self.parent_id }
    }
}

/// A confirmed box and the height of the block that included it: kept in the ring buffer,
/// restored by a rollback, and served in pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBox {
    pub inclusion_height: u32,
    pub utxo: Arc<UTxO>,
}

/// One key's share of a block, net of boxes created and spent within the block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyDelta {
    pub created: Vec<BoxId>,
    pub spent: Vec<Spend>,
}

impl KeyDelta {
    pub fn is_empty(&self) -> bool {
        self.created.is_empty() && self.spent.is_empty()
    }
}

/// A ring buffer entry: what one block did to the tracked keys.
///
/// Each box a tracked key gained or lost is stored once, in `boxes`; the per-key
/// deltas refer to it by id. A balance-only key has no boxes to page or replay, so its share
/// is only the change to its totals, in `balances`, which keeps no box in the ring. Keys the
/// block left unchanged have no entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockDelta {
    pub block: BlockRef,
    pub parent_id: HeaderId,
    /// The engine's count of applied blocks when this one was applied. A key loaded later
    /// (with a higher count) has no share in it.
    pub seq: u64,
    pub boxes: HashMap<BoxId, StoredBox>,
    pub keys: HashMap<KeyId, KeyDelta>,
    pub balances: HashMap<KeyId, Change>,
}

impl BlockDelta {
    /// The block before this one.
    pub fn parent(&self) -> BlockRef {
        BlockRef { height: self.block.height - 1, header_id: self.parent_id }
    }
}
