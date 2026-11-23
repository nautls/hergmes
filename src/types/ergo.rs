use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, de::Error};
use serde_json::value::RawValue;

use crate::types::{Digest, HashDigest, HexBytes, eip4::Asset};

pub type BoxId = HashDigest;
pub type TxId = HashDigest;
pub type TokenId = HashDigest;
pub type HeaderId = HashDigest;
pub type ErgoTree = HexBytes;
/// The Blake2b256 of a constant-segregated tree's template (see
/// [`crate::types::ergo_tree::template_hash`]).
pub type TemplateHash = HashDigest;

/// The genesis block's `parentId` (node 6.0.8, `Header.GenesisParentId`, all zeros): no
/// header has this id, so a walk back through parents ends at the block naming it.
pub const GENESIS_PARENT_ID: HeaderId = Digest([0; 32]);

/// A block header: what the relay reads of it, and the node's JSON for it, served to
/// clients as it came, so each is Fleet's `BlockHeader`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub id: HeaderId,
    pub parent_id: HeaderId,
    pub height: u32,
    /// The node's JSON, compacted: the node pretty-prints (`ApiRoute.printer` is
    /// `Printer.spaces2`). Compacted as text, so numbers stay exact (a v1 header's `d` is
    /// past `u64`) and fields keep the node's order. Shared, since a header lives in the
    /// header cache and in the indexed block it came with.
    pub json: Arc<str>,
}

impl BlockHeader {
    /// A header whose JSON holds only the fields the relay reads, for the simulator, which
    /// has no node to take the rest from.
    #[cfg(any(test, feature = "sim"))]
    pub fn new(id: HeaderId, parent_id: HeaderId, height: u32) -> Self {
        let json = format!(r#"{{"id":"{id}","parentId":"{parent_id}","height":{height}}}"#);
        Self { id, parent_id, height, json: json.into() }
    }
}

impl<'de> Deserialize<'de> for BlockHeader {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Read {
            id: HeaderId,
            #[serde(rename = "parentId")]
            parent_id: HeaderId,
            height: u32,
        }
        let raw = <Box<RawValue>>::deserialize(deserializer)?;
        let json = compact(raw.get());
        let read: Read = serde_json::from_str(&json).map_err(D::Error::custom)?;
        Ok(Self { id: read.id, parent_id: read.parent_id, height: read.height, json: json.into() })
    }
}

/// Valid JSON without the whitespace between tokens. Whitespace inside a string is kept.
fn compact(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    let (mut in_string, mut escaped) = (false, false);
    for c in json.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
        } else if c == '"' {
            in_string = true;
        } else if c.is_ascii_whitespace() {
            continue;
        }
        out.push(c);
    }
    out
}

/// A mempool transaction with its input boxes resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnconfirmedTransaction {
    pub id: TxId,
    pub inputs: Vec<UTxO>,
    pub outputs: Vec<UTxO>,
}

/// A box. Read through [`NodeBox`], which names the node's fields for every box shape; the
/// renames here are for writing it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "NodeBox")]
pub struct UTxO {
    #[serde(rename = "boxId")]
    pub id: BoxId,

    #[serde(rename = "ergoTree")]
    pub ergo_tree: ErgoTree,

    #[serde(rename = "creationHeight")]
    pub creation_height: u32,

    pub value: u64,

    #[serde(rename = "assets")]
    pub tokens: Vec<Token>,

    #[serde(rename = "additionalRegisters")]
    pub registers: NonMandatoryRegisters,

    pub index: u16,

    #[serde(rename = "transactionId")]
    pub transaction_id: TxId,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Token {
    #[serde(rename = "tokenId")]
    pub id: TokenId,
    pub amount: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct NonMandatoryRegisters {
    #[serde(rename = "R4")]
    pub r4: Option<HexBytes>,
    #[serde(rename = "R5")]
    pub r5: Option<HexBytes>,
    #[serde(rename = "R6")]
    pub r6: Option<HexBytes>,
    #[serde(rename = "R7")]
    pub r7: Option<HexBytes>,
    #[serde(rename = "R8")]
    pub r8: Option<HexBytes>,
    #[serde(rename = "R9")]
    pub r9: Option<HexBytes>,
}

/// A box as returned by the node's extra indexer (`/blockchain/...` endpoints).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedBox {
    pub utxo: UTxO,
    pub inclusion_height: u32,
}

impl<'de> Deserialize<'de> for IndexedBox {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        NodeBox::deserialize(deserializer)?.indexed()
    }
}

/// A box as `/blockchain/box/byId` returns it: any box the indexer knows, and whether a tx
/// spent it. Its own type, so the boxes of indexed blocks don't carry the flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoxById {
    pub indexed: IndexedBox,

    /// Whether `spentTransactionId` is set: `null` while unspent (node 6.0.8,
    /// `ApiExtraCodecs.indexedBoxEncoder`, from `spendingTxIdOpt`). The id itself is not read,
    /// and the field is required: a box without it can't be told unspent.
    pub spent: bool,
}

impl<'de> Deserialize<'de> for BoxById {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let read = NodeBox::deserialize(deserializer)?;
        let spent = read
            .spent
            .ok_or_else(|| D::Error::missing_field("spentTransactionId"))?;
        Ok(Self { indexed: read.indexed()?, spent })
    }
}

/// Every field the relay reads of a box, in any of the node's shapes: a bare box, an indexed
/// one, and a pool tx's input. Each shape is built from it, so the field names live here
/// alone. Not `#[serde(flatten)]` for the shapes that add fields to a box: it buffers every
/// box into a map before reading it, about twice the bytes allocated per box.
///
/// Only `boxId` is required here; each shape requires what it needs. A pool tx's input the
/// node could not resolve has only `boxId` and `spendingProof` (see
/// `clients::node::MempoolTransactionInput`).
#[derive(Deserialize)]
pub(crate) struct NodeBox {
    #[serde(rename = "boxId")]
    id: BoxId,
    #[serde(rename = "ergoTree")]
    ergo_tree: Option<ErgoTree>,
    #[serde(rename = "creationHeight")]
    creation_height: Option<u32>,
    value: Option<u64>,
    #[serde(rename = "assets")]
    tokens: Option<Vec<Token>>,
    #[serde(rename = "additionalRegisters")]
    registers: Option<NonMandatoryRegisters>,
    index: Option<u16>,
    #[serde(rename = "transactionId")]
    transaction_id: Option<TxId>,
    #[serde(rename = "inclusionHeight")]
    inclusion_height: Option<u32>,
    /// `None` if absent, else whether it is set.
    #[serde(rename = "spentTransactionId", default, deserialize_with = "is_set")]
    spent: Option<bool>,
}

/// A field a box shape requires, absent.
#[derive(Debug)]
pub(crate) struct MissingField(&'static str);

impl std::fmt::Display for MissingField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "missing field `{}`", self.0)
    }
}

impl TryFrom<NodeBox> for UTxO {
    type Error = MissingField;

    fn try_from(b: NodeBox) -> Result<Self, MissingField> {
        let need = MissingField;
        Ok(UTxO {
            id: b.id,
            ergo_tree: b.ergo_tree.ok_or(need("ergoTree"))?,
            creation_height: b.creation_height.ok_or(need("creationHeight"))?,
            value: b.value.ok_or(need("value"))?,
            tokens: b.tokens.ok_or(need("assets"))?,
            registers: b.registers.ok_or(need("additionalRegisters"))?,
            index: b.index.ok_or(need("index"))?,
            transaction_id: b.transaction_id.ok_or(need("transactionId"))?,
        })
    }
}

impl NodeBox {
    fn indexed<E: Error>(self) -> Result<IndexedBox, E> {
        let inclusion_height =
            (self.inclusion_height).ok_or_else(|| E::missing_field("inclusionHeight"))?;
        let utxo = UTxO::try_from(self).map_err(|MissingField(f)| E::missing_field(f))?;
        Ok(IndexedBox { utxo, inclusion_height })
    }
}

fn is_set<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<bool>, D::Error> {
    Ok(Some(Option::<serde::de::IgnoredAny>::deserialize(deserializer)?.is_some()))
}

/// A transaction inside an [`IndexedBlock`]. Inputs are full boxes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct IndexedTransaction {
    pub id: TxId,
    pub inputs: Vec<IndexedBox>,
    pub outputs: Vec<IndexedBox>,
}

/// A block from `/blockchain/block/byHeaderId/{id}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct IndexedBlock {
    pub header: BlockHeader,
    pub transactions: Vec<IndexedTransaction>,
}

/// A page of `/blockchain/transaction/byAddress`: the address's txs, newest first, and how
/// many it has in all.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AddressTransactions {
    pub items: Vec<AddressTransaction>,
    pub total: u32,
}

/// A tx of an address's history, with only what a summary for the address reads: the node
/// sends full indexed boxes on both sides, and the relay keeps their amounts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AddressTransaction {
    pub id: TxId,
    #[serde(rename = "inclusionHeight")]
    pub inclusion_height: u32,
    /// The block header's timestamp, in milliseconds.
    pub timestamp: u64,
    pub inputs: Vec<BoxAssets>,
    pub outputs: Vec<BoxAssets>,
}

/// A box's guard and what it holds, and nothing else.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BoxAssets {
    #[serde(rename = "ergoTree")]
    pub ergo_tree: ErgoTree,
    pub value: u64,
    #[serde(rename = "assets")]
    pub tokens: Vec<Token>,
}

/// Token metadata from `/blockchain/token/byId/{tokenId}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TokenInfo {
    #[serde(rename = "emissionAmount")]
    pub emission_amount: u64,
}

/// A token as `POST /blockchain/tokens` lists it (node 6.0.8,
/// `ApiExtraCodecs.IndexedTokenEncoder`), read from the mint by the indexer
/// (`IndexedToken.fromBox`): `name` and `description` are the minting box's R4 and R5 as
/// UTF-8, empty if absent or not bytes, and `decimals` is R6 as a decimal string or an `Int`, 0
/// otherwise, so it can be negative. None of it changes once the mint is indexed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TokenMetadata {
    pub id: TokenId,
    /// The first output of the mint tx holding the token. Not the token id, which is the mint
    /// tx's first input.
    #[serde(rename = "boxId")]
    pub minting_box_id: BoxId,
    /// Every unit minted, summed over the mint tx's outputs.
    #[serde(rename = "emissionAmount")]
    pub emission: u64,
    pub name: Box<str>,
    pub description: Box<str>,
    pub decimals: i32,
    /// The asset type and artwork, which the indexer does not read: filled in from the minting
    /// box.
    #[serde(skip)]
    pub asset: Option<Box<Asset>>,
}

/// Response of `POST /blockchain/balance`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BalanceResponse {
    pub confirmed: Balance,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Balance {
    #[serde(rename = "nanoErgs")]
    pub nano_ergs: u64,
    pub tokens: Vec<TokenBalance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TokenBalance {
    #[serde(rename = "tokenId")]
    pub token_id: TokenId,
    pub amount: u64,
}

#[cfg(test)]
#[path = "ergo_tests.rs"]
mod tests;
