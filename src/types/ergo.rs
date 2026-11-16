use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, de::Error};
use serde_json::value::RawValue;

use crate::types::{Digest, HashDigest, HexBytes};

pub type BoxId = HashDigest;
pub type TxId = HashDigest;
pub type TokenId = HashDigest;
pub type HeaderId = HashDigest;
pub type ErgoTree = HexBytes;

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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct IndexedBox {
    #[serde(flatten)]
    pub utxo: UTxO,

    #[serde(rename = "inclusionHeight")]
    pub inclusion_height: u32,
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
