//! What the relay sends, as JSON.
//!
//! These types borrow what the services return and serialize it in the client's shape. They
//! are separate from the node-facing types, whose derived `Serialize` writes amounts as
//! numbers and absent registers as `null`.
//!
//! Amounts are base-10 integer strings: ERG supply in nanoErgs and token amounts exceed 2^53,
//! which a JSON number does not carry exactly in JavaScript.

use std::{collections::BTreeMap, fmt::Display, sync::Arc};

use serde::{
    Serialize, Serializer,
    ser::{Error, SerializeMap, SerializeStruct},
};
use serde_json::value::RawValue;

use crate::{
    api::{
        AddressBalance, BoxPage, FoundBox, HeadersPage, WithPoolBoxes,
        history::{HistoryItem, HistoryPage},
    },
    engine::{BlockRef, StoredBox, Totals, overlay::PendingDelta},
    types::{
        HexBytes,
        address::Network,
        ergo::{HeaderId, NonMandatoryRegisters, TokenId, TokenMetadata, TxId, UTxO},
    },
};

/// An amount, written as a string.
struct Amount<T>(T);

impl<T: Display> Serialize for Amount<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

/// A box in Fleet's `Box<string>` shape, so clients hand it to Fleet as it is. Confirmed
/// boxes carry the height of the block that included them; mempool outputs do not.
pub struct WireBox<'a> {
    pub utxo: &'a UTxO,
    pub inclusion_height: Option<u32>,
}

impl<'a> From<&'a StoredBox> for WireBox<'a> {
    fn from(b: &'a StoredBox) -> Self {
        Self { utxo: &b.utxo, inclusion_height: Some(b.inclusion_height) }
    }
}

impl Serialize for WireBox<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let u = self.utxo;
        let fields = if self.inclusion_height.is_some() { 9 } else { 8 };
        let mut s = serializer.serialize_struct("Box", fields)?;
        s.serialize_field("boxId", &u.id)?;
        s.serialize_field("transactionId", &u.transaction_id)?;
        s.serialize_field("index", &u.index)?;
        s.serialize_field("ergoTree", &u.ergo_tree)?;
        s.serialize_field("creationHeight", &u.creation_height)?;
        s.serialize_field("value", &Amount(u.value))?;
        let assets: Vec<_> = u
            .tokens
            .iter()
            .map(|t| TokenAmount { token_id: &t.id, amount: Amount(t.amount) })
            .collect();
        s.serialize_field("assets", &assets)?;
        s.serialize_field("additionalRegisters", &Registers(&u.registers))?;
        if let Some(height) = self.inclusion_height {
            s.serialize_field("inclusionHeight", &height)?;
        }
        s.end()
    }
}

/// A token and an amount: a box's `assets` and a balance's `tokens` alike.
#[derive(Serialize)]
#[serde(bound(serialize = "T: Display"))]
struct TokenAmount<'a, T: Display> {
    #[serde(rename = "tokenId")]
    token_id: &'a TokenId,
    amount: Amount<T>,
}

/// The registers a box has, `R4` to `R9`; absent ones are left out, not `null`.
struct Registers<'a>(&'a NonMandatoryRegisters);

impl Serialize for Registers<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let r = self.0;
        let all: [(&str, &Option<HexBytes>); 6] = [
            ("R4", &r.r4),
            ("R5", &r.r5),
            ("R6", &r.r6),
            ("R7", &r.r7),
            ("R8", &r.r8),
            ("R9", &r.r9),
        ];
        let present = all.iter().filter(|(_, v)| v.is_some()).count();
        let mut map = serializer.serialize_map(Some(present))?;
        for (name, value) in all {
            if let Some(value) = value {
                map.serialize_entry(name, value)?;
            }
        }
        map.end()
    }
}

/// A block on the wire: the one a page or a streamed balance is as of, or a key's change.
#[derive(Serialize)]
pub(crate) struct AsOf<'a> {
    height: u32,
    #[serde(rename = "headerId")]
    header_id: &'a HeaderId,
}

impl<'a> From<&'a BlockRef> for AsOf<'a> {
    fn from(block: &'a BlockRef) -> Self {
        Self { height: block.height, header_id: &block.header_id }
    }
}

/// The height a one-off read is as of, with no `headerId`: a balance or a view with the
/// pool is not something to apply deltas to, so it must not look like a WebSocket `from`,
/// which needs both.
#[derive(Serialize)]
struct AsOfHeight {
    height: u32,
}

/// `GET /v1/{addresses,tokens}/.../boxes`: a page of a key's boxes, `nextCursor` `null`
/// exactly on the last page.
pub struct PageBody<'a>(pub &'a BoxPage);

impl Serialize for PageBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let page = self.0;
        let items: Vec<_> = page.items.iter().map(WireBox::from).collect();
        let mut s = serializer.serialize_struct("Page", 3)?;
        s.serialize_field("asOf", &AsOf::from(&page.as_of))?;
        s.serialize_field("items", &items)?;
        s.serialize_field("nextCursor", &page.next_cursor)?;
        s.end()
    }
}

/// `GET /v1/{addresses,tokens}/.../boxes/with-pool`: one list, confirmed boxes first. A pending
/// box is told apart by having no `inclusionHeight`, so clients hand the list to Fleet as it
/// is. `truncated` is always present, and `total` is the whole view's size, so a client can
/// tell a view cut to the limit from a complete one. `asOf` is the height only: the view has
/// the pool applied, so it is no point to stream from.
pub struct WithPoolBody<'a>(pub &'a WithPoolBoxes);

impl Serialize for WithPoolBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let view = self.0;
        let pending = view
            .pending
            .iter()
            .map(|utxo| WireBox { utxo, inclusion_height: None });
        let items: Vec<_> = view
            .confirmed
            .iter()
            .map(WireBox::from)
            .chain(pending)
            .collect();
        let mut s = serializer.serialize_struct("WithPool", 4)?;
        s.serialize_field("asOf", &AsOfHeight { height: view.as_of.height })?;
        s.serialize_field("items", &items)?;
        s.serialize_field("truncated", &view.truncated())?;
        s.serialize_field("total", &view.total)?;
        s.end()
    }
}

/// `GET /v1/addresses/{address}/balance`. The confirmed half is unsigned; the pending half
/// is a signed delta per asset, negative for what is leaving. `used` is REST only: the
/// streamed `balance` shows it turning true as its first non-zero value.
pub struct BalanceBody<'a>(pub &'a AddressBalance);

impl Serialize for BalanceBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let b = self.0;
        let mut s = serializer.serialize_struct("Balance", 4)?;
        s.serialize_field("asOf", &AsOfHeight { height: b.height })?;
        s.serialize_field("confirmed", &Confirmed(&b.confirmed))?;
        s.serialize_field("pending", &Pending(&b.pending))?;
        s.serialize_field("used", &b.used)?;
        s.end()
    }
}

pub(crate) struct Confirmed<'a>(pub &'a Totals);

impl Serialize for Confirmed<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        assets(serializer, self.0.nano_ergs, &self.0.tokens)
    }
}

pub(crate) struct Pending<'a>(pub &'a PendingDelta);

impl Serialize for Pending<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        assets(serializer, self.0.nano_ergs, &self.0.tokens)
    }
}

/// `{ nanoErgs, tokens: [{ tokenId, amount }] }`, tokens in id order.
fn assets<S: Serializer, T: Display + Copy>(
    serializer: S,
    nano_ergs: T,
    tokens: &BTreeMap<TokenId, T>,
) -> Result<S::Ok, S::Error> {
    let tokens: Vec<_> = tokens
        .iter()
        .map(|(id, &amount)| TokenAmount { token_id: id, amount: Amount(amount) })
        .collect();
    let mut s = serializer.serialize_struct("Assets", 2)?;
    s.serialize_field("nanoErgs", &Amount(nano_ergs))?;
    s.serialize_field("tokens", &tokens)?;
    s.end()
}

/// `GET /v1/addresses/{address}/transactions`: one list, newest first, pending txs on top. A
/// pending tx is told apart by having no `height` and no `timestamp`. `delta` is the tx's
/// signed net change for the address, per asset. No `asOf`: the page is not pinned to a
/// block, and each confirmed tx carries its own height.
pub struct HistoryBody<'a>(pub &'a HistoryPage);

impl Serialize for HistoryBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let page = self.0;
        let items: Vec<_> = page.items.iter().map(HistoryEntry).collect();
        let mut s = serializer.serialize_struct("History", 2)?;
        s.serialize_field("items", &items)?;
        s.serialize_field("nextCursor", &page.next_cursor)?;
        s.end()
    }
}

struct HistoryEntry<'a>(&'a HistoryItem);

impl Serialize for HistoryEntry<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let item = self.0;
        let fields = if item.included.is_some() { 4 } else { 2 };
        let mut s = serializer.serialize_struct("HistoryItem", fields)?;
        s.serialize_field("txId", &item.tx_id)?;
        if let Some(included) = item.included {
            s.serialize_field("height", &included.height)?;
            s.serialize_field("timestamp", &included.timestamp)?;
        }
        s.serialize_field("delta", &Pending(&item.delta))?;
        s.end()
    }
}

/// `GET /v1/headers`: the last headers up to the tip, newest first, each the node's JSON as
/// it came, and the tip as `asOf`, as on the other routes (it repeats the first item).
pub struct HeadersBody<'a>(pub &'a HeadersPage);

impl Serialize for HeadersBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let page = self.0;
        let items = page
            .items
            .iter()
            .map(|h| serde_json::from_str::<&RawValue>(&h.json))
            .collect::<Result<Vec<_>, _>>()
            .map_err(S::Error::custom)?;
        let mut s = serializer.serialize_struct("Headers", 2)?;
        s.serialize_field("asOf", &AsOf::from(&page.as_of))?;
        s.serialize_field("items", &items)?;
        s.end()
    }
}

/// `GET /v1/boxes/{boxId}`: the box alone, as one item of a view with the pool, so a pending
/// one has no `inclusionHeight`. No `asOf`: a confirmed box carries its own height.
pub struct BoxBody<'a>(pub &'a FoundBox);

impl Serialize for BoxBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let found = self.0;
        WireBox { utxo: &found.utxo, inclusion_height: found.inclusion_height }
            .serialize(serializer)
    }
}

/// `GET /v1/tokens/metadata`: the known tokens among those asked for, in their order.
pub struct TokensBody<'a>(pub &'a [Arc<TokenMetadata>]);

impl Serialize for TokensBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let items: Vec<_> = self.0.iter().map(|t| WireToken(t)).collect();
        let mut s = serializer.serialize_struct("Tokens", 1)?;
        s.serialize_field("items", &items)?;
        s.end()
    }
}

struct WireToken<'a>(&'a TokenMetadata);

impl Serialize for WireToken<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let t = self.0;
        let mut s = serializer.serialize_struct("Token", 6)?;
        s.serialize_field("tokenId", &t.id)?;
        s.serialize_field("mintingBoxId", &t.minting_box_id)?;
        s.serialize_field("name", &t.name)?;
        s.serialize_field("description", &t.description)?;
        s.serialize_field("decimals", &t.decimals)?;
        s.serialize_field("emission", &Amount(t.emission))?;
        s.end()
    }
}

/// `POST /v1/transactions`: the id of the tx the node took, sent or only checked.
#[derive(Serialize)]
pub struct SubmittedBody<'a> {
    #[serde(rename = "transactionId")]
    pub transaction_id: &'a TxId,
}

/// `GET /v1/status`. `tip` is `null` until the engine has applied a block.
pub struct StatusBody<'a> {
    pub network: Network,
    pub tip: Option<&'a BlockRef>,
    pub ready: bool,
}

impl Serialize for StatusBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("Status", 4)?;
        s.serialize_field("version", env!("CARGO_PKG_VERSION"))?;
        s.serialize_field("network", self.network.name())?;
        s.serialize_field("tip", &self.tip.map(AsOf::from))?;
        s.serialize_field("ready", &self.ready)?;
        s.end()
    }
}

/// An error: the code for programs, the message for people. A refused tx also has the
/// reason, from a fixed set like the code.
#[derive(Serialize)]
pub struct ErrorBody<'a> {
    pub error: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'a str>,
    pub message: &'a str,
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
