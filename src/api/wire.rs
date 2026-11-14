//! What the relay sends, as JSON (DESIGN.md, "REST: confirmed snapshot" and "Balances").
//!
//! These types borrow what the services return and serialize it in the client's shape. They
//! are separate from the node-facing types, whose derived `Serialize` writes amounts as
//! numbers and absent registers as `null`.
//!
//! Amounts are base-10 integer strings: ERG supply in nanoErgs and token amounts exceed 2^53,
//! which a JSON number does not carry exactly in JavaScript.

use std::{collections::BTreeMap, fmt::Display};

use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeStruct},
};

use crate::{
    api::{AddressBalance, BoxPage},
    engine::{BlockRef, Totals, overlay::PendingDelta},
    types::{
        HexBytes,
        address::Network,
        ergo::{HeaderId, NonMandatoryRegisters, TokenId, UTxO},
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
/// boxes carry the height of the block that included them; mempool outputs (step 9) do not.
pub struct WireBox<'a> {
    pub utxo: &'a UTxO,
    pub inclusion_height: Option<u32>,
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

/// The block a page is pinned to.
#[derive(Serialize)]
struct AsOf<'a> {
    height: u32,
    #[serde(rename = "headerId")]
    header_id: &'a HeaderId,
}

impl<'a> From<&'a BlockRef> for AsOf<'a> {
    fn from(block: &'a BlockRef) -> Self {
        Self { height: block.height, header_id: &block.header_id }
    }
}

/// `GET /v1/{addresses,tokens}/.../boxes`: a page of a key's boxes, `nextCursor` `null`
/// exactly on the last page.
pub struct PageBody<'a>(pub &'a BoxPage);

impl Serialize for PageBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let page = self.0;
        let items: Vec<_> = page
            .items
            .iter()
            .map(|b| WireBox { utxo: &b.utxo, inclusion_height: Some(b.inclusion_height) })
            .collect();
        let mut s = serializer.serialize_struct("Page", 3)?;
        s.serialize_field("asOf", &AsOf::from(&page.as_of))?;
        s.serialize_field("items", &items)?;
        s.serialize_field("nextCursor", &page.next_cursor)?;
        s.end()
    }
}

/// `GET /v1/addresses/{address}/balance`. The confirmed half is unsigned; the pending half
/// is a signed delta per asset, negative for what is leaving.
pub struct BalanceBody<'a>(pub &'a AddressBalance);

impl Serialize for BalanceBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Height {
            height: u32,
        }
        let b = self.0;
        let mut s = serializer.serialize_struct("Balance", 3)?;
        s.serialize_field("asOf", &Height { height: b.height })?;
        s.serialize_field("confirmed", &Confirmed(&b.confirmed))?;
        s.serialize_field("pending", &Pending(&b.pending))?;
        s.end()
    }
}

struct Confirmed<'a>(&'a Totals);

impl Serialize for Confirmed<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        assets(serializer, self.0.nano_ergs, &self.0.tokens)
    }
}

struct Pending<'a>(&'a PendingDelta);

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

/// An error: the code for programs, the message for people.
#[derive(Serialize)]
pub struct ErrorBody<'a> {
    pub error: &'a str,
    pub message: &'a str,
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
