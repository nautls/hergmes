//! The WebSocket's messages (DESIGN.md, "WebSocket: deltas"): what a client sends, parsed
//! strictly, and what the relay sends, in the shapes of the REST wire types.
//!
//! Every message the relay sends names its key as the client spelled it, so a client that
//! subscribed with a P2S encoding of a P2PK tree gets that encoding back.

use std::time::Duration;

use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use serde_json::value::{RawValue, to_raw_value};

use crate::{
    api::{
        ApiError, KeyPolicy,
        http::seconds,
        keys::{address_key, token_key},
        wire::{AsOf, Confirmed, Pending, WireBox},
    },
    engine::{BlockRef, Output, PendingTx, ResyncReason, TrackingKey},
    types::{
        address::Network,
        ergo::{BoxId, HeaderId},
    },
};

/// A key as the client wrote it: `{ "address": ... }` or `{ "tokenId": ... }`, exactly one.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum ClientKey {
    #[serde(rename = "address")]
    Address(String),
    #[serde(rename = "tokenId")]
    TokenId(String),
}

/// A key as every message for it spells it: the client's spelling, as JSON. Serialized once
/// per subscribe, and copied into each message for the key.
pub type Echo = Box<RawValue>;

impl ClientKey {
    /// The key as every message for it spells it.
    pub fn echo(&self) -> Echo {
        to_raw_value(self).expect("a key serializes")
    }

    /// The tracking key, through the same checks as a REST route: the network, the token id's
    /// form and the operator's policy.
    pub fn resolve(&self, network: Network, policy: &KeyPolicy) -> Result<TrackingKey, ApiError> {
        match self {
            Self::Address(address) => address_key(address, network, policy),
            Self::TokenId(id) => token_key(id, policy),
        }
    }
}

/// What a client sends.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum ClientMessage {
    /// With `from`, the key's boxes from that block on; without, its balance only.
    Subscribe {
        key: ClientKey,
        #[serde(default)]
        from: Option<BlockRef>,
        /// The client's name for this subscribe, echoed in its answer, so an answer to an
        /// older subscribe of the key is never taken for the newer one's.
        #[serde(default)]
        id: Option<SubscribeId>,
    },
    Unsubscribe {
        key: ClientKey,
    },
}

/// A subscribe's id, chosen by the client. An integer that JavaScript holds exactly.
pub type SubscribeId = u32;

impl ClientMessage {
    /// Parses a client's text message. A message that does not parse is malformed: the
    /// error says why, and the connection is closed with it.
    pub fn parse(text: &str) -> Result<Self, String> {
        serde_json::from_str(text).map_err(|e| e.to_string())
    }
}

/// Why a subscribe was refused: REST's codes, and two only the WebSocket has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    Api(ApiError),
    /// The connection holds as many subscriptions as it may.
    TooManySubscriptions,
    /// A token key has no balance, so it is subscribed only from a snapshot's block.
    FromRequired,
}

impl Reject {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Api(error) => error.code(),
            Self::TooManySubscriptions => "too_many_subscriptions",
            Self::FromRequired => "from_required",
        }
    }

    /// The wait before subscribing again, on the codes REST answers with a `503`.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Api(error) => error.retry_after(),
            Self::TooManySubscriptions | Self::FromRequired => None,
        }
    }
}

/// What the relay sends: the start of a subscription, a key's output, a `resync_required` of
/// the relay's own, or a refusal.
///
/// `subscribed`, and `resync_required` and `rejected` when they answer a subscribe, carry the
/// subscribe's `id` if it had one. A subscribe's answer may come after the client sent a
/// newer message for the key, and the id tells it apart from the newer subscribe's.
pub enum ServerMessage<'a> {
    /// A subscribe took effect: what follows for the key belongs to it. It marks where the
    /// new subscription starts, since after a replace or an unsubscribe the old one's
    /// messages may still be on their way, and every message names only the key.
    Subscribed {
        key: &'a RawValue,
        id: Option<SubscribeId>,
    },
    Output {
        key: &'a RawValue,
        output: &'a Output,
    },
    Resync {
        key: &'a RawValue,
        reason: ResyncReason,
        id: Option<SubscribeId>,
    },
    Rejected {
        key: &'a RawValue,
        reject: &'a Reject,
        id: Option<SubscribeId>,
    },
}

impl ServerMessage<'_> {
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).expect("a message serializes")
    }
}

/// An output's message with no `key`: the part every subscriber of the key shares. A
/// routed output goes to every subscriber of its key, on as many connections, and only the
/// key's spelling differs between them, so the body is serialized once (see
/// [`with_key`]).
pub fn keyless(output: &Output) -> Box<str> {
    struct Keyless<'a>(&'a Output);
    impl Serialize for Keyless<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serialize_output(serializer, None, self.0)
        }
    }
    serde_json::to_string(&Keyless(output))
        .expect("an output serializes")
        .into()
}

/// The text of [`ServerMessage::Output`] for `key`, from the output's [`keyless`] text:
/// the key goes right after `type`, where the message has it.
pub fn with_key(keyless: &str, key: &RawValue) -> String {
    const TYPE: &str = r#"{"type":""#;
    debug_assert!(keyless.starts_with(TYPE), "{keyless}");
    // A type is a fixed word, so its value ends at the next quote.
    let at = TYPE.len() + keyless[TYPE.len()..].find('"').expect("a closed type") + 1;
    let key = key.get();
    let mut text = String::with_capacity(keyless.len() + r#","key":"#.len() + key.len());
    text.push_str(&keyless[..at]);
    text.push_str(r#","key":"#);
    text.push_str(key);
    text.push_str(&keyless[at..]);
    text
}

impl Serialize for ServerMessage<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Output { key, output } => serialize_output(serializer, Some(key), output),
            Self::Subscribed { key, id } => {
                let s = start(serializer, "Subscribed", "subscribed", *id, Some(key), 0)?;
                s.end()
            }
            Self::Resync { key, reason, id } => {
                let name = "ResyncRequired";
                let mut s = start(serializer, name, "resync_required", *id, Some(key), 1)?;
                s.serialize_field("reason", reason)?;
                s.end()
            }
            Self::Rejected { key, reject, id } => {
                let retry_after = reject.retry_after().map(seconds);
                let fields = 1 + usize::from(retry_after.is_some());
                let mut s = start(serializer, "Rejected", "rejected", *id, Some(key), fields)?;
                s.serialize_field("reason", reject.code())?;
                if let Some(retry_after) = retry_after {
                    s.serialize_field("retryAfter", &retry_after)?;
                }
                s.end()
            }
        }
    }
}

/// A message's `type`; the subscribe's `id`, if the message answers a subscribe that had one;
/// and, unless the message is [`keyless`], its `key`. `fields` more are to come.
fn start<S: Serializer>(
    serializer: S,
    name: &'static str,
    kind: &'static str,
    id: Option<SubscribeId>,
    key: Option<&RawValue>,
    fields: usize,
) -> Result<S::SerializeStruct, S::Error> {
    let len = 1 + usize::from(id.is_some()) + usize::from(key.is_some()) + fields;
    let mut s = serializer.serialize_struct(name, len)?;
    s.serialize_field("type", kind)?;
    if let Some(id) = id {
        s.serialize_field("id", &id)?;
    }
    if let Some(key) = key {
        s.serialize_field("key", key)?;
    }
    Ok(s)
}

fn serialize_output<S: Serializer>(
    serializer: S,
    key: Option<&RawValue>,
    output: &Output,
) -> Result<S::Ok, S::Error> {
    match output {
        Output::Block { block, parent_id, created, spent, .. } => {
            let created: Vec<_> = created
                .iter()
                .map(|utxo| WireBox { utxo, inclusion_height: Some(block.height) })
                .collect();
            delta(serializer, "block", key, block, parent_id, &created, spent)
        }
        Output::Rollback { block, parent_id, created, spent, .. } => {
            let created: Vec<_> = created.iter().map(WireBox::from).collect();
            delta(serializer, "rollback", key, block, parent_id, &created, spent)
        }
        Output::Mempool { tip_height, pending, .. } => {
            let pending: Vec<_> = pending.iter().map(WirePendingTx).collect();
            let mut s = start(serializer, "Mempool", "mempool", None, key, 2)?;
            s.serialize_field("tipHeight", tip_height)?;
            s.serialize_field("pending", &pending)?;
            s.end()
        }
        Output::Balance { balance, .. } => {
            let mut s = start(serializer, "Balance", "balance", None, key, 3)?;
            s.serialize_field("asOf", &AsOf::from(&balance.as_of))?;
            s.serialize_field("confirmed", &Confirmed(&balance.confirmed))?;
            s.serialize_field("pending", &Pending(&balance.pending))?;
            s.end()
        }
        Output::ResyncRequired { reason, .. } => {
            let mut s = start(serializer, "ResyncRequired", "resync_required", None, key, 1)?;
            s.serialize_field("reason", reason)?;
            s.end()
        }
        Output::LoadRejected { .. } | Output::CapacityExceeded { .. } => {
            unreachable!("a load's answer goes to the loader, never to a subscriber")
        }
    }
}

fn delta<S: Serializer>(
    serializer: S,
    kind: &'static str,
    key: Option<&RawValue>,
    block: &BlockRef,
    parent_id: &HeaderId,
    created: &[WireBox<'_>],
    spent: &[crate::engine::Spend],
) -> Result<S::Ok, S::Error> {
    let mut s = start(serializer, "Delta", kind, None, key, 5)?;
    s.serialize_field("height", &block.height)?;
    s.serialize_field("headerId", &block.header_id)?;
    s.serialize_field("parentId", parent_id)?;
    s.serialize_field("created", created)?;
    s.serialize_field("spent", spent)?;
    s.end()
}

/// A pending tx as it affects one key: its outputs to the key, with no `inclusionHeight`,
/// and the key's boxes it spends, by id.
struct WirePendingTx<'a>(&'a PendingTx);

impl Serialize for WirePendingTx<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let tx = self.0;
        let created: Vec<_> = tx
            .created
            .iter()
            .map(|utxo| WireBox { utxo, inclusion_height: None })
            .collect();
        let spent: &[BoxId] = &tx.spent;
        let mut s = serializer.serialize_struct("PendingTx", 3)?;
        s.serialize_field("txId", &tx.tx_id)?;
        s.serialize_field("created", &created)?;
        s.serialize_field("spent", spent)?;
        s.end()
    }
}

#[cfg(test)]
#[path = "protocol_tests.rs"]
mod tests;
