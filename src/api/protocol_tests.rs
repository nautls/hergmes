//! The WebSocket's messages, checked on the raw JSON text like the REST wire types: what a
//! client may send, and the shape of every message the relay sends.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use super::*;
use crate::{
    api::{cursor::CursorError, wire::ParametersBody},
    engine::{Balance, Spend, StoredBox, Totals, overlay::PendingDelta},
    sim::{MAINNET_PARAMETERS, testing::digest},
    types::{
        HexBytes,
        ergo::{NonMandatoryRegisters, Token, UTxO},
    },
};

const ADDRESS: &str = "9fRusAarL1KkrWQVsxSRVYnvWxaAT2A96cKtNn9tvPh5XUyCisr";
const FEE_TESTNET: &str = "Bf1X9JgQTUtgntaer91B24n6kP8L2kqEiQqNf1z97BKo9UbnW3WRP9VXu8BXd1LsYCiYbHJEdWKxkF5YNx5n7m31wsDjbEuB3B13ZMDVBWkepGmWfGa71otpFViHDCuvbw1uNicAQnfuWfnj8fbCa4";

fn utxo(n: u8) -> Arc<UTxO> {
    Arc::new(UTxO {
        id: digest(n),
        ergo_tree: HexBytes::from(vec![0x00, 0x08, 0xcd]),
        creation_height: 7,
        value: 1_000_000,
        tokens: vec![Token { id: digest(9), amount: 5 }],
        registers: NonMandatoryRegisters::default(),
        index: 0,
        transaction_id: digest(n + 100),
    })
}

fn wire_box(utxo: &UTxO, height: Option<u32>) -> String {
    serde_json::to_string(&WireBox { utxo, inclusion_height: height }).unwrap()
}

fn hex(n: u8) -> String {
    digest(n).to_string()
}

fn address() -> ClientKey {
    ClientKey::Address(ADDRESS.to_owned())
}

/// [`address`] as every message for it spells it.
fn echo() -> Echo {
    address().echo()
}

/// An output's message, checked to be the same text whether serialized whole, as for a
/// catch-up, or from its shared keyless text, as for a routed output.
fn output(key: &RawValue, output: &Output) -> String {
    let whole = ServerMessage::Output { key, output }.to_text();
    let keyless = keyless(output);
    assert!(!keyless.contains(r#""key""#), "{keyless}");
    assert_eq!(with_key(&keyless, key), whole, "spliced from {keyless}");
    whole
}

fn tracking_key() -> TrackingKey {
    TrackingKey::Token(digest(9))
}

#[test]
fn subscribes_and_unsubscribes_parse() {
    let from = format!(r#"{{"height":12,"headerId":"{}"}}"#, hex(1));
    let cases = [
        (
            format!(r#"{{"op":"subscribe","key":{{"address":"{ADDRESS}"}},"from":{from}}}"#),
            ClientMessage::Subscribe {
                key: address(),
                from: Some(BlockRef { height: 12, header_id: digest(1) }),
                id: None,
            },
        ),
        (
            format!(r#"{{"op":"subscribe","key":{{"address":"{ADDRESS}"}}}}"#),
            ClientMessage::Subscribe { key: address(), from: None, id: None },
        ),
        (
            format!(r#"{{"op":"subscribe","key":{{"address":"{ADDRESS}"}},"from":null}}"#),
            ClientMessage::Subscribe { key: address(), from: None, id: None },
        ),
        (
            format!(r#"{{"key":{{"tokenId":"{}"}},"op":"subscribe","from":{from}}}"#, hex(2)),
            ClientMessage::Subscribe {
                key: ClientKey::TokenId(hex(2)),
                from: Some(BlockRef { height: 12, header_id: digest(1) }),
                id: None,
            },
        ),
        (
            format!(r#"{{"op":"subscribe","key":{{"template":"{}"}},"from":{from}}}"#, hex(3)),
            ClientMessage::Subscribe {
                key: ClientKey::Template(hex(3)),
                from: Some(BlockRef { height: 12, header_id: digest(1) }),
                id: None,
            },
        ),
        (
            format!(r#"{{"op":"subscribe","id":0,"key":{{"address":"{ADDRESS}"}}}}"#),
            ClientMessage::Subscribe { key: address(), from: None, id: Some(0) },
        ),
        (
            format!(
                r#"{{"op":"subscribe","key":{{"tokenId":"{}"}},"from":{from},"id":4294967295}}"#,
                hex(2)
            ),
            ClientMessage::Subscribe {
                key: ClientKey::TokenId(hex(2)),
                from: Some(BlockRef { height: 12, header_id: digest(1) }),
                id: Some(u32::MAX),
            },
        ),
        (
            format!(r#"{{"op":"subscribe","key":{{"address":"{ADDRESS}"}},"id":null}}"#),
            ClientMessage::Subscribe { key: address(), from: None, id: None },
        ),
        (
            format!(r#"{{"op":"unsubscribe","key":{{"address":"{ADDRESS}"}}}}"#),
            ClientMessage::Unsubscribe { key: address() },
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(ClientMessage::parse(&text), Ok(expected), "{text}");
    }
}

/// Malformed: the connection closes with the reason. A key that is well formed but refused
/// (a bad address, a token id of the wrong length) parses, and is `rejected` per key.
#[test]
fn malformed_messages_are_refused_and_keys_are_checked_later() {
    let header = hex(1);
    for text in [
        "",
        "not json",
        "[]",
        r#"{"op":"subscribe"}"#,
        r#"{"key":{"address":"x"}}"#,
        r#"{"op":"subscibe","key":{"address":"x"}}"#,
        r#"{"op":"Subscribe","key":{"address":"x"}}"#,
        r#"{"op":"subscribe","key":{}}"#,
        r#"{"op":"subscribe","key":{"address":"x","tokenId":"y"}}"#,
        r#"{"op":"subscribe","key":{"ergoTree":"0008cd"}}"#,
        r#"{"op":"subscribe","key":{"address":7}}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"extra":1}"#,
        r#"{"op":"unsubscribe","key":{"address":"x"},"from":null}"#,
        r#"{"op":"unsubscribe","key":{"address":"x"},"id":1}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"id":-1}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"id":4294967296}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"id":1.5}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"id":"1"}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"from":{"height":1}}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"from":{"height":-1,"headerId":"00"}}"#,
        r#"{"op":"subscribe","key":{"address":"x"},"from":{"height":1,"headerId":"zz"}}"#,
        &format!(
            r#"{{"op":"subscribe","key":{{"address":"x"}},"from":{{"height":1,"headerId":"{header}","x":0}}}}"#
        ),
        &format!(
            r#"{{"op":"subscribe","key":{{"address":"x"}},"from":{{"height":"1","headerId":"{header}"}}}}"#
        ),
    ] {
        let parsed = ClientMessage::parse(text);
        assert!(parsed.is_err(), "{text} parsed as {parsed:?}");
        assert!(!parsed.unwrap_err().is_empty(), "{text}: a reason");
    }
    for text in [
        r#"{"op":"subscribe","key":{"address":"x"}}"#,
        r#"{"op":"subscribe","key":{"tokenId":"ab"}}"#,
    ] {
        assert!(ClientMessage::parse(text).is_ok(), "{text}");
    }
}

#[test]
fn keys_resolve_through_the_rest_checks() {
    let open = KeyPolicy::default();
    let refused = |key: ClientKey| key.resolve(Network::Mainnet, &open).unwrap_err().code();
    assert_eq!(refused(ClientKey::Address("x".into())), "invalid_address");
    assert_eq!(refused(ClientKey::Address(FEE_TESTNET.into())), "invalid_address", "testnet");
    assert!(address().resolve(Network::Mainnet, &open).is_ok());
    assert_eq!(refused(ClientKey::TokenId("ab".into())), "invalid_token_id");
    let token = ClientKey::TokenId(hex(2).to_uppercase());
    assert_eq!(token.resolve(Network::Mainnet, &open), Ok(TrackingKey::Token(digest(2))));
    let none = KeyPolicy::new(None, [], Some(&[]), Some(&[]), &[]);
    assert_eq!(token.resolve(Network::Mainnet, &none).unwrap_err().code(), "key_not_allowed");
    assert_eq!(refused(ClientKey::Template("a".into())), "invalid_template");
    let template = ClientKey::Template(hex(3));
    assert_eq!(
        template.resolve(Network::Mainnet, &open),
        Ok(TrackingKey::Template(HexBytes::from(digest(3).0.to_vec())))
    );
    assert_eq!(
        template
            .resolve(Network::Mainnet, &none)
            .unwrap_err()
            .code(),
        "key_not_allowed"
    );
}

#[test]
fn a_block_carries_its_boxes_with_its_height_and_the_key_as_sent() {
    let created = utxo(1);
    let key = ClientKey::TokenId(hex(9).to_uppercase());
    let block = Output::Block {
        key: tracking_key(),
        block: BlockRef { height: 12, header_id: digest(20) },
        prev: BlockRef { height: 7, header_id: digest(21) },
        created: vec![created.clone()],
        spent: vec![Spend { box_id: digest(3), tx_id: digest(4) }],
    };
    let expected = format!(
        concat!(
            r#"{{"type":"block","key":{{"tokenId":"{}"}},"height":12,"headerId":"{}","#,
            r#""prev":{{"height":7,"headerId":"{}"}},"created":[{}],"#,
            r#""spent":[{{"boxId":"{}","txId":"{}"}}]}}"#
        ),
        hex(9).to_uppercase(),
        hex(20),
        hex(21),
        wire_box(&created, Some(12)),
        hex(3),
        hex(4),
    );
    assert_eq!(output(&key.echo(), &block), expected);
}

#[test]
fn a_rollback_restores_boxes_with_the_heights_that_included_them() {
    let restored = utxo(1);
    let rollback = Output::Rollback {
        key: tracking_key(),
        block: BlockRef { height: 12, header_id: digest(20) },
        prev: BlockRef { height: 7, header_id: digest(21) },
        created: vec![StoredBox { inclusion_height: 5, utxo: restored.clone() }],
        spent: vec![Spend { box_id: digest(3), tx_id: digest(4) }],
    };
    let expected = format!(
        concat!(
            r#"{{"type":"rollback","key":{{"address":"{}"}},"height":12,"headerId":"{}","#,
            r#""prev":{{"height":7,"headerId":"{}"}},"created":[{}],"#,
            r#""spent":[{{"boxId":"{}","txId":"{}"}}]}}"#
        ),
        ADDRESS,
        hex(20),
        hex(21),
        wire_box(&restored, Some(5)),
        hex(3),
        hex(4),
    );
    assert_eq!(output(&echo(), &rollback), expected);
}

#[test]
fn a_mempool_overlay_has_no_inclusion_heights_and_spends_by_id() {
    let created = utxo(1);
    let mempool = Output::Mempool {
        key: tracking_key(),
        pending: vec![PendingTx {
            tx_id: digest(30),
            created: vec![created.clone()],
            spent: vec![digest(3)],
        }],
        last_change: Some(BlockRef { height: 7, header_id: digest(21) }),
    };
    let expected = format!(
        concat!(
            r#"{{"type":"mempool","key":{{"address":"{}"}},"#,
            r#""lastChange":{{"height":7,"headerId":"{}"}},"#,
            r#""pending":[{{"txId":"{}","created":[{}],"spent":["{}"]}}]}}"#
        ),
        ADDRESS,
        hex(21),
        hex(30),
        wire_box(&created, None),
        hex(3),
    );
    assert_eq!(output(&echo(), &mempool), expected);
    assert!(!expected.contains("inclusionHeight"));
}

#[test]
fn a_balance_is_as_of_a_height_and_header_with_string_amounts() {
    let balance = Output::Balance {
        key: tracking_key(),
        balance: Balance {
            as_of: BlockRef { height: 12, header_id: digest(20) },
            confirmed: Totals { nano_ergs: u64::MAX, tokens: BTreeMap::from([(digest(9), 5)]) },
            pending: PendingDelta {
                nano_ergs: -(1i128 << 70),
                tokens: BTreeMap::from([(digest(9), -5)]),
            },
        },
        last_change: Some(BlockRef { height: 7, header_id: digest(21) }),
    };
    let expected = format!(
        concat!(
            r#"{{"type":"balance","key":{{"address":"{}"}},"#,
            r#""asOf":{{"height":12,"headerId":"{}"}},"lastChange":{{"height":7,"headerId":"{}"}},"#,
            r#""confirmed":{{"nanoErgs":"18446744073709551615","tokens":[{{"tokenId":"{}","amount":"5"}}]}},"#,
            r#""pending":{{"nanoErgs":"-1180591620717411303424","tokens":[{{"tokenId":"{}","amount":"-5"}}]}}}}"#
        ),
        ADDRESS,
        hex(20),
        hex(21),
        hex(9),
        hex(9),
    );
    assert_eq!(output(&echo(), &balance), expected);
}

#[test]
fn resyncs_name_their_reason() {
    let expected = |reason: &str| {
        format!(
            r#"{{"type":"resync_required","key":{{"address":"{ADDRESS}"}},"reason":"{reason}"}}"#
        )
    };
    let routed =
        Output::ResyncRequired { key: tracking_key(), reason: ResyncReason::ForkBeyondWindow };
    assert_eq!(output(&echo(), &routed), expected("fork_beyond_window"));
    for (reason, code) in [
        (ResyncReason::OutsideWindow, "outside_window"),
        (ResyncReason::NotTracked, "not_tracked"),
        (ResyncReason::SlowConsumer, "slow_consumer"),
    ] {
        let text = ServerMessage::Resync { key: &echo(), reason, id: None }.to_text();
        assert_eq!(text, expected(code));
    }
}

/// `retryAfter` exactly where REST sends `Retry-After`, with the same whole seconds.
#[test]
fn rejections_carry_retry_after_only_when_it_is_worth_waiting() {
    let rejected = |reject: Reject| {
        ServerMessage::Rejected { key: &echo(), reject: &reject, id: None }.to_text()
    };
    let expected = |tail: &str| {
        format!(r#"{{"type":"rejected","key":{{"address":"{ADDRESS}"}},"reason":{tail}}}"#)
    };
    let cases = [
        (Reject::Api(ApiError::CapacityExceeded), r#""capacity_exceeded","retryAfter":120"#),
        (
            Reject::Api(ApiError::NotReady { retry_after: Duration::from_millis(1500) }),
            r#""not_ready","retryAfter":2"#,
        ),
        (
            Reject::Api(ApiError::NodeUnavailable { retry_after: None }),
            r#""node_unavailable","retryAfter":1"#,
        ),
        (Reject::Api(ApiError::NodeError("http://10.0.0.1 timed out".into())), r#""node_error""#),
        (Reject::Api(ApiError::InvalidAddress("bad".into())), r#""invalid_address""#),
        (Reject::Api(ApiError::KeyNotAllowed), r#""key_not_allowed""#),
        (Reject::Api(ApiError::TooManyBoxes { max: 5 }), r#""too_many_boxes""#),
        (Reject::Api(ApiError::InvalidCursor(CursorError::Malformed)), r#""invalid_cursor""#),
        (Reject::TooManySubscriptions, r#""too_many_subscriptions""#),
        (Reject::FromRequired, r#""from_required""#),
    ];
    for (reject, tail) in cases {
        assert_eq!(rejected(reject), expected(tail));
    }
}

#[test]
fn the_tip_has_no_key() {
    let tip = BlockRef { height: 9, header_id: hex(3).parse().unwrap() };
    let text = ServerMessage::Tip { tip, parameters: None }.to_text();
    assert_eq!(text, format!(r#"{{"type":"tip","height":9,"headerId":"{}"}}"#, hex(3)));
}

#[test]
fn a_tip_carries_the_parameters_as_rest_serves_them() {
    let tip = BlockRef { height: 9, header_id: hex(3).parse().unwrap() };
    let text = ServerMessage::Tip { tip, parameters: Some(MAINNET_PARAMETERS) }.to_text();
    let message: serde_json::Value = serde_json::from_str(&text).unwrap();
    let rest = serde_json::to_value(ParametersBody(&MAINNET_PARAMETERS)).unwrap();
    assert_eq!(message["parameters"], rest);
    assert_eq!(message["height"], 9);
}

#[test]
fn subscribed_names_the_key_as_sent() {
    let text = ServerMessage::Subscribed { key: &echo(), id: None }.to_text();
    assert_eq!(text, format!(r#"{{"type":"subscribed","key":{{"address":"{ADDRESS}"}}}}"#));
}

/// A subscribe's answers carry its `id` right after `type`, and only when it had one.
#[test]
fn answers_carry_their_subscribes_id() {
    let key = &echo();
    let reject = Reject::Api(ApiError::NotReady { retry_after: Duration::from_secs(2) });
    let reason = ResyncReason::NotTracked;
    let cases = [
        (ServerMessage::Subscribed { key, id: Some(7) }, r#""type":"subscribed","id":7,"#, ""),
        (
            ServerMessage::Resync { key, reason, id: Some(7) },
            r#""type":"resync_required","id":7,"#,
            r#","reason":"not_tracked""#,
        ),
        (
            ServerMessage::Rejected { key, reject: &reject, id: Some(0) },
            r#""type":"rejected","id":0,"#,
            r#","reason":"not_ready","retryAfter":2"#,
        ),
    ];
    for (message, head, tail) in cases {
        let expected = format!(r#"{{{head}"key":{{"address":"{ADDRESS}"}}{tail}}}"#);
        assert_eq!(message.to_text(), expected);
    }
}
