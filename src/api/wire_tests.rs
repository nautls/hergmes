//! The wire shape, checked on the raw JSON text: a test that parsed the body back would
//! read a `u64` number exactly and pass with amounts written as numbers.

use std::{collections::BTreeMap, sync::Arc};

use super::*;
use crate::{
    sim::testing::digest,
    types::ergo::{NonMandatoryRegisters, Token},
};

/// 2^53 + 1, the first integer a JavaScript number can't hold.
const ABOVE_2_53: u64 = 9_007_199_254_740_993;

fn utxo() -> UTxO {
    UTxO {
        id: digest(1),
        ergo_tree: HexBytes(vec![0x00, 0x08, 0xcd]),
        creation_height: 7,
        value: ABOVE_2_53,
        tokens: vec![
            Token { id: digest(2), amount: i64::MAX as u64 },
            Token { id: digest(3), amount: 1 },
        ],
        registers: NonMandatoryRegisters {
            r4: Some(HexBytes(vec![0x0e, 0x01])),
            r6: Some(HexBytes(vec![0x04, 0x02])),
            ..Default::default()
        },
        index: 3,
        transaction_id: digest(4),
    }
}

fn json(value: impl Serialize) -> String {
    serde_json::to_string(&value).unwrap()
}

fn hex(n: u8) -> String {
    digest(n).to_string()
}

#[test]
fn a_box_is_in_fleets_shape_with_string_amounts_and_present_registers_only() {
    let utxo = utxo();
    let expected = format!(
        concat!(
            r#"{{"boxId":"{}","transactionId":"{}","index":3,"ergoTree":"0008cd","#,
            r#""creationHeight":7,"value":"9007199254740993","#,
            r#""assets":[{{"tokenId":"{}","amount":"9223372036854775807"}},"#,
            r#"{{"tokenId":"{}","amount":"1"}}],"#,
            r#""additionalRegisters":{{"R4":"0e01","R6":"0402"}},"inclusionHeight":12}}"#
        ),
        hex(1),
        hex(4),
        hex(2),
        hex(3),
    );
    assert_eq!(json(WireBox { utxo: &utxo, inclusion_height: Some(12) }), expected);
}

#[test]
fn an_amount_above_2_53_round_trips_exactly() {
    let utxo = utxo();
    let value: serde_json::Value =
        serde_json::from_str(&json(WireBox { utxo: &utxo, inclusion_height: None })).unwrap();
    let amount = value["value"].as_str().expect("a string");
    assert_eq!(amount.parse::<u64>().unwrap(), ABOVE_2_53);
    // Read as a JavaScript number would be, it loses the last unit.
    assert_ne!(ABOVE_2_53 as f64 as u64, ABOVE_2_53);
}

#[test]
fn a_mempool_box_has_no_inclusion_height_and_no_registers_is_an_empty_object() {
    let utxo = UTxO { registers: NonMandatoryRegisters::default(), ..utxo() };
    let text = json(WireBox { utxo: &utxo, inclusion_height: None });
    assert!(!text.contains("inclusionHeight"), "{text}");
    assert!(text.contains(r#""additionalRegisters":{}"#), "{text}");
}

#[test]
fn a_page_has_its_block_and_a_null_cursor_on_the_last_page() {
    let as_of = BlockRef { height: 12, header_id: digest(9) };
    let mut page = BoxPage {
        as_of,
        items: vec![StoredBox { utxo: Arc::new(utxo()), inclusion_height: 12 }],
        next_cursor: None,
    };
    let text = json(PageBody(&page));
    let start = format!(r#"{{"asOf":{{"height":12,"headerId":"{}"}},"items":[{{"#, hex(9));
    assert!(text.starts_with(&start), "{text}");
    assert!(text.ends_with(r#""inclusionHeight":12}],"nextCursor":null}"#), "{text}");

    page.next_cursor = Some("abc".to_owned());
    assert!(json(PageBody(&page)).ends_with(r#""nextCursor":"abc"}"#));
}

#[test]
fn a_balance_has_string_amounts_signed_pending_and_tokens_in_id_order() {
    let balance = AddressBalance {
        height: 5,
        confirmed: Totals {
            nano_ergs: ABOVE_2_53,
            tokens: BTreeMap::from([(digest(3), 1), (digest(2), i64::MAX as u64)]),
        },
        pending: PendingDelta {
            nano_ergs: -10_001_000_000,
            tokens: BTreeMap::from([(digest(2), -(i64::MAX as i128) * 2)]),
        },
        used: true,
    };
    let expected = format!(
        concat!(
            r#"{{"asOf":{{"height":5}},"#,
            r#""confirmed":{{"nanoErgs":"9007199254740993","tokens":["#,
            r#"{{"tokenId":"{}","amount":"9223372036854775807"}},"#,
            r#"{{"tokenId":"{}","amount":"1"}}]}},"#,
            r#""pending":{{"nanoErgs":"-10001000000","tokens":["#,
            r#"{{"tokenId":"{}","amount":"-18446744073709551614"}}]}},"used":true}}"#
        ),
        hex(2),
        hex(3),
        hex(2),
    );
    assert_eq!(json(BalanceBody(&balance)), expected);
}

#[test]
fn an_empty_balance_has_zero_and_no_tokens() {
    let balance = AddressBalance {
        height: 1,
        confirmed: Totals::default(),
        pending: PendingDelta::default(),
        used: false,
    };
    assert_eq!(
        json(BalanceBody(&balance)),
        concat!(
            r#"{"asOf":{"height":1},"confirmed":{"nanoErgs":"0","tokens":[]},"#,
            r#""pending":{"nanoErgs":"0","tokens":[]},"used":false}"#
        )
    );
}

#[test]
fn status_names_the_network_and_has_a_null_tip_before_the_first_block() {
    let version = env!("CARGO_PKG_VERSION");
    let status = StatusBody { network: Network::Testnet, tip: None, ready: false };
    assert_eq!(
        json(status),
        format!(r#"{{"version":"{version}","network":"testnet","tip":null,"ready":false}}"#)
    );

    let tip = BlockRef { height: 3, header_id: digest(9) };
    let status = StatusBody { network: Network::Mainnet, tip: Some(&tip), ready: true };
    assert_eq!(
        json(status),
        format!(
            r#"{{"version":"{version}","network":"mainnet","tip":{{"height":3,"headerId":"{}"}},"ready":true}}"#,
            hex(9)
        )
    );
}

#[test]
fn a_view_with_the_pool_lists_confirmed_boxes_then_pending_ones_and_says_if_it_was_cut() {
    let confirmed = Arc::new(utxo());
    let pending = Arc::new(UTxO { id: digest(9), ..utxo() });
    let view = WithPoolBoxes {
        as_of: BlockRef { height: 12, header_id: digest(5) },
        confirmed: vec![StoredBox { utxo: confirmed.clone(), inclusion_height: 11 }],
        pending: vec![pending.clone()],
        total: 2,
    };
    let body = |truncated: bool, total: usize| {
        format!(
            r#"{{"asOf":{{"height":12}},"items":[{},{}],"truncated":{truncated},"total":{total}}}"#,
            json(WireBox { utxo: &confirmed, inclusion_height: Some(11) }),
            json(WireBox { utxo: &pending, inclusion_height: None }),
        )
    };
    assert_eq!(json(WithPoolBody(&view)), body(false, 2));
    let cut = WithPoolBoxes { total: 3, ..view };
    assert_eq!(json(WithPoolBody(&cut)), body(true, 3));
}

/// A history: pending txs with no `height` or `timestamp`, signed amounts as strings, and no
/// `asOf`.
#[test]
fn a_history_page() {
    use crate::api::history::{HistoryItem, HistoryPage, Included};
    let page = HistoryPage {
        items: vec![
            HistoryItem {
                tx_id: digest(1),
                included: None,
                delta: PendingDelta {
                    nano_ergs: -(ABOVE_2_53 as i128),
                    tokens: BTreeMap::from([(digest(2), 3)]),
                },
            },
            HistoryItem {
                tx_id: digest(4),
                included: Some(Included { height: 12, timestamp: 1_731_600_000_000 }),
                delta: PendingDelta::default(),
            },
        ],
        next_cursor: Some("abc".to_owned()),
    };
    assert_eq!(
        json(HistoryBody(&page)),
        format!(
            concat!(
                r#"{{"items":["#,
                r#"{{"txId":"{tx1}","delta":{{"nanoErgs":"-9007199254740993","tokens":[{{"tokenId":"{token}","amount":"3"}}]}}}},"#,
                r#"{{"txId":"{tx4}","height":12,"timestamp":1731600000000,"delta":{{"nanoErgs":"0","tokens":[]}}}}"#,
                r#"],"nextCursor":"abc"}}"#
            ),
            tx1 = hex(1),
            token = hex(2),
            tx4 = hex(4),
        )
    );
    let last = HistoryPage { items: Vec::new(), next_cursor: None };
    assert_eq!(json(HistoryBody(&last)), r#"{"items":[],"nextCursor":null}"#);
}
