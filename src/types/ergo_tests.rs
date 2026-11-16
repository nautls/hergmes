//! The header type against the recorded fixtures: what it reads, and the JSON it keeps.

use serde_json::Value;

use super::*;
use crate::fixtures;

const ID: &str = "41f89ff983dc639d0950522e819012854bbcbecdc41c0739c685b7e53fb27a02";

#[test]
fn compact_drops_whitespace_between_tokens_only() {
    let json = "{ \"a\" : [ 1 ,\n\t2 ],\r\n \"b\": \"x y\\\" z\" , \"c\" : \"\\\\\" }";
    assert_eq!(compact(json), r#"{"a":[1,2],"b":"x y\" z","c":"\\"}"#);
}

#[test]
fn a_header_by_id_keeps_the_node_json_compacted() {
    let text = fixtures::text("header_by_id");
    let header: BlockHeader = serde_json::from_str(&text).unwrap();
    assert_eq!(header.id.to_string(), fixtures::sample("headerId"));
    assert_eq!(header.id.to_string(), ID);
    assert_eq!(header.height, 1915472);
    assert_eq!(
        header.parent_id.to_string(),
        "482ffa5aad2077b2e8924d8cbca8d6f3f21818c25cb8828f25e071efc4d9f6aa"
    );
    assert!(!header.json.contains(char::is_whitespace), "{}", header.json);
    // Every field, as the node sent it, in its order.
    let sent: Value = serde_json::from_str(&text).unwrap();
    let kept: Value = serde_json::from_str(&header.json).unwrap();
    assert_eq!(kept, sent);
    // `Value` sorts keys, so the order is checked on the text.
    assert!(header.json.starts_with(r#"{"extensionId":"#), "{}", header.json);
}

#[test]
fn an_indexed_block_carries_the_same_header_but_its_size() {
    let block: IndexedBlock = fixtures::load("indexed_block");
    let by_id: BlockHeader = fixtures::load("header_by_id");
    assert_eq!((block.header.id, block.header.parent_id), (by_id.id, by_id.parent_id));
    let mut indexed: Value = serde_json::from_str(&block.header.json).unwrap();
    let mut read: Value = serde_json::from_str(&by_id.json).unwrap();
    // `HeaderSerializer.parse` counts the type byte a database read consumed before it.
    assert_eq!(indexed["size"].as_u64().unwrap() + 1, read["size"].as_u64().unwrap());
    indexed["size"] = Value::Null;
    read["size"] = Value::Null;
    assert_eq!(indexed, read);
}

#[test]
fn numbers_are_kept_exactly() {
    let text = format!(
        r#"{{ "id": "{ID}", "parentId": "{ID}", "height": 7,
             "powSolutions": {{ "d": 123456789012345678901234567890123 }} }}"#
    );
    let header: BlockHeader = serde_json::from_str(&text).unwrap();
    assert!(
        header
            .json
            .contains(r#""d":123456789012345678901234567890123"#),
        "{}",
        header.json
    );
}

#[test]
fn a_header_missing_a_field_the_relay_reads_is_refused() {
    let text = format!(r#"{{ "id": "{ID}", "height": 7 }}"#);
    assert!(serde_json::from_str::<BlockHeader>(&text).is_err());
}
