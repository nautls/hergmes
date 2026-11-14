//! The key policy: classes judged by tree, deny winning, `none`, and configured keys always
//! admitted.

use blake2::{Blake2b256, Digest};

use super::*;
use crate::{
    api::{
        ApiError,
        keys::{address_key, token_key},
    },
    types::{
        HexBytes,
        address::{Network, encode},
    },
};

const P2PK_TREE: &str = "0008cd0278011ec0cf5feb92d61adb51dcb75876627ace6fd9446ab4cabc5313ab7b39a7";
const P2SH_TREE: &str =
    "00ea02d193b4cbe4e37e0e040004300e18fd53c43ebbc8b5c53f2ccf270d1bc22740eb3855463f5faed4087e";
/// The miner fee contract.
const P2S_TREE: &str = "1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304";
const NFT: &str = "20fa2bf23962cdf51b07722d6237c0c7b8a44f78856c0f7ec308dc1ef1a92a51";
const TOKEN: &str = "8c27dd9d8a35aac1e3167d58858c0a8b4059b277da790552e37eba22df9b9035";

fn tree(hex: &str) -> TrackingKey {
    TrackingKey::Tree(HexBytes(hex::decode(hex).unwrap()))
}

fn token(id: &str) -> TrackingKey {
    TrackingKey::Token(id.parse().unwrap())
}

/// The address the relay's encoder gives a tree: P2PK and P2SH trees get their own type.
fn address(hex: &str) -> String {
    encode(&hex::decode(hex).unwrap(), Network::Mainnet)
}

/// A mainnet P2S address carrying any tree, as a client can write one for a P2PK or P2SH
/// tree.
fn as_p2s(hex: &str) -> String {
    let mut bytes = vec![0x03];
    bytes.extend(hex::decode(hex).unwrap());
    let checksum = Blake2b256::digest(&bytes);
    bytes.extend_from_slice(&checksum[..4]);
    bs58::encode(bytes).into_string()
}

fn rules(entries: &[&str]) -> Vec<AddressRule> {
    entries
        .iter()
        .map(|e| AddressRule::parse(e).unwrap())
        .collect()
}

fn decoded(entries: &[&str]) -> Vec<Address> {
    entries
        .iter()
        .map(|e| Address::decode(e).unwrap())
        .collect()
}

/// Whether the policy admits an address, through the funnel every route uses.
fn admits_address(policy: &KeyPolicy, address: &str) -> bool {
    match address_key(address, Network::Mainnet, policy) {
        Ok(_) => true,
        Err(ApiError::KeyNotAllowed) => false,
        Err(other) => panic!("{address}: {other}"),
    }
}

fn admits_token(policy: &KeyPolicy, id: &str) -> bool {
    match token_key(id, policy) {
        Ok(_) => true,
        Err(ApiError::KeyNotAllowed) => false,
        Err(other) => panic!("{id}: {other}"),
    }
}

#[test]
fn the_p2s_form_of_a_p2pk_or_p2sh_tree_is_that_tree() {
    for hex in [P2PK_TREE, P2SH_TREE] {
        let reencoded = Address::decode(&as_p2s(hex)).unwrap();
        assert_ne!(as_p2s(hex), address(hex), "two encodings");
        assert_eq!(reencoded.ergo_tree(), Address::decode(&address(hex)).unwrap().ergo_tree());
    }
}

#[test]
fn no_rules_admit_every_key() {
    let policy = KeyPolicy::default();
    for hex in [P2PK_TREE, P2SH_TREE, P2S_TREE] {
        assert!(policy.admits(&tree(hex)), "{hex}");
    }
    assert!(policy.admits(&token(NFT)));
    assert_eq!(KeyPolicy::new(None, &[], None, &[]), policy);
}

#[test]
fn classes_are_judged_by_tree_whatever_the_encoding() {
    let wallet = KeyPolicy::new(Some(&rules(&["p2pk", "P2SH"])), &[], None, &[]);
    for hex in [P2PK_TREE, P2SH_TREE] {
        assert!(admits_address(&wallet, &address(hex)), "{hex}");
        assert!(admits_address(&wallet, &as_p2s(hex)), "{hex} re-encoded as P2S is still {hex}");
    }
    assert!(!admits_address(&wallet, &address(P2S_TREE)));

    // A P2S-only rule can't be met by writing a P2PK tree as a P2S address.
    let contracts = KeyPolicy::new(Some(&rules(&["p2s"])), &[], None, &[]);
    assert!(admits_address(&contracts, &address(P2S_TREE)));
    for hex in [P2PK_TREE, P2SH_TREE] {
        assert!(!admits_address(&contracts, &address(hex)), "{hex}");
        assert!(!admits_address(&contracts, &as_p2s(hex)), "{hex} re-encoded as P2S");
    }
}

#[test]
fn listed_addresses_are_admitted_besides_the_classes() {
    let fee = address(P2S_TREE);
    let wallet = KeyPolicy::new(Some(&rules(&["p2pk", &fee])), &[], None, &[]);
    assert!(admits_address(&wallet, &fee));
    assert!(admits_address(&wallet, &address(P2PK_TREE)));
    assert!(!admits_address(&wallet, &address(P2SH_TREE)));

    // A listed P2PK tree, matched by tree, so under either encoding.
    let one = KeyPolicy::new(Some(&rules(&[&as_p2s(P2PK_TREE)])), &[], None, &[]);
    assert!(admits_address(&one, &address(P2PK_TREE)));
    assert!(!admits_address(&one, &fee));
}

#[test]
fn none_admits_no_address_or_token_but_the_configured_ones() {
    let configured = [tree(P2S_TREE), token(NFT)];
    let policy = KeyPolicy::new(Some(&[]), &[], Some(&[]), &configured);
    assert!(admits_address(&policy, &address(P2S_TREE)));
    assert!(admits_token(&policy, NFT));
    assert!(!admits_address(&policy, &address(P2PK_TREE)));
    assert!(!admits_token(&policy, TOKEN));
}

#[test]
fn tokens_are_any_unless_listed() {
    let open = KeyPolicy::new(Some(&rules(&["p2pk"])), &[], None, &[]);
    assert!(admits_token(&open, NFT), "an address rule leaves tokens alone");
    let dapp = KeyPolicy::new(None, &[], Some(&[NFT.parse().unwrap()]), &[]);
    assert!(admits_token(&dapp, NFT));
    assert!(admits_token(&dapp, &NFT.to_uppercase()));
    assert!(!admits_token(&dapp, TOKEN));
    assert!(admits_address(&dapp, &address(P2S_TREE)), "a token rule leaves addresses alone");
}

#[test]
fn deny_wins_over_every_allow_rule() {
    let fee = address(P2S_TREE);
    let denied = decoded(&[&fee, &as_p2s(P2PK_TREE)]);
    let open = KeyPolicy::new(None, &denied, None, &[]);
    let class = KeyPolicy::new(Some(&rules(&["p2pk", "p2s"])), &denied, None, &[]);
    let listed = KeyPolicy::new(Some(&rules(&[&fee, &address(P2PK_TREE)])), &denied, None, &[]);
    for policy in [&open, &class, &listed] {
        assert!(!admits_address(policy, &fee), "{policy:?}");
        // Denied as a P2S address, so refused as its P2PK address too.
        assert!(!admits_address(policy, &address(P2PK_TREE)), "{policy:?}");
        assert!(!admits_address(policy, &as_p2s(P2PK_TREE)), "{policy:?}");
    }
    assert!(admits_address(&open, &address(P2SH_TREE)));
    assert!(admits_token(&open, NFT), "a deny rule leaves tokens alone");
}

#[test]
fn configured_keys_are_admitted_whatever_the_rules() {
    let configured = [tree(P2S_TREE), token(TOKEN)];
    let policy =
        KeyPolicy::new(Some(&rules(&["p2pk"])), &[], Some(&[NFT.parse().unwrap()]), &configured);
    assert!(admits_address(&policy, &address(P2S_TREE)));
    assert!(admits_token(&policy, TOKEN));
    assert!(!admits_address(&policy, &address(P2SH_TREE)));
}

#[test]
fn an_address_rule_is_a_class_in_any_case_or_an_address() {
    for (entry, class) in [
        ("p2pk", AddressType::P2PK),
        ("P2PK", AddressType::P2PK),
        ("p2sh", AddressType::P2SH),
        ("P2s", AddressType::P2S),
    ] {
        assert_eq!(AddressRule::parse(entry), Ok(AddressRule::Class(class)), "{entry}");
    }
    let fee = address(P2S_TREE);
    assert_eq!(AddressRule::parse(&fee), Ok(AddressRule::Address(Address::decode(&fee).unwrap())));
    for entry in ["p2p", "p2pkh", "any", "none"] {
        assert!(AddressRule::parse(entry).is_err(), "{entry}");
    }
}
