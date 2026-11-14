use std::collections::HashMap;

use super::*;
use crate::types::address::{AddressType, encode_as_p2s};

const MAINNET: &str = "9fRusAarL1KkrWQVsxSRVYnvWxaAT2A96cKtNn9tvPh5XUyCisr";
const MAINNET_TREE: &str =
    "0008cd0278011ec0cf5feb92d61adb51dcb75876627ace6fd9446ab4cabc5313ab7b39a7";
const FEE_TESTNET: &str = "Bf1X9JgQTUtgntaer91B24n6kP8L2kqEiQqNf1z97BKo9UbnW3WRP9VXu8BXd1LsYCiYbHJEdWKxkF5YNx5n7m31wsDjbEuB3B13ZMDVBWkepGmWfGa71otpFViHDCuvbw1uNicAQnfuWfnj8fbCa4";
const FEE_MAINNET: &str = "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe";
const NFT: &str = "20fa2bf23962cdf51b07722d6237c0c7b8a44f78856c0f7ec308dc1ef1a92a51";
const TOKEN: &str = "8c27dd9d8a35aac1e3167d58858c0a8b4059b277da790552e37eba22df9b9035";

fn config(vars: &[(&str, &str)]) -> Result<Config, ConfigError> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Config::from_vars(|name| vars.get(name).cloned())
}

fn invalid_var(result: Result<Config, ConfigError>) -> &'static str {
    match result {
        Err(ConfigError::Invalid { var, .. }) => var,
        other => panic!("expected an invalid variable, got {other:?}"),
    }
}

#[test]
fn only_the_node_url_is_required() {
    let config = config(&[(vars::NODE_URL, "http://127.0.0.1:9053")]).unwrap();
    assert_eq!(config.node_url, "http://127.0.0.1:9053");
    assert_eq!(config.engine, engine::Config::default());
    assert_eq!(config.follower, follower::Config::default());
    assert_eq!(config.loader, loader::Config::default());
    assert_eq!(config.loader.max_boxes, 50_000);
    assert_eq!(config.engine.max_tracked_boxes, 2_000_000);
    assert_eq!(config.token_supply_limit, SUPPLY_LIMIT);
    assert!(config.track_addresses.is_empty() && config.track_tokens.is_empty());
    assert_eq!(config.allow_addresses, None, "every address");
    assert!(config.deny_addresses.is_empty());
    assert_eq!(config.allow_tokens, None, "every token within the supply limit");
    assert_eq!(config.box_cache_bytes, 128 * 1024 * 1024);
    assert_eq!(config.listen_addr.to_string(), "127.0.0.1:8080", "loopback until chosen");

    assert!(matches!(self::config(&[]), Err(ConfigError::Missing(vars::NODE_URL))));
    let empty = self::config(&[(vars::NODE_URL, "  ")]);
    assert!(matches!(empty, Err(ConfigError::Missing(vars::NODE_URL))));
}

#[test]
fn every_setting_is_read() {
    let config = config(&[
        (vars::NODE_URL, "http://node"),
        (vars::WINDOW_BLOCKS, "30"),
        (vars::GRACE_BLOCKS, "0"),
        (vars::POLL_INTERVAL_MS, "250"),
        (vars::MEMPOOL_REFRESH_MS, "5000"),
        (vars::TOKEN_SUPPLY_LIMIT, "7"),
        (vars::TRACK_ADDRESSES, &format!(" {MAINNET} ,")),
        (vars::TRACK_TOKENS, &format!("{NFT},{TOKEN}")),
        (vars::ALLOW_ADDRESSES, &format!("p2pk, P2SH,{MAINNET}")),
        (vars::DENY_ADDRESSES, FEE_MAINNET),
        (vars::ALLOW_TOKENS, &format!("{TOKEN},")),
        (vars::BOX_CACHE_MB, "3"),
        (vars::MAX_BOXES_PER_KEY, "40"),
        (vars::MAX_TRACKED_BOXES, "900"),
        (vars::LISTEN_ADDR, "0.0.0.0:9000"),
    ])
    .unwrap();
    assert_eq!(config.engine, engine::Config { window: 30, grace: 0, max_tracked_boxes: 900 });
    assert_eq!(config.loader.max_boxes, 40);
    assert_eq!(config.follower.interval, Duration::from_millis(250));
    assert_eq!(config.follower.mempool.refresh, Duration::from_secs(5));
    assert_eq!(config.token_supply_limit, 7);
    assert_eq!(config.track_addresses, vec![Address::decode(MAINNET).unwrap()]);
    assert_eq!(config.track_tokens, vec![NFT.parse().unwrap(), TOKEN.parse().unwrap()]);
    assert_eq!(
        config.allow_addresses,
        Some(vec![
            AddressRule::Class(AddressType::P2PK),
            AddressRule::Class(AddressType::P2SH),
            AddressRule::Address(Address::decode(MAINNET).unwrap()),
        ])
    );
    assert_eq!(config.deny_addresses, vec![Address::decode(FEE_MAINNET).unwrap()]);
    assert_eq!(config.allow_tokens, Some(vec![TOKEN.parse().unwrap()]));
    assert_eq!(config.box_cache_bytes, 3 * 1024 * 1024);
    assert_eq!(config.listen_addr.to_string(), "0.0.0.0:9000");
}

#[test]
fn a_box_cache_of_zero_is_allowed() {
    let config = config(&[(vars::NODE_URL, "http://node"), (vars::BOX_CACHE_MB, "0")]);
    assert_eq!(config.unwrap().box_cache_bytes, 0, "it disables the cache");
}

#[test]
fn bad_values_name_their_variable() {
    let url = (vars::NODE_URL, "http://node");
    for (var, value) in [
        (vars::WINDOW_BLOCKS, "0"),
        (vars::WINDOW_BLOCKS, "-1"),
        (vars::GRACE_BLOCKS, "five"),
        (vars::POLL_INTERVAL_MS, "0"),
        (vars::MEMPOOL_REFRESH_MS, "1.5"),
        (vars::TOKEN_SUPPLY_LIMIT, "lots"),
        (vars::TRACK_ADDRESSES, "9fRusAarL1KkrWQVsxSRVYnvWxaAT2A96cKtNn9tvPh5XUyCisR"),
        (vars::TRACK_TOKENS, "abcd"),
        (vars::ALLOW_ADDRESSES, "p2pkh"),
        (vars::ALLOW_ADDRESSES, ","),
        (vars::ALLOW_ADDRESSES, "none,p2pk"),
        (vars::DENY_ADDRESSES, "p2s"),
        (vars::DENY_ADDRESSES, "none"),
        (vars::ALLOW_TOKENS, "abcd"),
        (vars::ALLOW_TOKENS, " , "),
        (vars::ALLOW_TOKENS, &format!("{NFT},NONE")),
        (vars::ALLOW_ADDRESSES, "any,p2pk"),
        (vars::ALLOW_TOKENS, "any,none"),
        (vars::ALLOW_TOKENS, &format!("{NFT},Any")),
        (vars::DENY_ADDRESSES, "any"),
        (vars::BOX_CACHE_MB, "-1"),
        (vars::BOX_CACHE_MB, "18446744073709551615"),
        (vars::MAX_BOXES_PER_KEY, "0"),
        (vars::MAX_BOXES_PER_KEY, "1e6"),
        (vars::MAX_TRACKED_BOXES, "0"),
        (vars::MAX_TRACKED_BOXES, "-5"),
        (vars::LISTEN_ADDR, "localhost:8080"),
        (vars::LISTEN_ADDR, "127.0.0.1"),
        (vars::LISTEN_ADDR, "127.0.0.1:65536"),
    ] {
        assert_eq!(invalid_var(config(&[url, (var, value)])), var, "{var}={value}");
    }
}

#[test]
fn tracked_keys_are_trees_and_tokens_on_the_node_network() {
    let config = config(&[
        (vars::NODE_URL, "http://node"),
        (vars::TRACK_ADDRESSES, &format!("{MAINNET},{MAINNET}")),
        (vars::TRACK_TOKENS, NFT),
    ])
    .unwrap();
    let keys = config.tracked_keys(Network::Mainnet).unwrap();
    assert_eq!(
        keys,
        vec![
            TrackingKey::Tree(crate::types::HexBytes(hex::decode(MAINNET_TREE).unwrap())),
            TrackingKey::Token(NFT.parse().unwrap()),
        ],
        "each key once"
    );
    let wrong = config.tracked_keys(Network::Testnet);
    assert!(matches!(wrong, Err(ConfigError::Invalid { var: vars::TRACK_ADDRESSES, .. })));

    let testnet =
        self::config(&[(vars::NODE_URL, "http://node"), (vars::TRACK_ADDRESSES, FEE_TESTNET)])
            .unwrap();
    assert!(testnet.tracked_keys(Network::Mainnet).is_err());
    assert_eq!(testnet.tracked_keys(Network::Testnet).unwrap().len(), 1);
}

#[test]
fn none_allows_nothing() {
    let config = config(&[
        (vars::NODE_URL, "http://node"),
        (vars::ALLOW_ADDRESSES, " None "),
        (vars::ALLOW_TOKENS, "none"),
    ])
    .unwrap();
    assert_eq!(config.allow_addresses, Some(vec![]));
    assert_eq!(config.allow_tokens, Some(vec![]));

    // Empty entries are skipped, as in any list.
    for value in ["none,", " , NONE ,"] {
        let config = self::config(&[(vars::NODE_URL, "http://node"), (vars::ALLOW_TOKENS, value)]);
        assert_eq!(config.unwrap().allow_tokens, Some(vec![]), "{value:?}");
    }
}

#[test]
fn the_key_policy_admits_the_configured_keys_and_follows_the_rules() {
    let config = config(&[
        (vars::NODE_URL, "http://node"),
        (vars::TRACK_ADDRESSES, FEE_MAINNET),
        (vars::TRACK_TOKENS, NFT),
        (vars::ALLOW_ADDRESSES, "p2sh"),
        (vars::DENY_ADDRESSES, MAINNET),
        (vars::ALLOW_TOKENS, "none"),
    ])
    .unwrap();
    let keys = config.tracked_keys(Network::Mainnet).unwrap();
    let policy = config.key_policy(Network::Mainnet, &keys).unwrap();
    for key in &keys {
        assert!(policy.admits(key), "{key:?} is configured");
    }
    let p2pk = TrackingKey::Tree(crate::types::HexBytes(hex::decode(MAINNET_TREE).unwrap()));
    assert!(!policy.admits(&p2pk), "denied");
    assert!(!policy.admits(&TrackingKey::Token(TOKEN.parse().unwrap())), "no token listed");
}

#[test]
fn listed_addresses_must_be_on_the_node_network() {
    for var in [vars::ALLOW_ADDRESSES, vars::DENY_ADDRESSES] {
        let config = config(&[(vars::NODE_URL, "http://node"), (var, FEE_TESTNET)]).unwrap();
        assert!(config.key_policy(Network::Testnet, &[]).is_ok(), "{var}");
        let wrong = config.key_policy(Network::Mainnet, &[]);
        assert!(matches!(wrong, Err(ConfigError::Invalid { var: v, .. }) if v == var), "{var}");
    }
    let class = config(&[(vars::NODE_URL, "http://node"), (vars::ALLOW_ADDRESSES, "p2s")]);
    assert!(class.unwrap().key_policy(Network::Testnet, &[]).is_ok(), "a class has no network");
}

/// A deny rule naming a configured address is a typo or a conflict, so it stops startup,
/// even when the two name the tree through different encodings.
#[test]
fn denying_a_configured_address_stops_startup() {
    let as_p2s = encode_as_p2s(&hex::decode(MAINNET_TREE).unwrap(), Network::Mainnet);

    for denied in [MAINNET, as_p2s.as_str()] {
        let config = config(&[
            (vars::NODE_URL, "http://node"),
            (vars::TRACK_ADDRESSES, MAINNET),
            (vars::DENY_ADDRESSES, &format!("{FEE_MAINNET},{denied}")),
        ])
        .unwrap();
        let keys = config.tracked_keys(Network::Mainnet).unwrap();
        let error = config.key_policy(Network::Mainnet, &keys).unwrap_err();
        assert!(
            matches!(&error, ConfigError::Invalid { var: vars::DENY_ADDRESSES, reason }
                if reason.contains(vars::TRACK_ADDRESSES)),
            "{denied}: {error}"
        );
    }
}

/// An address both allowed and denied would be refused on every request, so it stops
/// startup like a denied configured key, through either encoding of its tree.
#[test]
fn allowing_and_denying_an_address_stops_startup() {
    let as_p2s = encode_as_p2s(&hex::decode(MAINNET_TREE).unwrap(), Network::Mainnet);
    for denied in [MAINNET, as_p2s.as_str()] {
        let config = config(&[
            (vars::NODE_URL, "http://node"),
            (vars::ALLOW_ADDRESSES, &format!("p2s,{MAINNET}")),
            (vars::DENY_ADDRESSES, denied),
        ])
        .unwrap();
        let error = config.key_policy(Network::Mainnet, &[]).unwrap_err();
        assert!(
            matches!(&error, ConfigError::Invalid { var: vars::DENY_ADDRESSES, reason }
                if reason.contains(vars::ALLOW_ADDRESSES)),
            "{denied}: {error}"
        );
    }
    // Denying an address of an allowed class is what deny is for.
    let config = config(&[
        (vars::NODE_URL, "http://node"),
        (vars::ALLOW_ADDRESSES, "p2pk"),
        (vars::DENY_ADDRESSES, MAINNET),
    ])
    .unwrap();
    assert!(config.key_policy(Network::Mainnet, &[]).is_ok());
}

/// `any` names the default, so writing it is the same as leaving the variable unset.
#[test]
fn any_is_the_default_written_down() {
    for value in ["any", " ANY ", "any,", ", Any"] {
        let config = config(&[
            (vars::NODE_URL, "http://node"),
            (vars::ALLOW_ADDRESSES, value),
            (vars::ALLOW_TOKENS, value),
        ])
        .unwrap();
        assert_eq!(config.allow_addresses, None, "{value:?}");
        assert_eq!(config.allow_tokens, None, "{value:?}");
        let policy = config.key_policy(Network::Mainnet, &[]).unwrap();
        assert_eq!(policy.to_string(), "addresses: any; tokens: any", "{value:?}");
    }
}
