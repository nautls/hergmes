use super::*;
use crate::types::address::{AddressType, encode_as_p2s};

const MAINNET: &str = "9fRusAarL1KkrWQVsxSRVYnvWxaAT2A96cKtNn9tvPh5XUyCisr";
const MAINNET_TREE: &str =
    "0008cd0278011ec0cf5feb92d61adb51dcb75876627ace6fd9446ab4cabc5313ab7b39a7";
const FEE_TESTNET: &str = "Bf1X9JgQTUtgntaer91B24n6kP8L2kqEiQqNf1z97BKo9UbnW3WRP9VXu8BXd1LsYCiYbHJEdWKxkF5YNx5n7m31wsDjbEuB3B13ZMDVBWkepGmWfGa71otpFViHDCuvbw1uNicAQnfuWfnj8fbCa4";
const FEE_MAINNET: &str = "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe";
const NFT: &str = "20fa2bf23962cdf51b07722d6237c0c7b8a44f78856c0f7ec308dc1ef1a92a51";
const TOKEN: &str = "8c27dd9d8a35aac1e3167d58858c0a8b4059b277da790552e37eba22df9b9035";

/// A file with a node URL and `rest`, and no environment variables.
fn config(rest: &str) -> Result<Config, ConfigError> {
    with_vars(&format!("node_url = \"http://node\"\n{rest}"), &[])
}

fn with_vars(text: &str, vars: &[(&str, &str)]) -> Result<Config, ConfigError> {
    Config::from_toml(text, |name| {
        vars.iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.to_string())
    })
}

fn invalid_key(result: Result<Config, ConfigError>) -> &'static str {
    match result {
        Err(ConfigError::Invalid { key, .. }) => key,
        other => panic!("expected an invalid key, got {other:?}"),
    }
}

fn syntax_error(result: Result<Config, ConfigError>) -> String {
    match result {
        Err(error @ ConfigError::Syntax(_)) => error.to_string(),
        other => panic!("expected a syntax error, got {other:?}"),
    }
}

fn defaults(config: &Config) {
    assert_eq!(config.engine, engine::Config::default());
    assert_eq!(config.follower, follower::Config::default());
    assert_eq!(config.loader, loader::Config::default());
    assert_eq!(config.loader.max_boxes, 50_000);
    assert_eq!(config.engine.max_tracked_boxes, 2_000_000);
    assert_eq!(config.token_supply_limit, SUPPLY_LIMIT);
    assert_eq!(config.box_cache_bytes, 128 * 1024 * 1024);
    assert_eq!(config.listen_addr.to_string(), "127.0.0.1:8080", "loopback until chosen");
    assert_eq!(config.ws.max_subscriptions, 100);
    assert_eq!(config.allow_origins, OriginPolicy::Any);
}

#[test]
fn only_the_node_url_is_required() {
    for config in [
        with_vars("", &[(vars::NODE_URL, "http://127.0.0.1:9053")]).unwrap(),
        with_vars("node_url = \"http://127.0.0.1:9053\"", &[]).unwrap(),
    ] {
        assert_eq!(config.node_url, "http://127.0.0.1:9053");
        defaults(&config);
        assert!(config.track_addresses.is_empty() && config.track_tokens.is_empty());
        assert_eq!(config.allow_addresses, None, "every address");
        assert!(config.deny_addresses.is_empty());
        assert_eq!(config.allow_tokens, None, "every token within the supply limit");
        assert_eq!(config.metrics_addr, None, "no metrics until chosen");
    }

    for vars in [&[][..], &[(vars::NODE_URL, "")], &[(vars::NODE_URL, "  ")]] {
        let missing = with_vars("", vars);
        assert!(matches!(missing, Err(ConfigError::MissingNodeUrl)), "{vars:?}");
    }
    let empty = with_vars("node_url = \" \"", &[]);
    assert_eq!(invalid_key(empty), keys::NODE_URL);
}

/// Neither source wins over the other, so setting both is refused, even to one value.
#[test]
fn a_setting_comes_from_the_file_or_its_variable() {
    let node = (vars::NODE_URL, "http://node");
    for (key, var, value) in [
        (keys::NODE_URL, vars::NODE_URL, "http://node"),
        (keys::LISTEN_ADDR, vars::LISTEN_ADDR, "0.0.0.0:8080"),
        (keys::METRICS_ADDR, vars::METRICS_ADDR, "0.0.0.0:9100"),
    ] {
        // The node URL, unless the setting is the node URL, comes from its variable.
        let others: &[(&str, &str)] = if var == vars::NODE_URL { &[] } else { &[node] };
        let file = format!("{key} = \"{value}\"");
        let read = |config: Config| match key {
            keys::NODE_URL => config.node_url,
            keys::LISTEN_ADDR => config.listen_addr.to_string(),
            _ => config.metrics_addr.unwrap().to_string(),
        };

        let from_file = with_vars(&file, &[others, &[(var, " ")]].concat());
        assert_eq!(read(from_file.unwrap()), value, "{key}, with a blank {var}");
        let from_var = with_vars("", &[others, &[(var, &format!(" {value} "))]].concat());
        assert_eq!(read(from_var.unwrap()), value, "{var}");
        for set in [value, "1.2.3.4:5"] {
            let twice = with_vars(&file, &[others, &[(var, set)]].concat());
            assert!(
                matches!(twice, Err(ConfigError::Twice { key: k, var: v }) if k == key && v == var),
                "{key} and {var}={set}"
            );
        }
    }
    for var in [vars::LISTEN_ADDR, vars::METRICS_ADDR] {
        let bad = with_vars("", &[node, (var, "localhost:8080")]);
        assert_eq!(invalid_key(bad), var, "a bad value names its variable");
    }
}

#[test]
fn every_setting_is_read() {
    let config = config(&format!(
        r#"
        window_blocks = 30
        grace_secs = 0
        poll_interval_ms = 250
        mempool_refresh_ms = 5000
        token_supply_limit = 7
        track_addresses = ["{MAINNET}"]
        track_tokens = ["{NFT}", "{TOKEN}"]
        allow_addresses = ["p2pk", "P2SH", "{MAINNET}"]
        deny_addresses = ["{FEE_MAINNET}"]
        allow_tokens = ["{TOKEN}"]
        box_cache_mb = 3
        max_boxes_per_key = 40
        max_tracked_boxes = 900
        listen_addr = "0.0.0.0:9000"
        metrics_addr = "127.0.0.1:9100"
        max_subscriptions_per_connection = 8
        "#
    ))
    .unwrap();
    let engine = engine::Config {
        window: 30,
        grace: Duration::ZERO,
        max_tracked_boxes: 900,
        ..engine::Config::DEFAULT
    };
    assert_eq!(config.engine, engine);
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
    assert_eq!(config.metrics_addr.unwrap().to_string(), "127.0.0.1:9100");
    assert_eq!(config.ws.max_subscriptions, 8);
}

/// The example documents every key with its default, so each line uncommented must parse to
/// the defaults: a renamed key or a changed default shows up here.
#[test]
fn the_example_file_shows_the_keys_and_their_defaults() {
    let example = include_str!("../hergmes.example.toml");
    let uncommented: String = example
        .lines()
        .map(|line| match line.strip_prefix("# ") {
            Some(setting) if setting.split_once(" = ").is_some_and(|(k, _)| is_key(k)) => setting,
            _ => line,
        })
        .map(|line| format!("{line}\n"))
        .collect();
    assert!(uncommented.lines().filter(|l| l.contains(" = ")).count() >= 17, "{uncommented}");

    let config = with_vars(&uncommented, &[]).unwrap();
    defaults(&config);
    assert_eq!(config.allow_addresses.unwrap().len(), 2, "an example, not the default");
    assert_eq!(config.allow_tokens, Some(vec![]), "an example, not the default");
}

fn is_key(word: &str) -> bool {
    !word.is_empty() && word.chars().all(|c| c.is_ascii_lowercase() || c == '_')
}

#[test]
fn a_box_cache_of_zero_is_allowed() {
    let config = config("box_cache_mb = 0");
    assert_eq!(config.unwrap().box_cache_bytes, 0, "it disables the cache");
}

#[test]
fn bad_values_name_their_key() {
    let bad_address = "9fRusAarL1KkrWQVsxSRVYnvWxaAT2A96cKtNn9tvPh5XUyCisR";
    for (key, value) in [
        (keys::WINDOW_BLOCKS, "0"),
        (keys::POLL_INTERVAL_MS, "0"),
        (keys::MEMPOOL_REFRESH_MS, "0"),
        (keys::TRACK_ADDRESSES, &format!("[\"{bad_address}\"]")),
        (keys::TRACK_TOKENS, "[\"abcd\"]"),
        (keys::ALLOW_ADDRESSES, "[\"p2pkh\"]"),
        (keys::ALLOW_ADDRESSES, "[\"\"]"),
        (keys::DENY_ADDRESSES, "[\"p2s\"]"),
        (keys::DENY_ADDRESSES, "[\"none\"]"),
        (keys::ALLOW_TOKENS, "[\"abcd\"]"),
        (keys::BOX_CACHE_MB, &i64::MAX.to_string()),
        (keys::MAX_BOXES_PER_KEY, "0"),
        (keys::MAX_TRACKED_BOXES, "0"),
        (keys::MAX_SUBSCRIPTIONS_PER_CONNECTION, "0"),
    ] {
        assert_eq!(invalid_key(config(&format!("{key} = {value}"))), key, "{key} = {value}");
    }
}

/// Values of the wrong type fail as the file is parsed, and the message names the key.
#[test]
fn mistyped_values_name_their_key() {
    for (key, value) in [
        ("window_blocks", "-1"),
        ("grace_secs", "\"five\""),
        ("mempool_refresh_ms", "1.5"),
        ("token_supply_limit", "\"lots\""),
        ("track_addresses", &format!("\"{MAINNET}\"")),
        ("allow_tokens", "\"none\""),
        ("box_cache_mb", "-1"),
        ("max_tracked_boxes", "-5"),
        ("listen_addr", "\"localhost:8080\""),
        ("listen_addr", "\"127.0.0.1\""),
        ("listen_addr", "\"127.0.0.1:65536\""),
        ("metrics_addr", "9100"),
        ("max_subscriptions_per_connection", "\"many\""),
    ] {
        let error = syntax_error(config(&format!("{key} = {value}")));
        assert!(error.contains(key), "{key} = {value}: {error}");
    }
}

/// A misspelled key would otherwise leave its default in force, an open allowlist among them.
#[test]
fn unknown_keys_are_refused() {
    for key in ["allow_address", "HERGMES_ALLOW_ADDRESSES", "ergo_node_url"] {
        let error = syntax_error(config(&format!("{key} = []")));
        assert!(error.contains(key), "{key}: {error}");
    }
    assert!(syntax_error(config("[engine]\nwindow_blocks = 3")).contains("engine"));
}

#[test]
fn tracked_keys_are_trees_and_tokens_on_the_node_network() {
    let config = config(&format!(
        "track_addresses = [\"{MAINNET}\", \"{MAINNET}\"]\ntrack_tokens = [\"{NFT}\"]"
    ))
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
    assert!(matches!(wrong, Err(ConfigError::Invalid { key: keys::TRACK_ADDRESSES, .. })));

    let testnet = self::config(&format!("track_addresses = [\"{FEE_TESTNET}\"]")).unwrap();
    assert!(testnet.tracked_keys(Network::Mainnet).is_err());
    assert_eq!(testnet.tracked_keys(Network::Testnet).unwrap().len(), 1);
}

#[test]
fn an_empty_allowlist_allows_nothing() {
    let config = config("allow_addresses = []\nallow_tokens = []").unwrap();
    assert_eq!(config.allow_addresses, Some(vec![]));
    assert_eq!(config.allow_tokens, Some(vec![]));
    let policy = config.key_policy(Network::Mainnet, &[]).unwrap();
    assert_eq!(policy.to_string(), "addresses: none; tokens: none");
}

#[test]
fn an_omitted_allowlist_allows_any() {
    let config = config("").unwrap();
    let policy = config.key_policy(Network::Mainnet, &[]).unwrap();
    assert_eq!(policy.to_string(), "addresses: any; tokens: any");
}

/// The words environment variables used are refused with a hint, rather than failing as an
/// unknown address or token id.
#[test]
fn any_and_none_are_not_allowlist_entries() {
    for key in [keys::ALLOW_ADDRESSES, keys::ALLOW_TOKENS] {
        for entry in ["any", "NONE"] {
            let error = config(&format!("{key} = [\"{entry}\"]")).unwrap_err();
            assert!(
                matches!(&error, ConfigError::Invalid { key: k, reason }
                    if k == &key && reason.contains("[]")),
                "{key}: {error}"
            );
        }
    }
}

#[test]
fn the_key_policy_admits_the_configured_keys_and_follows_the_rules() {
    let config = config(&format!(
        r#"
        track_addresses = ["{FEE_MAINNET}"]
        track_tokens = ["{NFT}"]
        allow_addresses = ["p2sh"]
        deny_addresses = ["{MAINNET}"]
        allow_tokens = []
        "#
    ))
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
    for key in [keys::ALLOW_ADDRESSES, keys::DENY_ADDRESSES] {
        let config = config(&format!("{key} = [\"{FEE_TESTNET}\"]")).unwrap();
        assert!(config.key_policy(Network::Testnet, &[]).is_ok(), "{key}");
        let wrong = config.key_policy(Network::Mainnet, &[]);
        assert!(matches!(wrong, Err(ConfigError::Invalid { key: k, .. }) if k == key), "{key}");
    }
    let class = config("allow_addresses = [\"p2s\"]");
    assert!(class.unwrap().key_policy(Network::Testnet, &[]).is_ok(), "a class has no network");
}

/// A deny rule naming a configured address is a typo or a conflict, so it stops startup,
/// even when the two name the tree through different encodings.
#[test]
fn denying_a_configured_address_stops_startup() {
    let as_p2s = encode_as_p2s(&hex::decode(MAINNET_TREE).unwrap(), Network::Mainnet);

    for denied in [MAINNET, as_p2s.as_str()] {
        let config = config(&format!(
            "track_addresses = [\"{MAINNET}\"]\ndeny_addresses = [\"{FEE_MAINNET}\", \"{denied}\"]"
        ))
        .unwrap();
        let keys = config.tracked_keys(Network::Mainnet).unwrap();
        let error = config.key_policy(Network::Mainnet, &keys).unwrap_err();
        assert!(
            matches!(&error, ConfigError::Invalid { key: keys::DENY_ADDRESSES, reason }
                if reason.contains(keys::TRACK_ADDRESSES)),
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
        let config = config(&format!(
            "allow_addresses = [\"p2s\", \"{MAINNET}\"]\ndeny_addresses = [\"{denied}\"]"
        ))
        .unwrap();
        let error = config.key_policy(Network::Mainnet, &[]).unwrap_err();
        assert!(
            matches!(&error, ConfigError::Invalid { key: keys::DENY_ADDRESSES, reason }
                if reason.contains(keys::ALLOW_ADDRESSES)),
            "{denied}: {error}"
        );
    }
    // Denying an address of an allowed class is what deny is for.
    let config =
        config(&format!("allow_addresses = [\"p2pk\"]\ndeny_addresses = [\"{MAINNET}\"]")).unwrap();
    assert!(config.key_policy(Network::Mainnet, &[]).is_ok());
}

#[test]
fn the_config_flag_names_the_file() {
    let parse = |list: &[&str]| args(list.iter().map(OsString::from));
    let run = |path: Option<&str>| Args::Run { config: path.map(PathBuf::from) };
    assert_eq!(parse(&[]).unwrap(), run(None));
    assert_eq!(parse(&["--config", "a.toml"]).unwrap(), run(Some("a.toml")));
    assert_eq!(parse(&["--config=b.toml"]).unwrap(), run(Some("b.toml")));
    for bad in [&["--config"][..], &["--config", "a", "--config=b"], &["-c", "a"], &["a.toml"]] {
        assert!(matches!(parse(bad), Err(ConfigError::Usage(_))), "{bad:?}");
    }
    for help in [&["--help"][..], &["-h"], &["--config", "a.toml", "--help"], &["--bogus", "-h"]] {
        assert_eq!(parse(help).unwrap(), Args::Help, "{help:?}");
    }
}

/// The flag's file is read even if it is missing, so a wrong path fails rather than falling
/// back to the defaults; without the flag, only the working directory's file is read.
#[test]
fn the_file_is_the_flags_or_the_one_in_the_directory() {
    let dir = std::env::temp_dir().join(format!("hergmes-config-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let missing = dir.join("missing.toml");
    assert_eq!(locate(Some(missing.clone()), &dir), Some(missing.clone()));
    assert!(matches!(Config::load(Some(&missing)), Err(ConfigError::Read { .. })));
    assert_eq!(locate(None, &dir), None);

    let found = dir.join(DEFAULT_PATH);
    std::fs::write(&found, "").unwrap();
    assert_eq!(locate(None, &dir), Some(found));
    assert_eq!(locate(Some(missing.clone()), &dir), Some(missing), "the flag wins");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Something at the default path that can't be read stops startup, rather than leave the
/// relay on its defaults, which allow any key.
#[test]
fn an_unreadable_default_file_is_not_skipped() {
    let dir = std::env::temp_dir().join(format!("hergmes-config-bad-{}", std::process::id()));
    let path = dir.join(DEFAULT_PATH);
    std::fs::create_dir_all(&path).unwrap();
    assert_eq!(locate(None, &dir), Some(path.clone()), "a directory");
    assert!(matches!(Config::load(Some(&path)), Err(ConfigError::Read { .. })));
    std::fs::remove_dir(&path).unwrap();

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(dir.join("unmounted.toml"), &path).unwrap();
        assert_eq!(locate(None, &dir), Some(path.clone()), "a broken link");
        assert!(matches!(Config::load(Some(&path)), Err(ConfigError::Read { .. })));
        std::fs::remove_file(&path).unwrap();

        // A directory that can't be searched hides whether the file is there.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
        let privileged = std::fs::read_dir(&dir).is_ok();
        let located = locate(None, &dir);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        if !privileged {
            assert_eq!(located, Some(path.clone()), "a directory that can't be searched");
        }
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Errors name keys through [`keys`], so each must be a key the file accepts: the example
/// file is parsed with every key, and this ties the two lists.
#[test]
fn the_named_keys_are_the_file_keys() {
    let mut named = vec![
        keys::NODE_URL,
        keys::WINDOW_BLOCKS,
        keys::GRACE_SECS,
        keys::POLL_INTERVAL_MS,
        keys::MEMPOOL_REFRESH_MS,
        keys::TOKEN_SUPPLY_LIMIT,
        keys::TRACK_ADDRESSES,
        keys::TRACK_TOKENS,
        keys::ALLOW_ADDRESSES,
        keys::DENY_ADDRESSES,
        keys::ALLOW_TOKENS,
        keys::BOX_CACHE_MB,
        keys::MAX_BOXES_PER_KEY,
        keys::MAX_TRACKED_BOXES,
        keys::MAX_SUBSCRIPTIONS_PER_CONNECTION,
        keys::LISTEN_ADDR,
        keys::ALLOW_ORIGINS,
        keys::METRICS_ADDR,
    ];
    let mut in_example: Vec<&str> = include_str!("../hergmes.example.toml")
        .lines()
        .filter_map(|line| line.strip_prefix("# ")?.split_once(" = "))
        .map(|(key, _)| key)
        .filter(|key| is_key(key))
        .collect();
    named.sort();
    in_example.sort();
    assert_eq!(named, in_example);
}

#[test]
fn allow_origins_lists_schemes_and_exact_origins() {
    let listed =
        config(r#"allow_origins = ["chrome-extension://*", "https://Wallet.example"]"#).unwrap();
    assert_eq!(
        listed.allow_origins,
        OriginPolicy::Listed {
            exact: vec!["https://wallet.example".to_owned()],
            schemes: vec!["chrome-extension".to_owned()],
        }
    );
    assert_eq!(config("allow_origins = [\"*\"]").unwrap().allow_origins, OriginPolicy::Any);
    assert_eq!(
        config("allow_origins = []").unwrap().allow_origins,
        OriginPolicy::Listed { exact: vec![], schemes: vec![] },
        "browsers refused, requests without an Origin served"
    );
}

#[test]
fn an_origin_a_browser_never_sends_stops_startup() {
    for entry in ["https://wallet.example/", "wallet.example", "https://*.example"] {
        let result = config(&format!("allow_origins = [\"{entry}\"]"));
        assert_eq!(invalid_key(result), keys::ALLOW_ORIGINS, "{entry}");
    }
}
