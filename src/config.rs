//! Configuration, read from environment variables (and `.env`).
//!
//! The relay's own settings start with `HERGMES_`, so they can't clash with other variables
//! in a shared environment. `ERGO_NODE_URL` names the node, not the relay, and has no prefix.
//!
//! The cap on subscriptions per connection is not here yet: nothing reads it until the
//! WebSocket (step 9).

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use crate::{
    adapters::{follower, loader, mempool, tokens::SUPPLY_LIMIT},
    api::policy::{AddressRule, KeyPolicy},
    engine::{self, TrackingKey},
    types::{
        address::{Address, Network},
        ergo::TokenId,
    },
};

/// The variables read, with their defaults.
pub mod vars {
    /// URL of an indexed Ergo node. Required.
    pub const NODE_URL: &str = "ERGO_NODE_URL";
    /// N, in blocks.
    pub const WINDOW_BLOCKS: &str = "HERGMES_WINDOW_BLOCKS";
    /// G, in blocks.
    pub const GRACE_BLOCKS: &str = "HERGMES_GRACE_BLOCKS";
    /// How often `/info` is read, in milliseconds.
    pub const POLL_INTERVAL_MS: &str = "HERGMES_POLL_INTERVAL_MS";
    /// How often the mempool is fetched even if `/info` shows no change, in milliseconds.
    pub const MEMPOOL_REFRESH_MS: &str = "HERGMES_MEMPOOL_REFRESH_MS";
    /// The largest token emission that can be tracked.
    pub const TOKEN_SUPPLY_LIMIT: &str = "HERGMES_TOKEN_SUPPLY_LIMIT";
    /// Addresses tracked for the life of the process, comma-separated.
    pub const TRACK_ADDRESSES: &str = "HERGMES_TRACK_ADDRESSES";
    /// Token ids tracked for the life of the process, comma-separated.
    pub const TRACK_TOKENS: &str = "HERGMES_TRACK_TOKENS";
    /// The addresses clients may name: `p2pk`, `p2sh`, `p2s` or addresses, comma-separated,
    /// or `none`, or `any`, the default.
    pub const ALLOW_ADDRESSES: &str = "HERGMES_ALLOW_ADDRESSES";
    /// Addresses clients may not name, comma-separated. Deny wins over allow.
    pub const DENY_ADDRESSES: &str = "HERGMES_DENY_ADDRESSES";
    /// The token ids clients may name, comma-separated, or `none`, or `any`, the default:
    /// any token within the supply limit.
    pub const ALLOW_TOKENS: &str = "HERGMES_ALLOW_TOKENS";
    /// The box cache's budget, in MiB. 0 disables the cache.
    pub const BOX_CACHE_MB: &str = "HERGMES_BOX_CACHE_MB";
    /// The most boxes a key can have when it is loaded.
    pub const MAX_BOXES_PER_KEY: &str = "HERGMES_MAX_BOXES_PER_KEY";
    /// The most boxes tracked over all keys, checked when a key is loaded.
    pub const MAX_TRACKED_BOXES: &str = "HERGMES_MAX_TRACKED_BOXES";
    /// The IP address and port the REST API listens on.
    pub const LISTEN_ADDR: &str = "HERGMES_LISTEN_ADDR";
}

/// Loopback by default, so a fresh install is not exposed until the operator chooses: usually
/// behind a reverse proxy, or `0.0.0.0:8080` in a container.
pub const LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);

/// The box cache's default budget: about 32k to 1.2M boxes, at 100 B to 4 KB each.
pub const BOX_CACHE_MB: u64 = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub node_url: String,
    pub engine: engine::Config,
    pub follower: follower::Config,
    pub loader: loader::Config,
    pub token_supply_limit: u64,
    /// Addresses tracked from the start, checked against the node's network once it is known.
    pub track_addresses: Vec<Address>,
    pub track_tokens: Vec<TokenId>,
    /// The address allowlist; `None` when unset, empty for `none`.
    pub allow_addresses: Option<Vec<AddressRule>>,
    pub deny_addresses: Vec<Address>,
    /// The token allowlist; `None` when unset, empty for `none`.
    pub allow_tokens: Option<Vec<TokenId>>,
    /// The box cache's budget in bytes, read in MiB; 0 disables the cache.
    pub box_cache_bytes: u64,
    pub listen_addr: SocketAddr,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("environment variable `{0}` must be set")]
    Missing(&'static str),
    #[error("environment variable `{var}`: {reason}")]
    Invalid { var: &'static str, reason: String },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// Reads the configuration through `var`. An empty value counts as unset.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let get = |name: &str| {
            var(name)
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        let engine_defaults = engine::Config::default();
        let follower_defaults = follower::Config::default();
        let loader_defaults = loader::Config::default();

        let window = number(vars::WINDOW_BLOCKS, get(vars::WINDOW_BLOCKS), engine_defaults.window)?;
        if window == 0 {
            return Err(invalid(vars::WINDOW_BLOCKS, "the window keeps at least the tip"));
        }
        let interval = millis(
            vars::POLL_INTERVAL_MS,
            get(vars::POLL_INTERVAL_MS),
            follower_defaults.interval,
        )?;
        let cache_mb = number(vars::BOX_CACHE_MB, get(vars::BOX_CACHE_MB), BOX_CACHE_MB)?;
        let box_cache_bytes = cache_mb
            .checked_mul(1024 * 1024)
            .ok_or_else(|| invalid(vars::BOX_CACHE_MB, &format!("{cache_mb} MiB is too large")))?;
        let max_boxes = at_least_one(
            vars::MAX_BOXES_PER_KEY,
            get(vars::MAX_BOXES_PER_KEY),
            loader_defaults.max_boxes,
        )?;
        let max_tracked_boxes = at_least_one(
            vars::MAX_TRACKED_BOXES,
            get(vars::MAX_TRACKED_BOXES),
            engine_defaults.max_tracked_boxes,
        )?;
        let refresh = millis(
            vars::MEMPOOL_REFRESH_MS,
            get(vars::MEMPOOL_REFRESH_MS),
            follower_defaults.mempool.refresh,
        )?;

        Ok(Self {
            node_url: get(vars::NODE_URL).ok_or(ConfigError::Missing(vars::NODE_URL))?,
            engine: engine::Config {
                window,
                grace: number(vars::GRACE_BLOCKS, get(vars::GRACE_BLOCKS), engine_defaults.grace)?,
                max_tracked_boxes,
            },
            follower: follower::Config { interval, mempool: mempool::Config { refresh } },
            loader: loader::Config { max_boxes, ..loader_defaults },
            token_supply_limit: number(
                vars::TOKEN_SUPPLY_LIMIT,
                get(vars::TOKEN_SUPPLY_LIMIT),
                SUPPLY_LIMIT,
            )?,
            track_addresses: list(vars::TRACK_ADDRESSES, get(vars::TRACK_ADDRESSES), address)?,
            track_tokens: list(vars::TRACK_TOKENS, get(vars::TRACK_TOKENS), token_id)?,
            allow_addresses: allowlist(vars::ALLOW_ADDRESSES, get(vars::ALLOW_ADDRESSES), |a| {
                AddressRule::parse(a).map_err(|e| e.to_string())
            })?,
            deny_addresses: list(vars::DENY_ADDRESSES, get(vars::DENY_ADDRESSES), address)?,
            allow_tokens: allowlist(vars::ALLOW_TOKENS, get(vars::ALLOW_TOKENS), token_id)?,
            box_cache_bytes,
            listen_addr: number(vars::LISTEN_ADDR, get(vars::LISTEN_ADDR), LISTEN_ADDR)?,
        })
    }

    /// The keys tracked from the start. Fails if an address is for another network than the
    /// node's ([`Address::check_network`]).
    pub fn tracked_keys(&self, network: Network) -> Result<Vec<TrackingKey>, ConfigError> {
        let mut keys = Vec::new();
        for address in &self.track_addresses {
            address
                .check_network(network)
                .map_err(|e| invalid(vars::TRACK_ADDRESSES, &format!("{address}: {e}")))?;
            keys.push(TrackingKey::Tree(address.ergo_tree().clone()));
        }
        keys.extend(self.track_tokens.iter().map(|id| TrackingKey::Token(*id)));
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// The key policy, admitting the configured `keys` ([`Config::tracked_keys`]) always.
    /// Fails if a listed address is for another network than the node's, or if a deny rule
    /// names a configured or an allowed address: that is a typo or a conflict, better seen at
    /// startup than as refused requests. Addresses are compared by tree, so two encodings of
    /// one tree conflict too.
    pub fn key_policy(
        &self,
        network: Network,
        keys: &[TrackingKey],
    ) -> Result<KeyPolicy, ConfigError> {
        let allowed = self
            .allow_addresses
            .iter()
            .flatten()
            .filter_map(|rule| match rule {
                AddressRule::Address(address) => Some(address),
                AddressRule::Class(_) => None,
            });
        let mut allowed_trees = Vec::new();
        for address in allowed {
            address
                .check_network(network)
                .map_err(|e| invalid(vars::ALLOW_ADDRESSES, &format!("{address}: {e}")))?;
            allowed_trees.push(address.ergo_tree());
        }
        for address in &self.deny_addresses {
            address
                .check_network(network)
                .map_err(|e| invalid(vars::DENY_ADDRESSES, &format!("{address}: {e}")))?;
            if keys.contains(&TrackingKey::Tree(address.ergo_tree().clone())) {
                return Err(invalid(
                    vars::DENY_ADDRESSES,
                    &format!("{address} is tracked through {}", vars::TRACK_ADDRESSES),
                ));
            }
            if allowed_trees.contains(&address.ergo_tree()) {
                return Err(invalid(
                    vars::DENY_ADDRESSES,
                    &format!("{address} is also allowed through {}", vars::ALLOW_ADDRESSES),
                ));
            }
        }
        Ok(KeyPolicy::new(
            self.allow_addresses.as_deref(),
            &self.deny_addresses,
            self.allow_tokens.as_deref(),
            keys,
        ))
    }
}

fn address(entry: &str) -> Result<Address, String> {
    Address::decode(entry).map_err(|e| e.to_string())
}

fn token_id(entry: &str) -> Result<TokenId, String> {
    entry.parse::<TokenId>().map_err(|e| e.to_string())
}

fn invalid(var: &'static str, reason: &str) -> ConfigError {
    ConfigError::Invalid { var, reason: reason.to_owned() }
}

fn number<T: std::str::FromStr>(
    var: &'static str,
    value: Option<String>,
    default: T,
) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    match value {
        None => Ok(default),
        Some(v) => v
            .parse()
            .map_err(|e: T::Err| invalid(var, &format!("{v:?}: {e}"))),
    }
}

/// A count of at least 1: a cap of 0 would refuse every key that has a box.
fn at_least_one(
    var: &'static str,
    value: Option<String>,
    default: usize,
) -> Result<usize, ConfigError> {
    let n = number(var, value, default)?;
    if n == 0 {
        return Err(invalid(var, "must be at least 1"));
    }
    Ok(n)
}

/// A duration in milliseconds, at least 1: tokio's interval panics on zero.
fn millis(
    var: &'static str,
    value: Option<String>,
    default: Duration,
) -> Result<Duration, ConfigError> {
    let ms = number(var, value, default.as_millis() as u64)?;
    if ms == 0 {
        return Err(invalid(var, "must be at least 1 ms"));
    }
    Ok(Duration::from_millis(ms))
}

fn list<T>(
    var: &'static str,
    value: Option<String>,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Vec<T>, ConfigError> {
    value
        .iter()
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| parse(v).map_err(|e| invalid(var, &format!("{v:?}: {e}"))))
        .collect()
}

/// An allowlist: `None` when unset or `any`, which allows everything, and empty for `none`.
/// `any` names the default, so it can be written down, as `.env.example` and the startup
/// log do. `none` is the only way to allow nothing, so a list that is only commas is refused
/// rather than read as `none`. Either word stands alone, and empty entries are skipped as in
/// any list, so `none,` is `none`.
fn allowlist<T>(
    var: &'static str,
    value: Option<String>,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Option<Vec<T>>, ConfigError> {
    let Some(value) = value else { return Ok(None) };
    let entries: Vec<&str> = value
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .collect();
    let word = ["any", "none"]
        .into_iter()
        .find(|word| entries.iter().any(|e| e.eq_ignore_ascii_case(word)));
    match (word, entries.len()) {
        (Some("any"), 1) => Ok(None),
        (Some(_), 1) => Ok(Some(Vec::new())),
        (Some(word), _) => {
            Err(invalid(var, &format!("`{word}` can't be combined with other entries")))
        }
        (None, 0) => Err(invalid(var, "no entries; use `none` to allow nothing")),
        (None, _) => entries
            .into_iter()
            .map(|e| parse(e).map_err(|error| invalid(var, &format!("{e:?}: {error}"))))
            .collect::<Result<_, _>>()
            .map(Some),
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
