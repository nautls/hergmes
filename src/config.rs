//! Configuration, read from a TOML file and a few environment variables.
//!
//! The file is optional: `--config <path>` names it, or else `hergmes.toml` is read from the
//! working directory if it is there. Every setting has a default but the node URL.
//!
//! The settings that depend on where the relay runs rather than on what it does, the node URL
//! and the two listen addresses, can also come from an environment variable ([`vars`]), so a
//! container can set them without a file. Each comes from the file or its variable and never
//! from both, so no precedence decides which one is in force.

use std::{
    ffi::OsString,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;

use crate::{
    adapters::{follower, loader, mempool, tokens::SUPPLY_LIMIT},
    api::{
        origins::OriginPolicy,
        policy::{AddressRule, KeyPolicy},
        ws,
    },
    engine::{self, TrackingKey},
    types::{
        address::{Address, Network},
        ergo::{Template, TokenId},
        ergo_tree::parse_template,
    },
};

/// The file read when `--config` is not given, looked for in the working directory only: a
/// relay started from another directory must not pick up a stray file from a parent.
pub const DEFAULT_PATH: &str = "hergmes.toml";

/// The environment variables that can set a setting instead of its key. A blank one counts as
/// unset, as an empty `ERGO_NODE_URL=` in a compose file means.
pub mod vars {
    /// Names the node, not the relay, so it has no `HERGMES_` prefix.
    pub const NODE_URL: &str = "ERGO_NODE_URL";
    pub const LISTEN_ADDR: &str = "HERGMES_LISTEN_ADDR";
    pub const METRICS_ADDR: &str = "HERGMES_METRICS_ADDR";
}

/// The keys of the file, as errors name them.
pub mod keys {
    pub const NODE_URL: &str = "node_url";
    pub const WINDOW_BLOCKS: &str = "window_blocks";
    pub const GRACE_SECS: &str = "grace_secs";
    pub const POLL_INTERVAL_MS: &str = "poll_interval_ms";
    pub const MEMPOOL_REFRESH_MS: &str = "mempool_refresh_ms";
    pub const TOKEN_SUPPLY_LIMIT: &str = "token_supply_limit";
    pub const TRACK_ADDRESSES: &str = "track_addresses";
    pub const TRACK_TOKENS: &str = "track_tokens";
    pub const TRACK_TEMPLATES: &str = "track_templates";
    pub const ALLOW_ADDRESSES: &str = "allow_addresses";
    pub const DENY_ADDRESSES: &str = "deny_addresses";
    pub const ALLOW_TOKENS: &str = "allow_tokens";
    pub const ALLOW_TEMPLATES: &str = "allow_templates";
    pub const BOX_CACHE_MB: &str = "box_cache_mb";
    pub const MAX_BOXES_PER_KEY: &str = "max_boxes_per_key";
    pub const MAX_TRACKED_UNITS: &str = "max_tracked_units";
    pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: &str = "max_subscriptions_per_connection";
    pub const LISTEN_ADDR: &str = "listen_addr";
    pub const ALLOW_ORIGINS: &str = "allow_origins";
    pub const METRICS_ADDR: &str = "metrics_addr";
}

/// Loopback by default, so a fresh install is not exposed until the operator chooses: usually
/// behind a reverse proxy, or `0.0.0.0:8080` in a container.
pub const LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);

/// The box cache's default budget: about 28k of the largest boxes (4 KB) to 195k of the
/// smallest, and about 185k of mainnet's usual ones.
pub const BOX_CACHE_MB: u64 = 128;

/// The file as written, before its values are checked. Every key is optional, and an unknown
/// key is refused, so a misspelled setting stops startup instead of leaving its default in
/// force. `hergmes.example.toml` documents each key.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    node_url: Option<String>,
    window_blocks: Option<u32>,
    grace_secs: Option<u64>,
    poll_interval_ms: Option<u64>,
    mempool_refresh_ms: Option<u64>,
    token_supply_limit: Option<u64>,
    #[serde(default)]
    track_addresses: Vec<String>,
    #[serde(default)]
    track_tokens: Vec<String>,
    #[serde(default)]
    track_templates: Vec<String>,
    /// Omitted, every address is allowed; empty, none is.
    allow_addresses: Option<Vec<String>>,
    #[serde(default)]
    deny_addresses: Vec<String>,
    /// Omitted, every token within the supply limit is allowed; empty, none is.
    allow_tokens: Option<Vec<String>>,
    /// Templates' bytes in hex. Omitted, every template is allowed; empty, none is.
    allow_templates: Option<Vec<String>>,
    box_cache_mb: Option<u64>,
    max_boxes_per_key: Option<usize>,
    max_tracked_units: Option<usize>,
    listen_addr: Option<SocketAddr>,
    /// Omitted, every origin is allowed, as with `["*"]`.
    allow_origins: Option<Vec<String>>,
    max_subscriptions_per_connection: Option<usize>,
    metrics_addr: Option<SocketAddr>,
}

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
    pub track_templates: Vec<Template>,
    /// The address allowlist; `None` when omitted, empty when it allows nothing.
    pub allow_addresses: Option<Vec<AddressRule>>,
    pub deny_addresses: Vec<Address>,
    /// The token allowlist; `None` when omitted, empty when it allows nothing.
    pub allow_tokens: Option<Vec<TokenId>>,
    /// The template allowlist; `None` when omitted, empty when it allows nothing.
    pub allow_templates: Option<Vec<Template>>,
    /// The box cache's budget in bytes, read in MiB; 0 disables the cache.
    pub box_cache_bytes: u64,
    pub listen_addr: SocketAddr,
    /// The browser pages and extensions that may use the API and the WebSocket.
    pub allow_origins: OriginPolicy,
    /// Where the metrics are served, apart from the public API; `None` serves none.
    pub metrics_addr: Option<SocketAddr>,
    pub ws: ws::Config,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0}; see `hergmes --help`")]
    Usage(String),
    #[error("failed to read {}: {source}", path.display())]
    Read { path: PathBuf, source: std::io::Error },
    #[error("{0}")]
    Syntax(#[from] toml::de::Error),
    #[error(
        "the node URL must be set, as `{}` in the config file or as `{}`",
        keys::NODE_URL,
        vars::NODE_URL
    )]
    MissingNodeUrl,
    #[error("`{key}` is set both in the config file and as `{var}`; set it in one place")]
    Twice { key: &'static str, var: &'static str },
    /// A bad value, named by its key or by its environment variable.
    #[error("`{key}`: {reason}")]
    Invalid { key: &'static str, reason: String },
}

/// What `--help` prints.
pub const HELP: &str = "\
Usage: hergmes [--config <path>]

Reads its settings from the file --config names, or else from hergmes.toml in the working
directory if it is there; hergmes.example.toml documents each one. ERGO_NODE_URL,
HERGMES_LISTEN_ADDR and HERGMES_METRICS_ADDR can set node_url, listen_addr and metrics_addr
instead, but a setting can't be in both places. RUST_LOG sets the log filter, and
MALLOC_ARENA_MAX overrides the glibc malloc arena cap of 4.";

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
pub enum Args {
    /// Run the relay, with the file `--config` names, if any.
    Run { config: Option<PathBuf> },
    /// Print [`HELP`] and exit.
    Help,
}

/// Reads the arguments after the program name. `--help` or `-h` anywhere asks for help, and
/// any other argument than `--config <path>` or `--config=<path>` is refused rather than
/// ignored, so a mistyped flag doesn't start the relay on its defaults.
pub fn args(args: impl IntoIterator<Item = OsString>) -> Result<Args, ConfigError> {
    let args: Vec<OsString> = args.into_iter().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(Args::Help);
    }
    let mut args = args.into_iter();
    let mut path = None;
    while let Some(arg) = args.next() {
        let value = if arg == "--config" {
            args.next()
                .ok_or_else(|| ConfigError::Usage("`--config` needs a path".to_owned()))?
        } else if let Some(value) = arg.to_str().and_then(|a| a.strip_prefix("--config=")) {
            value.into()
        } else {
            let arg = arg.to_string_lossy();
            return Err(ConfigError::Usage(format!("unknown argument `{arg}`")));
        };
        if path.replace(PathBuf::from(value)).is_some() {
            return Err(ConfigError::Usage("`--config` is given twice".to_owned()));
        }
    }
    Ok(Args::Run { config: path })
}

/// The file to read: the one `--config` names, which must exist, or else [`DEFAULT_PATH`] in
/// `dir` unless nothing is there, or else none. Anything at the default path is read, and a
/// broken link, a directory or an unreadable file fails as it is read: falling back to the
/// defaults would open the relay to any key.
pub fn locate(flag: Option<PathBuf>, dir: &Path) -> Option<PathBuf> {
    flag.or_else(|| {
        let path = dir.join(DEFAULT_PATH);
        match path.symlink_metadata() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            _ => Some(path),
        }
    })
}

impl Config {
    /// Reads the file at `path`, if any, and the [`vars`] from the environment.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let text = match path {
            Some(path) => std::fs::read_to_string(path)
                .map_err(|source| ConfigError::Read { path: path.to_owned(), source })?,
            None => String::new(),
        };
        Self::from_toml(&text, |name| std::env::var(name).ok())
    }

    /// Reads the configuration from the file's `text` and the [`vars`], looked up with `var`.
    pub fn from_toml(
        text: &str,
        var: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let file: File = toml::from_str(text)?;
        let engine_defaults = engine::Config::default();
        let follower_defaults = follower::Config::default();
        let loader_defaults = loader::Config::default();

        if file
            .node_url
            .as_ref()
            .is_some_and(|url| url.trim().is_empty())
        {
            return Err(invalid(keys::NODE_URL, "must not be empty"));
        }
        let node_url = either(keys::NODE_URL, file.node_url, vars::NODE_URL, &var)?
            .map(|url| url.trim().to_owned())
            .ok_or(ConfigError::MissingNodeUrl)?;
        let window = file.window_blocks.unwrap_or(engine_defaults.window);
        if window == 0 {
            return Err(invalid(keys::WINDOW_BLOCKS, "the window keeps at least the tip"));
        }
        let cache_mb = file.box_cache_mb.unwrap_or(BOX_CACHE_MB);
        let box_cache_bytes = cache_mb
            .checked_mul(1024 * 1024)
            .ok_or_else(|| invalid(keys::BOX_CACHE_MB, &format!("{cache_mb} MiB is too large")))?;

        Ok(Self {
            node_url,
            engine: engine::Config {
                window,
                grace: grace(file.grace_secs, engine_defaults)?,
                // Not a setting: it protects requests that belong together, not a policy.
                evict_after: engine_defaults.evict_after,
                // Not a setting either: it bounds a block's time, which no deployment wants
                // longer.
                untrack_units_per_block: engine_defaults.untrack_units_per_block,
                max_tracked_units: at_least_one(
                    keys::MAX_TRACKED_UNITS,
                    file.max_tracked_units,
                    engine_defaults.max_tracked_units,
                )?,
            },
            follower: follower::Config {
                interval: millis(
                    keys::POLL_INTERVAL_MS,
                    file.poll_interval_ms,
                    follower_defaults.interval,
                )?,
                wake_gap: follower_defaults.wake_gap,
                mempool: mempool::Config {
                    refresh: millis(
                        keys::MEMPOOL_REFRESH_MS,
                        file.mempool_refresh_ms,
                        follower_defaults.mempool.refresh,
                    )?,
                },
            },
            loader: loader::Config {
                max_boxes: at_least_one(
                    keys::MAX_BOXES_PER_KEY,
                    file.max_boxes_per_key,
                    loader_defaults.max_boxes,
                )?,
                ..loader_defaults
            },
            token_supply_limit: file.token_supply_limit.unwrap_or(SUPPLY_LIMIT),
            track_addresses: list(keys::TRACK_ADDRESSES, &file.track_addresses, address)?,
            track_tokens: list(keys::TRACK_TOKENS, &file.track_tokens, token_id)?,
            track_templates: list(keys::TRACK_TEMPLATES, &file.track_templates, parse_template)?,
            allow_addresses: allowlist(keys::ALLOW_ADDRESSES, file.allow_addresses, |a| {
                AddressRule::parse(a).map_err(|e| e.to_string())
            })?,
            deny_addresses: list(keys::DENY_ADDRESSES, &file.deny_addresses, address)?,
            allow_tokens: allowlist(keys::ALLOW_TOKENS, file.allow_tokens, token_id)?,
            allow_templates: allowlist(
                keys::ALLOW_TEMPLATES,
                file.allow_templates,
                parse_template,
            )?,
            box_cache_bytes,
            listen_addr: either(keys::LISTEN_ADDR, file.listen_addr, vars::LISTEN_ADDR, &var)?
                .unwrap_or(LISTEN_ADDR),
            allow_origins: match file.allow_origins {
                None => OriginPolicy::Any,
                Some(entries) => OriginPolicy::parse(&entries)
                    .map_err(|reason| invalid(keys::ALLOW_ORIGINS, &reason))?,
            },
            metrics_addr: either(keys::METRICS_ADDR, file.metrics_addr, vars::METRICS_ADDR, &var)?,
            ws: ws::Config {
                max_subscriptions: at_least_one(
                    keys::MAX_SUBSCRIPTIONS_PER_CONNECTION,
                    file.max_subscriptions_per_connection,
                    ws::Config::default().max_subscriptions,
                )?,
            },
        })
    }

    /// The keys tracked from the start. Fails if an address is for another network than the
    /// node's ([`Address::check_network`]).
    pub fn tracked_keys(&self, network: Network) -> Result<Vec<TrackingKey>, ConfigError> {
        let mut keys = Vec::new();
        for address in &self.track_addresses {
            address
                .check_network(network)
                .map_err(|e| invalid(keys::TRACK_ADDRESSES, &format!("{address}: {e}")))?;
            keys.push(TrackingKey::Tree(address.ergo_tree().clone()));
        }
        keys.extend(self.track_tokens.iter().map(|id| TrackingKey::Token(*id)));
        keys.extend(
            self.track_templates
                .iter()
                .map(|t| TrackingKey::Template(t.clone())),
        );
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
                .map_err(|e| invalid(keys::ALLOW_ADDRESSES, &format!("{address}: {e}")))?;
            allowed_trees.push(address.ergo_tree());
        }
        for address in &self.deny_addresses {
            address
                .check_network(network)
                .map_err(|e| invalid(keys::DENY_ADDRESSES, &format!("{address}: {e}")))?;
            if keys.contains(&TrackingKey::Tree(address.ergo_tree().clone())) {
                return Err(invalid(
                    keys::DENY_ADDRESSES,
                    &format!("{address} is tracked through `{}`", keys::TRACK_ADDRESSES),
                ));
            }
            if allowed_trees.contains(&address.ergo_tree()) {
                return Err(invalid(
                    keys::DENY_ADDRESSES,
                    &format!("{address} is also allowed through `{}`", keys::ALLOW_ADDRESSES),
                ));
            }
        }
        Ok(KeyPolicy::new(
            self.allow_addresses.as_deref(),
            &self.deny_addresses,
            self.allow_tokens.as_deref(),
            self.allow_templates.as_deref(),
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

fn invalid(key: &'static str, reason: &str) -> ConfigError {
    ConfigError::Invalid { key, reason: reason.to_owned() }
}

/// A setting from the file's `key` or from the variable `name`, which must not both be set.
fn either<T: std::str::FromStr>(
    key: &'static str,
    file: Option<T>,
    name: &'static str,
    var: impl Fn(&str) -> Option<String>,
) -> Result<Option<T>, ConfigError>
where
    T::Err: std::fmt::Display,
{
    let value = var(name)
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty());
    match (file, value) {
        (Some(_), Some(_)) => Err(ConfigError::Twice { key, var: name }),
        (file, None) => Ok(file),
        (None, Some(v)) => v
            .parse()
            .map(Some)
            .map_err(|e: T::Err| invalid(name, &format!("{v:?}: {e}"))),
    }
}

/// The grace period, at least the engine's wait before eviction: a shorter one would untrack
/// a key between two requests that belong together (the pages of one walk), which that wait
/// is there to protect.
fn grace(secs: Option<u64>, defaults: engine::Config) -> Result<Duration, ConfigError> {
    let grace = secs.map_or(defaults.grace, Duration::from_secs);
    if grace < defaults.evict_after {
        let least = defaults.evict_after.as_secs();
        return Err(invalid(keys::GRACE_SECS, &format!("must be at least {least}")));
    }
    Ok(grace)
}

/// A count of at least 1: a cap of 0 would refuse every key that has a box, or every
/// subscription.
fn at_least_one(
    key: &'static str,
    value: Option<usize>,
    default: usize,
) -> Result<usize, ConfigError> {
    match value.unwrap_or(default) {
        0 => Err(invalid(key, "must be at least 1")),
        n => Ok(n),
    }
}

/// A duration in milliseconds, at least 1: tokio's interval panics on zero.
fn millis(
    key: &'static str,
    value: Option<u64>,
    default: Duration,
) -> Result<Duration, ConfigError> {
    match value {
        None => Ok(default),
        Some(0) => Err(invalid(key, "must be at least 1 ms")),
        Some(ms) => Ok(Duration::from_millis(ms)),
    }
}

fn list<T>(
    key: &'static str,
    entries: &[String],
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Vec<T>, ConfigError> {
    entries
        .iter()
        .map(|e| parse(e).map_err(|error| invalid(key, &format!("{e:?}: {error}"))))
        .collect()
}

/// An allowlist: `None` when omitted, which allows everything, and empty to allow nothing.
/// The words `any` and `none` that environment variables needed are refused with a hint, as
/// they would otherwise fail as an unknown address, token id or template.
fn allowlist<T>(
    key: &'static str,
    entries: Option<Vec<String>>,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Option<Vec<T>>, ConfigError> {
    let Some(entries) = entries else { return Ok(None) };
    if let Some(word) = entries
        .iter()
        .find(|e| e.eq_ignore_ascii_case("any") || e.eq_ignore_ascii_case("none"))
    {
        return Err(invalid(
            key,
            &format!("{word:?}: omit the key to allow any, or write `[]` to allow none"),
        ));
    }
    list(key, &entries, parse).map(Some)
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
