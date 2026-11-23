//! The operator's key policy: which keys clients may name.
//!
//! It is an admission check at the edge, so the engine never sees a refused key. Addresses
//! are judged by their ErgoTree, never by the type in the address's header: a P2S address
//! can carry a P2PK or P2SH tree, and both encodings are one key, so judging the header
//! would let a client re-encode an address to get past a rule.

use std::{collections::HashSet, fmt};

use crate::{
    engine::TrackingKey,
    types::{
        address::{Address, AddressError, AddressType},
        ergo::{TemplateHash, TokenId},
    },
};

/// One entry of the address allowlist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressRule {
    /// Every address whose tree is of this type.
    Class(AddressType),
    /// One address, matched by its tree.
    Address(Address),
}

impl AddressRule {
    /// `p2pk`, `p2sh` or `p2s` in any case, or an address.
    pub fn parse(entry: &str) -> Result<Self, AddressError> {
        let class =
            [("p2pk", AddressType::P2PK), ("p2sh", AddressType::P2SH), ("p2s", AddressType::P2S)]
                .into_iter()
                .find(|(name, _)| entry.eq_ignore_ascii_case(name));
        match class {
            Some((_, class)) => Ok(Self::Class(class)),
            None => Address::decode(entry).map(Self::Address),
        }
    }
}

/// Which keys are admitted. It holds only the keys the rules name, as trees, token ids and
/// template hashes, so it costs nothing per key it has not been told about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPolicy {
    /// The address classes admitted whole; `None` when there is no address rule, so every
    /// address is.
    classes: Option<Vec<AddressType>>,
    /// Whether a token not named is admitted: there is no token rule.
    any_token: bool,
    /// Whether a template not named is admitted: there is no template rule.
    any_template: bool,
    /// Keys admitted by name: the listed addresses, tokens and templates, and the configured
    /// keys.
    allowed: HashSet<TrackingKey>,
    /// Addresses refused by name. The config refuses a configured key here at startup.
    denied: HashSet<TrackingKey>,
}

impl Default for KeyPolicy {
    /// No rules: every key is admitted.
    fn default() -> Self {
        Self {
            classes: None,
            any_token: true,
            any_template: true,
            allowed: HashSet::new(),
            denied: HashSet::new(),
        }
    }
}

impl KeyPolicy {
    /// `allow_addresses`, `allow_tokens` and `allow_templates` are `None` when unset, which
    /// admits every key of that kind, and empty for `none`. `configured` are the keys tracked from the start,
    /// always admitted: they are the operator's own choice.
    pub fn new<'a>(
        allow_addresses: Option<&[AddressRule]>,
        deny_addresses: impl IntoIterator<Item = &'a Address>,
        allow_tokens: Option<&[TokenId]>,
        allow_templates: Option<&[TemplateHash]>,
        configured: &[TrackingKey],
    ) -> Self {
        let mut allowed: HashSet<TrackingKey> = configured.iter().cloned().collect();
        let mut classes = Vec::new();
        for rule in allow_addresses.unwrap_or_default() {
            match rule {
                AddressRule::Class(class) => classes.push(*class),
                AddressRule::Address(address) => {
                    allowed.insert(TrackingKey::Tree(address.ergo_tree().clone()));
                }
            }
        }
        allowed.extend(
            allow_tokens
                .unwrap_or_default()
                .iter()
                .map(|id| TrackingKey::Token(*id)),
        );
        allowed.extend(
            allow_templates
                .unwrap_or_default()
                .iter()
                .map(|hash| TrackingKey::Template(*hash)),
        );
        let denied = deny_addresses
            .into_iter()
            .map(|a| TrackingKey::Tree(a.ergo_tree().clone()))
            .collect();
        Self {
            classes: allow_addresses.map(|_| classes),
            any_token: allow_tokens.is_none(),
            any_template: allow_templates.is_none(),
            allowed,
            denied,
        }
    }

    /// Whether clients may name `key`. Deny wins over every allow rule.
    pub fn admits(&self, key: &TrackingKey) -> bool {
        if self.denied.contains(key) {
            return false;
        }
        if self.allowed.contains(key) {
            return true;
        }
        match key {
            TrackingKey::Tree(tree) => self
                .classes
                .as_ref()
                .is_none_or(|classes| classes.contains(&AddressType::of_tree(&tree.0))),
            TrackingKey::Token(_) => self.any_token,
            TrackingKey::Template(_) => self.any_template,
        }
    }
}

/// One line for the startup log, so an operator sees which rules are in force, and a
/// misspelled variable, which leaves its rule out, shows as `any`. Keys named in the rules
/// are counted, not listed: which keys an instance serves is no business of its log.
impl fmt::Display for KeyPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let named =
            |kind: fn(&TrackingKey) -> bool| self.allowed.iter().filter(|k| kind(k)).count();
        let trees = named(|k| matches!(k, TrackingKey::Tree(_)));
        let mut addresses = match &self.classes {
            None => vec!["any".to_owned()],
            Some(classes) => classes
                .iter()
                .map(|class| match class {
                    AddressType::P2PK => "p2pk".to_owned(),
                    AddressType::P2SH => "p2sh".to_owned(),
                    AddressType::P2S => "p2s".to_owned(),
                })
                .collect(),
        };
        if self.classes.is_some() && trees > 0 {
            addresses.push(format!("{trees} by name"));
        }
        if addresses.is_empty() {
            addresses.push("none".to_owned());
        }
        if !self.denied.is_empty() {
            addresses.push(format!("{} denied", self.denied.len()));
        }
        let rule = |any: bool, named: usize| match (any, named) {
            (true, _) => "any".to_owned(),
            (false, 0) => "none".to_owned(),
            (false, n) => format!("{n} by name"),
        };
        let tokens = rule(self.any_token, named(|k| matches!(k, TrackingKey::Token(_))));
        let templates = rule(self.any_template, named(|k| matches!(k, TrackingKey::Template(_))));
        write!(f, "addresses: {}; tokens: {tokens}; templates: {templates}", addresses.join(", "))
    }
}

#[cfg(test)]
#[path = "policy_tests.rs"]
mod tests;
