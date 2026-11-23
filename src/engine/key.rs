use std::collections::BTreeMap;

use crate::types::{
    HexBytes,
    ergo::{ErgoTree, Template, TokenId, UTxO},
    ergo_tree::template,
};

/// What a client tracks: the boxes guarded by an ErgoTree, the boxes holding a token, or the
/// boxes guarded by any constant-segregated tree of one template (a contract's every
/// instance, whatever its constants).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TrackingKey {
    Tree(ErgoTree),
    Token(TokenId),
    Template(Template),
}

impl TrackingKey {
    pub fn matches(&self, utxo: &UTxO) -> bool {
        match self {
            TrackingKey::Tree(tree) => utxo.ergo_tree == *tree,
            TrackingKey::Token(id) => utxo.tokens.iter().any(|t| t.id == *id),
            TrackingKey::Template(bytes) => template(&utxo.ergo_tree.0) == Some(&bytes.0[..]),
        }
    }
}

/// Whether `keys` holds a template key, so a box's template is read only when one could
/// match it.
pub fn has_templates<V>(keys: &BTreeMap<TrackingKey, V>) -> bool {
    let first = TrackingKey::Template(HexBytes::from(Vec::new()));
    keys.range(first..).next().is_some()
}

#[cfg(test)]
#[path = "key_tests.rs"]
mod tests;
