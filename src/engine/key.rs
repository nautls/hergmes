use std::collections::BTreeMap;

use crate::types::{
    Digest,
    ergo::{ErgoTree, TemplateHash, TokenId, UTxO},
    ergo_tree::template_hash,
};

/// What a client tracks: the boxes guarded by an ErgoTree, the boxes holding a token, or the
/// boxes guarded by any constant-segregated tree of one template (a contract's every
/// instance, whatever its constants).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TrackingKey {
    Tree(ErgoTree),
    Token(TokenId),
    /// Last, so the template keys of a map in key order are its tail (see [`has_templates`]).
    Template(TemplateHash),
}

impl TrackingKey {
    pub fn matches(&self, utxo: &UTxO) -> bool {
        match self {
            TrackingKey::Tree(tree) => utxo.ergo_tree == *tree,
            TrackingKey::Token(id) => utxo.tokens.iter().any(|t| t.id == *id),
            TrackingKey::Template(hash) => template_hash(&utxo.ergo_tree.0) == Some(*hash),
        }
    }
}

/// Whether `keys` holds a template key, so a box's template is hashed only when one could
/// match it.
pub fn has_templates<V>(keys: &BTreeMap<TrackingKey, V>) -> bool {
    keys.range(TrackingKey::Template(Digest([0; 32]))..)
        .next()
        .is_some()
}

#[cfg(test)]
#[path = "key_tests.rs"]
mod tests;
