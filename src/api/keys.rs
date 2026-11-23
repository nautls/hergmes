//! Tracking keys from what clients send: an address, a token id, or a template hash.
//!
//! Every route that takes a key gets it here, so the operator's [`KeyPolicy`] covers all of
//! them, and a refused key costs no node call.

use crate::{
    api::{ApiError, policy::KeyPolicy},
    engine::TrackingKey,
    types::{
        address::{Address, Network},
        ergo::{TemplateHash, TokenId},
    },
};

/// An address's key, its ErgoTree, for an address of the node's network only
/// ([`Address::check_network`]) that the policy admits.
pub fn address_key(
    address: &str,
    network: Network,
    policy: &KeyPolicy,
) -> Result<TrackingKey, ApiError> {
    let address = Address::decode(address)
        .and_then(|a| a.check_network(network).map(|()| a))
        .map_err(|e| ApiError::InvalidAddress(e.to_string()))?;
    admitted(TrackingKey::Tree(address.into_ergo_tree()), policy)
}

/// A token's key, if the policy admits it. The id is 64 hex characters, in either case.
pub fn token_key(id: &str, policy: &KeyPolicy) -> Result<TrackingKey, ApiError> {
    let id: TokenId = id.parse().map_err(|_| ApiError::InvalidTokenId)?;
    admitted(TrackingKey::Token(id), policy)
}

/// A template's key, if the policy admits it. The hash is 64 hex characters, in either case.
/// Whether its trees have segregated constants shows only once its boxes are read.
pub fn template_key(hash: &str, policy: &KeyPolicy) -> Result<TrackingKey, ApiError> {
    let hash: TemplateHash = hash.parse().map_err(|_| ApiError::InvalidTemplateHash)?;
    admitted(TrackingKey::Template(hash), policy)
}

fn admitted(key: TrackingKey, policy: &KeyPolicy) -> Result<TrackingKey, ApiError> {
    if !policy.admits(&key) {
        return Err(ApiError::KeyNotAllowed);
    }
    Ok(key)
}

#[cfg(test)]
#[path = "keys_tests.rs"]
mod tests;
