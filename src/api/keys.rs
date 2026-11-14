//! Tracking keys from what clients send: an address, or a token id.

use crate::{
    api::ApiError,
    engine::TrackingKey,
    types::{
        address::{Address, Network},
        ergo::TokenId,
    },
};

/// An address's key, its ErgoTree, for an address of the node's network only
/// ([`Address::check_network`]).
pub fn address_key(address: &str, network: Network) -> Result<TrackingKey, ApiError> {
    let address = Address::decode(address)
        .and_then(|a| a.check_network(network).map(|()| a))
        .map_err(|e| ApiError::InvalidAddress(e.to_string()))?;
    Ok(TrackingKey::Tree(address.into_ergo_tree()))
}

/// A token's key. The id is 64 hex characters, in either case.
pub fn token_key(id: &str) -> Result<TrackingKey, ApiError> {
    let id: TokenId = id.parse().map_err(|_| ApiError::InvalidTokenId)?;
    Ok(TrackingKey::Token(id))
}

#[cfg(test)]
#[path = "keys_tests.rs"]
mod tests;
