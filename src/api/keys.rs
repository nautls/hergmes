//! Tracking keys from what clients send: an address, or a token id.

use crate::{
    api::ApiError,
    engine::TrackingKey,
    types::{
        address::{Address, Network},
        ergo::TokenId,
    },
};

/// An address's key, its ErgoTree. An address of another network than the node's is
/// refused: a testnet and a mainnet P2PK address with the same public key have the same
/// tree, so the relay would serve the other network's boxes under it.
pub fn address_key(address: &str, network: Network) -> Result<TrackingKey, ApiError> {
    let address = Address::decode(address).map_err(|e| ApiError::InvalidAddress(e.to_string()))?;
    if address.network() != network {
        return Err(ApiError::InvalidAddress(format!(
            "a {:?} address, but the node is on {network:?}",
            address.network()
        )));
    }
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
