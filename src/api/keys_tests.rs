//! Keys from addresses and token ids: the node's network only, and token ids in either case.

use super::*;
use crate::types::HexBytes;

fn open() -> KeyPolicy {
    KeyPolicy::default()
}

const FEE_CONTRACT: &str = "1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304";
const FEE_MAINNET: &str = "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe";
const FEE_TESTNET: &str = "Bf1X9JgQTUtgntaer91B24n6kP8L2kqEiQqNf1z97BKo9UbnW3WRP9VXu8BXd1LsYCiYbHJEdWKxkF5YNx5n7m31wsDjbEuB3B13ZMDVBWkepGmWfGa71otpFViHDCuvbw1uNicAQnfuWfnj8fbCa4";
const NFT: &str = "20fa2bf23962cdf51b07722d6237c0c7b8a44f78856c0f7ec308dc1ef1a92a51";

#[test]
fn an_address_is_its_tree_on_the_nodes_network_only() {
    let tree = TrackingKey::Tree(HexBytes(hex::decode(FEE_CONTRACT).unwrap()));
    assert_eq!(address_key(FEE_MAINNET, Network::Mainnet, &open()), Ok(tree.clone()));
    assert_eq!(address_key(FEE_TESTNET, Network::Testnet, &open()), Ok(tree));
    // The same tree, so the same key: only the network tells them apart.
    for (address, node) in [(FEE_TESTNET, Network::Mainnet), (FEE_MAINNET, Network::Testnet)] {
        let error = address_key(address, node, &open()).unwrap_err();
        assert_eq!(error.code(), "invalid_address", "{error}");
    }
}

#[test]
fn a_bad_address_is_invalid() {
    let bad_checksum = format!("{}x", &FEE_MAINNET[..FEE_MAINNET.len() - 1]);
    for address in ["", "0OIl", &bad_checksum] {
        let error = address_key(address, Network::Mainnet, &open()).unwrap_err();
        assert_eq!(error.code(), "invalid_address", "{address:?}: {error}");
    }
}

#[test]
fn a_token_id_is_64_hex_characters_in_either_case() {
    let key = TrackingKey::Token(NFT.parse().unwrap());
    assert_eq!(token_key(NFT, &open()), Ok(key.clone()));
    assert_eq!(token_key(&NFT.to_uppercase(), &open()), Ok(key));
    for id in ["", &NFT[..62], &format!("{NFT}00"), &NFT.replace('a', "g")] {
        assert_eq!(token_key(id, &open()), Err(ApiError::InvalidTokenId), "{id:?}");
    }
}

/// A bad key is refused for what it is, before the policy is asked: the code tells the
/// client to fix the request, not that the relay won't serve it.
#[test]
fn a_bad_key_is_invalid_whatever_the_policy() {
    let nothing = KeyPolicy::new(Some(&[]), &[], Some(&[]), &[]);
    for (address, node) in [("0OIl", Network::Mainnet), (FEE_TESTNET, Network::Mainnet)] {
        let error = address_key(address, node, &nothing).unwrap_err();
        assert_eq!(error.code(), "invalid_address", "{address}");
    }
    assert_eq!(token_key("abcd", &nothing), Err(ApiError::InvalidTokenId));
    assert_eq!(address_key(FEE_MAINNET, Network::Mainnet, &nothing), Err(ApiError::KeyNotAllowed));
    assert_eq!(token_key(NFT, &nothing), Err(ApiError::KeyNotAllowed));
}
