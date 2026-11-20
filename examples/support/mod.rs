//! Synthetic chain data shared by the examples: ids, trees and boxes made from a number, so
//! a run builds millions of distinct boxes without a node.

use hergmes::types::{Digest, HexBytes, ergo::UTxO};

/// 32 bytes that differ for every `n`, spread over the whole digest as real ids are, so the
/// maps' orderings see no pattern.
pub fn digest(n: u64) -> Digest<32> {
    let mut bytes = [0; 32];
    let mut x = n.wrapping_add(1);
    for chunk in bytes.chunks_mut(8) {
        // SplitMix64: a different, well-mixed word per chunk.
        x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        chunk.copy_from_slice(&(z ^ (z >> 31)).to_be_bytes());
    }
    Digest(bytes)
}

/// The P2PK tree of key `n`: `0008cd` and a 33-byte key, the commonest guard.
pub fn tree(n: u64) -> HexBytes {
    let mut bytes = vec![0x00, 0x08, 0xcd, 0x02];
    bytes.extend_from_slice(&digest(n).0);
    HexBytes(bytes)
}

/// A plain box: ERG only, no tokens and no registers, the smallest a box gets.
pub fn plain_box(n: u64, tree: &HexBytes) -> UTxO {
    UTxO {
        id: digest(n),
        ergo_tree: tree.clone(),
        creation_height: 1,
        value: 1_000_000,
        tokens: Vec::new(),
        registers: Default::default(),
        index: 0,
        transaction_id: digest(n ^ (1 << 63)),
    }
}
