//! Synthetic chain data shared by the examples: ids, trees, boxes and blocks made from a
//! number, so a run builds millions of distinct boxes without a node, and engines loaded
//! with them.

use std::sync::Arc;

use hergmes::{
    engine::{BlockRef, Engine, Event, LoadedBox, Query, Reply, TrackingKey},
    types::{
        Digest, HexBytes,
        ergo::{BlockHeader, IndexedBlock, IndexedBox, IndexedTransaction, UTxO},
    },
};

/// Where each kind of synthetic id starts, so no two kinds collide.
pub const BLOCK_IDS: u64 = 1 << 62;
pub const RING_BOXES: u64 = 1 << 61;
pub const TX_IDS: u64 = 1 << 60;
/// A key's boxes are numbered from `key << KEY_SHIFT`: room for a million boxes a key.
pub const KEY_SHIFT: u32 = 20;
/// Height of the first block.
pub const FIRST: u32 = 1_000_000;

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

/// The block at `height`, child of the one below it, paying one new box to each of `keys`.
pub fn block(height: u32, keys: &[u64]) -> IndexedBlock {
    let header = BlockHeader {
        id: digest(BLOCK_IDS + u64::from(height)),
        parent_id: digest(BLOCK_IDS + u64::from(height) - 1),
        height,
        json: "{}".into(),
    };
    let outputs = keys
        .iter()
        .enumerate()
        .map(|(i, &k)| IndexedBox {
            utxo: plain_box(RING_BOXES + u64::from(height) * 16 + i as u64, &tree(k)),
            inclusion_height: height,
        })
        .collect();
    let tx =
        IndexedTransaction { id: digest(TX_IDS + u64::from(height)), inputs: Vec::new(), outputs };
    IndexedBlock { header, transactions: vec![tx] }
}

pub fn tip(engine: &mut Engine) -> BlockRef {
    let Reply::Tip(Some(tip)) = engine.query(Query::Tip) else {
        unreachable!("a block was applied")
    };
    tip
}

/// Loads keys `from..from + count`, `boxes` each, at the engine's tip.
pub fn load(engine: &mut Engine, from: usize, count: usize, boxes: usize) {
    let as_of = tip(engine);
    for k in from..from + count {
        let tree = tree(k as u64);
        let loaded = (0..boxes)
            .map(|b| LoadedBox {
                utxo: Arc::new(plain_box(((k as u64) << KEY_SHIFT) + b as u64, &tree)),
                inclusion_height: as_of.height - (b % 100_000) as u32,
            })
            .collect();
        engine
            .apply(Event::KeyLoaded { key: TrackingKey::Tree(tree), as_of, boxes: loaded })
            .expect("a load is never an error");
    }
}
