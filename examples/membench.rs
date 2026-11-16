//! The memory benchmark (DESIGN.md, "Memory benchmark"): two runs, each checking one
//! estimate against the process's resident memory.
//!
//! ```sh
//! cargo run --release --example membench -- engine <keys> <boxes-per-key>
//! cargo run --release --example membench -- cache <boxes>
//! ```
//!
//! `engine` loads synthetic address keys straight into an [`Engine`], with no adapters and
//! no cache, and reports the growth per tracked box: the id state DESIGN.md puts at about
//! 135 bytes, and 600 a key. `cache` fills a [`BoxCache`] with small boxes and reports the growth against
//! the cache's own weighted size, which is what the budget bounds.
//!
//! `engine` also prints what a load and a scrape's `Query::Stats` take on average, in
//! microseconds: both must stay flat, or nearly, as the keys grow.
//!
//! One run is one process, so nothing but the structure under test grows. A run's figure
//! still holds what the allocator keeps of freed memory (a load's full boxes are freed once
//! the engine has their ids), so run each at two sizes and take the slope: with the boxes per
//! key fixed, that share is the same at both sizes and cancels.
//!
//! An example rather than a test: the test harness's threads and other tests' allocations
//! would count in resident memory.

use std::sync::Arc;

use hergmes::{
    adapters::{BoxCache, cache::weight},
    api::metrics::resident_memory,
    engine::{BlockRef, Config, Engine, Event, LoadedBox, Query, Reply, TrackingKey},
    types::{
        Digest, HexBytes,
        ergo::{BlockHeader, IndexedBlock, UTxO, UnconfirmedTransaction},
    },
};

/// Pending txs in the benchmark's mempool snapshot: a busy pool, a fifth of the node's cap.
const POOL_TXS: u64 = 200;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let number = |i: usize| -> usize {
        let arg = args.get(i).unwrap_or_else(|| usage());
        arg.parse().unwrap_or_else(|_| usage())
    };
    match args.first().map(String::as_str) {
        Some("engine") => engine(number(1), number(2)),
        Some("cache") => cache(number(1)),
        _ => usage(),
    }
}

fn usage() -> ! {
    eprintln!("usage: membench engine <keys> <boxes-per-key> | membench cache <boxes>");
    std::process::exit(2)
}

fn resident() -> u64 {
    resident_memory().expect("resident memory is read from /proc, on Linux")
}

/// 32 bytes that differ for every `n`, spread over the whole digest as real ids are, so the
/// maps' orderings see no pattern.
fn digest(n: u64) -> Digest<32> {
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
fn tree(n: u64) -> HexBytes {
    let mut bytes = vec![0x00, 0x08, 0xcd, 0x02];
    bytes.extend_from_slice(&digest(n).0);
    HexBytes(bytes)
}

/// A plain box: ERG only, no tokens and no registers, the smallest a box gets.
fn plain_box(n: u64, tree: &HexBytes) -> UTxO {
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

fn engine(keys: usize, boxes_per_key: usize) {
    let boxes = keys * boxes_per_key;
    let mut engine = Engine::new(Config { max_tracked_boxes: boxes, ..Config::DEFAULT });
    let tip = BlockRef { height: 1_000_000, header_id: digest(u64::MAX) };
    let header = BlockHeader {
        id: tip.header_id,
        parent_id: digest(u64::MAX - 1),
        height: tip.height,
        json: "{}".into(),
    };
    engine
        .apply(Event::BlockApplied(IndexedBlock { header, transactions: Vec::new() }))
        .expect("the first block");

    // A pool as a busy node holds, paying no tracked key: every load decides the snapshot
    // again and reads the new key's overlay from it, as it does in the relay.
    let stranger = tree(u64::MAX);
    let txs: Vec<UnconfirmedTransaction> = (0..POOL_TXS)
        .map(|t| UnconfirmedTransaction {
            id: digest(u64::MAX - 2 - 3 * t),
            inputs: vec![plain_box(u64::MAX - 3 - 3 * t, &stranger)],
            outputs: vec![plain_box(u64::MAX - 4 - 3 * t, &stranger)],
        })
        .collect();
    engine
        .apply(Event::MempoolSnapshot { txs: txs.into(), observed_tip: tip })
        .expect("a snapshot is never an error");

    let before = resident();
    let loading = std::time::Instant::now();
    for k in 0..keys {
        let tree = tree(k as u64);
        // Inclusion heights spread below the tip, as a real key's are.
        let loaded: Vec<LoadedBox> = (0..boxes_per_key)
            .map(|b| LoadedBox {
                utxo: Arc::new(plain_box((k * boxes_per_key + b) as u64, &tree)),
                inclusion_height: tip.height - (b % 100_000) as u32,
            })
            .collect();
        let outputs = engine
            .apply(Event::KeyLoaded { key: TrackingKey::Tree(tree), as_of: tip, boxes: loaded })
            .expect("a load is never an error");
        assert!(outputs.len() <= 1, "tracked, with at most its first balance: {outputs:?}");
    }
    let after = resident();
    let load_micros = loading.elapsed().as_micros() / keys as u128;

    // What a scrape costs the engine task: the walk of the key index, timed over a few runs.
    let started = std::time::Instant::now();
    for _ in 0..9 {
        engine.query(Query::Stats);
    }
    let stats_micros = started.elapsed().as_micros() / 9;
    let Reply::Stats(stats) = engine.query(Query::Stats) else { unreachable!() };
    assert_eq!((stats.address_keys, stats.tracked_boxes), (keys, boxes), "every key was loaded");
    let grown = after.saturating_sub(before);
    println!(
        "engine keys={keys} boxes_per_key={boxes_per_key} boxes={boxes} rss_before={before} \
         rss_after={after} grown={grown} bytes_per_box={:.1} load_micros={load_micros} stats_micros={stats_micros}",
        grown as f64 / boxes as f64
    );
}

fn cache(boxes: usize) {
    let tree = tree(0);
    let each = u64::from(weight(&plain_box(0, &tree)));
    // Room for every box and a little more, so nothing is evicted: the run measures what the
    // cache holds, not what it turned away.
    let budget = each * boxes as u64 + each * 16;
    let cache = BoxCache::new(budget);

    let before = resident();
    for n in 0..boxes {
        cache.insert(Arc::new(plain_box(n as u64, &tree)));
    }
    let stats = cache.stats();
    let after = resident();

    assert_eq!(stats.entries, boxes as u64, "nothing was evicted");
    let grown = after.saturating_sub(before);
    println!(
        "cache boxes={boxes} weight_per_box={each} weighted_bytes={} rss_before={before} \
         rss_after={after} grown={grown} bytes_per_box={:.1} grown_over_weighted={:.2}",
        stats.weighted_bytes,
        grown as f64 / boxes as f64,
        grown as f64 / stats.weighted_bytes as f64
    );
}
