//! What tracking a template key costs the engine: hashing each box's template, over mainnet's
//! own mix of trees.
//!
//! ```sh
//! cargo run --release --example templatebench -- <keys> <block-outputs> <pool-txs>
//! ```
//!
//! The trees are the ones the recorded mainnet block and mempool hold, each as often as it
//! occurs there (`tests/fixtures/indexed_block.json`, `mempool.json`), so the share of
//! segregated trees and their sizes are mainnet's.
//!
//! - `hash`: [`template_hash`] alone, per box, over the block's and the pool's mixes, and for
//!   the kinds of tree in them: a tree without segregated constants answers at its header, a
//!   segregated one is walked and hashed, so the largest tree is the slowest.
//! - `events`: two engines tracking `<keys>` address keys of ten boxes each, one of them also
//!   a template key holding no box. Each is given the same events: a block of
//!   `<block-outputs>` outputs, and mempool snapshots of `<pool-txs>` txs, built from the
//!   mixes with fresh ids. The template key matches none of them, so the difference is the
//!   hashing alone: every block output once; every pending input and output in the overlays,
//!   and again in the check of which inputs are confirmed.
//!
//! Each figure is the median of [`RUNS`], the two engines taking turns. An example rather than
//! a test: it measures, and a time is no assertion on a shared CI machine.

use std::{hint::black_box, sync::Arc, time::Instant};

use hergmes::{
    engine::{Config, Engine, Event, TrackingKey},
    types::{
        HexBytes,
        ergo::{
            BlockHeader, IndexedBlock, IndexedBox, IndexedTransaction, UTxO, UnconfirmedTransaction,
        },
        ergo_tree::template_hash,
    },
};
use serde_json::Value;
use support::{BLOCK_IDS, FIRST, TX_IDS, block, digest, load, plain_box, tip};

mod support;

const RUNS: usize = 11;
/// Where this example's box ids start, apart from the other kinds.
const IDS: u64 = 1 << 58;
/// Hash passes over a mix, for a stable time per box.
const HASH_PASSES: usize = 2_000;

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap_or_else(|_| usage()))
        .collect();
    let [keys, outputs, pool] = args[..] else { usage() };
    let block_mix = trees("indexed_block");
    let pool_mix = trees("mempool");
    hash(&block_mix, &pool_mix);
    events(keys, outputs, pool, &block_mix, &pool_mix);
}

fn usage() -> ! {
    eprintln!("usage: templatebench <keys> <block-outputs> <pool-txs>");
    std::process::exit(2)
}

/// Every tree in a recorded fixture, as often as it occurs.
fn trees(fixture: &str) -> Vec<HexBytes> {
    let path = format!("{}/tests/fixtures/{fixture}.json", env!("CARGO_MANIFEST_DIR"));
    let json: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut out = Vec::new();
    collect(&json, &mut out);
    out
}

fn collect(json: &Value, out: &mut Vec<HexBytes>) {
    match json {
        Value::Object(map) => {
            for (key, value) in map {
                match value.as_str() {
                    Some(tree) if key == "ergoTree" => {
                        out.push(HexBytes::from(hex::decode(tree).unwrap()));
                    }
                    _ => collect(value, out),
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|v| collect(v, out)),
        _ => {}
    }
}

/// Nanoseconds per call of `template_hash` over `trees`.
fn hash_ns(trees: &[&HexBytes]) -> f64 {
    let started = Instant::now();
    for _ in 0..HASH_PASSES {
        for tree in trees {
            black_box(template_hash(black_box(&tree.0)));
        }
    }
    started.elapsed().as_nanos() as f64 / (HASH_PASSES * trees.len()) as f64
}

fn hash(block_mix: &[HexBytes], pool_mix: &[HexBytes]) {
    let all: Vec<&HexBytes> = block_mix.iter().chain(pool_mix).collect();
    let segregated: Vec<&HexBytes> = all
        .iter()
        .copied()
        .filter(|t| template_hash(&t.0).is_some())
        .collect();
    let plain: Vec<&HexBytes> = all
        .iter()
        .copied()
        .filter(|t| template_hash(&t.0).is_none())
        .collect();
    let largest = *segregated.iter().max_by_key(|t| t.0.len()).unwrap();
    let mean = |ts: &[&HexBytes]| ts.iter().map(|t| t.0.len()).sum::<usize>() / ts.len();
    println!(
        "hash block_ns_per_box={:.0} pool_ns_per_box={:.0} segregated_ns={:.0} \
         (n={} mean_bytes={}) plain_ns={:.1} (n={}) largest_ns={:.0} (bytes={})",
        hash_ns(&block_mix.iter().collect::<Vec<_>>()),
        hash_ns(&pool_mix.iter().collect::<Vec<_>>()),
        hash_ns(&segregated),
        segregated.len(),
        mean(&segregated),
        hash_ns(&plain),
        plain.len(),
        hash_ns(&[largest]),
        largest.0.len(),
    );
}

/// The mix's `i`-th tree, cycling.
fn pick(mix: &[HexBytes], i: usize) -> &HexBytes {
    &mix[i % mix.len()]
}

/// A block of `outputs` boxes guarded by the mix's trees, as one tx: inputs cost no hashing
/// (spends are found by box id).
fn mixed_block(height: u32, outputs: usize, mix: &[HexBytes]) -> IndexedBlock {
    let mut b = block(height, &[]);
    let base = IDS + (u64::from(height) << 20);
    let outputs = (0..outputs)
        .map(|i| IndexedBox {
            utxo: plain_box(base + i as u64, pick(mix, i)),
            inclusion_height: height,
        })
        .collect();
    b.transactions = vec![IndexedTransaction {
        id: digest(TX_IDS + (1 << 40) + u64::from(height)),
        inputs: Vec::new(),
        outputs,
    }];
    b.header = BlockHeader {
        id: digest(BLOCK_IDS + u64::from(height)),
        parent_id: digest(BLOCK_IDS + u64::from(height) - 1),
        height,
        json: "{}".into(),
    };
    b
}

/// A pool of `txs` txs, each with the mainnet pool's shape (35 inputs and 54 outputs over 20
/// txs: two in, three out), guarded by the mix's trees. `variant` keeps snapshots apart.
fn mixed_pool(txs: usize, mix: &[HexBytes], variant: u64) -> Arc<[UnconfirmedTransaction]> {
    let base = IDS + (1 << 50) + (variant << 40);
    let boxes = |t: usize, n: usize, from: u64| -> Vec<UTxO> {
        (0..n)
            .map(|j| {
                let i = t * 5 + j + from as usize;
                plain_box(base + i as u64 * 8 + from, pick(mix, i))
            })
            .collect()
    };
    (0..txs)
        .map(|t| UnconfirmedTransaction {
            id: digest(base + (1 << 30) + t as u64),
            inputs: boxes(t, 2, 0),
            outputs: boxes(t, 3, 2),
        })
        .collect::<Vec<_>>()
        .into()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn engine(keys: usize, template: bool) -> Engine {
    let mut engine =
        Engine::new(Config { max_tracked_units: keys * 10 + 1_000, ..Config::DEFAULT });
    engine
        .apply(Event::BlockApplied(block(FIRST, &[])))
        .expect("the first block");
    load(&mut engine, 0, keys, 10);
    if template {
        let as_of = tip(&mut engine);
        let key = TrackingKey::Template(digest(IDS - 1));
        engine
            .apply(Event::KeyLoaded { key, as_of, boxes: Vec::new() })
            .expect("a load is never an error");
    }
    engine
}

fn ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn events(keys: usize, outputs: usize, pool: usize, block_mix: &[HexBytes], pool_mix: &[HexBytes]) {
    let mut engines = [engine(keys, false), engine(keys, true)];
    let at = tip(&mut engines[0]);
    let snapshots = [mixed_pool(pool, pool_mix, 0), mixed_pool(pool, pool_mix, 1)];
    let mut snapshot_ms = [Vec::new(), Vec::new()];
    let mut block_ms = [Vec::new(), Vec::new()];
    for run in 0..RUNS {
        for (e, engine) in engines.iter_mut().enumerate() {
            let txs = snapshots[run % 2].clone();
            let started = Instant::now();
            engine
                .apply(Event::MempoolSnapshot { txs, observed_tip: at })
                .expect("a snapshot is never an error");
            snapshot_ms[e].push(ms(started));
        }
    }
    for run in 0..RUNS {
        let next = mixed_block(at.height + 1 + run as u32, outputs, block_mix);
        for (e, engine) in engines.iter_mut().enumerate() {
            let next = next.clone();
            let started = Instant::now();
            engine
                .apply(Event::BlockApplied(next))
                .expect("the next block");
            block_ms[e].push(ms(started));
        }
    }
    let [snapshot_none, snapshot_template] = snapshot_ms.map(median);
    let [block_none, block_template] = block_ms.map(median);
    println!(
        "events keys={keys} block_outputs={outputs} pool_txs={pool} \
         block_ms={block_none:.3} block_template_ms={block_template:.3} \
         snapshot_ms={snapshot_none:.3} snapshot_template_ms={snapshot_template:.3}"
    );
}
