//! What tracking a template key costs the engine: reading each box's template, over mainnet's
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
//! - `trees`: per segregated tree, the walk of its constants to its template ([`template`]),
//!   which is what the engine does per box while a template key is tracked, and for
//!   comparison that and a Blake2b of the template ([`template_hash`]), which only a load
//!   does now. A tree without segregated constants answers at its header (`plain_ns`).
//! - `events`: two engines tracking `<keys>` address keys of ten boxes each, one of them also
//!   a template key holding no box. Each is given the same events: a block of
//!   `<block-outputs>` outputs, and mempool snapshots of `<pool-txs>` txs, built from the
//!   mixes with fresh ids. The template key matches none of them, so the difference is reading
//!   the templates and looking each one up: every block output once; every pending input and
//!   output in the overlays, and again in the check of which inputs are confirmed.
//!
//! Each figure is the median of [`RUNS`], the two engines taking turns. An example rather than
//! a test: it measures, and a time is no assertion on a shared CI machine.

use std::{hint::black_box, sync::Arc, time::Instant};

use hergmes::{
    engine::{Config, Engine, Event, Query, Reply, TrackingKey},
    types::{
        HexBytes,
        ergo::{
            BlockHeader, IndexedBlock, IndexedBox, IndexedTransaction, UTxO, UnconfirmedTransaction,
        },
        ergo_tree::{template, template_hash},
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
    trees_line(&block_mix, &pool_mix);
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

/// Nanoseconds per call of `f` over `trees`.
fn per_tree<T>(trees: &[&HexBytes], f: impl Fn(&[u8]) -> T) -> f64 {
    let started = Instant::now();
    for _ in 0..HASH_PASSES {
        for tree in trees {
            black_box(f(black_box(&tree.0)));
        }
    }
    started.elapsed().as_nanos() as f64 / (HASH_PASSES * trees.len()) as f64
}

fn trees_line(block_mix: &[HexBytes], pool_mix: &[HexBytes]) {
    let all: Vec<&HexBytes> = block_mix.iter().chain(pool_mix).collect();
    let (segregated, plain): (Vec<&HexBytes>, Vec<&HexBytes>) =
        all.iter().partition(|t| template(&t.0).is_some());
    let largest = *segregated.iter().max_by_key(|t| t.0.len()).unwrap();
    let mean = segregated.iter().map(|t| t.0.len()).sum::<usize>() / segregated.len();
    let walk = |trees: &[&HexBytes]| per_tree(trees, |t| template(t).map(<[u8]>::len));
    let hash = |trees: &[&HexBytes]| per_tree(trees, template_hash);
    println!(
        "trees segregated={} (mean_bytes={mean}) plain={} walk_ns={:.0} hash_ns={:.0} \
         plain_ns={:.1} largest_bytes={} largest_walk_ns={:.0} largest_hash_ns={:.0}",
        segregated.len(),
        plain.len(),
        walk(&segregated),
        hash(&segregated),
        walk(&plain),
        largest.0.len(),
        walk(&[largest]),
        hash(&[largest]),
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

/// A template that matches no box: the template of the mix's commonest segregated tree, its
/// last byte changed, so the lookups of that tree's boxes compare it to its last byte.
fn decoy(mix: &[HexBytes]) -> HexBytes {
    let mut counts = std::collections::HashMap::new();
    for t in mix.iter().filter_map(|t| template(&t.0)) {
        *counts.entry(t).or_insert(0) += 1;
    }
    let (common, _) = counts
        .into_iter()
        .max_by_key(|(t, n)| (*n, t.len()))
        .unwrap();
    let mut bytes = common.to_vec();
    *bytes.last_mut().unwrap() ^= 0xff;
    HexBytes::from(bytes)
}

fn engine(keys: usize, template: Option<&HexBytes>) -> Engine {
    let mut engine =
        Engine::new(Config { max_tracked_units: keys * 20 + 10_000, ..Config::DEFAULT });
    engine
        .apply(Event::BlockApplied(block(FIRST, &[])))
        .expect("the first block");
    load(&mut engine, 0, keys, 10);
    let Reply::Stats(stats) = engine.query(Query::Stats) else { unreachable!() };
    assert_eq!(stats.address_keys, keys, "every address key fits the cap");
    if let Some(template) = template {
        let as_of = tip(&mut engine);
        let key = TrackingKey::Template(template.clone());
        let outputs = engine
            .apply(Event::KeyLoaded { key, as_of, boxes: Vec::new() })
            .expect("a load is never an error");
        assert!(outputs.is_empty(), "the template key is tracked: {outputs:?}");
    }
    engine
}

fn ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn events(keys: usize, outputs: usize, pool: usize, block_mix: &[HexBytes], pool_mix: &[HexBytes]) {
    let decoy = decoy(block_mix);
    let mut engines = [engine(keys, None), engine(keys, Some(&decoy))];
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
