//! What a mempool snapshot and a block cost the engine as tracked keys grow, and how much of a
//! snapshot is the mempool's own walk of the keys.
//!
//! ```sh
//! cargo run --release --example eventbench -- <keys> [<keys> …]
//! ```
//!
//! For each count (at least `2 × TOUCHED`, the keys the snapshots pay), loads that many
//! address keys of ten boxes each, then times:
//!
//! - `snapshot`: a mempool snapshot applied to the engine. Snapshots alternate between two
//!   that share a busy pool of strangers' txs and each pay [`TOUCHED`] tracked keys, so every
//!   one changes the overlays of twice that many keys, as a pool that moves does.
//! - `mempool`: the same snapshots applied to a [`Mempool`] alone that tracks the same keys:
//!   its overlays, without the engine's balances. `snapshot` less `mempool` is what the
//!   engine adds: the balances of the keys whose overlay changed, and the event's handling.
//! - `block`: a block paying four distinct tracked keys, with no snapshot ahead of it.
//!
//! Each figure is the median of [`RUNS`]. An example rather than a test: it measures, and a
//! time is no assertion on a shared CI machine.

use std::{sync::Arc, time::Instant};

use hergmes::{
    engine::{Config, Engine, Event, Mempool, TrackingKey},
    types::ergo::UnconfirmedTransaction,
};
use support::{FIRST, block, digest, load, plain_box, tip, tree};

mod support;

/// Strangers' txs in every snapshot: a busy pool, a fifth of the node's cap.
const POOL_TXS: u64 = 200;
/// Tracked keys each snapshot pays.
const TOUCHED: u64 = 20;
const RUNS: usize = 11;
/// Where the snapshots' ids start, apart from the other examples' kinds.
const POOL_IDS: u64 = 1 << 59;

fn main() {
    let counts: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap_or_else(|_| usage()))
        .collect();
    if counts.is_empty() || counts.iter().any(|&keys| keys < 2 * TOUCHED as usize) {
        usage();
    }
    for keys in counts {
        run(keys);
    }
}

fn usage() -> ! {
    eprintln!("usage: eventbench <keys> [<keys> …], each at least {}", 2 * TOUCHED);
    std::process::exit(2)
}

/// A snapshot of the strangers' pool and one tx paying each of `paid`. `variant` keeps the
/// two snapshots' own txs apart.
fn snapshot(paid: std::ops::Range<u64>, variant: u64) -> Arc<[UnconfirmedTransaction]> {
    let stranger = tree(u64::MAX);
    let mut txs: Vec<UnconfirmedTransaction> = (0..POOL_TXS)
        .map(|t| UnconfirmedTransaction {
            id: digest(POOL_IDS + 3 * t),
            inputs: vec![plain_box(POOL_IDS + 3 * t + 1, &stranger)],
            outputs: vec![plain_box(POOL_IDS + 3 * t + 2, &stranger)],
        })
        .collect();
    let base = POOL_IDS + (1 << 20) * (variant + 1);
    txs.extend(paid.map(|k| UnconfirmedTransaction {
        id: digest(base + 3 * k),
        inputs: vec![plain_box(base + 3 * k + 1, &stranger)],
        outputs: vec![plain_box(base + 3 * k + 2, &tree(k))],
    }));
    txs.into()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn run(keys: usize) {
    let mut engine =
        Engine::new(Config { max_tracked_units: keys * 10 + 1_000, ..Config::DEFAULT });
    engine
        .apply(Event::BlockApplied(block(FIRST, &[])))
        .expect("the first block");
    load(&mut engine, 0, keys, 10);
    let at = tip(&mut engine);
    let snapshots = [snapshot(0..TOUCHED, 0), snapshot(TOUCHED..2 * TOUCHED, 1)];

    let mut snapshot_ms = Vec::new();
    for run in 0..RUNS {
        let txs = snapshots[run % 2].clone();
        let started = Instant::now();
        engine
            .apply(Event::MempoolSnapshot { txs, observed_tip: at })
            .expect("a snapshot is never an error");
        snapshot_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }

    let mut mempool = Mempool::default();
    for k in 0..keys {
        mempool.track(TrackingKey::Tree(tree(k as u64)));
    }
    let mut mempool_ms = Vec::new();
    for run in 0..RUNS {
        let txs = snapshots[run % 2].clone();
        let started = Instant::now();
        mempool.apply_snapshot(txs, at, Some(at), |_| true);
        mempool_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }

    let mut block_ms = Vec::new();
    for run in 0..RUNS as u64 {
        let paid: Vec<u64> = (0..4).map(|i| (run * 4 + i) % keys as u64).collect();
        let next = block(at.height + 1 + run as u32, &paid);
        let started = Instant::now();
        engine
            .apply(Event::BlockApplied(next))
            .expect("the next block");
        block_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }

    let (snapshot, mempool, block) = (median(snapshot_ms), median(mempool_ms), median(block_ms));
    let per_key = |ms: f64| ms * 1000.0 / keys as f64;
    println!(
        "events keys={keys} snapshot_ms={snapshot:.2} mempool_ms={mempool:.2} \
         engine_rest_ms={:.2} block_ms={block:.2} snapshot_us_per_key={:.3} \
         mempool_us_per_key={:.3} block_us_per_key={:.3}",
        snapshot - mempool,
        per_key(snapshot),
        per_key(mempool),
        per_key(block),
    );
}
