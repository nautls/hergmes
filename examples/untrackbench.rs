//! What untracking idle keys costs the block that does it.
//!
//! ```sh
//! cargo run --release --example untrackbench -- <active-keys> <active-boxes-per-key> \
//!     <idle-keys> <idle-boxes-per-key> <ring-blocks> [<untrack-boxes-per-block>]
//! ```
//!
//! Loads the idle keys, lets the grace period pass, loads the active keys, and applies
//! `<ring-blocks>` blocks, each paying two idle and two active keys so the ring holds deltas
//! of both. Then it applies one more block twice, to two copies of that engine: once while no
//! key is past the grace period yet (`baseline`), and once a second later, when every idle
//! key is (`sweep`). The difference is what the untrack adds to the block. Each is timed
//! three times on fresh copies. Then, from a fourth copy, blocks follow until every idle key
//! is gone: how many it took (`blocks`) and the slowest (`slowest_ms`).
//!
//! The engine untracks at most `<untrack-boxes-per-block>` boxes a block (the engine's
//! default without it): a figure past every idle box measures the whole sweep in one block.
//!
//! Comparing runs tells where the time goes: the same boxes over few keys or many separates a
//! cost per box from a cost per key, and `<ring-blocks>` at 0 against the soak's ~345 shows
//! what the walk of the ring adds.
//!
//! An example rather than a test: it measures, and a time is no assertion on a shared CI
//! machine.

use std::time::{Duration, Instant};

use hergmes::{
    engine::{Config, Engine, Event, Query, Reply, TrackingKey, key_charge},
    types::ergo::IndexedBlock,
};
use support::{FIRST, block, load, tree};

mod support;

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap_or_else(|_| usage()))
        .collect();
    let budget = match args.len() {
        5 => Config::DEFAULT.untrack_units_per_block,
        6 => args[5],
        _ => usage(),
    };
    let [active_keys, active_boxes, idle_keys, idle_boxes, ring] = args[..5] else { usage() };
    // The timed block pays an active key, and the sweep needs a key to untrack.
    if active_keys == 0 || idle_keys == 0 {
        usage();
    }
    run(active_keys, active_boxes, idle_keys, idle_boxes, ring, budget);
}

fn usage() -> ! {
    eprintln!(
        "usage: untrackbench <active-keys> <active-boxes-per-key> <idle-keys> \
         <idle-boxes-per-key> <ring-blocks> [<untrack-boxes-per-block>], with at least one \
         active and one idle key"
    );
    std::process::exit(2)
}

fn address_keys(engine: &mut Engine) -> usize {
    tracked(engine).0
}

/// The address keys and boxes tracked.
fn tracked(engine: &mut Engine) -> (usize, usize) {
    let Reply::Stats(stats) = engine.query(Query::Stats) else { unreachable!() };
    (stats.address_keys, stats.tracked_boxes)
}

/// Applies `block` to `engine` and returns the time it took.
fn apply(engine: &mut Engine, block: &IndexedBlock) -> f64 {
    let started = Instant::now();
    engine
        .apply(Event::BlockApplied(block.clone()))
        .expect("the next block");
    started.elapsed().as_secs_f64() * 1000.0
}

/// Applies `block` to a copy of `engine` with its clock at `now`, and returns the time it
/// took and the keys and boxes left.
fn timed(engine: &Engine, now: Duration, block: &IndexedBlock) -> (f64, (usize, usize)) {
    let mut copy = engine.clone();
    copy.set_now(now);
    let ms = apply(&mut copy, block);
    (ms, tracked(&mut copy))
}

fn run(
    active_keys: usize,
    active_boxes: usize,
    idle_keys: usize,
    idle_boxes: usize,
    ring: usize,
    budget: usize,
) {
    let config = Config {
        untrack_units_per_block: budget,
        // Room for every key, its charge included, and the ring's boxes: nothing is evicted,
        // so the sweep alone untracks. A P2PK key with no token is charged the most a load
        // here is.
        max_tracked_units: (active_keys + idle_keys) * key_charge(&TrackingKey::Tree(tree(0)), 0)
            + active_keys * active_boxes
            + idle_keys * idle_boxes
            + 4 * ring
            + 16,
        ..Config::DEFAULT
    };
    let grace = config.grace;
    let mut engine = Engine::new(config);
    engine
        .apply(Event::BlockApplied(block(FIRST, &[])))
        .expect("the first block");

    // Idle keys first, at the clock's start; the active ones a grace period later.
    load(&mut engine, active_keys, idle_keys, idle_boxes);
    engine.set_now(grace);
    load(&mut engine, 0, active_keys, active_boxes);

    let mut height = FIRST;
    for n in 0..ring as u64 {
        height += 1;
        // Two idle and two active keys a block, spread over them all.
        let pays: Vec<u64> = [
            n % active_keys.max(1) as u64,
            (n * 7 + 1) % active_keys.max(1) as u64,
            active_keys as u64 + n % idle_keys.max(1) as u64,
            active_keys as u64 + (n * 7 + 1) % idle_keys.max(1) as u64,
        ]
        .into_iter()
        .filter(|&k| (k as usize) < active_keys + idle_keys)
        .collect();
        engine
            .apply(Event::BlockApplied(block(height, &pays)))
            .expect("a ring block");
    }
    assert_eq!(address_keys(&mut engine), active_keys + idle_keys, "every key is tracked");

    let next = block(height + 1, &[0, 1.min(active_keys as u64 - 1)]);
    let after_grace = grace + Duration::from_secs(1);
    let mut baseline = Vec::new();
    let mut sweep = Vec::new();
    let (mut first_keys, mut gone) = (0, 0);
    for _ in 0..3 {
        // At `grace` exactly the idle keys are not past it yet: nothing goes.
        let (ms, (kept, kept_boxes)) = timed(&engine, grace, &next);
        assert_eq!(kept, active_keys + idle_keys, "the baseline untracks nothing");
        baseline.push(ms);
        let (ms, (left, left_boxes)) = timed(&engine, after_grace, &next);
        assert!(left < kept, "the sweep untracks some idle key");
        // Counted, not worked out: the ring blocks paid idle keys boxes of their own.
        (first_keys, gone) = (kept - left, kept_boxes - left_boxes);
        sweep.push(ms);
    }

    // The blocks that follow, until every idle key is gone.
    let mut copy = engine.clone();
    copy.set_now(after_grace);
    let (mut blocks, mut slowest) = (0, 0.0_f64);
    while address_keys(&mut copy) > active_keys {
        blocks += 1;
        slowest = slowest.max(apply(&mut copy, &block(height + blocks, &[0])));
    }
    let median = |v: &mut Vec<f64>| {
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    let added = median(&mut sweep) - median(&mut baseline);
    let list = |v: &[f64]| {
        v.iter()
            .map(|ms| format!("{ms:.1}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    println!(
        "untrack active_keys={active_keys} active_boxes={} idle_keys={idle_keys} \
         idle_boxes={idle_boxes} ring={ring} budget={budget} baseline_ms=[{}] sweep_ms=[{}] \
         added_ms={added:.1} first_block_keys={first_keys} first_block_boxes={gone} \
         ns_per_box={:.0} us_per_key={:.2} blocks={blocks} slowest_ms={slowest:.1}",
        active_keys * active_boxes,
        list(&baseline),
        list(&sweep),
        added * 1e6 / gone.max(1) as f64,
        added * 1e3 / first_keys.max(1) as f64,
    );
}
