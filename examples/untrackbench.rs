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
    engine::{Config, Engine, Event, Query, Reply},
    types::ergo::IndexedBlock,
};
use support::{FIRST, block, load};

mod support;

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap_or_else(|_| usage()))
        .collect();
    let budget = match args.len() {
        5 => Config::DEFAULT.untrack_boxes_per_block,
        6 => args[5],
        _ => usage(),
    };
    let [active_keys, active_boxes, idle_keys, idle_boxes, ring] = args[..5] else { usage() };
    run(active_keys, active_boxes, idle_keys, idle_boxes, ring, budget);
}

fn usage() -> ! {
    eprintln!(
        "usage: untrackbench <active-keys> <active-boxes-per-key> <idle-keys> \
         <idle-boxes-per-key> <ring-blocks> [<untrack-boxes-per-block>]"
    );
    std::process::exit(2)
}

fn address_keys(engine: &mut Engine) -> usize {
    let Reply::Stats(stats) = engine.query(Query::Stats) else { unreachable!() };
    stats.address_keys
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
/// took and the keys left.
fn timed(engine: &Engine, now: Duration, block: &IndexedBlock) -> (f64, usize) {
    let mut copy = engine.clone();
    copy.set_now(now);
    let ms = apply(&mut copy, block);
    (ms, address_keys(&mut copy))
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
        untrack_boxes_per_block: budget,
        // Room for every key and the ring's boxes: nothing is evicted, so the sweep alone
        // untracks.
        max_tracked_boxes: active_keys * active_boxes + idle_keys * idle_boxes + 4 * ring + 16,
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
    let mut first_keys = 0;
    for _ in 0..3 {
        // At `grace` exactly the idle keys are not past it yet: nothing goes.
        let (ms, left) = timed(&engine, grace, &next);
        assert_eq!(left, active_keys + idle_keys, "the baseline untracks nothing");
        baseline.push(ms);
        let (ms, left) = timed(&engine, after_grace, &next);
        assert!(left < active_keys + idle_keys, "the sweep untracks some idle key");
        first_keys = active_keys + idle_keys - left;
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
    let gone = first_keys * idle_boxes;
    let list = |v: &[f64]| {
        v.iter()
            .map(|ms| format!("{ms:.1}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    println!(
        "untrack active_keys={active_keys} active_boxes={} idle_keys={idle_keys} idle_boxes={gone} \
         ring={ring} budget={budget} baseline_ms=[{}] sweep_ms=[{}] added_ms={added:.1} \
         first_block_keys={first_keys} ns_per_box={:.0} us_per_key={:.2} blocks={blocks} \
         slowest_ms={slowest:.1}",
        active_keys * active_boxes,
        list(&baseline),
        list(&sweep),
        added * 1e6 / gone.max(1) as f64,
        added * 1e3 / first_keys.max(1) as f64,
    );
}
