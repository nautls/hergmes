//! What a block costs the engine task as subscribed keys grow: the engine, and the routing of
//! its outputs to every subscriber's sink.
//!
//! ```sh
//! cargo run --release --example routebench -- <keys> [<keys> …]
//! ```
//!
//! For each count, loads that many address keys of ten boxes each into an engine on its own
//! task, then times a block paying four of them, as its caller sees it (from the send to the
//! reply), with:
//!
//! - `none`: no subscribers. The engine, and routing finding no one to send to.
//! - `held`: one box subscription per key, whose receivers nobody reads. Routing allocates
//!   and sends every key's output, but no receiver waits, so no task is woken.
//! - `drained`: the same, each receiver drained by a task of its own, as a connection's is.
//!   Every send wakes that task, on whichever worker runs it.
//!
//! `held` less `none` is routing's sends; `drained` less `held` is the wakes and the
//! receivers' work competing for the workers. The receivers only drop what they get: a real
//! connection also encodes and writes it, so `drained` is a lower bound.
//!
//! Each figure is the median of [`RUNS`]. An example rather than a test: it measures, and a
//! time is no assertion on a shared CI machine.

use std::{thread, time::Instant};

use hergmes::{
    adapters::EngineHandle,
    api::ws::SINK_CAPACITY,
    engine::{BlockRef, Config, Engine, Event, TrackingKey},
    runtime::task::{self, Handle, Subscription},
};
use support::{FIRST, block, load, tip, tree};
use tokio::sync::mpsc;

mod support;

/// Blocks timed, after one warm-up block that is not. A block sends a key it pays two
/// messages (`block` and `balance`), so all of them must fit in a `held` sink: a full one
/// ends its subscription, and later blocks would route to fewer sinks.
const RUNS: usize = 11;
const _: () = assert!(2 * (RUNS + 1) <= SINK_CAPACITY);

#[derive(Clone, Copy)]
enum Subscribers {
    None,
    Held,
    Drained,
}

fn main() {
    let counts: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap_or_else(|_| usage()))
        .collect();
    if counts.is_empty() || counts.contains(&0) {
        usage();
    }
    let workers = thread::available_parallelism().map_or(1, |n| n.get());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    for keys in counts {
        let none = runtime.block_on(run(keys, Subscribers::None));
        let held = runtime.block_on(run(keys, Subscribers::Held));
        let drained = runtime.block_on(run(keys, Subscribers::Drained));
        let per_key = |ms: f64| ms * 1000.0 / keys as f64;
        println!(
            "route keys={keys} workers={workers} none_ms={none:.2} held_ms={held:.2} \
             drained_ms={drained:.2} send_us_per_key={:.3} wake_us_per_key={:.3}",
            per_key(held - none),
            per_key(drained - held),
        );
    }
}

fn usage() -> ! {
    eprintln!("usage: routebench <keys> [<keys> …], each at least 1");
    std::process::exit(2)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

async fn run(keys: usize, subscribers: Subscribers) -> f64 {
    // No cap in the way: each key is charged its memory on top of its boxes.
    let mut engine = Engine::new(Config { max_tracked_units: usize::MAX, ..Config::DEFAULT });
    engine
        .apply(Event::BlockApplied(block(FIRST, &[])))
        .expect("the first block");
    load(&mut engine, 0, keys, 10);
    let at = tip(&mut engine);
    let (handle, engine_task) = task::spawn(engine);

    // Kept to the end, so no sink closes and no subscription ends during the blocks.
    let mut receivers = Vec::new();
    let mut drains = Vec::new();
    if !matches!(subscribers, Subscribers::None) {
        for k in 0..keys {
            let (sink, mut feed) = mpsc::channel(SINK_CAPACITY);
            subscribe(&handle, k, at, sink).await;
            match subscribers {
                Subscribers::Held => receivers.push(feed),
                _ => drains.push(tokio::spawn(async move { while feed.recv().await.is_some() {} })),
            }
        }
    }

    // Block 0 warms up (first allocations, the drains' first wakes) and is not timed.
    let mut block_ms = Vec::new();
    for run in 0..=RUNS as u64 {
        let paid: Vec<u64> = (0..4).map(|i| (run * 4 + i) % keys as u64).collect();
        let next = block(at.height + 1 + run as u32, &paid);
        let started = Instant::now();
        handle
            .apply(Event::BlockApplied(next))
            .await
            .expect("the engine task runs")
            .expect("the next block");
        if run > 0 {
            block_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        }
    }

    drop(receivers);
    drop(handle);
    engine_task.await.expect("the engine task ends");
    for drain in drains {
        drain.await.expect("a drain ends");
    }
    median(block_ms)
}

/// Subscribes key `k` with its boxes from `at`, the tip: live at once, with nothing to catch
/// up.
async fn subscribe(handle: &Handle, k: usize, at: BlockRef, sink: task::Sink) {
    let key = TrackingKey::Tree(tree(k as u64));
    match handle
        .subscribe(key, Some(at), sink)
        .await
        .expect("the engine task runs")
    {
        Subscription::Live { .. } => {}
        Subscription::Resync(reason) => panic!("key {k} not subscribed: {reason:?}"),
    }
}
