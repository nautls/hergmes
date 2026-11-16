//! The limit on history reads in flight, on a paused clock against a slow `SimChain`.

use std::{sync::Arc, time::Duration};

use tokio::{task::JoinSet, time::sleep};

use super::*;
use crate::sim::{Chain, Endpoint, SimChain, SimConfig};

fn reads(sim: &SimChain) -> usize {
    sim.calls()
        .iter()
        .filter(|e| **e == Endpoint::AddressTransactions)
        .count()
}

/// `count` reads started together against a node that takes `latency` for each.
fn start(
    latency: Duration,
    count: usize,
) -> (SimChain, JoinSet<Result<AddressTransactions, HistoryError>>) {
    let sim = SimChain::new(Chain::new(SimConfig::default()));
    sim.set_latency(Endpoint::AddressTransactions, latency);
    let tree = sim.lock().trees()[0].clone();
    let reader = Arc::new(HistoryReader::new(sim.clone()));
    let mut tasks = JoinSet::new();
    for _ in 0..count {
        let (reader, tree) = (reader.clone(), tree.clone());
        tasks.spawn(async move { reader.read(&tree, 0, 10).await });
    }
    (sim, tasks)
}

/// The fifth read reaches the node only when one of the first four has returned, and every
/// read is answered.
#[tokio::test(start_paused = true)]
async fn reads_past_the_limit_wait_for_one_in_flight() {
    let latency = Duration::from_secs(10);
    let (sim, mut tasks) = start(latency, MAX_READS + 2);
    sleep(latency - Duration::from_millis(1)).await;
    assert_eq!(reads(&sim), MAX_READS, "the rest wait for a permit");
    sleep(Duration::from_millis(2)).await;
    assert_eq!(reads(&sim), MAX_READS + 2, "the first four returned their permits");
    while let Some(read) = tasks.join_next().await {
        assert!(read.unwrap().is_ok());
    }
}

/// A read that waited as long as a read may take is answered `Busy`, without reaching the
/// node, and its wait holds no permit: the reads in flight finish.
#[tokio::test(start_paused = true)]
async fn a_read_that_waits_too_long_gives_up() {
    let (sim, mut tasks) = start(HISTORY_TIMEOUT + Duration::from_secs(5), MAX_READS + 1);
    let mut results = Vec::new();
    while let Some(read) = tasks.join_next().await {
        results.push(read.unwrap());
    }
    assert!(matches!(results[0], Err(HistoryError::Busy)), "the first to end: {results:?}");
    assert!(results[1..].iter().all(Result::is_ok), "{results:?}");
    assert_eq!(reads(&sim), MAX_READS, "the read that gave up never reached the node");
}

/// A failed read returns its permit like any other: more failures than there are permits,
/// one after the other, and the next read still gets one.
#[tokio::test(start_paused = true)]
async fn a_failed_read_frees_its_permit() {
    let failures = MAX_READS as u32 + 1;
    let mut chain = Chain::new(SimConfig { recovery_reads: failures, ..SimConfig::default() });
    chain.mine(0, 2, 1);
    let sim = SimChain::new(chain);
    let tree = sim.lock().trees()[0].clone();
    let reader = HistoryReader::new(sim.clone());
    // The indexer answers `503` while it recovers from the reorg.
    sim.lock().reorg(1, 1, 5);
    for _ in 0..failures {
        let failed = reader.read(&tree, 0, 10).await;
        assert!(matches!(failed, Err(HistoryError::Node(_))), "{failed:?}");
    }
    assert!(reader.read(&tree, 0, 10).await.is_ok());
}
