use tokio::{sync::watch, task::JoinHandle};

use super::*;
use crate::{engine::BlockRef, runtime::settling, types::Digest};

fn block() -> BlockRef {
    BlockRef { height: 1, header_id: Digest([0; 32]) }
}

/// Lets the waiting task run until it blocks again.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

async fn finished(task: JoinHandle<bool>) -> bool {
    settle().await;
    assert!(task.is_finished(), "ready once all three hold");
    task.await.unwrap()
}

#[tokio::test]
async fn ready_waits_for_the_tip_the_first_sync_and_the_configured_keys() {
    let (tip, tip_rx) = watch::channel(None);
    let (started, started_rx) = watch::channel(false);
    let (settled, mut pending) = settling(1);
    let task = tokio::spawn(until_ready(tip_rx, started_rx, settled));

    settle().await;
    assert!(!task.is_finished(), "nothing holds yet");
    started.send_replace(true);
    settle().await;
    assert!(!task.is_finished(), "the engine has no tip");
    tip.send_replace(Some(block()));
    settle().await;
    assert!(!task.is_finished(), "a configured key is still loading");
    drop(pending.pop());
    assert!(finished(task).await);
}

#[tokio::test]
async fn ready_holds_in_any_order() {
    let (tip, tip_rx) = watch::channel(None);
    let (started, started_rx) = watch::channel(false);
    let (settled, pending) = settling(2);
    let task = tokio::spawn(until_ready(tip_rx, started_rx, settled));

    drop(pending);
    tip.send_replace(Some(block()));
    settle().await;
    assert!(!task.is_finished(), "the follower has not synced");
    started.send_replace(true);
    assert!(finished(task).await);
}

#[tokio::test]
async fn ready_is_never_reached_if_the_engine_is_gone() {
    let (tip, tip_rx) = watch::channel(None);
    let (_started, started_rx) = watch::channel(true);
    let task = tokio::spawn(until_ready(tip_rx, started_rx, Settled::now()));

    drop(tip);
    assert!(!finished(task).await);
}
