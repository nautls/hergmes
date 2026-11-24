//! When the relay is ready, for the service manager: the moment `/v1/status` first answers
//! `ready`.

use tokio::sync::watch;

use super::Settled;
use crate::engine::BlockRef;

/// Resolves once the relay is ready as `/v1/status` reports it: the engine has a tip, the
/// follower has brought it to the node's tip, and the configured keys have settled. Each of
/// these only ever turns on, so once ready the relay stays so. `false` if the engine or the
/// follower is gone first, when the relay never gets there.
pub async fn until_ready(
    mut tip: watch::Receiver<Option<BlockRef>>,
    mut started: watch::Receiver<bool>,
    settled: Settled,
) -> bool {
    if started.wait_for(|started| *started).await.is_err()
        || tip.wait_for(Option::is_some).await.is_err()
    {
        return false;
    }
    settled.wait().await;
    true
}

#[cfg(test)]
#[path = "ready_tests.rs"]
mod tests;
