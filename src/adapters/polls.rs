//! Quick polls while the engine or the indexer catches up with the node.
//!
//! Right after a block, the node has it before the engine (the follower ticks once a second)
//! and sometimes before the indexer. An attempt that finds either behind costs one or two
//! cheap node calls and usually settles within that first second, so waiting a whole retry
//! would answer up to a second late. A cause that lasts (an indexer resyncing after a
//! restart) must not be polled for hours, though, so the quick polls last one retry from the
//! first attempt found behind, then attempts fall back to one a retry.
//!
//! The run is shared by every attempt of one reader, since the cause is the node's, the
//! indexer's or the engine's and not a key's: attempts started later (a keeper's after
//! giving up, clients' of other keys) wait retries rather than each poll a second of its own.

use std::time::Duration;

use tokio::time::Instant;

/// What an attempt says about the engine and the indexer keeping up with the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checks {
    /// Stopped before its read: the engine or the indexer was not at the node's tip.
    Behind,
    /// Got past them: whatever came next, the engine and the indexer were at the node's tip.
    Passed,
    /// Neither: an error before or during the checks (a node `503`, a failed read), which
    /// says nothing about them.
    Unknown,
}

/// A run of attempts that stopped before their reads: when it began, and when the last one
/// did. Attempts poll during its first `retry`.
#[derive(Debug, Default)]
pub struct Polls {
    run: Option<(Instant, Instant)>,
}

impl Polls {
    /// Records an attempt that answered at `now`, and says whether it polls. A run ends at an
    /// attempt that got past the checks, and lapses once no attempt was behind for two
    /// `retry`s: past that it was no one cause seen throughout (every attempt gave up, and
    /// the next to come finds a block's ordinary lag), so a new run polls again. An error
    /// that says nothing of the checks leaves the run as it is.
    pub fn record(&mut self, now: Instant, checks: Checks, retry: Duration) -> bool {
        match checks {
            Checks::Behind => {
                let since = match self.run {
                    Some((since, last)) if now - last <= 2 * retry => since,
                    _ => now,
                };
                self.run = Some((since, now));
                now - since < retry
            }
            Checks::Passed => {
                self.run = None;
                false
            }
            Checks::Unknown => false,
        }
    }
}

#[cfg(test)]
#[path = "polls_tests.rs"]
mod tests;
