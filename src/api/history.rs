//! The history service: an address's txs, newest first, its pending txs on top.
//!
//! A thin read over the indexer, which keeps every address's tx list: the engine keeps no
//! history, a read starts no tracking, and nothing is kept between reads. The engine gives
//! only the pending txs, from its snapshot.

use std::time::Duration;

use tokio::time::sleep;
use tracing::debug;

use crate::{
    adapters::{
        EngineHandle, HistoryReader,
        history::{HistoryError, Turn},
    },
    api::{ApiError, cursor::TxCursor},
    clients::NodeSource,
    engine::{
        PendingTxsAt, Query, Reply, TrackingKey,
        overlay::{self, PendingDelta},
    },
    types::ergo::{AddressTransaction, AddressTransactions, ErgoTree, TxId},
};

/// Txs per page when the client names no limit: about a wallet screen.
pub const DEFAULT_LIMIT: u32 = 25;
/// The most txs per page. Each one costs the node a header read and a read per input and
/// output box (`IndexedErgoTransaction.retrieveBody`), so this, not the node's 16384, is the
/// budget.
pub const MAX_LIMIT: u32 = 100;
/// `Retry-After` for a page that gave up waiting behind slow reads. It waited 55 s for reads
/// that take as long, so a client told to come back in a second would queue again at once,
/// and each wave of retries would lengthen the queue.
pub const BUSY_RETRY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Attempts at a first page whose confirmed txs above the engine's tip may run past the
    /// page, before giving up with `not_ready`.
    pub attempts: u32,
    /// The wait between attempts: about one follower tick, after which the engine has
    /// applied the blocks the indexer had.
    pub retry: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self { attempts: 3, retry: Duration::from_secs(1) }
    }
}

/// A tx of an address's history: its net change for the address, its fee, and the block
/// that included it, or none while it is pending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryItem {
    pub tx_id: TxId,
    pub included: Option<Included>,
    pub delta: PendingDelta,
    pub fee: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Included {
    pub height: u32,
    /// The block's timestamp, in milliseconds.
    pub timestamp: u64,
}

/// A page of an address's history, newest first: on the first page its pending txs, then
/// confirmed ones. `next_cursor` is `None` exactly when no older tx is left; a page can be
/// short and still have one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPage {
    pub items: Vec<HistoryItem>,
    pub next_cursor: Option<String>,
}

pub struct History<S> {
    reader: HistoryReader<S>,
    config: Config,
}

impl<S: NodeSource> History<S> {
    pub fn new(source: S, config: Config) -> Self {
        Self { reader: HistoryReader::new(source), config }
    }

    /// A page of the history of the address guarded by `tree`: the first without a cursor,
    /// and `limit` txs ([`DEFAULT_LIMIT`], at most [`MAX_LIMIT`]) below the cursor's anchor
    /// with one.
    pub async fn page(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<HistoryPage, ApiError> {
        let limit = match limit {
            None => DEFAULT_LIMIT,
            Some(n @ 1..=MAX_LIMIT) => n,
            Some(_) => return Err(ApiError::InvalidLimit { max: MAX_LIMIT }),
        };
        let key = TrackingKey::Tree(tree.clone());
        match cursor {
            None => self.first(engine, tree, &key, limit).await,
            Some(cursor) => {
                let cursor = TxCursor::decode(cursor, &key).map_err(ApiError::InvalidCursor)?;
                self.next(tree, &key, cursor, limit).await
            }
        }
    }

    /// The pending txs, then the indexer's newest page, read in that order. The engine
    /// applies a block only once the indexer has it, and the indexer writes a block's txs, its
    /// address lists and its height in one batch (`ExtraIndexer.saveProgress`), so the page is
    /// at or above the engine's tip `E`: no tx falls between the two reads, and the only fault
    /// left is a tx pending at `E` and confirmed above it. The pending txs on the page are
    /// dropped. The page is newest first, so the overlap can run past it only when even its
    /// oldest tx is above `E`; then, if more follow and a pending tx is left, both reads are
    /// done again once the engine has caught up. Dropping by id needs no heights, so it also
    /// covers an engine still on an orphaned branch, for a tick after a reorg, while the
    /// indexer is on the new one.
    async fn first(
        &self,
        engine: &impl EngineHandle,
        tree: &ErgoTree,
        key: &TrackingKey,
        limit: u32,
    ) -> Result<HistoryPage, ApiError> {
        for attempt in 1..=self.config.attempts {
            // The turn first: a wait for it between the two reads would leave the pending
            // txs as old as the wait, up to several blocks.
            let turn = self.turn().await?;
            let pending = pending_txs(engine, tree).await?;
            let page = self.read(&turn, tree, 0, limit).await?;
            let total = page.total;
            // No tip yet: no snapshot, so nothing pending to overlap.
            let Some(PendingTxsAt { as_of, txs }) = pending else {
                return page_of(tree, key, page, 0, total, None, Vec::new());
            };
            let above = |tx: &AddressTransaction| tx.inclusion_height > as_of.height;
            let pending: Vec<HistoryItem> = txs
                .into_iter()
                .filter(|p| !page.items.iter().any(|t| t.id == p.tx_id))
                .map(pending_item)
                .collect();
            let more = total as usize > page.items.len();
            if !(page.items.last().is_some_and(above) && more && !pending.is_empty()) {
                return page_of(tree, key, page, 0, total, None, pending);
            }
            debug!(
                engine = as_of.height,
                oldest = page.items.last().map(|t| t.inclusion_height),
                attempt,
                "History page entirely above the engine's tip, trying again"
            );
            drop(turn);
            if attempt < self.config.attempts {
                sleep(self.config.retry).await;
            }
        }
        Err(ApiError::NotReady { retry_after: self.config.retry })
    }

    /// The `limit` txs below the cursor's anchor. The read starts at the anchor as the
    /// cursor's `total` places it, so a list that only grew returns the anchor after the txs
    /// that arrived since, which are dropped. An anchor missing from a read placed by a length
    /// the list no longer has (txs arrived, or a reorg dropped newer ones), or with nothing
    /// below it in the read, is read again with the list's real length, up to `attempts`
    /// reads: more only while the list keeps changing faster than the reads. An anchor found
    /// at another position, or missing from a read placed by the list's own length, was
    /// reached by a reorg.
    async fn next(
        &self,
        tree: &ErgoTree,
        key: &TrackingKey,
        cursor: TxCursor,
        limit: u32,
    ) -> Result<HistoryPage, ApiError> {
        let mut basis = cursor.total;
        for read in 1..=self.config.attempts {
            let Some(offset) = basis.checked_sub(cursor.pos + 1) else {
                return Err(ApiError::CursorExpired);
            };
            let turn = self.turn().await?;
            let page = self.read(&turn, tree, offset, limit + 1).await?;
            let last = read == self.config.attempts;
            match anchor(&page, offset, &cursor) {
                Anchor::Found(at) if at + 1 < page.items.len() || last => {
                    let anchor = Some(cursor.anchor);
                    return page_of(tree, key, page, at + 1, cursor.pos, anchor, Vec::new());
                }
                Anchor::Moved => return Err(ApiError::CursorExpired),
                // Placed by the list's length as it was read: the anchor is not at its
                // position any more.
                Anchor::Missing if page.total == basis => return Err(ApiError::CursorExpired),
                Anchor::Found(_) | Anchor::Missing => {}
            }
            debug!(
                pos = cursor.pos,
                basis,
                total = page.total,
                read,
                "Nothing below the history anchor in the read, reading again"
            );
            basis = page.total;
        }
        Err(ApiError::NotReady { retry_after: self.config.retry })
    }

    /// A turn at the node, held for a read and what the read is cut down from.
    async fn turn(&self) -> Result<Turn<'_>, ApiError> {
        self.reader.turn().await.map_err(api_error)
    }

    async fn read(
        &self,
        turn: &Turn<'_>,
        tree: &ErgoTree,
        offset: u32,
        limit: u32,
    ) -> Result<AddressTransactions, ApiError> {
        self.reader
            .read(turn, tree, offset, limit)
            .await
            .map_err(api_error)
    }
}

fn api_error(error: HistoryError) -> ApiError {
    match error {
        HistoryError::Node(error) => ApiError::from(&error),
        // The reads ahead are slow, not failing, so the same request works once they end:
        // but it has just waited as long as a read may take, so not at once.
        HistoryError::Busy => ApiError::NotReady { retry_after: BUSY_RETRY },
    }
}

enum Anchor {
    /// At this index of the read, and still at the cursor's position.
    Found(usize),
    /// In the read, at another position: the list was rewritten under it.
    Moved,
    Missing,
}

/// Where the cursor's anchor is in a read at `offset`. The read's newest tx is at position
/// `total - 1 - offset` from the oldest end, and each later one a position lower.
fn anchor(page: &AddressTransactions, offset: u32, cursor: &TxCursor) -> Anchor {
    let Some(at) = page.items.iter().position(|t| t.id == cursor.anchor) else {
        return Anchor::Missing;
    };
    let pos = i64::from(page.total) - 1 - i64::from(offset) - at as i64;
    if pos == i64::from(cursor.pos) { Anchor::Found(at) } else { Anchor::Moved }
}

/// `pending`, then the read's txs from index `from`, with a cursor on the oldest of them.
/// `above` is the position, from the oldest end, just above the first tx served: the list's
/// length on a first page, the anchor's (`anchor`) on a next one. Each tx served is one lower.
/// A page that serves nothing though older txs are left keeps the anchor, so `null` is never
/// sent before the end. A read listing more txs below `above` than there are is the node's
/// fault: `node_error`, rather than a cursor the relay would then refuse.
fn page_of(
    tree: &ErgoTree,
    key: &TrackingKey,
    page: AddressTransactions,
    from: usize,
    above: u32,
    anchor: Option<TxId>,
    pending: Vec<HistoryItem>,
) -> Result<HistoryPage, ApiError> {
    let AddressTransactions { items, total } = page;
    let served: Vec<AddressTransaction> = items.into_iter().skip(from).collect();
    let count = u32::try_from(served.len()).expect("a page holds at most 101 txs");
    let oldest = above.checked_sub(count).ok_or_else(|| {
        ApiError::NodeError(format!("the indexer listed {count} txs below position {above}"))
    })?;
    let next_cursor = served
        .last()
        .map(|t| t.id)
        .or(anchor)
        .filter(|_| oldest > 0)
        .map(|anchor| TxCursor { anchor, pos: oldest, total }.encode(key));
    let confirmed = served.into_iter().map(|t| confirmed_item(tree, t));
    Ok(HistoryPage { items: pending.into_iter().chain(confirmed).collect(), next_cursor })
}

fn confirmed_item(tree: &ErgoTree, tx: AddressTransaction) -> HistoryItem {
    HistoryItem {
        tx_id: tx.id,
        included: Some(Included { height: tx.inclusion_height, timestamp: tx.timestamp }),
        // The indexer lists only txs that touch the address; an empty delta if it ever
        // listed one that doesn't.
        delta: overlay::tx_delta(tree, &tx.inputs, &tx.outputs).unwrap_or_default(),
        fee: overlay::tx_fee(&tx.outputs),
    }
}

fn pending_item(tx: overlay::TxDelta) -> HistoryItem {
    HistoryItem { tx_id: tx.tx_id, included: None, delta: tx.delta, fee: tx.fee }
}

/// The pending txs that touch the address guarded by `tree`, tracked or not, and the tip they
/// were decided at; `None` before the engine's first block. Also what tells the balance a
/// zero-balance address is used.
pub(super) async fn pending_txs(
    engine: &impl EngineHandle,
    tree: &ErgoTree,
) -> Result<Option<PendingTxsAt>, ApiError> {
    match engine.query(Query::PendingTxs(tree.clone())).await? {
        Reply::PendingTxs(pending) => Ok(pending),
        other => unreachable!("`Query::PendingTxs` answered {other:?}"),
    }
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
