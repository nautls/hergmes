//! The page service: a page of a key's unspent boxes, pinned to the first page's block
//! (DESIGN.md, "REST: confirmed snapshot").
//!
//! The ids come from the engine, so every page of a snapshot is as of one block whatever
//! happens between requests; the boxes come from the box fetcher. A key that is not tracked
//! is loaded first, which a token key does only after the supply check, and only once the
//! configured keys have settled.

use std::sync::Arc;

use crate::{
    adapters::{BoxFetcher, EngineHandle, Loader, TokenChecker},
    api::{ApiError, Cursor},
    clients::NodeSource,
    engine::{BlockRef, PageError, PageIds, Query, Reply, StoredBox, TrackingKey},
    runtime::Settled,
    types::ergo::BoxId,
};

/// Boxes per page when the request names no limit.
pub const DEFAULT_LIMIT: u32 = 500;
/// The most boxes per page: one `withPool/byIds` chunk, so a cold page is one node call.
pub const MAX_LIMIT: u32 = crate::adapters::boxes::CHUNK as u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoxPage {
    pub as_of: BlockRef,
    pub items: Vec<StoredBox>,
    /// `None` exactly on the last page.
    pub next_cursor: Option<String>,
}

pub struct Pages<S> {
    /// Shared with the keepers, so their loads and requests' loads of one key coalesce.
    loader: Arc<Loader<S>>,
    tokens: Arc<TokenChecker<S>>,
    fetcher: BoxFetcher<S>,
    /// Whether the configured keys have settled; no load starts for a client before.
    settled: Settled,
}

impl<S: NodeSource> Pages<S> {
    pub fn new(
        loader: Arc<Loader<S>>,
        tokens: Arc<TokenChecker<S>>,
        fetcher: BoxFetcher<S>,
    ) -> Self {
        Self { loader, tokens, fetcher, settled: Settled::now() }
    }

    /// Starts no load until `settled` is, so at startup the configured keys take the total cap
    /// before clients can. A request that would load answers `not_ready` until then.
    pub fn waiting_for(mut self, settled: Settled) -> Self {
        self.settled = settled;
        self
    }

    /// Whether a request may load a key that is not tracked.
    pub fn loads(&self) -> bool {
        self.settled.is_settled()
    }

    /// A page of `key`'s boxes: the first one, at the tip, without a cursor; the next one
    /// after `cursor`, at the block it is pinned to. At most `limit` boxes (default
    /// [`DEFAULT_LIMIT`], at most [`MAX_LIMIT`]), in `(inclusionHeight, boxId)` order.
    pub async fn page(
        &self,
        engine: &impl EngineHandle,
        key: &TrackingKey,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<BoxPage, ApiError> {
        let limit = match limit {
            None => DEFAULT_LIMIT,
            Some(n @ 1..=MAX_LIMIT) => n,
            Some(_) => return Err(ApiError::InvalidLimit),
        } as usize;
        let cursor = cursor
            .map(|c| Cursor::decode(c, key))
            .transpose()
            .map_err(ApiError::InvalidCursor)?;

        // One more than the page, so the last page is known without another read.
        let PageIds { as_of, mut boxes } = self.ids(engine, key, cursor, limit + 1).await?;
        let next_cursor = (boxes.len() > limit).then(|| {
            boxes.truncate(limit);
            let after = *boxes.last().expect("a limit is at least 1");
            Cursor { at: as_of, after }.encode(key)
        });

        let ids: Vec<BoxId> = boxes.iter().map(|b| b.box_id).collect();
        let found = self.fetcher.fetch(engine, &ids).await?;
        let items = boxes
            .iter()
            .zip(found)
            .map(|(b, utxo)| match utxo {
                Some(utxo) => Ok(StoredBox { utxo, inclusion_height: b.inclusion_height }),
                // A page never leaves a box out.
                None => Err(ApiError::BoxUnavailable { box_id: b.box_id }),
            })
            .collect::<Result<_, _>>()?;
        Ok(BoxPage { as_of, items, next_cursor })
    }

    /// Loads `key` for a client: only once the configured keys have settled, and a token
    /// only after the supply check. A key already tracked is a duplicate the engine ignores.
    /// Pages and the WebSocket's balance-only subscriptions load through here.
    pub async fn load(
        &self,
        engine: &impl EngineHandle,
        key: &TrackingKey,
    ) -> Result<(), ApiError> {
        if !self.loads() {
            return Err(ApiError::NotReady { retry_after: self.loader.config().retry });
        }
        if let TrackingKey::Token(id) = key {
            self.tokens.check(id).await?;
        }
        if let Err(error) = self.loader.load(engine, key).await {
            return Err((&*error).into());
        }
        Ok(())
    }

    /// The page's ids, loading the key if it is not tracked and the page is a first one.
    async fn ids(
        &self,
        engine: &impl EngineHandle,
        key: &TrackingKey,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<PageIds, ApiError> {
        let query = || Query::Page {
            key: key.clone(),
            at: cursor.map(|c| c.at),
            after: cursor.map(|c| c.after),
            limit,
        };
        match page_ids(engine, query()).await? {
            Ok(ids) => return Ok(ids),
            Err(PageError::CursorExpired) => return Err(ApiError::CursorExpired),
            // Untracked since the page before: idle for the grace period, or its load block
            // orphaned. A load now would be pinned above the cursor's block, so the page
            // would expire anyway; the load is not made.
            Err(PageError::NotTracked) if cursor.is_some() => return Err(ApiError::CursorExpired),
            Err(PageError::NotTracked) => {}
        }

        self.load(engine, key).await?;
        match page_ids(engine, query()).await? {
            Ok(ids) => Ok(ids),
            // Untracked again since the load: its load block was orphaned.
            Err(PageError::NotTracked) => {
                Err(ApiError::NotReady { retry_after: self.loader.config().retry })
            }
            Err(PageError::CursorExpired) => unreachable!("a first page is at the tip"),
        }
    }
}

async fn page_ids(
    engine: &impl EngineHandle,
    query: Query,
) -> Result<Result<PageIds, PageError>, ApiError> {
    match engine.query(query).await? {
        Reply::Page(page) => Ok(page),
        other => unreachable!("`Query::Page` answered {other:?}"),
    }
}

#[cfg(test)]
#[path = "pages_tests.rs"]
mod tests;
