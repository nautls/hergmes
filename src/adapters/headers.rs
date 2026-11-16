//! The last headers up to a tip, for clients' signing context.
//!
//! Headers are cached by id. A header never changes under its id, so the cache is never
//! stale, and an orphan left in it is never reached: a walk starts at the relay's own tip and
//! follows `parentId`, so it stays on the relay's chain through reorgs. The block consumer
//! fills the cache with the headers of the blocks it applies, so while the relay follows the
//! chain no walk calls the node; a miss (at startup the engine holds one block) is fetched by
//! id.

use moka::{policy::EvictionPolicy, sync::Cache};
use tracing::debug;

use crate::{
    clients::{NodeError, NodeSource},
    engine::BlockRef,
    types::ergo::{BlockHeader, GENESIS_PARENT_ID, HeaderId},
};

/// The most headers a walk returns: what a signing context holds (sigma-rust,
/// `ErgoStateContext::headers`).
pub const MAX_HEADERS: usize = 10;

/// Headers the cache holds: a walk's 10, and room for the branches of reorgs and the blocks a
/// burst applies before a walk reads them. About 1 KB each.
const CAPACITY: u64 = 32;

/// Headers by id, shared by the block consumer, which fills it, and the readers.
///
/// Least recently used goes first. moka's default, TinyLFU, admits a new entry only if it is
/// read more often than the one it would evict, so once the cache is full of headers that
/// every walk reads, a new tip's header would be refused and fetched again on each walk.
#[derive(Clone)]
pub struct HeaderCache(Cache<HeaderId, BlockHeader>);

impl HeaderCache {
    pub fn new() -> Self {
        Self(
            Cache::builder()
                .max_capacity(CAPACITY)
                .eviction_policy(EvictionPolicy::lru())
                .build(),
        )
    }

    pub fn insert(&self, header: &BlockHeader) {
        self.0.insert(header.id, header.clone());
    }

    /// Drops a header, for tests that make the walk fetch it.
    #[cfg(any(test, feature = "sim"))]
    pub fn invalidate(&self, id: &HeaderId) {
        self.0.invalidate(id);
    }

    #[cfg(any(test, feature = "sim"))]
    pub fn contains(&self, id: &HeaderId) -> bool {
        self.0.contains_key(id)
    }
}

impl Default for HeaderCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Why a walk failed. Each is the node's failure: the headers of the relay's chain are
/// stored on it.
#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error("the node has no header {0}")]
    Unknown(HeaderId),
    #[error("the node's header {got:?} is not the block {wanted:?}")]
    Mismatch { wanted: BlockRef, got: BlockRef },
}

/// Walks back from a tip through the cache, fetching what it misses.
pub struct HeaderReader<S> {
    source: S,
    cache: HeaderCache,
}

impl<S: NodeSource> HeaderReader<S> {
    pub fn new(source: S, cache: HeaderCache) -> Self {
        Self { source, cache }
    }

    /// Up to `n` headers ending at `tip`, newest first: the tip, its parent, and so on.
    /// Fewer only when the walk reaches genesis.
    pub async fn back_from(
        &self,
        tip: BlockRef,
        n: usize,
    ) -> Result<Vec<BlockHeader>, HeaderError> {
        let mut headers = Vec::with_capacity(n);
        let mut wanted = tip;
        while headers.len() < n {
            let header = self.get(wanted).await?;
            let parent_id = header.parent_id;
            headers.push(header);
            if parent_id == GENESIS_PARENT_ID {
                break;
            }
            wanted = BlockRef { height: wanted.height.saturating_sub(1), header_id: parent_id };
        }
        Ok(headers)
    }

    /// The header of `wanted`, checked against its height too: a header is cached by the id
    /// it carries, so the id always matches, but a parent's height must be one less.
    async fn get(&self, wanted: BlockRef) -> Result<BlockHeader, HeaderError> {
        let header = match self.cache.0.get(&wanted.header_id) {
            Some(header) => header,
            None => {
                let header = self
                    .source
                    .header(&wanted.header_id)
                    .await?
                    .ok_or(HeaderError::Unknown(wanted.header_id))?;
                debug!(height = wanted.height, id = %wanted.header_id, "Header fetched");
                // Under its own id, so cached even if it is not `wanted`'s.
                self.cache.insert(&header);
                header
            }
        };
        let got = BlockRef::from(&header);
        if got != wanted {
            return Err(HeaderError::Mismatch { wanted, got });
        }
        Ok(header)
    }
}

#[cfg(test)]
#[path = "headers_tests.rs"]
mod tests;
