//! The box fetcher: full boxes for the ids the engine keeps, from the cheapest source that
//! has them.

use std::{collections::HashMap, sync::Arc};

use tracing::debug;

use crate::{
    adapters::{AdapterError, BoxCache, EngineHandle},
    clients::NodeSource,
    engine::{Query, Reply},
    types::ergo::{BoxId, UTxO},
};

/// Ids per `POST /utxo/withPool/byIds`. The route sets no limit of its own
/// (`UtxoApiRoute.withPoolByIds`); this keeps a request body near 70 KB, far below the HTTP
/// server's default entity limit.
pub const CHUNK: usize = 1000;

pub struct BoxFetcher<S> {
    source: S,
    /// Shared with the loader, which seeds it with the boxes of each first load.
    cache: BoxCache,
    chunk: usize,
}

impl<S: NodeSource> BoxFetcher<S> {
    pub fn new(source: S, cache: BoxCache) -> Self {
        Self::with_chunk(source, cache, CHUNK)
    }

    pub fn with_chunk(source: S, cache: BoxCache, chunk: usize) -> Self {
        assert!(chunk >= 1);
        Self { source, cache, chunk }
    }

    /// The boxes of `ids`, in the same order; `None` for an id no source knows.
    ///
    /// Sources in order: the cache; the engine's ring buffer (the boxes tracked keys gained
    /// or lost in the last N blocks, the spent ones gone from the node's UTXO set); the
    /// node's UTXO set and mempool
    /// (`withPool/byIds`, in chunks, matched by id since it leaves missing ids out); and the
    /// indexer (`box/byId`, which also has spent boxes, for a block the node applied before
    /// the engine). Boxes found are cached.
    pub async fn fetch(
        &self,
        engine: &impl EngineHandle,
        ids: &[BoxId],
    ) -> Result<Vec<Option<Arc<UTxO>>>, AdapterError> {
        let mut hits: HashMap<BoxId, Arc<UTxO>> = HashMap::new();
        for id in ids {
            if let Some(b) = self.cache.get(id) {
                hits.insert(*id, b);
            }
        }
        let mut missing = missing(ids, &hits);

        // Boxes from the other sources, which are the only ones cached: a hit is not
        // written again, since `get` already marked it used, and an insert would weigh it
        // and queue a write for moka's housekeeping.
        let mut found: HashMap<BoxId, Arc<UTxO>> = HashMap::new();
        if !missing.is_empty() {
            match engine.query(Query::Boxes(missing.clone())).await? {
                Reply::Boxes(boxes) => found.extend(boxes.into_iter().map(|b| (b.id, b))),
                other => unreachable!("`Query::Boxes` answered {other:?}"),
            }
            missing = self::missing(&missing, &found);
        }
        let from_ring = found.len();

        for chunk in missing.chunks(self.chunk) {
            let boxes = self.source.boxes_with_pool(chunk).await?;
            found.extend(boxes.into_iter().map(|b| (b.id, Arc::new(b))));
        }
        missing = self::missing(&missing, &found);
        let from_node = found.len() - from_ring;

        for id in &missing {
            if let Some(b) = self.source.indexed_box(id).await? {
                found.insert(*id, Arc::new(b.indexed.utxo));
            }
        }
        let from_indexer = found.len() - from_ring - from_node;
        let cached = hits.len();
        debug!(ids = ids.len(), cached, from_ring, from_node, from_indexer, "Boxes fetched");

        for b in found.values() {
            self.cache.insert(b.clone());
        }
        found.extend(hits);
        Ok(ids.iter().map(|id| found.get(id).cloned()).collect())
    }
}

/// The ids not found yet, each once, in their first order.
fn missing(ids: &[BoxId], found: &HashMap<BoxId, Arc<UTxO>>) -> Vec<BoxId> {
    let mut seen = std::collections::HashSet::new();
    ids.iter()
        .filter(|id| !found.contains_key(id) && seen.insert(**id))
        .copied()
        .collect()
}

#[cfg(test)]
#[path = "boxes_tests.rs"]
mod tests;
