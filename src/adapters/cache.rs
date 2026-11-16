//! The box cache: full boxes by id, under a budget in bytes.
//!
//! A box is about 100 B to 4 KB, most of it registers, so a cap on the number of boxes would
//! bound nothing; each box is weighed by its estimated size in memory instead. Boxes never
//! change, so the cache needs eviction but never invalidation. The loader seeds it with the
//! boxes of each first load and the box fetcher reads it.

use std::{
    mem::size_of,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use moka::{policy::EvictionPolicy, sync::Cache};

use crate::types::ergo::{BoxId, Token, UTxO};

/// What each cached box costs besides its own bytes: the `Arc`'s counts, the key, and what
/// the weight's other terms can't see, which is moka's bookkeeping for the entry (hash table
/// slot, entry info, access-order node) and the allocator's rounding of the box's
/// allocations. That last part is measured, not derived: with 128 here, a cache of the
/// smallest boxes took 205 bytes per entry more than its weight (PLAN.md, step 10;
/// `examples/membench.rs`), so it is 128 + 205. Run the benchmark again after a moka upgrade.
const ENTRY: usize = 2 * size_of::<usize>() + size_of::<BoxId>() + 333;

/// Full boxes by id, shared by the loader and the box fetcher. Cloning shares the cache.
///
/// A moka `sync::Cache` with the LRU policy. moka's default policy admits a new entry into
/// a full cache only if it is used more often than what it would evict, which would turn
/// away a load's fresh boxes, exactly the ones the next page asks for. `sync` rather than
/// `future`: nothing here waits on I/O, so no operation needs to be async.
///
/// moka bounds the weighted size best-effort: inserts land at once and evictions happen in
/// its housekeeping, so the size can pass the budget until that runs (moka 0.12.16,
/// `sync::Cache`).
#[derive(Clone)]
pub struct BoxCache {
    /// `None` when the budget is 0: no cache at all, rather than relying on how moka treats
    /// a capacity of 0.
    cache: Option<Cache<BoxId, Arc<UTxO>>>,
    budget: u64,
    /// Reads answered and not, for the hit rate an operator sizes the budget by.
    reads: Arc<Reads>,
    /// Inserts made, for tests that check what is written.
    #[cfg(test)]
    inserts: Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Default)]
struct Reads {
    hits: AtomicU64,
    misses: AtomicU64,
}

/// What the cache holds and how its reads went, for the metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub entries: u64,
    /// The cached boxes' weights, summed: what the budget bounds.
    pub weighted_bytes: u64,
    pub hits: u64,
    pub misses: u64,
}

impl BoxCache {
    /// A cache holding at most `budget` bytes of boxes, as [`weight`] counts them. A budget
    /// of 0 disables it.
    pub fn new(budget: u64) -> Self {
        let cache = (budget > 0).then(|| {
            Cache::builder()
                .max_capacity(budget)
                .weigher(|_, b: &Arc<UTxO>| weight(b))
                .eviction_policy(EvictionPolicy::lru())
                .build()
        });
        Self {
            cache,
            budget,
            reads: Default::default(),
            #[cfg(test)]
            inserts: Default::default(),
        }
    }

    /// A cache that holds nothing.
    pub fn disabled() -> Self {
        Self::new(0)
    }

    /// A disabled cache counts no reads: it has no hit rate.
    pub fn get(&self, id: &BoxId) -> Option<Arc<UTxO>> {
        let found = self.cache.as_ref()?.get(id);
        let count = if found.is_some() { &self.reads.hits } else { &self.reads.misses };
        count.fetch_add(1, Ordering::Relaxed);
        found
    }

    /// The cache's size and read counts. Settles first: moka's counts are as of its last
    /// housekeeping.
    pub fn stats(&self) -> CacheStats {
        let Some(cache) = &self.cache else {
            return CacheStats::default();
        };
        self.settle();
        CacheStats {
            entries: cache.entry_count(),
            weighted_bytes: cache.weighted_size(),
            hits: self.reads.hits.load(Ordering::Relaxed),
            misses: self.reads.misses.load(Ordering::Relaxed),
        }
    }

    /// Caches `b`, unless it alone is larger than the budget.
    pub fn insert(&self, b: Arc<UTxO>) {
        let Some(cache) = &self.cache else { return };
        #[cfg(test)]
        self.inserts.fetch_add(1, Ordering::Relaxed);
        if u64::from(weight(&b)) <= self.budget {
            cache.insert(b.id, b);
        }
    }

    /// Runs moka's housekeeping, which applies pending writes and evictions. With no other
    /// thread writing meanwhile, one run applies all of them (moka 0.12.16,
    /// `sync/base_cache.rs`, `do_run_pending_tasks`), which is how tests and the memory
    /// benchmark read the size at a given moment.
    pub fn settle(&self) {
        if let Some(cache) = &self.cache {
            cache.run_pending_tasks();
        }
    }

    /// The inserts made so far, cached or refused for size.
    #[cfg(test)]
    pub fn inserts(&self) -> usize {
        self.inserts.load(Ordering::Relaxed)
    }

    /// The cached boxes, in no order.
    #[cfg(test)]
    pub fn boxes(&self) -> Vec<Arc<UTxO>> {
        self.cache
            .iter()
            .flat_map(|c| c.iter().map(|(_, b)| b))
            .collect()
    }
}

/// A box's estimated size in memory, in bytes: the `UTxO` itself, the bytes of its tree,
/// registers and tokens, and [`ENTRY`]. Lengths, not capacities, so the weight depends only
/// on what the box holds. A box shared with the ring buffer is counted in full: the budget
/// bounds the cache's own share.
pub fn weight(b: &UTxO) -> u32 {
    let r = &b.registers;
    let registers: usize = [&r.r4, &r.r5, &r.r6, &r.r7, &r.r8, &r.r9]
        .into_iter()
        .flatten()
        .map(|bytes| bytes.0.len())
        .sum();
    let bytes = size_of::<UTxO>()
        + b.ergo_tree.0.len()
        + registers
        + b.tokens.len() * size_of::<Token>()
        + ENTRY;
    // A box is at most 4096 bytes serialized (`SigmaConstants.MaxBoxSize`), far below this.
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
