//! The token metadata service: names, descriptions, decimals and emissions, for clients that
//! show tokens.
//!
//! A token id here is not a key: the read keeps no engine state and streams nothing, so it is
//! outside the key policy and the supply limit, and a wallet can name any token it holds.

use std::{collections::HashMap, mem::size_of, sync::Arc};

use moka::{policy::EvictionPolicy, sync::Cache};

use crate::{
    adapters::cache::ENTRY,
    api::ApiError,
    clients::NodeSource,
    types::ergo::{TokenId, TokenMetadata},
};

/// The most ids a request names: 65 characters each with the comma, so 100 keep the URL
/// near 6.5 KB, under nginx's default 8 KB request line.
pub const MAX_IDS: usize = 100;

/// The bytes of metadata kept. `name` and `description` are a mint's R4 and R5 (node 6.0.8,
/// `IndexedToken.fromBox`), together under 4 KB since a box is at most 4096 bytes
/// (`SigmaConstants.MaxBoxSize`), so one token weighs from about 500 bytes to about 4.5 KB:
/// weighing by bytes keeps the budget true where a count would be off by up to nine times. A
/// typical token weighs about 550 bytes, most of it the entry's overhead, so this keeps about
/// 7,500.
pub const CACHE_BUDGET: u64 = 4 << 20;

pub struct Tokens<S> {
    source: S,
    /// Tokens served so far, least recently read evicted first. A token never changes once
    /// its mint is indexed, so nothing is invalidated. Unknown ids are not kept: the indexer
    /// may reach their mint later. A mint undone by a reorg stays until evicted, which serves
    /// a token that may not exist; the same tx usually mints it again on the new branch.
    cache: Cache<TokenId, Arc<TokenMetadata>>,
}

impl<S: NodeSource> Tokens<S> {
    pub fn new(source: S) -> Self {
        Self { source, cache: cache(CACHE_BUDGET) }
    }

    /// The metadata of the tokens named in `ids`, comma-separated, in their order and each
    /// once; unknown tokens are left out. One node call, for the ids not kept, or none. A
    /// node failure fails the whole request.
    pub async fn metadata(&self, ids: Option<&str>) -> Result<Vec<Arc<TokenMetadata>>, ApiError> {
        let ids = parse(ids)?;
        let mut found: HashMap<TokenId, Arc<TokenMetadata>> = HashMap::with_capacity(ids.len());
        let mut missing = Vec::new();
        for id in &ids {
            match self.cache.get(id) {
                Some(token) => _ = found.insert(*id, token),
                None => missing.push(*id),
            }
        }
        if !missing.is_empty() {
            let fetched = self
                .source
                .tokens(&missing)
                .await
                .map_err(|e| ApiError::from(&e))?;
            for token in fetched {
                let token = Arc::new(token);
                self.cache.insert(token.id, token.clone());
                found.insert(token.id, token);
            }
        }
        Ok(ids.iter().filter_map(|id| found.remove(id)).collect())
    }
}

/// The ids, deduplicated in order: absent, empty or over [`MAX_IDS`] is `invalid_ids`, and
/// any id that is not 64 hex characters, empty ones included, is `invalid_token_id`.
fn parse(ids: Option<&str>) -> Result<Vec<TokenId>, ApiError> {
    let ids = ids
        .filter(|ids| !ids.is_empty())
        .ok_or(ApiError::InvalidIds)?;
    if ids.split(',').nth(MAX_IDS).is_some() {
        return Err(ApiError::InvalidIds);
    }
    let mut parsed: Vec<TokenId> = Vec::new();
    for id in ids.split(',') {
        let id = id.parse().map_err(|_| ApiError::InvalidTokenId)?;
        if !parsed.contains(&id) {
            parsed.push(id);
        }
    }
    Ok(parsed)
}

/// An LRU under `budget` bytes, as [`weight`] counts them. LRU rather than moka's default,
/// which turns away a new token unless it is read more often than what it would evict: a
/// wallet's tokens are read in a burst when it opens, then seldom.
fn cache(budget: u64) -> Cache<TokenId, Arc<TokenMetadata>> {
    Cache::builder()
        .max_capacity(budget)
        .weigher(|_, t: &Arc<TokenMetadata>| weight(t))
        .eviction_policy(EvictionPolicy::lru())
        .build()
}

/// A token's estimated size in memory: the struct, its strings and the entry's overhead,
/// which is the box cache's (`ENTRY`): the same key size, `Arc` and moka bookkeeping.
fn weight(t: &TokenMetadata) -> u32 {
    let bytes = size_of::<TokenMetadata>() + t.name.len() + t.description.len() + ENTRY;
    // A box is at most 4096 bytes serialized (`SigmaConstants.MaxBoxSize`), far below this.
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

#[cfg(test)]
#[path = "tokens_tests.rs"]
mod tests;
