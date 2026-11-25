//! The token metadata service: names, descriptions, decimals, emissions, mint txs, standards
//! and EIP-4 asset types and artwork, for clients that show tokens.
//!
//! A token id here is not a key: the read keeps no engine state and streams nothing, so it is
//! outside the key policy and the supply limit, and a wallet can name any token it holds.

use std::{collections::HashMap, mem::size_of, sync::Arc};

use futures_util::{StreamExt, TryStreamExt, stream};
use moka::{policy::EvictionPolicy, sync::Cache};

use crate::{
    adapters::cache::ENTRY,
    api::ApiError,
    clients::NodeSource,
    types::ergo::{IndexedToken, TokenId, TokenMetadata},
};

/// The most ids a request names: 65 characters each with the comma, so 100 keep the URL
/// near 6.5 KB, under nginx's default 8 KB request line.
pub const MAX_IDS: usize = 100;

/// The most minting boxes read at once. The node reads boxes one per request (node 6.0.8,
/// `BlockchainApiRoute.getBoxByIdR`; `box/range` lists ids, not boxes), so a request naming
/// [`MAX_IDS`] new tokens makes that many reads, and this keeps them from all landing at once.
const BOX_READS: usize = 8;

/// The bytes of metadata kept. `name` and `description` are a mint's R4 and R5 (node 6.0.8,
/// `IndexedToken.fromBox`) and the asset's links its R9, all under 4 KB since a box is at most
/// 4096 bytes (`SigmaConstants.MaxBoxSize`), so one token weighs from about 500 bytes to about
/// 4.6 KB: weighing by bytes keeps the budget true where a count would be off by up to nine
/// times. A typical token weighs about 550 bytes, most of it the entry's overhead, and an NFT
/// with its links about 750, so this keeps 5,500 to 7,500.
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
    /// once; unknown tokens, and tokens whose minting box the indexer no longer knows, are
    /// left out. For the ids not kept, one node call, then one per
    /// token found to read its minting box; none if every id is kept. A node failure fails
    /// the whole request.
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
            // Each token is kept as soon as its box is read, so a failure keeps the reads done
            // before it and a retry makes only the rest. The first failure stops the reads not
            // yet made, sparing a node that is failing.
            let mut reads = stream::iter(fetched)
                .map(|token| self.minted(token))
                .buffer_unordered(BOX_READS);
            while let Some(token) = reads.try_next().await? {
                if let Some(token) = token {
                    let token = Arc::new(token);
                    self.cache.insert(token.id, token.clone());
                    found.insert(token.id, token);
                }
            }
        }
        Ok(ids.iter().filter_map(|id| found.remove(id)).collect())
    }

    /// `token` with what its minting box adds. The indexer lists a token only once it has
    /// indexed the mint, so a box it does not know was lost to a reorg since, and with it the
    /// mint tx: the token is left out and not kept, as for an unknown id.
    async fn minted(&self, token: IndexedToken) -> Result<Option<TokenMetadata>, ApiError> {
        let minting = self
            .source
            .indexed_box(&token.minting_box_id)
            .await
            .map_err(|e| ApiError::from(&e))?;
        let Some(minting) = minting else {
            tracing::debug!(token = %token.id, "minting box unknown, token left out");
            return Ok(None);
        };
        Ok(Some(TokenMetadata::minted(token, &minting.indexed.utxo)))
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

/// A token's estimated size in memory: the struct, what its standard holds and the entry's
/// overhead, which is the box cache's (`ENTRY`): the same key size, `Arc` and moka bookkeeping.
fn weight(t: &TokenMetadata) -> u32 {
    let bytes = size_of::<TokenMetadata>() + t.standard.heap_size() + ENTRY;
    // A box is at most 4096 bytes serialized (`SigmaConstants.MaxBoxSize`), far below this.
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

#[cfg(test)]
#[path = "tokens_tests.rs"]
mod tests;
