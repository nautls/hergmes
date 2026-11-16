//! The box service: one box by id, as spendable now, for clients that hold an id and want
//! the box, like Fleet's `getBoxes({ where: { boxId } })`.
//!
//! A box id is not a key: the read keeps no engine state and streams nothing, so it is outside
//! the key policy. It costs one engine query and at most one node call.

use std::sync::Arc;

use crate::{
    adapters::EngineHandle,
    api::ApiError,
    clients::NodeSource,
    engine::{PoolBox, Query, Reply},
    types::ergo::{BoxId, UTxO},
};

/// A box a tx can spend now: confirmed, with the height of the block that included it, or
/// created by a pool tx, with none. One item of a view with the pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundBox {
    pub utxo: Arc<UTxO>,
    pub inclusion_height: Option<u32>,
}

pub struct Boxes<S> {
    source: S,
}

impl<S: NodeSource> Boxes<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }

    /// The box `id` if a tx can spend it now, else `box_not_found`.
    ///
    /// The engine's pool snapshot first, with no node call: a box a pool tx spends is not
    /// found, and one a pool tx creates is served pending. Otherwise the indexer, which also
    /// knows spent boxes: an unspent one is served confirmed, a spent or unknown one is not
    /// found. Not `/utxo/withPool/byId`, which serves boxes pool txs spend and can't tell a
    /// confirmed box from a pending one (node 6.0.8, `UtxoApiRoute.withPoolById`). Not the box
    /// cache either: it can't say whether a box is spent, so a hit would still need the
    /// indexer.
    pub async fn by_id(
        &self,
        engine: &impl EngineHandle,
        id: &BoxId,
    ) -> Result<FoundBox, ApiError> {
        let pool = match engine.query(Query::PoolBox(*id)).await? {
            Reply::PoolBox(answer) => answer,
            other => unreachable!("`Query::PoolBox` answered {other:?}"),
        };
        match pool {
            Some(PoolBox::Spent) => Err(ApiError::BoxNotFound),
            Some(PoolBox::Created(utxo)) => Ok(FoundBox { utxo, inclusion_height: None }),
            None => match self
                .source
                .indexed_box(id)
                .await
                .map_err(|e| ApiError::from(&e))?
            {
                Some(found) if !found.spent => Ok(FoundBox {
                    utxo: Arc::new(found.indexed.utxo),
                    inclusion_height: Some(found.indexed.inclusion_height),
                }),
                _ => Err(ApiError::BoxNotFound),
            },
        }
    }
}

#[cfg(test)]
#[path = "boxes_tests.rs"]
mod tests;
