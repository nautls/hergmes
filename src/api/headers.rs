//! The header service: the last headers up to the engine's tip, newest first, for a
//! wallet's signing context (DESIGN.md, "Headers").

use crate::{
    adapters::{
        EngineHandle, HeaderReader, engine_tip,
        headers::{HeaderError, MAX_HEADERS},
    },
    api::{ApiError, http::DEFAULT_RETRY},
    clients::NodeSource,
    engine::BlockRef,
    types::ergo::BlockHeader,
};

/// The most headers a request returns, and the default: a signing context's 10.
pub const MAX_LIMIT: u32 = MAX_HEADERS as u32;

/// The headers ending at the tip, newest first, and that tip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadersPage {
    pub as_of: BlockRef,
    pub items: Vec<BlockHeader>,
}

pub struct Headers<S> {
    reader: HeaderReader<S>,
}

impl<S: NodeSource> Headers<S> {
    pub fn new(reader: HeaderReader<S>) -> Self {
        Self { reader }
    }

    /// `limit` headers ([`MAX_LIMIT`] by default, and at most), from one read of the
    /// engine's tip. A header never changes under its id, so a block applied during the
    /// walk can't mix two chains into the answer.
    pub async fn last(
        &self,
        engine: &impl EngineHandle,
        limit: Option<u32>,
    ) -> Result<HeadersPage, ApiError> {
        let limit = match limit {
            None => MAX_LIMIT,
            Some(n @ 1..=MAX_LIMIT) => n,
            Some(_) => return Err(ApiError::InvalidLimit { max: MAX_LIMIT }),
        };
        let as_of = engine_tip(engine)
            .await?
            .ok_or(ApiError::NotReady { retry_after: DEFAULT_RETRY })?;
        let items = self.reader.back_from(as_of, limit as usize).await?;
        Ok(HeadersPage { as_of, items })
    }
}

impl From<HeaderError> for ApiError {
    fn from(error: HeaderError) -> Self {
        match error {
            HeaderError::Node(error) => (&error).into(),
            // The node stores the headers of the relay's chain.
            other => Self::NodeError(other.to_string()),
        }
    }
}

#[cfg(test)]
#[path = "headers_tests.rs"]
mod tests;
