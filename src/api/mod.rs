//! Transport: REST snapshot and balance endpoints, and the WebSocket delta stream.
//!
//! The services here hold what a request does, without HTTP: the page service reads a
//! key's box ids from the engine, loading the key first if it is not tracked, and fills in
//! full boxes, for pages pinned to a block and for the view with the pool (the overlay
//! applied); the balance service answers an address's balance, from the engine if it is
//! tracked and from the indexer and the engine's mempool snapshot if not; the history service
//! reads an address's txs from the indexer; the header service walks back from the engine's
//! tip; the box service reads one box by id, from the engine's pool snapshot or the indexer;
//! the transaction service hands a serialized tx to the node; the token service reads tokens'
//! metadata from the indexer, keeping what it read. The HTTP layer (`http`)
//! parses, calls them and serializes through the wire types (`wire`). The WebSocket (`ws`)
//! runs a task per connection (`connection`), speaking the messages in `protocol`.

use std::time::Duration;

use crate::{
    adapters::{AdapterError, EngineGone, loader::LoadError, tokens::TokenError},
    clients::NodeError,
    types::ergo::BoxId,
};

pub mod balances;
pub mod boxes;
pub mod connection;
pub mod cursor;
pub mod headers;
pub mod history;
pub mod http;
pub mod keys;
pub mod metrics;
pub mod origins;
pub mod pages;
pub mod policy;
pub mod protocol;
pub mod tokens;
pub mod transactions;
pub mod wire;
pub mod ws;

pub use balances::{AddressBalance, Balances};
pub use boxes::{Boxes, FoundBox};
pub use cursor::Cursor;
pub use headers::{Headers, HeadersPage};
pub use history::{History, HistoryPage};
pub use metrics::Metrics;
pub use origins::OriginPolicy;
pub use pages::{BoxPage, Pages, WithPoolBoxes};
pub use policy::KeyPolicy;
pub use tokens::Tokens;
pub use transactions::{Refusal, Transactions};

/// Why a request failed, one variant per error code. The code is for programs, the message
/// for people.
///
/// Node and load failures are flattened to what a client needs, the code and a wait: a
/// coalesced load hands its error to every waiting request as a shared `Arc<LoadError>`,
/// which can't be moved out of, and `NodeError` is not `Clone`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApiError {
    #[error("invalid address: {0}")]
    InvalidAddress(String),
    #[error("a token id is 64 hex characters")]
    InvalidTokenId,
    #[error("a template is 1 to 4,096 bytes in hex")]
    InvalidTemplate,
    /// No `ids`, or more than [`tokens::MAX_IDS`].
    #[error("ids is 1 to {} comma-separated token ids", tokens::MAX_IDS)]
    InvalidIds,
    #[error("a box id is 64 hex characters")]
    InvalidBoxId,
    /// `broadcast` is neither `true` nor `false`.
    #[error("broadcast is true or false")]
    InvalidBroadcast,
    #[error("invalid cursor: {0}")]
    InvalidCursor(cursor::CursorError),
    /// Not a number, or outside 1 to the route's `max`.
    #[error("limit must be between 1 and {max}")]
    InvalidLimit { max: u32 },
    /// The operator's key policy refuses the key; permanent for the key.
    #[error("this relay does not serve this key")]
    KeyNotAllowed,
    /// The operator's origin policy refuses the page or extension the request came from.
    #[error("this relay does not serve this origin")]
    OriginNotAllowed,
    #[error("the indexer does not know this token")]
    UnknownToken,
    /// Spent, by a block or a pool tx, or unknown: no tx can spend it now.
    #[error("no unspent box has this id")]
    BoxNotFound,
    /// The node refused the client's tx; `message` is the node's, or the relay's when it
    /// refused the body itself.
    #[error("{message}")]
    TransactionRejected { reason: Refusal, message: String },
    #[error("the cursor's block is no longer served; start the snapshot again")]
    CursorExpired,
    #[error("token supply {emission} is above the limit {limit}")]
    SupplyTooLarge { emission: u64, limit: u64 },
    #[error("the key has more than {max} boxes")]
    TooManyBoxes { max: usize },
    /// A template whose trees have no segregated constants; permanent for the key.
    #[error("the relay tracks templates of trees with segregated constants only")]
    UnsupportedTemplate,
    #[error("the relay tracks as many boxes as it can; try again later")]
    CapacityExceeded,
    /// A load that did not settle, an untracked address's balance whose halves never met at
    /// one height, or a history page the indexer kept moving under or that waited behind
    /// slow reads; `retry_after` is the next wait.
    #[error("not ready yet; try again")]
    NotReady { retry_after: Duration },
    /// The node answered `503`, with its `Retry-After` if it sent one.
    #[error("the node is unavailable")]
    NodeUnavailable { retry_after: Option<Duration> },
    #[error("the node failed: {0}")]
    NodeError(String),
    #[error("box {box_id} is not available from any source")]
    BoxUnavailable { box_id: BoxId },
    /// The engine stopped, which only a panic does; the process is exiting.
    #[error(transparent)]
    EngineGone(#[from] EngineGone),
}

impl ApiError {
    /// The error code, shared with the WebSocket's `rejected` reasons.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidAddress(_) => "invalid_address",
            Self::InvalidTokenId => "invalid_token_id",
            Self::InvalidTemplate => "invalid_template",
            Self::InvalidIds => "invalid_ids",
            Self::InvalidBoxId => "invalid_box_id",
            Self::InvalidBroadcast => "invalid_broadcast",
            Self::InvalidCursor(_) => "invalid_cursor",
            Self::InvalidLimit { .. } => "invalid_limit",
            Self::KeyNotAllowed => "key_not_allowed",
            Self::OriginNotAllowed => "origin_not_allowed",
            Self::UnknownToken => "unknown_token",
            Self::BoxNotFound => "box_not_found",
            Self::TransactionRejected { .. } => "transaction_rejected",
            Self::CursorExpired => "cursor_expired",
            Self::SupplyTooLarge { .. } => "supply_too_large",
            Self::TooManyBoxes { .. } => "too_many_boxes",
            Self::UnsupportedTemplate => "unsupported_template",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::NotReady { .. } => "not_ready",
            Self::NodeUnavailable { .. } => "node_unavailable",
            Self::NodeError(_) => "node_error",
            Self::BoxUnavailable { .. } => "box_unavailable",
            Self::EngineGone(_) => "internal_error",
        }
    }
}

impl From<&NodeError> for ApiError {
    fn from(error: &NodeError) -> Self {
        match error {
            NodeError::Unavailable { retry_after } => {
                Self::NodeUnavailable { retry_after: *retry_after }
            }
            other => Self::NodeError(other.to_string()),
        }
    }
}

impl From<&LoadError> for ApiError {
    fn from(error: &LoadError) -> Self {
        match error {
            LoadError::Node(error) => error.into(),
            // A node fault: the indexer returned a box that does not match the key.
            error @ LoadError::Mismatch { .. } => Self::NodeError(error.to_string()),
            LoadError::GaveUp { wait, .. } => Self::NotReady { retry_after: *wait },
            LoadError::TooManyBoxes { max } => Self::TooManyBoxes { max: *max },
            LoadError::UnsupportedTemplate => Self::UnsupportedTemplate,
            LoadError::CapacityExceeded { .. } => Self::CapacityExceeded,
            LoadError::EngineGone(gone) => Self::EngineGone(*gone),
        }
    }
}

impl From<TokenError> for ApiError {
    fn from(error: TokenError) -> Self {
        match error {
            TokenError::SupplyTooLarge { emission, limit } => {
                Self::SupplyTooLarge { emission, limit }
            }
            TokenError::UnknownToken => Self::UnknownToken,
            TokenError::Node(error) => (&error).into(),
        }
    }
}

impl From<AdapterError> for ApiError {
    fn from(error: AdapterError) -> Self {
        match error {
            AdapterError::Node(error) => (&error).into(),
            AdapterError::EngineGone(gone) => Self::EngineGone(gone),
        }
    }
}
