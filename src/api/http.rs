//! The REST routes: parse, call the page and balance services, serialize.
//!
//! Errors are `{ "error": "<code>", "message": "..." }`, and every 503 carries `Retry-After`.
//! Any origin may read the responses. They are not compressed: that is the reverse proxy's
//! job, like TLS (DESIGN.md, "Serving").

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::PathRejection},
    http::{HeaderValue, Method, StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
    routing::get,
};
use tokio::sync::watch;
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, warn};

use crate::{
    adapters::{EngineHandle, engine_tip},
    api::{
        ApiError, Balances, KeyPolicy, Pages,
        keys::{address_key, token_key},
        wire::{BalanceBody, ErrorBody, PageBody, StatusBody, WithPoolBody},
    },
    clients::NodeSource,
    engine::TrackingKey,
    types::address::Network,
};

/// `Retry-After` for `capacity_exceeded`: about one block, which is when idle keys leave.
pub const CAPACITY_RETRY: Duration = Duration::from_secs(120);
/// `Retry-After` for a `503` that carries no wait of its own: a node `503` without the
/// header, or a box no source had. About one follower tick, after which the engine has
/// caught up with the node or the indexer has recovered.
pub const DEFAULT_RETRY: Duration = Duration::from_secs(1);

/// What the routes serve from.
pub struct Api<S, E> {
    pub engine: E,
    pub pages: Pages<S>,
    pub balances: Balances<S>,
    /// The node's network: addresses of another one are refused.
    pub network: Network,
    /// The keys clients may name.
    pub policy: KeyPolicy,
    /// Whether the follower's last tick left the engine at the node's tip.
    pub synced: watch::Receiver<bool>,
}

/// The routes, with CORS. Errors carry its headers too, so a browser client can read an
/// error's code and `Retry-After`.
pub fn router<S, E>(api: Arc<Api<S, E>>) -> Router
where
    S: NodeSource + 'static,
    E: EngineHandle + 'static,
{
    // A public, read-only API with no credentials, used by wallets and dapps on origins no
    // one knows in advance. `Retry-After` is exposed so a browser client can honour it.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET])
        .expose_headers([RETRY_AFTER]);
    Router::new()
        .route("/v1/addresses/{address}/boxes", get(address_boxes::<S, E>))
        .route("/v1/tokens/{token_id}/boxes", get(token_boxes::<S, E>))
        .route("/v1/addresses/{address}/boxes/with-pool", get(address_with_pool::<S, E>))
        .route("/v1/tokens/{token_id}/boxes/with-pool", get(token_with_pool::<S, E>))
        .route("/v1/addresses/{address}/balance", get(address_balance::<S, E>))
        .route("/v1/status", get(status::<S, E>))
        .with_state(api)
        .layer(cors)
}

/// The query of a page request: `cursor` and `limit`. Read as pairs, so a value that is not
/// a number, or a repeated name, is the relay's error rather than the extractor's; the
/// first value of a name counts, and unknown names are ignored.
type Params = Query<Vec<(String, String)>>;

fn param<'a>(params: &'a Params, name: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

fn limit(params: &Params) -> Result<Option<u32>, ApiError> {
    param(params, "limit")
        .map(|v| v.parse().map_err(|_| ApiError::InvalidLimit))
        .transpose()
}

async fn address_boxes<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    address: Result<Path<String>, PathRejection>,
    params: Params,
) -> Result<Response, ApiError> {
    let address = address.map_err(|e| ApiError::InvalidAddress(e.body_text()))?;
    let key = address_key(&address, api.network, &api.policy)?;
    page(&api, &key, &params).await
}

async fn token_boxes<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    token_id: Result<Path<String>, PathRejection>,
    params: Params,
) -> Result<Response, ApiError> {
    let token_id = token_id.map_err(|_| ApiError::InvalidTokenId)?;
    let key = token_key(&token_id, &api.policy)?;
    page(&api, &key, &params).await
}

async fn page<S: NodeSource, E: EngineHandle>(
    api: &Api<S, E>,
    key: &TrackingKey,
    params: &Params,
) -> Result<Response, ApiError> {
    let limit = limit(params)?;
    let page = api
        .pages
        .page(&api.engine, key, param(params, "cursor"), limit)
        .await?;
    Ok(Json(PageBody(&page)).into_response())
}

async fn address_with_pool<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    address: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let address = address.map_err(|e| ApiError::InvalidAddress(e.body_text()))?;
    let key = address_key(&address, api.network, &api.policy)?;
    with_pool(&api, &key).await
}

async fn token_with_pool<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    token_id: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let token_id = token_id.map_err(|_| ApiError::InvalidTokenId)?;
    let key = token_key(&token_id, &api.policy)?;
    with_pool(&api, &key).await
}

async fn with_pool<S: NodeSource, E: EngineHandle>(
    api: &Api<S, E>,
    key: &TrackingKey,
) -> Result<Response, ApiError> {
    let view = api.pages.with_pool(&api.engine, key).await?;
    Ok(Json(WithPoolBody(&view)).into_response())
}

async fn address_balance<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    address: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let address = address.map_err(|e| ApiError::InvalidAddress(e.body_text()))?;
    let TrackingKey::Tree(tree) = address_key(&address, api.network, &api.policy)? else {
        unreachable!("an address's key is its tree")
    };
    let balance = api.balances.balance(&api.engine, &tree).await?;
    Ok(Json(BalanceBody(&balance)).into_response())
}

/// Cheap enough for health checks: one engine query and no node call. `ready` is the
/// engine having a tip and being at the node's, and the configured keys having settled, so
/// that a key not yet tracked can be loaded.
async fn status<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
) -> Result<Response, ApiError> {
    let tip = engine_tip(&api.engine).await?;
    let ready = tip.is_some() && *api.synced.borrow() && api.pages.loads();
    Ok(Json(StatusBody { network: api.network, tip: tip.as_ref(), ready }).into_response())
}

impl ApiError {
    /// The HTTP status of the code (DESIGN.md, "Errors").
    pub fn status(&self) -> StatusCode {
        match self {
            Self::InvalidAddress(_)
            | Self::InvalidTokenId
            | Self::InvalidCursor(_)
            | Self::InvalidLimit => StatusCode::BAD_REQUEST,
            Self::KeyNotAllowed => StatusCode::FORBIDDEN,
            Self::UnknownToken => StatusCode::NOT_FOUND,
            Self::CursorExpired => StatusCode::GONE,
            Self::SupplyTooLarge { .. } | Self::TooManyBoxes { .. } => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::CapacityExceeded
            | Self::NotReady { .. }
            | Self::NodeUnavailable { .. }
            | Self::BoxUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::NodeError(_) => StatusCode::BAD_GATEWAY,
            Self::EngineGone(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// How long a client should wait before trying again: set on every `503`, and only
    /// there.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::CapacityExceeded => Some(CAPACITY_RETRY),
            Self::NotReady { retry_after } => Some(*retry_after),
            Self::NodeUnavailable { retry_after } => Some(retry_after.unwrap_or(DEFAULT_RETRY)),
            Self::BoxUnavailable { .. } => Some(DEFAULT_RETRY),
            Self::InvalidAddress(_)
            | Self::InvalidTokenId
            | Self::InvalidCursor(_)
            | Self::InvalidLimit
            | Self::KeyNotAllowed
            | Self::UnknownToken
            | Self::CursorExpired
            | Self::SupplyTooLarge { .. }
            | Self::TooManyBoxes { .. }
            | Self::NodeError(_)
            | Self::EngineGone(_) => None,
        }
    }

    /// The message for people. A node failure's details stay in the log: they can name the
    /// node's address, which is no client's business.
    fn public_message(&self) -> String {
        match self {
            Self::NodeError(_) => "the node failed".to_owned(),
            Self::EngineGone(_) => "internal error".to_owned(),
            other => other.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        if status.is_server_error() && status != StatusCode::SERVICE_UNAVAILABLE {
            warn!(code = self.code(), error = %self, "Request failed");
        } else {
            debug!(code = self.code(), error = %self, "Request refused");
        }
        let message = self.public_message();
        let body = ErrorBody { error: self.code(), message: &message };
        let mut response = (status, Json(body)).into_response();
        if let Some(wait) = self.retry_after() {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from(seconds(wait)));
        }
        response
    }
}

/// Whole seconds, rounded up, and at least 1: a wait under a second must not read as "now".
pub(crate) fn seconds(wait: Duration) -> u64 {
    let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
    secs.max(1)
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
