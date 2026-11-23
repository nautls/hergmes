//! The REST routes: parse, call the page, balance, history, header, box, token and
//! transaction services, serialize.
//!
//! Errors are `{ "error": "<code>", "message": "..." }`, with a `reason` for a refused tx,
//! and every 503 carries `Retry-After`.
//! Any origin the origin policy admits may read the responses. They are not compressed:
//! that is the reverse proxy's job, like TLS.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State,
        rejection::{PathRejection, StringRejection},
    },
    http::{
        HeaderValue, Method, StatusCode,
        header::{CONTENT_TYPE, RETRY_AFTER},
    },
    middleware::map_response_with_state,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tokio::sync::watch;
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, warn};

use crate::{
    adapters::{EngineHandle, engine_tip},
    api::{
        ApiError, Balances, Boxes, Headers, History, KeyPolicy, Metrics, Pages, Tokens,
        Transactions, headers, history,
        keys::{address_key, template_key, token_key},
        metrics::Transport,
        pages,
        transactions::{self, MAX_HEX_CHARS},
        wire::{
            BalanceBody, BoxBody, ErrorBody, HeadersBody, HistoryBody, PageBody, StatusBody,
            SubmittedBody, TokensBody, WithPoolBody,
        },
    },
    clients::NodeSource,
    engine::TrackingKey,
    types::{address::Network, ergo::BoxId},
};

/// The most a tx body is read to: the longest tx, and room for whitespace around it, which
/// the service leaves out. A longer body is refused as `too_large` unread.
const MAX_TX_BODY: usize = MAX_HEX_CHARS + 1024;

/// `Retry-After` for `capacity_exceeded`. Room frees only as keys go idle past the engine's
/// `evict_after` (a minute by default), so a sooner retry finds none; twice that leaves time
/// for more keys to go idle.
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
    pub history: History<S>,
    pub headers: Headers<S>,
    pub boxes: Boxes<S>,
    pub tokens: Tokens<S>,
    pub transactions: Transactions<S>,
    /// The node's network: addresses of another one are refused.
    pub network: Network,
    /// The keys clients may name.
    pub policy: KeyPolicy,
    /// Whether the follower's last tick left the engine at the node's tip.
    pub synced: watch::Receiver<bool>,
    /// Counts the refusals sent to clients.
    pub metrics: Metrics,
}

/// The routes, with CORS. Errors carry its headers too, so a browser client can read an
/// error's code and `Retry-After`.
pub fn router<S, E>(api: Arc<Api<S, E>>) -> Router
where
    S: NodeSource + 'static,
    E: EngineHandle + 'static,
{
    // A public API with no credentials, used by wallets and dapps on origins no one knows in
    // advance; an operator narrows them with the origin policy, which runs ahead of CORS
    // (`origins::guard`). It reads, and sends txs the node validates. `Content-Type` is allowed
    // so a tx sent with a type other than `text/plain` passes the preflight. `Retry-After` is
    // exposed so a browser client can honour it.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([CONTENT_TYPE])
        .expose_headers([RETRY_AFTER]);
    let metrics = api.metrics.clone();
    Router::new()
        .route("/v1/addresses/{address}/boxes", get(address_boxes::<S, E>))
        .route("/v1/tokens/{token_id}/boxes", get(token_boxes::<S, E>))
        .route("/v1/addresses/{address}/boxes/with-pool", get(address_with_pool::<S, E>))
        .route("/v1/tokens/{token_id}/boxes/with-pool", get(token_with_pool::<S, E>))
        .route("/v1/templates/{template}/boxes", get(template_boxes::<S, E>))
        .route("/v1/templates/{template}/boxes/with-pool", get(template_with_pool::<S, E>))
        .route("/v1/addresses/{address}/balance", get(address_balance::<S, E>))
        .route("/v1/addresses/{address}/transactions", get(address_history::<S, E>))
        .route("/v1/headers", get(last_headers::<S, E>))
        .route("/v1/boxes/{box_id}", get(box_by_id::<S, E>))
        .route("/v1/tokens/metadata", get(token_metadata::<S, E>))
        .route(
            "/v1/transactions",
            post(submit_transaction::<S, E>).layer(DefaultBodyLimit::max(MAX_TX_BODY)),
        )
        .route("/v1/status", get(status::<S, E>))
        .with_state(api)
        .layer(map_response_with_state(metrics, count_refusal))
        .layer(cors)
}

/// An error response's code, left on it for [`count_refusal`]: `IntoResponse` has no state
/// to count with.
#[derive(Debug, Clone, Copy)]
struct Refused(&'static str);

async fn count_refusal(State(metrics): State<Metrics>, response: Response) -> Response {
    if let Some(Refused(code)) = response.extensions().get() {
        metrics.rejected(Transport::Rest, code);
    }
    response
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

/// The `limit` asked for, if any. One that is not a number is refused with the route's
/// `max`; the service checks the range.
fn limit(params: &Params, max: u32) -> Result<Option<u32>, ApiError> {
    param(params, "limit")
        .map(|v| v.parse().map_err(|_| ApiError::InvalidLimit { max }))
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

async fn template_boxes<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    template: Result<Path<String>, PathRejection>,
    params: Params,
) -> Result<Response, ApiError> {
    let template = template.map_err(|_| ApiError::InvalidTemplate)?;
    let key = template_key(&template, &api.policy)?;
    page(&api, &key, &params).await
}

async fn page<S: NodeSource, E: EngineHandle>(
    api: &Api<S, E>,
    key: &TrackingKey,
    params: &Params,
) -> Result<Response, ApiError> {
    let limit = limit(params, pages::MAX_LIMIT)?;
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

async fn template_with_pool<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    template: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let template = template.map_err(|_| ApiError::InvalidTemplate)?;
    let key = template_key(&template, &api.policy)?;
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

async fn address_history<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    address: Result<Path<String>, PathRejection>,
    params: Params,
) -> Result<Response, ApiError> {
    let address = address.map_err(|e| ApiError::InvalidAddress(e.body_text()))?;
    let TrackingKey::Tree(tree) = address_key(&address, api.network, &api.policy)? else {
        unreachable!("an address's key is its tree")
    };
    let limit = limit(&params, history::MAX_LIMIT)?;
    let page = api
        .history
        .page(&api.engine, &tree, param(&params, "cursor"), limit)
        .await?;
    Ok(Json(HistoryBody(&page)).into_response())
}

async fn last_headers<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    params: Params,
) -> Result<Response, ApiError> {
    let limit = limit(&params, headers::MAX_LIMIT)?;
    let page = api.headers.last(&api.engine, limit).await?;
    Ok(Json(HeadersBody(&page)).into_response())
}

/// Outside the key policy: a box id is not a key (see [`Boxes`]).
async fn box_by_id<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    box_id: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let box_id = box_id.map_err(|_| ApiError::InvalidBoxId)?;
    let id: BoxId = box_id.parse().map_err(|_| ApiError::InvalidBoxId)?;
    let found = api.boxes.by_id(&api.engine, &id).await?;
    Ok(Json(BoxBody(&found)).into_response())
}

/// Outside the key policy: a token id here is not a key (see [`Tokens`]).
async fn token_metadata<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    params: Params,
) -> Result<Response, ApiError> {
    let tokens = api.tokens.metadata(param(&params, "ids")).await?;
    Ok(Json(TokensBody(&tokens)).into_response())
}

/// The body is the tx's bytes in hex, best as `text/plain`, so a browser sends it without a
/// CORS preflight; any content type is read. `?broadcast=false` only validates it. A body
/// over the limit, or not UTF-8, is refused in the service's terms, never the framework's.
async fn submit_transaction<S: NodeSource, E: EngineHandle>(
    State(api): State<Arc<Api<S, E>>>,
    params: Params,
    body: Result<String, StringRejection>,
) -> Result<Response, ApiError> {
    let broadcast = match param(&params, "broadcast") {
        None | Some("true") => true,
        Some("false") => false,
        Some(_) => return Err(ApiError::InvalidBroadcast),
    };
    let body = body.map_err(|rejection| match rejection.status() {
        StatusCode::PAYLOAD_TOO_LARGE => transactions::too_large(),
        _ => transactions::unparseable(),
    })?;
    let id = api.transactions.submit(&body, broadcast).await?;
    Ok(Json(SubmittedBody { transaction_id: &id }).into_response())
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
    /// The HTTP status of the code.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::InvalidAddress(_)
            | Self::InvalidTokenId
            | Self::InvalidTemplate
            | Self::InvalidIds
            | Self::InvalidBoxId
            | Self::InvalidBroadcast
            | Self::InvalidCursor(_)
            | Self::InvalidLimit { .. }
            | Self::TransactionRejected { .. } => StatusCode::BAD_REQUEST,
            Self::KeyNotAllowed | Self::OriginNotAllowed => StatusCode::FORBIDDEN,
            Self::UnknownToken | Self::BoxNotFound => StatusCode::NOT_FOUND,
            Self::CursorExpired => StatusCode::GONE,
            Self::SupplyTooLarge { .. } | Self::TooManyBoxes { .. } | Self::UnsupportedTemplate => {
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
            | Self::InvalidTemplate
            | Self::InvalidIds
            | Self::InvalidBoxId
            | Self::InvalidBroadcast
            | Self::InvalidCursor(_)
            | Self::InvalidLimit { .. }
            | Self::TransactionRejected { .. }
            | Self::KeyNotAllowed
            | Self::OriginNotAllowed
            | Self::UnknownToken
            | Self::BoxNotFound
            | Self::CursorExpired
            | Self::SupplyTooLarge { .. }
            | Self::TooManyBoxes { .. }
            | Self::UnsupportedTemplate
            | Self::NodeError(_)
            | Self::EngineGone(_) => None,
        }
    }

    /// The message for people. A node failure's details stay in the log: they can name the
    /// node's address, which is no client's business. A refused tx's message is the node's
    /// own text about the client's tx, so it is passed on.
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
        let reason = match &self {
            Self::TransactionRejected { reason, .. } => Some(reason.code()),
            _ => None,
        };
        let body = ErrorBody { error: self.code(), reason, message: &message };
        let mut response = (status, Json(body)).into_response();
        response.extensions_mut().insert(Refused(self.code()));
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
