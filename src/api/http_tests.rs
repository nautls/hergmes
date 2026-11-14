//! The routes through the whole router, against `SimChain`: status codes, `Retry-After`, the
//! body shape and CORS. What a page or a balance holds is checked by the
//! services' own properties; here only that the routes reach them and serialize the result.

use std::sync::Mutex;

use axum::{
    body::Body,
    http::{
        Request,
        header::{
            ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
            ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN,
        },
    },
};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::time::Instant;
use tower::ServiceExt;

use super::*;
use crate::{
    adapters::{BoxCache, BoxFetcher, EngineGone, Follower, Loader, TokenChecker},
    api::cursor::CursorError,
    engine::{Config, Engine},
    runtime::{Settled, settling},
    sim::{Chain, SimChain, SimConfig, testing::ready, token_id},
    types::{
        address::{self, Network},
        ergo::ErgoTree,
    },
};

/// Every error, with its status and the `Retry-After` it must carry (DESIGN.md, "Errors").
fn every_error() -> Vec<(ApiError, StatusCode, Option<&'static str>)> {
    use StatusCode as S;
    vec![
        (ApiError::InvalidAddress("bad".into()), S::BAD_REQUEST, None),
        (ApiError::InvalidTokenId, S::BAD_REQUEST, None),
        (ApiError::InvalidCursor(CursorError::Malformed), S::BAD_REQUEST, None),
        (ApiError::InvalidLimit, S::BAD_REQUEST, None),
        (ApiError::UnknownToken, S::NOT_FOUND, None),
        (ApiError::CursorExpired, S::GONE, None),
        (ApiError::SupplyTooLarge { emission: 101, limit: 100 }, S::UNPROCESSABLE_ENTITY, None),
        (ApiError::TooManyBoxes { max: 5 }, S::UNPROCESSABLE_ENTITY, None),
        (ApiError::CapacityExceeded, S::SERVICE_UNAVAILABLE, Some("120")),
        (
            ApiError::NotReady { retry_after: Duration::from_millis(1500) },
            S::SERVICE_UNAVAILABLE,
            Some("2"),
        ),
        (
            ApiError::NodeUnavailable { retry_after: Some(Duration::from_secs(30)) },
            S::SERVICE_UNAVAILABLE,
            Some("30"),
        ),
        (ApiError::NodeUnavailable { retry_after: None }, S::SERVICE_UNAVAILABLE, Some("1")),
        (
            ApiError::BoxUnavailable { box_id: crate::sim::testing::digest(1) },
            S::SERVICE_UNAVAILABLE,
            Some("1"),
        ),
        (ApiError::NodeError("http://10.0.0.1:9053 timed out".into()), S::BAD_GATEWAY, None),
        (ApiError::EngineGone(EngineGone), S::INTERNAL_SERVER_ERROR, None),
    ]
}

async fn body_json(response: Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{e}: {bytes:?}"))
}

#[tokio::test]
async fn every_error_has_its_status_code_and_retry_after() {
    for (error, status, retry_after) in every_error() {
        let code = error.code();
        let response = error.into_response();
        assert_eq!(response.status(), status, "{code}");
        let header = response
            .headers()
            .get(RETRY_AFTER)
            .map(|v| v.to_str().unwrap().to_owned());
        assert_eq!(header.as_deref(), retry_after, "{code}");
        let body = body_json(response).await;
        assert_eq!(body["error"], code);
        let message = body["message"].as_str().expect("a message");
        assert!(!message.is_empty(), "{code}");
        assert!(!message.contains("10.0.0.1"), "{code}: the node's address leaked: {message}");
    }
}

#[test]
fn every_variant_is_in_the_table() {
    let codes: std::collections::BTreeSet<_> =
        every_error().iter().map(|(e, ..)| e.code()).collect();
    assert_eq!(codes.len(), 14, "{codes:?}");
}

#[test]
fn waits_round_up_to_whole_seconds() {
    assert_eq!(seconds(Duration::ZERO), 1);
    assert_eq!(seconds(Duration::from_millis(1)), 1);
    assert_eq!(seconds(Duration::from_secs(1)), 1);
    assert_eq!(seconds(Duration::from_millis(1001)), 2);
}

/// The router over a simulated node and an engine in this process.
struct Fixture {
    sim: SimChain,
    api: Arc<Api<SimChain, Mutex<Engine>>>,
    follower: Follower<SimChain>,
    router: Router,
}

impl Fixture {
    fn new() -> Self {
        Self::with_chain(Chain::new(SimConfig::default()))
    }

    fn with_chain(chain: Chain) -> Self {
        Self::waiting_for(chain, Settled::now())
    }

    /// Loading no key for a client until `settled` is.
    fn waiting_for(chain: Chain, settled: Settled) -> Self {
        let sim = SimChain::new(chain);
        let cache = BoxCache::new(1 << 20);
        let loader = Arc::new(Loader::new(sim.clone(), Default::default()).seeding(cache.clone()));
        let follower = Follower::new(sim.clone(), Default::default());
        let api = Arc::new(Api {
            engine: Mutex::new(Engine::new(Config::default())),
            pages: Pages::new(
                loader,
                Arc::new(TokenChecker::new(sim.clone(), 100)),
                BoxFetcher::new(sim.clone(), cache),
            )
            .waiting_for(settled),
            balances: Balances::new(sim.clone(), Default::default()),
            network: Network::Mainnet,
            synced: follower.synced(),
        });
        let router = router(api.clone());
        Self { sim, api, follower, router }
    }

    /// One follower tick: the engine reaches the node's tip.
    fn tick(&mut self) {
        ready(self.follower.tick(&self.api.engine, Instant::now())).unwrap();
    }

    fn tree(&self, index: usize) -> ErgoTree {
        self.sim.lock().trees()[index].clone()
    }

    fn address(&self, index: usize) -> String {
        address::encode(&self.tree(index).0, Network::Mainnet)
    }

    async fn send(&self, request: Request<Body>) -> Response {
        self.router.clone().oneshot(request).await.unwrap()
    }

    async fn get(&self, uri: &str) -> Response {
        self.send(Request::get(uri).body(Body::empty()).unwrap())
            .await
    }

    /// The JSON body of a `200`.
    async fn ok(&self, uri: &str) -> Value {
        let response = self.get(uri).await;
        let status = response.status();
        let body = body_json(response).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        body
    }

    /// The code of an error, after checking its status.
    async fn error(&self, uri: &str, status: StatusCode) -> String {
        let response = self.get(uri).await;
        assert_eq!(response.status(), status, "{uri}");
        body_json(response).await["error"]
            .as_str()
            .unwrap()
            .to_owned()
    }
}

#[tokio::test]
async fn status_is_ready_once_the_engine_is_at_the_nodes_tip() {
    let mut f = Fixture::new();
    let status = f.ok("/v1/status").await;
    assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(status["network"], "mainnet");
    assert_eq!(status["tip"], Value::Null);
    assert_eq!(status["ready"], false);

    f.tick();
    let tip = f.sim.lock().tip();
    let status = f.ok("/v1/status").await;
    assert_eq!(status["tip"]["height"], tip.height);
    assert_eq!(status["tip"]["headerId"], tip.header_id.to_string());
    assert_eq!(status["ready"], true);
    let calls = f.sim.calls().len();
    f.ok("/v1/status").await;
    assert_eq!(f.sim.calls().len(), calls, "no node call");

    // The node moves on and the indexer lags: the engine keeps its tip but is behind.
    f.sim.lock().set_indexer_lag(1);
    f.sim.lock().mine(0, 1, 1);
    f.tick();
    let status = f.ok("/v1/status").await;
    assert_eq!(status["tip"]["height"], tip.height);
    assert_eq!(status["ready"], false, "the indexer lags, so the engine is behind");
}

/// Until the configured keys settle, a key not tracked can't be loaded, so the relay is not
/// ready and a first page is `not_ready`.
#[tokio::test]
async fn status_is_not_ready_until_the_configured_keys_settle() {
    let (settled, pending) = settling(1);
    let mut f = Fixture::waiting_for(Chain::new(SimConfig::default()), settled);
    f.tick();
    assert_eq!(f.ok("/v1/status").await["ready"], false);
    let uri = format!("/v1/addresses/{}/boxes", f.address(0));
    assert_eq!(f.error(&uri, StatusCode::SERVICE_UNAVAILABLE).await, "not_ready");

    drop(pending);
    assert_eq!(f.ok("/v1/status").await["ready"], true);
    f.ok(&uri).await;
}

#[tokio::test]
async fn an_address_snapshot_pages_through_every_box() {
    let mut f = Fixture::new();
    f.tick();
    let address = f.address(0);
    let first = f
        .ok(&format!("/v1/addresses/{address}/boxes?limit=1&unknown=x"))
        .await;
    let as_of = first["asOf"].clone();
    let mut ids = vec![first["items"][0]["boxId"].clone()];
    let mut cursor = first["nextCursor"].as_str().expect("more pages").to_owned();
    loop {
        let page = f
            .ok(&format!("/v1/addresses/{address}/boxes?cursor={cursor}&limit=1"))
            .await;
        assert_eq!(page["asOf"], as_of);
        ids.extend(
            page["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| b["boxId"].clone()),
        );
        match page["nextCursor"].as_str() {
            Some(next) => cursor = next.to_owned(),
            None => break,
        }
    }
    let tree = f.tree(0);
    let expected = f
        .sim
        .lock()
        .utxo()
        .values()
        .filter(|b| b.ergo_tree == tree)
        .count();
    assert_eq!(ids.len(), expected);
    ids.dedup();
    assert_eq!(ids.len(), expected, "no box twice");
}

#[tokio::test]
async fn a_page_serializes_boxes_in_the_wire_shape() {
    let mut f = Fixture::new();
    f.tick();
    let page = f.ok(&format!("/v1/addresses/{}/boxes", f.address(0))).await;
    assert_eq!(page["nextCursor"], Value::Null);
    let item = &page["items"][0];
    for field in [
        "boxId",
        "transactionId",
        "index",
        "ergoTree",
        "creationHeight",
        "value",
        "assets",
        "additionalRegisters",
        "inclusionHeight",
    ] {
        assert!(!item[field].is_null(), "{field} in {item}");
    }
    assert!(item["value"].is_string(), "{item}");
    assert_eq!(item["ergoTree"], f.tree(0).to_string());
}

#[tokio::test]
async fn a_token_snapshot_is_served_and_checked_first() {
    let mut f = Fixture::new();
    f.tick();
    let page = f
        .ok(&format!("/v1/tokens/{}/boxes", token_id("small-100")))
        .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 4);

    let upper = token_id("small-100").to_string().to_uppercase();
    f.ok(&format!("/v1/tokens/{upper}/boxes")).await;

    let over = format!("/v1/tokens/{}/boxes", token_id("over-101"));
    assert_eq!(f.error(&over, StatusCode::UNPROCESSABLE_ENTITY).await, "supply_too_large");
    let unknown = format!("/v1/tokens/{}/boxes", token_id("nope"));
    assert_eq!(f.error(&unknown, StatusCode::NOT_FOUND).await, "unknown_token");
}

#[tokio::test(start_paused = true)]
async fn balances_are_served_for_untracked_and_tracked_addresses() {
    let mut f = Fixture::new();
    f.tick();
    let tip = f.sim.lock().tip();
    let uri = format!("/v1/addresses/{}/balance", f.address(1));
    let untracked = f.ok(&uri).await;
    assert_eq!(untracked["asOf"]["height"], tip.height);
    assert!(untracked["confirmed"]["nanoErgs"].is_string(), "{untracked}");
    assert_eq!(untracked["pending"]["nanoErgs"], "0");

    // A page loads the address; its balance then comes from the engine.
    f.ok(&format!("/v1/addresses/{}/boxes", f.address(1))).await;
    let calls = f.sim.calls().len();
    let tracked = f.ok(&uri).await;
    assert_eq!(f.sim.calls().len(), calls, "no node call");
    assert_eq!(tracked, untracked, "both paths agree");
}

#[tokio::test(start_paused = true)]
async fn an_engine_with_no_tip_answers_503_with_retry_after() {
    let f = Fixture::new();
    let response = f
        .get(&format!("/v1/addresses/{}/balance", f.address(0)))
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[RETRY_AFTER], "1");
    assert_eq!(body_json(response).await["error"], "not_ready");
}

#[tokio::test]
async fn bad_requests_are_refused_with_their_code() {
    let mut f = Fixture::new();
    f.tick();
    let address = f.address(0);
    let boxes = format!("/v1/addresses/{address}/boxes");
    let testnet = address::encode(&f.tree(0).0, Network::Testnet);
    let token = token_id("small-100");
    let other_cursor = {
        let page = f
            .ok(&format!("/v1/addresses/{}/boxes?limit=1", f.address(1)))
            .await;
        page["nextCursor"].as_str().unwrap().to_owned()
    };
    for (uri, code) in [
        ("/v1/addresses/notanaddress/boxes".to_owned(), "invalid_address"),
        (format!("/v1/addresses/{testnet}/boxes"), "invalid_address"),
        (format!("/v1/addresses/{testnet}/balance"), "invalid_address"),
        ("/v1/addresses/%FF/balance".to_owned(), "invalid_address"),
        ("/v1/tokens/abcd/boxes".to_owned(), "invalid_token_id"),
        (format!("/v1/tokens/{token}0/boxes"), "invalid_token_id"),
        (format!("{boxes}?limit=0"), "invalid_limit"),
        (format!("{boxes}?limit=1001"), "invalid_limit"),
        (format!("{boxes}?limit=-1"), "invalid_limit"),
        (format!("{boxes}?limit=ten"), "invalid_limit"),
        (format!("{boxes}?limit=99999999999"), "invalid_limit"),
        (format!("{boxes}?limit="), "invalid_limit"),
        (format!("{boxes}?cursor=xyz"), "invalid_cursor"),
        (format!("{boxes}?cursor={other_cursor}"), "invalid_cursor"),
    ] {
        assert_eq!(f.error(&uri, StatusCode::BAD_REQUEST).await, code, "{uri}");
    }
    // The first value of a repeated name counts.
    f.ok(&format!("{boxes}?limit=1&limit=ten")).await;
    f.ok(&format!("{boxes}?limit=1000")).await;
}

#[tokio::test]
async fn unknown_routes_and_methods_are_plain_http_errors() {
    let f = Fixture::new();
    assert_eq!(f.get("/v1/nothing").await.status(), StatusCode::NOT_FOUND);
    assert_eq!(f.get("/v1/status/").await.status(), StatusCode::NOT_FOUND, "no trailing slash");
    assert_eq!(f.get("/v1/ws").await.status(), StatusCode::NOT_FOUND, "step 9");
    let post = Request::post("/v1/status").body(Body::empty()).unwrap();
    assert_eq!(f.send(post).await.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn any_origin_may_read_responses_and_errors() {
    let f = Fixture::new();
    let request = |uri: &str| {
        Request::get(uri)
            .header(ORIGIN, "https://wallet.example")
            .body(Body::empty())
            .unwrap()
    };
    let ok = f.send(request("/v1/status")).await;
    assert_eq!(ok.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    let error = f.send(request("/v1/tokens/abcd/boxes")).await;
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    let exposed = error.headers()[ACCESS_CONTROL_EXPOSE_HEADERS]
        .to_str()
        .unwrap();
    assert!(exposed.contains("retry-after"), "{exposed}");

    let preflight = Request::options("/v1/status")
        .header(ORIGIN, "https://wallet.example")
        .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .body(Body::empty())
        .unwrap();
    let preflight = f.send(preflight).await;
    assert_eq!(preflight.status(), StatusCode::OK);
    assert_eq!(preflight.headers()[ACCESS_CONTROL_ALLOW_METHODS], "GET");
}
