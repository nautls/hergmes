//! The routes through the whole router, against `SimChain`: status codes, `Retry-After`, the
//! body shape and CORS. What a page or a balance holds is checked by the
//! services' own properties; here only that the routes reach them and serialize the result.

use std::sync::Mutex;

use axum::{
    body::Body,
    http::{
        Request,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
            ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, CONTENT_TYPE, ORIGIN,
        },
    },
};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::time::Instant;
use tower::ServiceExt;

use super::*;
use crate::{
    adapters::{BoxCache, BoxFetcher, EngineGone, Follower, HeaderReader, Loader, TokenChecker},
    api::{
        Refusal,
        cursor::{CursorError, TxCursor},
    },
    engine::{Config, Engine},
    runtime::{Settled, settling},
    sim::{self, Chain, SimChain, SimConfig, testing::ready, token_id},
    types::{
        Digest,
        address::{self, Network},
        ergo::ErgoTree,
    },
};

/// Every error, with its status and the `Retry-After` it must carry.
fn every_error() -> Vec<(ApiError, StatusCode, Option<&'static str>)> {
    use StatusCode as S;
    vec![
        (ApiError::InvalidAddress("bad".into()), S::BAD_REQUEST, None),
        (ApiError::InvalidTokenId, S::BAD_REQUEST, None),
        (ApiError::InvalidTemplate, S::BAD_REQUEST, None),
        (ApiError::InvalidIds, S::BAD_REQUEST, None),
        (ApiError::InvalidBoxId, S::BAD_REQUEST, None),
        (ApiError::InvalidBroadcast, S::BAD_REQUEST, None),
        (
            ApiError::TransactionRejected {
                reason: Refusal::FeeTooLow,
                message: "Min fee not met: 0.001 ergs required, 1.0E-4 ergs given".into(),
            },
            S::BAD_REQUEST,
            None,
        ),
        (ApiError::InvalidCursor(CursorError::Malformed), S::BAD_REQUEST, None),
        (ApiError::InvalidLimit { max: 1000 }, S::BAD_REQUEST, None),
        (ApiError::KeyNotAllowed, S::FORBIDDEN, None),
        (ApiError::UnknownToken, S::NOT_FOUND, None),
        (ApiError::BoxNotFound, S::NOT_FOUND, None),
        (ApiError::CursorExpired, S::GONE, None),
        (ApiError::SupplyTooLarge { emission: 101, limit: 100 }, S::UNPROCESSABLE_ENTITY, None),
        (ApiError::TooManyBoxes { max: 5 }, S::UNPROCESSABLE_ENTITY, None),
        (ApiError::UnsupportedTemplate, S::UNPROCESSABLE_ENTITY, None),
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
        // Only a refused tx has a reason, and its message is the node's.
        if code == "transaction_rejected" {
            assert_eq!(body["reason"], "fee_too_low");
            assert!(message.starts_with("Min fee not met"), "{message}");
        } else {
            assert!(body.get("reason").is_none(), "{code}: {body}");
        }
    }
}

#[test]
fn every_variant_is_in_the_table() {
    let codes: std::collections::BTreeSet<_> =
        every_error().iter().map(|(e, ..)| e.code()).collect();
    assert_eq!(codes.len(), 22, "{codes:?}");
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
        Self::build(chain, settled, KeyPolicy::default())
    }

    /// Admitting only the keys `policy` admits.
    fn with_policy(chain: Chain, policy: KeyPolicy) -> Self {
        Self::build(chain, Settled::now(), policy)
    }

    fn build(chain: Chain, settled: Settled, policy: KeyPolicy) -> Self {
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
            history: History::new(sim.clone(), Default::default()),
            headers: Headers::new(HeaderReader::new(sim.clone(), follower.headers())),
            boxes: Boxes::new(sim.clone()),
            tokens: Tokens::new(sim.clone()),
            transactions: Transactions::new(sim.clone()),
            network: Network::Mainnet,
            policy,
            synced: follower.synced(),
            metrics: Default::default(),
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

/// A template's snapshot holds the boxes of every tree of the template; a template of trees
/// without segregated constants is refused, and so is its view with the pool.
#[tokio::test]
async fn a_template_snapshot_is_served_across_its_trees() {
    let mut f = Fixture::new();
    f.tick();
    let template = sim::script_template();
    let page = f.ok(&format!("/v1/templates/{template}/boxes")).await;
    let trees: std::collections::BTreeSet<&str> = (page["items"].as_array().unwrap().iter())
        .map(|b| b["ergoTree"].as_str().unwrap())
        .collect();
    let scripts = f.sim.lock().trees().len() - 2;
    let expected = [f.tree(scripts).to_string(), f.tree(scripts + 1).to_string()];
    assert_eq!(trees, expected.iter().map(String::as_str).collect());
    let upper = template.to_string().to_uppercase();
    f.ok(&format!("/v1/templates/{upper}/boxes/with-pool"))
        .await;

    // The node's template of a tree without segregated constants: its bytes after the header.
    let p2pk = hex::encode(&f.tree(0).0[1..]);
    for uri in
        [format!("/v1/templates/{p2pk}/boxes"), format!("/v1/templates/{p2pk}/boxes/with-pool")]
    {
        let code = f.error(&uri, StatusCode::UNPROCESSABLE_ENTITY).await;
        assert_eq!(code, "unsupported_template", "{uri}");
    }
}

/// The view with the pool is one list with no cursor: confirmed boxes with `inclusionHeight`,
/// then pending ones without, for addresses and tokens alike.
#[tokio::test]
async fn boxes_with_pool_list_pending_boxes_after_confirmed_ones() {
    let mut f = Fixture::new();
    f.tick();
    let (spent, pending) = {
        let mut chain = f.sim.lock();
        let spent = crate::sim::testing::box_of(&chain, 0);
        let to = chain.trees()[0].clone();
        let tx = chain.transfer(&[spent], &to);
        chain.submit(tx.clone());
        (spent, tx.outputs[0].id)
    };
    f.tick();
    let view = f
        .ok(&format!("/v1/addresses/{}/boxes/with-pool?limit=1", f.address(0)))
        .await;
    let tip = f.sim.lock().tip();
    // The height only: no `headerId`, so it can't be passed as a WebSocket `from`.
    assert_eq!(view["asOf"], serde_json::json!({ "height": tip.height }));
    assert!(view.get("nextCursor").is_none(), "{view}");
    assert_eq!(view["truncated"], false);
    assert_eq!(view["total"], view["items"].as_array().unwrap().len());
    let items = view["items"].as_array().unwrap();
    let ids: Vec<&str> = items.iter().map(|b| b["boxId"].as_str().unwrap()).collect();
    assert!(!ids.contains(&spent.to_string().as_str()), "the spent box is left out");
    let (last, confirmed) = items.split_last().unwrap();
    assert_eq!(last["boxId"], pending.to_string());
    assert!(last.get("inclusionHeight").is_none(), "{last}");
    assert!(confirmed.iter().all(|b| b["inclusionHeight"].is_u64()));
    let tree = f.tree(0);
    let held = f
        .sim
        .lock()
        .utxo()
        .values()
        .filter(|b| b.ergo_tree == tree)
        .count();
    assert_eq!(confirmed.len(), held - 1);

    let token = f
        .ok(&format!("/v1/tokens/{}/boxes/with-pool", token_id("small-100")))
        .await;
    assert_eq!(token["items"].as_array().unwrap().len(), 4);
    let over = format!("/v1/tokens/{}/boxes/with-pool", token_id("over-101"));
    assert_eq!(f.error(&over, StatusCode::UNPROCESSABLE_ENTITY).await, "supply_too_large");
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
    let history = format!("/v1/addresses/{address}/transactions");
    let testnet = address::encode(&f.tree(0).0, Network::Testnet);
    let other_history = TxCursor { anchor: Digest([1; 32]), pos: 0, total: 2 }
        .encode(&TrackingKey::Tree(f.tree(1)));
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
        ("/v1/addresses/notanaddress/boxes/with-pool".to_owned(), "invalid_address"),
        (format!("/v1/addresses/{testnet}/boxes/with-pool"), "invalid_address"),
        ("/v1/tokens/abcd/boxes/with-pool".to_owned(), "invalid_token_id"),
        (format!("/v1/tokens/{token}0/boxes"), "invalid_token_id"),
        ("/v1/templates/abc/boxes".to_owned(), "invalid_template"),
        ("/v1/templates/abc/boxes/with-pool".to_owned(), "invalid_template"),
        (format!("{boxes}?limit=0"), "invalid_limit"),
        (format!("{boxes}?limit=1001"), "invalid_limit"),
        (format!("{boxes}?limit=-1"), "invalid_limit"),
        (format!("{boxes}?limit=ten"), "invalid_limit"),
        (format!("{boxes}?limit=99999999999"), "invalid_limit"),
        (format!("{boxes}?limit="), "invalid_limit"),
        (format!("{boxes}?cursor=xyz"), "invalid_cursor"),
        (format!("{boxes}?cursor={other_cursor}"), "invalid_cursor"),
        ("/v1/addresses/notanaddress/transactions".to_owned(), "invalid_address"),
        (format!("/v1/addresses/{testnet}/transactions"), "invalid_address"),
        (format!("{history}?limit=0"), "invalid_limit"),
        (format!("{history}?limit=101"), "invalid_limit"),
        (format!("{history}?limit=ten"), "invalid_limit"),
        (format!("{history}?cursor=xyz"), "invalid_cursor"),
        (format!("{history}?cursor={other_cursor}"), "invalid_cursor"),
        (format!("{history}?cursor={other_history}"), "invalid_cursor"),
    ] {
        assert_eq!(f.error(&uri, StatusCode::BAD_REQUEST).await, code, "{uri}");
    }
    // The first value of a repeated name counts.
    f.ok(&format!("{boxes}?limit=1&limit=ten")).await;
    f.ok(&format!("{boxes}?limit=1000")).await;
}

/// Every route that takes a key goes through the policy, and a refused key costs no node
/// call.
#[tokio::test(start_paused = true)]
async fn every_route_refuses_a_key_the_policy_does_not_admit() {
    let chain = Chain::new(SimConfig::default());
    let denied = address::Address::from_ergo_tree(chain.trees()[0].clone(), Network::Mainnet);
    let policy = KeyPolicy::new(None, [&denied], Some(&[]), Some(&[]), &[]);
    let mut f = Fixture::with_policy(chain, policy);
    f.tick();
    let calls = f.sim.calls().len();
    let template = sim::script_template();
    for uri in [
        format!("/v1/templates/{template}/boxes"),
        format!("/v1/templates/{template}/boxes/with-pool"),
        format!("/v1/addresses/{}/boxes", f.address(0)),
        format!("/v1/addresses/{}/balance", f.address(0)),
        format!("/v1/tokens/{}/boxes", token_id("small-100")),
        format!("/v1/addresses/{}/boxes/with-pool", f.address(0)),
        format!("/v1/tokens/{}/boxes/with-pool", token_id("small-100")),
        format!("/v1/addresses/{}/transactions", f.address(0)),
    ] {
        assert_eq!(f.error(&uri, StatusCode::FORBIDDEN).await, "key_not_allowed", "{uri}");
    }
    assert_eq!(f.sim.calls().len(), calls, "no node call");

    f.ok(&format!("/v1/addresses/{}/boxes", f.address(1))).await;
    f.ok(&format!("/v1/addresses/{}/balance", f.address(1)))
        .await;
}

/// An address's history, newest first, paged to the end: each confirmed tx once, with its
/// height, timestamp and change; a pending tx on top of the first page, with neither.
#[tokio::test]
async fn an_address_history_pages_newest_first_with_pending_on_top() {
    let mut f = Fixture::new();
    let tree = f.tree(0);
    {
        let mut chain = f.sim.lock();
        for index in 1..=2 {
            let input = sim::testing::box_of(&chain, index);
            let tx = chain.transfer(&[input], &tree);
            chain.mine_txs(vec![tx]);
        }
        let input = sim::testing::box_of(&chain, 3);
        let tx = chain.transfer(&[input], &tree);
        chain.submit(tx);
    }
    f.tick();
    let history = format!("/v1/addresses/{}/transactions", f.address(0));
    let first = f.ok(&format!("{history}?limit=1")).await;
    assert_eq!(first.as_object().unwrap().keys().collect::<Vec<_>>(), ["items", "nextCursor"]);
    let items = first["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "the pending tx, then one confirmed");
    assert_eq!(items[0].as_object().unwrap().keys().collect::<Vec<_>>(), ["delta", "txId"]);
    assert!(items[0]["delta"]["nanoErgs"].is_string());
    let keys = items[1].as_object().unwrap().keys().collect::<Vec<_>>();
    assert_eq!(keys, ["delta", "height", "timestamp", "txId"]);
    let tip = f.sim.lock().tip().height;
    assert_eq!(items[1]["height"], tip);
    assert_eq!(items[1]["timestamp"], sim::timestamp(tip));

    let mut ids = vec![items[1]["txId"].clone()];
    let mut cursor = first["nextCursor"].as_str().expect("more pages").to_owned();
    loop {
        let page = f.ok(&format!("{history}?cursor={cursor}&limit=1")).await;
        let items = page["items"].as_array().unwrap();
        assert!(items.iter().all(|i| !i["height"].is_null()), "pending only on the first page");
        ids.extend(items.iter().map(|i| i["txId"].clone()));
        match page["nextCursor"].as_str() {
            Some(next) => cursor = next.to_owned(),
            None => break,
        }
    }
    let total = ready(f.sim.address_transactions(&tree, 0, 1))
        .unwrap()
        .total;
    assert_eq!(ids.len(), total as usize);
    ids.dedup();
    assert_eq!(ids.len(), total as usize, "no tx twice");

    // The default limit holds the whole history, the pending tx on top.
    let all = f.ok(&history).await;
    assert_eq!(all["items"].as_array().unwrap().len(), total as usize + 1);
    assert_eq!(all["nextCursor"], Value::Null);
}

#[tokio::test]
async fn headers_are_the_last_ones_up_to_the_tip_newest_first() {
    let mut chain = Chain::new(SimConfig::default());
    for seed in 1..=14 {
        chain.mine(0, 1, seed);
    }
    let mut f = Fixture::with_chain(chain);
    assert_eq!(
        f.error("/v1/headers", StatusCode::SERVICE_UNAVAILABLE)
            .await,
        "not_ready"
    );
    f.tick();
    let tip = f.sim.lock().tip();

    let body = f.ok("/v1/headers").await;
    assert_eq!(body["asOf"]["height"], tip.height);
    assert_eq!(body["asOf"]["headerId"], tip.header_id.to_string());
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 10, "ten by default");
    assert_eq!(items[0]["id"], body["asOf"]["headerId"], "the first is the tip");
    for (i, pair) in items.windows(2).enumerate() {
        assert_eq!(pair[1]["id"], pair[0]["parentId"], "item {i}'s parent follows it");
        assert_eq!(pair[1]["height"].as_u64().unwrap() + 1, pair[0]["height"].as_u64().unwrap());
    }
    // Each item is the header's JSON as the node (here the simulator) gave it.
    let block = f.sim.lock().block(&tip.header_id).unwrap();
    assert_eq!(items[0], serde_json::from_str::<Value>(&block.header.json).unwrap());

    let three = f.ok("/v1/headers?limit=3").await;
    assert_eq!(three["items"].as_array().unwrap()[..], items[..3]);
    for bad in ["0", "11", "ten", "-1"] {
        let uri = format!("/v1/headers?limit={bad}");
        assert_eq!(f.error(&uri, StatusCode::BAD_REQUEST).await, "invalid_limit", "{bad}");
    }
}

#[tokio::test]
async fn unknown_routes_and_methods_are_plain_http_errors() {
    let f = Fixture::new();
    assert_eq!(f.get("/v1/nothing").await.status(), StatusCode::NOT_FOUND);
    assert_eq!(f.get("/v1/status/").await.status(), StatusCode::NOT_FOUND, "no trailing slash");
    assert_eq!(f.get("/v1/ws").await.status(), StatusCode::NOT_FOUND, "its own router");
    assert_eq!(f.get("/metrics").await.status(), StatusCode::NOT_FOUND, "never on the public port");
    let post = Request::post("/v1/status").body(Body::empty()).unwrap();
    assert_eq!(f.send(post).await.status(), StatusCode::METHOD_NOT_ALLOWED);
}

/// Each refusal is counted once under its code, and a served request under none.
#[tokio::test]
async fn refusals_are_counted_by_code() {
    let mut f = Fixture::new();
    f.tick();
    let boxes = format!("/v1/addresses/{}/boxes", f.address(0));
    let count = |code: &str| {
        let series = format!("hergmes_rejections_total{{transport=\"rest\",code=\"{code}\"}}");
        f.api.metrics.count(&series)
    };
    f.ok(&boxes).await;
    f.ok("/v1/status").await;
    assert_eq!(f.get("/v1/nothing").await.status(), StatusCode::NOT_FOUND);
    assert_eq!(count("invalid_limit") + count("invalid_address"), 0);
    for _ in 0..2 {
        f.error(&format!("{boxes}?limit=0"), StatusCode::BAD_REQUEST)
            .await;
    }
    f.error("/v1/addresses/nope/boxes", StatusCode::BAD_REQUEST)
        .await;
    assert_eq!((count("invalid_limit"), count("invalid_address")), (2, 1));
    assert_eq!(count("too_many_boxes"), 0);
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
    assert_eq!(preflight.headers()[ACCESS_CONTROL_ALLOW_METHODS], "GET,POST");

    // A tx sent as JSON passes the preflight.
    let preflight = Request::options("/v1/transactions")
        .header(ORIGIN, "https://wallet.example")
        .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
        .body(Body::empty())
        .unwrap();
    let preflight = f.send(preflight).await;
    assert_eq!(preflight.status(), StatusCode::OK);
    assert_eq!(preflight.headers()[ACCESS_CONTROL_ALLOW_HEADERS], "content-type");

    // A tx is a simple request (`text/plain`, no preflight), and its refusal is readable.
    let post = Request::post("/v1/transactions")
        .header(ORIGIN, "https://wallet.example")
        .header(CONTENT_TYPE, "text/plain")
        .body(Body::from("zz"))
        .unwrap();
    let refused = f.send(post).await;
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    assert_eq!(refused.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
}

#[tokio::test(start_paused = true)]
async fn a_box_by_id_is_served_alone_in_either_case() {
    let mut f = Fixture::new();
    f.tick();
    let (key, utxo) = f.sim.lock().utxo().into_iter().next().unwrap();
    let id = utxo.id.to_string();
    for id in [id.clone(), id.to_uppercase()] {
        let body = f.ok(&format!("/v1/boxes/{id}")).await;
        assert_eq!(body["boxId"], utxo.id.to_string());
        assert_eq!(body["inclusionHeight"], key.inclusion_height);
        assert_eq!(body["value"], utxo.value.to_string());
        assert!(body.get("asOf").is_none(), "{body}");
    }
    for bad in ["abcd", &format!("{id}0"), "%FF"] {
        let uri = format!("/v1/boxes/{bad}");
        assert_eq!(f.error(&uri, StatusCode::BAD_REQUEST).await, "invalid_box_id", "{uri}");
    }
    let unknown = format!("/v1/boxes/{}", "ab".repeat(32));
    assert_eq!(f.error(&unknown, StatusCode::NOT_FOUND).await, "box_not_found");
}

/// A box id is not a key: a box of an address the policy refuses is served.
#[tokio::test(start_paused = true)]
async fn a_box_by_id_is_outside_the_key_policy() {
    let chain = Chain::new(SimConfig::default());
    let tree = chain.trees()[0].clone();
    let denied = address::Address::from_ergo_tree(tree.clone(), Network::Mainnet);
    let policy = KeyPolicy::new(None, [&denied], Some(&[]), None, &[]);
    let mut f = Fixture::with_policy(chain, policy);
    f.tick();
    let held = f
        .sim
        .lock()
        .utxo()
        .into_values()
        .find(|b| b.ergo_tree == tree)
        .unwrap();
    let body = f.ok(&format!("/v1/boxes/{}", held.id)).await;
    assert_eq!(body["boxId"], held.id.to_string());
}

/// Tokens in the wire shape, in request order, the unknown id left out; refused ids get
/// their codes. Outside the key policy and the supply limit: a relay that admits no token
/// still serves the metadata of one over the limit.
#[tokio::test]
async fn token_metadata_is_served_for_any_token() {
    let policy = KeyPolicy::new(None, [], Some(&[]), None, &[]);
    let f = Fixture::with_policy(Chain::new(SimConfig::default()), policy);
    let large = token_id("large");
    let nft = token_id("nft-a");
    let unknown = "ab".repeat(32);
    let body = f
        .ok(&format!("/v1/tokens/metadata?ids={large},{unknown},{nft}"))
        .await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let expected = f.sim.lock().tokens(&[large]).remove(0);
    assert_eq!(
        items[0],
        serde_json::json!({
            "tokenId": large.to_string(),
            "mintingBoxId": expected.minting_box_id.to_string(),
            "name": "large",
            "description": "",
            "decimals": 0,
            "emission": "1000000",
        })
    );
    assert_eq!(items[1]["tokenId"], nft.to_string());
    assert_eq!(
        f.ok(&format!("/v1/tokens/metadata?ids={unknown}")).await["items"],
        serde_json::json!([])
    );

    let over = vec![nft.to_string(); 101].join(",");
    for (uri, code) in [
        ("/v1/tokens/metadata".to_owned(), "invalid_ids"),
        ("/v1/tokens/metadata?ids=".to_owned(), "invalid_ids"),
        (format!("/v1/tokens/metadata?ids={over}"), "invalid_ids"),
        ("/v1/tokens/metadata?ids=abcd".to_owned(), "invalid_token_id"),
        (format!("/v1/tokens/metadata?ids={nft},"), "invalid_token_id"),
    ] {
        assert_eq!(f.error(&uri, StatusCode::BAD_REQUEST).await, code, "{uri}");
    }
}

impl Fixture {
    async fn post(&self, uri: &str, body: impl Into<Body>) -> Response {
        let request = Request::post(uri)
            .header(CONTENT_TYPE, "text/plain")
            .body(body.into())
            .unwrap();
        self.send(request).await
    }

    /// A refused tx's reason, after checking it is a refusal.
    async fn refused(&self, uri: &str, body: impl Into<Body>) -> String {
        let response = self.post(uri, body).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body = body_json(response).await;
        assert_eq!(body["error"], "transaction_rejected", "{body}");
        assert!(!body["message"].as_str().unwrap().is_empty());
        body["reason"].as_str().unwrap().to_owned()
    }
}

/// The relay refuses a body that is not hex, not UTF-8 or too long with the reason the node
/// would give, unread past the limit and with no node call; `broadcast` is `true` or `false`.
#[tokio::test]
async fn a_tx_body_is_checked_before_the_node_sees_it() {
    let f = Fixture::new();
    let calls = f.sim.calls().len();
    assert_eq!(f.refused("/v1/transactions", "zz").await, "unparseable");
    assert_eq!(f.refused("/v1/transactions", vec![0xff, 0xfe]).await, "unparseable");
    let over = "0".repeat(MAX_TX_BODY + 1);
    assert_eq!(f.refused("/v1/transactions", over).await, "too_large");
    let over = "00".repeat(crate::api::transactions::MAX_TX_BYTES + 1);
    assert_eq!(f.refused("/v1/transactions?broadcast=false", over).await, "too_large");
    // Unread past the limit or read and checked, a body too long gets one message.
    let mut messages = Vec::new();
    for over in ["0".repeat(MAX_TX_BODY + 1), "00".repeat(MAX_HEX_CHARS)] {
        let body = body_json(f.post("/v1/transactions", over).await).await;
        messages.push(body["message"].clone());
    }
    assert_eq!(messages[0], messages[1]);
    for value in ["", "yes", "TRUE", "1"] {
        let uri = format!("/v1/transactions?broadcast={value}");
        let response = f.post(&uri, "00").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(body_json(response).await["error"], "invalid_broadcast", "{uri}");
    }
    assert_eq!(f.sim.calls().len(), calls, "{:?}", &f.sim.calls()[calls..]);

    // Hex goes to the node, which here takes no tx: a node failure, not a refusal.
    for uri in ["/v1/transactions", "/v1/transactions?broadcast=true&broadcast=x"] {
        let response = f.post(uri, "00").await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{uri}");
    }
    let response = f.get("/v1/transactions").await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

/// Through the router to a node that answers: the id on success, the node's message and its
/// reason on a refusal.
#[tokio::test]
async fn a_tx_is_answered_with_its_id_or_the_nodes_refusal() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

    use crate::{clients::NodeClient, fixtures};

    let server = MockServer::start().await;
    let recorded = |name: &str| {
        ResponseTemplate::new(fixtures::status(name))
            .set_body_raw(fixtures::text(name), "application/json")
    };
    Mock::given(matchers::path("/transactions/checkBytes"))
        .respond_with(recorded("tx_check_signed"))
        .mount(&server)
        .await;
    Mock::given(matchers::path("/transactions/bytes"))
        .respond_with(recorded("tx_send_invalid"))
        .mount(&server)
        .await;
    let node = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
    let f = Fixture::new();
    let api = Arc::new(Api {
        engine: Mutex::new(Engine::new(Config::default())),
        pages: Pages::new(
            Arc::new(Loader::new(node.clone(), Default::default())),
            Arc::new(TokenChecker::new(node.clone(), 100)),
            BoxFetcher::new(node.clone(), BoxCache::new(0)),
        ),
        balances: Balances::new(node.clone(), Default::default()),
        history: History::new(node.clone(), Default::default()),
        headers: Headers::new(HeaderReader::new(node.clone(), f.follower.headers())),
        boxes: Boxes::new(node.clone()),
        tokens: Tokens::new(node.clone()),
        transactions: Transactions::new(node),
        network: Network::Mainnet,
        policy: KeyPolicy::default(),
        synced: f.follower.synced(),
        metrics: Default::default(),
    });
    let router = router(api);
    let post = |uri: &str| {
        Request::post(uri)
            .body(Body::from(fixtures::signed_tx()))
            .unwrap()
    };

    let checked = router
        .clone()
        .oneshot(post("/v1/transactions?broadcast=false"))
        .await
        .unwrap();
    assert_eq!(checked.status(), StatusCode::OK);
    let id: String = fixtures::load("tx_check_signed");
    assert_eq!(body_json(checked).await, serde_json::json!({ "transactionId": id }));

    let sent = router.oneshot(post("/v1/transactions")).await.unwrap();
    assert_eq!(sent.status(), StatusCode::BAD_REQUEST);
    let body = body_json(sent).await;
    let node_error: crate::clients::node::ApiError = fixtures::load("tx_send_invalid");
    assert_eq!(
        body,
        serde_json::json!({
            "error": "transaction_rejected",
            "reason": "invalid",
            "message": node_error.detail.unwrap(),
        })
    );
}
