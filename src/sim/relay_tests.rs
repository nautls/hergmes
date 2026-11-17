//! The sim relay's control routes, through the whole router: the ops' JSON form, seeded
//! draws, and that the oracle's expected state agrees with what the relay serves whenever a
//! route offers it, and is refused whenever the engine was not shown the chain.

use std::collections::HashSet;

use axum::{
    body::Body,
    http::{Request, header::CONTENT_TYPE},
};
use http_body_util::BodyExt;
use proptest::prelude::*;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::*;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn call(router: &Router, method: &str, uri: &str, body: Option<Value>) -> (u16, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(body) => {
            request = request.header(CONTENT_TYPE, "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = router
        .clone()
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, value)
}

async fn get_json(router: &Router, uri: &str) -> (u16, Value) {
    call(router, "GET", uri, None).await
}

async fn post_json(router: &Router, uri: &str, body: Value) -> (u16, Value) {
    call(router, "POST", uri, Some(body)).await
}

fn every_op() -> Vec<SimOp> {
    vec![
        SimOp::Mine { from_pool: 2, fresh: 3, seed: u64::MAX },
        SimOp::Reorg { depth: 1, extra: 2, seed: 7 },
        SimOp::MempoolAdd { seed: 9_007_199_254_740_993 },
        SimOp::MempoolConflict { pick: 4, seed: 0 },
        SimOp::MempoolDrop { pick: u32::MAX },
        SimOp::MempoolRecheckRace { pick: 1 },
        SimOp::IndexerLag { lag: 2 },
    ]
}

#[test]
fn ops_are_tagged_with_camel_case_fields_and_seeds_as_strings() {
    let op = SimOp::Mine { from_pool: 2, fresh: 3, seed: u64::MAX };
    assert_eq!(
        serde_json::to_value(op).unwrap(),
        json!({ "op": "mine", "fromPool": 2, "fresh": 3, "seed": "18446744073709551615" })
    );
    assert_eq!(
        serde_json::to_value(SimOp::IndexerLag { lag: 1 }).unwrap(),
        json!({ "op": "indexerLag", "lag": 1 })
    );
    assert_eq!(
        serde_json::to_value(SimOp::MempoolRecheckRace { pick: 1 }).unwrap(),
        json!({ "op": "mempoolRecheckRace", "pick": 1 })
    );
    for op in every_op() {
        let text = serde_json::to_string(&op).unwrap();
        assert_eq!(serde_json::from_str::<SimOp>(&text).unwrap(), op, "{text}");
    }
}

#[test]
fn ops_refuse_numeric_seeds_unknown_fields_and_unknown_ops() {
    for text in [
        r#"{ "op": "mempoolAdd", "seed": 7 }"#,
        r#"{ "op": "mempoolAdd", "seed": "-1" }"#,
        r#"{ "op": "mempoolAdd", "seed": "18446744073709551616" }"#,
        r#"{ "op": "mempoolAdd", "seed": "7", "extra": 1 }"#,
        r#"{ "op": "mempool_add", "seed": "7" }"#,
        r#"{ "seed": "7" }"#,
    ] {
        assert!(serde_json::from_str::<SimOp>(text).is_err(), "{text}");
    }
}

#[test]
fn draws_are_seeded_and_reach_every_op() {
    let mut kinds = HashSet::new();
    for seed in 0..500 {
        let op = draw(seed);
        assert_eq!(draw(seed), op, "seed {seed}");
        if let SimOp::Reorg { depth, .. } = op {
            assert!((1..=MAX_REORG_DEPTH).contains(&depth));
        }
        kinds.insert(std::mem::discriminant(&op));
    }
    assert_eq!(kinds.len(), every_op().len(), "every kind of op is drawn");
}

#[test]
fn random_applies_and_answers_the_drawn_op() {
    runtime().block_on(async {
        let (router, relay, _task) = SimRelay::start(chain()).await;
        let mut expected = chain();
        for seed in [1u64, u64::MAX, 42, 9_007_199_254_740_993] {
            let (status, op) =
                post_json(&router, "/sim/random", json!({ "seed": seed.to_string() })).await;
            assert_eq!(status, 200);
            let op: SimOp = serde_json::from_value(op).unwrap();
            assert_eq!(op, draw(seed));
            expected.apply(&op);
        }
        let chain = relay.sim().lock();
        assert_eq!(chain.tip(), expected.tip());
        assert_eq!(chain.mempool(), expected.mempool());
    });
}

#[test]
fn ops_replay_a_run() {
    runtime().block_on(async {
        let (router, relay, _task) = SimRelay::start(chain()).await;
        let ops = every_op();
        let (status, _) = post_json(&router, "/sim/ops", serde_json::to_value(&ops).unwrap()).await;
        assert_eq!(status, 204);
        let mut expected = chain();
        for op in &ops {
            expected.apply(op);
        }
        let chain = relay.sim().lock();
        assert_eq!(chain.tip(), expected.tip());
        assert_eq!(chain.mempool(), expected.mempool());
        assert_eq!(chain.indexer_lag(), 2);
    });
}

#[test]
fn keys_lists_the_trees_as_addresses_and_the_tokens() {
    runtime().block_on(async {
        let (router, relay, _task) = SimRelay::start(chain()).await;
        let (status, keys) = get_json(&router, "/sim/keys").await;
        assert_eq!(status, 200);
        let chain = relay.sim().lock();
        let addresses: Vec<String> = chain
            .trees()
            .iter()
            .map(|t| address::encode(&t.0, Network::Mainnet))
            .collect();
        assert_eq!(keys["addresses"], json!(addresses));
        let tokens = keys["tokens"].as_array().unwrap();
        assert_eq!(tokens.len(), chain.known_tokens().len());
        for (token, (id, emission)) in tokens.iter().zip(chain.known_tokens()) {
            assert_eq!(token["tokenId"], json!(id.to_string()));
            assert_eq!(token["emission"], json!(emission.to_string()));
        }
    });
}

#[test]
fn bad_keys_are_400() {
    runtime().block_on(async {
        let (router, _relay, _task) = SimRelay::start(chain()).await;
        assert_eq!(get_json(&router, "/sim/expected/addresses/nope").await.0, 400);
        assert_eq!(get_json(&router, "/sim/expected/tokens/nope").await.0, 400);
    });
}

/// The boxes a tx can spend now, from an expected state: its confirmed boxes and pending
/// outputs, minus every box a pending tx spends, as `with-pool` lists them.
fn spendable(expected: &Value) -> Vec<Value> {
    let pending = expected["pending"].as_array().unwrap();
    let spent: HashSet<&str> = pending
        .iter()
        .flat_map(|tx| tx["spent"].as_array().unwrap())
        .map(|id| id.as_str().unwrap())
        .collect();
    let created = pending
        .iter()
        .flat_map(|tx| tx["created"].as_array().unwrap());
    expected["boxes"]
        .as_array()
        .unwrap()
        .iter()
        .chain(created)
        .filter(|b| !spent.contains(b["boxId"].as_str().unwrap()))
        .cloned()
        .collect()
}

/// Checks the expected state of each key against the relay's own answers: the key's page,
/// its view with the pool and, for an address, its balance. The relay's services are checked
/// against the oracle by their own properties; this checks that the route reads the oracle
/// at the engine's tip and the pool the engine was given.
async fn agrees(router: &Router, tip: &Value) {
    let (_, keys) = get_json(router, "/sim/keys").await;
    let addresses = keys["addresses"].as_array().unwrap().iter();
    let tokens = keys["tokens"].as_array().unwrap().iter();
    let routes = addresses
        .map(|a| ("addresses", a.as_str().unwrap().to_string()))
        .chain(tokens.map(|t| ("tokens", t["tokenId"].as_str().unwrap().to_string())));
    for (kind, key) in routes {
        let (status, page) = get_json(router, &format!("/v1/{kind}/{key}/boxes?limit=1000")).await;
        if status != 200 {
            // Over the supply limit, or minted in the pool only: the indexer does not know it.
            assert!(kind == "tokens" && [404, 422].contains(&status), "{status} {page}");
            continue;
        }
        let (status, expected) = get_json(router, &format!("/sim/expected/{kind}/{key}")).await;
        assert_eq!(status, 200, "{expected}");
        assert_eq!(&expected["tip"], tip);
        assert_eq!(page["asOf"], expected["tip"]);
        assert_eq!(page["nextCursor"], Value::Null);
        assert_eq!(page["items"], expected["boxes"], "{kind} {key}'s boxes");

        let (_, view) = get_json(router, &format!("/v1/{kind}/{key}/boxes/with-pool")).await;
        assert_eq!(view["items"], json!(spendable(&expected)), "{kind} {key} with the pool");

        if kind == "addresses" {
            let (_, balance) = get_json(router, &format!("/v1/{kind}/{key}/balance")).await;
            assert_eq!(balance["confirmed"], expected["balance"]["confirmed"]);
            assert_eq!(balance["pending"], expected["balance"]["pending"]);
            assert_eq!(history(router, &key).await, expected["history"], "{key}'s history");
        } else {
            assert_eq!(expected.get("balance"), None);
            assert_eq!(expected.get("history"), None);
        }
    }
}

/// An address's whole history, read from the route page by page.
async fn history(router: &Router, address: &str) -> Value {
    let mut items = Vec::new();
    let mut path = format!("/v1/addresses/{address}/transactions?limit=100");
    loop {
        let (status, page) = get_json(router, &path).await;
        assert_eq!(status, 200, "{page}");
        items.extend(page["items"].as_array().unwrap().iter().cloned());
        let Some(cursor) = page["nextCursor"].as_str() else {
            return Value::Array(items);
        };
        path = format!("/v1/addresses/{address}/transactions?limit=100&cursor={cursor}");
    }
}

/// What a test does next.
#[derive(Debug, Clone)]
enum Step {
    Random(u64),
    Tick,
    Settle,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => any::<u64>().prop_map(Step::Random),
        2 => Just(Step::Tick),
        1 => Just(Step::Settle),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// The expected state is served exactly when the last tick or settle synced and no op
    /// came after it, and then agrees with the relay. A settle always syncs.
    #[test]
    fn expected_is_served_only_at_a_checkpoint_and_agrees(
        steps in prop::collection::vec(step(), 1..30),
    ) {
        runtime().block_on(async {
            let (router, relay, _task) = SimRelay::start(chain()).await;
            let address = relay.keys()["addresses"][0].as_str().unwrap().to_string();
            let probe = format!("/sim/expected/addresses/{address}");
            let mut tip = get_json(&router, "/v1/status").await.1["tip"].clone();
            // The start ticked once, with no op since.
            assert_eq!(get_json(&router, &probe).await.0, 200);
            let mut checkpoint;
            for step in steps {
                match step {
                    Step::Random(seed) => {
                        let (status, _) =
                            post_json(&router, "/sim/random", json!({ "seed": seed.to_string() }))
                                .await;
                        assert_eq!(status, 200);
                        checkpoint = false;
                    }
                    Step::Tick => {
                        let lagging = relay.sim().lock().indexer_lag() > 0;
                        let (status, tick) = post_json(&router, "/sim/tick", json!(null)).await;
                        assert_eq!(status, 200);
                        checkpoint = tick["synced"] == json!(true);
                        // No recovery reads: only a lag keeps the engine from the tip.
                        assert!(checkpoint || lagging, "a tick with no lag syncs");
                        tip = tick["tip"].clone();
                        if checkpoint {
                            let chain_tip = relay.sim().lock().tip();
                            assert_eq!(tip, serde_json::to_value(AsOf::from(&chain_tip)).unwrap());
                        }
                    }
                    Step::Settle => {
                        let (status, settled) =
                            post_json(&router, "/sim/settle", json!(null)).await;
                        assert_eq!(status, 200, "{settled}");
                        let chain = relay.sim().lock();
                        assert_eq!(chain.indexer_lag(), 0);
                        assert!(!chain.is_recovering());
                        drop(chain);
                        checkpoint = true;
                        tip = settled["tip"].clone();
                    }
                }
                let (status, _) = get_json(&router, &probe).await;
                assert_eq!(status, if checkpoint { 200 } else { 409 }, "{step:?}");
                if checkpoint {
                    agrees(&router, &tip).await;
                }
            }
        });
    }
}
