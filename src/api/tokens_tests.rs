//! The token metadata service: what a request is answered with, against `SimChain`; which ids
//! reach the node and what is kept, against the node client and the recorded batch.

use std::time::Duration;

use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, method, path},
};

use super::*;
use crate::{
    clients::NodeClient,
    fixtures::{self, load, sample},
    sim::{Chain, Endpoint, GENESIS_TOKENS, SimChain, SimConfig, token_id},
    types::address::Network,
};

fn unknown() -> TokenId {
    "ab".repeat(32).parse().unwrap()
}

fn sim() -> (SimChain, Tokens<SimChain>) {
    let sim = SimChain::new(Chain::new(SimConfig::default()));
    (sim.clone(), Tokens::new(sim))
}

fn joined(ids: &[TokenId]) -> String {
    ids.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn ids_of(tokens: &[Arc<TokenMetadata>]) -> Vec<TokenId> {
    tokens.iter().map(|t| t.id).collect()
}

/// Every genesis token as the indexer lists it, in request order, duplicates once and the
/// unknown id left out; ids in either case.
#[tokio::test]
async fn known_tokens_are_served_in_request_order_once_each() {
    let (sim, tokens) = sim();
    let mut ids: Vec<TokenId> = GENESIS_TOKENS
        .iter()
        .rev()
        .map(|(name, ..)| token_id(name))
        .collect();
    ids.insert(2, unknown());
    let mut asked = joined(&ids);
    asked.push(',');
    asked.push_str(&ids[0].to_string().to_uppercase());

    let served = tokens.metadata(Some(&asked)).await.unwrap();
    let expected = sim.lock().tokens(&ids);
    assert_eq!(expected.len(), GENESIS_TOKENS.len());
    assert_eq!(served.iter().map(|t| (**t).clone()).collect::<Vec<_>>(), expected);
}

/// Tokens never change, so a token served once costs no node call again; an unknown id is
/// asked again, since the indexer may reach its mint later.
#[tokio::test]
async fn known_tokens_are_kept_and_unknown_ids_asked_again() {
    let (sim, tokens) = sim();
    let nft = token_id("nft-a");
    let asked = joined(&[nft, unknown()]);
    tokens.metadata(Some(&asked)).await.unwrap();
    let calls = sim.calls().len();

    tokens.metadata(Some(&nft.to_string())).await.unwrap();
    assert_eq!(sim.calls().len(), calls, "no node call for a kept token");

    let served = tokens.metadata(Some(&asked)).await.unwrap();
    assert_eq!(ids_of(&served), [nft]);
    assert_eq!(sim.calls()[calls..], [Endpoint::Tokens], "the unknown id is asked again");
}

/// A token asked for before its mint is indexed is served once it is.
#[tokio::test]
async fn a_token_unknown_before_its_mint_is_served_after() {
    let (sim, tokens) = sim();
    let (tx, id) = {
        let mut chain = sim.lock();
        let input = chain.utxo().into_values().next().unwrap();
        let to = chain.trees()[0].clone();
        (chain.mint(&input.id, 7, &to), input.id)
    };
    sim.lock().submit(tx.clone());
    assert!(
        tokens
            .metadata(Some(&id.to_string()))
            .await
            .unwrap()
            .is_empty()
    );

    sim.lock().mine_txs(vec![tx.clone()]);
    let served = tokens.metadata(Some(&id.to_string())).await.unwrap();
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].emission, 7);
    assert_eq!(served[0].minting_box_id, tx.outputs[0].id);
    assert_ne!(served[0].minting_box_id, id, "the minting box is not the token id");
}

#[tokio::test]
async fn ids_are_refused_before_any_node_call() {
    let (sim, tokens) = sim();
    let id = token_id("nft-a").to_string();
    let over = vec![id.as_str(); MAX_IDS + 1].join(",");
    for (ids, code) in [
        (None, "invalid_ids"),
        (Some(""), "invalid_ids"),
        (Some(over.as_str()), "invalid_ids"),
        (Some("abcd"), "invalid_token_id"),
        (Some(&format!("{id},")), "invalid_token_id"),
        (Some(&format!("{id},,{id}")), "invalid_token_id"),
        (Some(&format!("{id}0")), "invalid_token_id"),
        (Some(&format!(" {id}")), "invalid_token_id"),
    ] {
        let error = tokens.metadata(ids).await.unwrap_err();
        assert_eq!(error.code(), code, "{ids:?}");
    }
    assert!(sim.calls().is_empty(), "no node call");

    let most = vec![id.as_str(); MAX_IDS].join(",");
    assert_eq!(tokens.metadata(Some(&most)).await.unwrap().len(), 1);
}

async fn node(server: &MockServer) -> Tokens<NodeClient> {
    Tokens::new(NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet))
}

/// The node is asked only for the ids not kept, once each, in request order.
#[tokio::test]
async fn only_missing_ids_reach_the_node() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    let recorded: Vec<Value> = load("tokens");
    let [nft, small, large] = ["nft", "smallSupplyToken", "largeSupplyToken"].map(sample);
    Mock::given(method("POST"))
        .and(path("/blockchain/tokens"))
        .and(body_json(json!([nft])))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([recorded[0]])))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/blockchain/tokens"))
        .and(body_json(json!([small, large])))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([recorded[1], recorded[2]])))
        .expect(1)
        .mount(&server)
        .await;

    tokens.metadata(Some(&nft)).await.unwrap();
    let served = tokens
        .metadata(Some(&format!("{small},{nft},{small},{large}")))
        .await
        .unwrap();
    let names: Vec<&str> = served.iter().map(|t| &*t.name).collect();
    assert_eq!(names, ["ERGUSD-PT", "Emission Contract NFT", "Reemission Token"]);
}

/// The recorded batch, its unknown id left out.
#[tokio::test]
async fn the_recorded_batch_is_served() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    let requested = fixtures::manifest()["fixtures"]["tokens"]["body"].clone();
    Mock::given(path("/blockchain/tokens"))
        .and(body_json(&requested))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(fixtures::text("tokens"), "application/json"),
        )
        .mount(&server)
        .await;
    let ids: Vec<TokenId> = serde_json::from_value(requested).unwrap();
    let served = tokens.metadata(Some(&joined(&ids))).await.unwrap();
    assert_eq!(ids_of(&served), [ids[0], ids[1], ids[3]]);
}

/// A node `503` fails the whole request with its wait, even if some ids are kept.
#[tokio::test]
async fn a_503_fails_the_request_with_its_wait() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    Mock::given(path("/blockchain/tokens"))
        .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "7"))
        .mount(&server)
        .await;
    let error = tokens.metadata(Some(&sample("nft"))).await.unwrap_err();
    assert_eq!(error, ApiError::NodeUnavailable { retry_after: Some(Duration::from_secs(7)) });
}

/// A token weighs its strings, so the budget bounds tokens with long descriptions too.
#[test]
fn the_cache_stays_within_its_budget() {
    let token = |n: u8, description: usize| TokenMetadata {
        id: crate::sim::testing::digest(n),
        minting_box_id: crate::sim::testing::digest(n),
        emission: 1,
        name: "n".into(),
        description: "d".repeat(description).into(),
        decimals: 0,
    };
    assert_eq!(weight(&token(0, 1000)) - weight(&token(0, 0)), 1000);

    let cache = cache(1 << 20);
    for n in 0..=u8::MAX {
        // About the longest a box can hold.
        let token = Arc::new(token(n, 4000));
        cache.insert(token.id, token);
    }
    cache.run_pending_tasks();
    assert!(cache.weighted_size() <= 1 << 20, "{}", cache.weighted_size());
    assert!(cache.entry_count() >= 200, "{}", cache.entry_count());
}
