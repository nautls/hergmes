//! The token metadata service: what a request is answered with, against `SimChain`; which ids
//! reach the node and what is kept, against the node client and the recorded batch.

use std::time::Duration;

use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, method, path, path_regex},
};

use super::*;
use crate::{
    clients::NodeClient,
    fixtures::{self, load, sample},
    sim::{Chain, Endpoint, GENESIS_TOKENS, SimChain, SimConfig, token_id},
    types::{
        address::Network,
        eip4::{Asset, AssetType, Eip4, Standard},
    },
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

fn eip4(token: &TokenMetadata) -> &Eip4 {
    match &token.standard {
        Standard::Eip4(eip4) => eip4,
        Standard::None => panic!("{} follows EIP-4", token.id),
    }
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
    let expected: Vec<TokenMetadata> = {
        let chain = sim.lock();
        chain
            .tokens(&ids)
            .into_iter()
            .map(|t| {
                let minting = chain.indexed_box(&t.minting_box_id).unwrap();
                TokenMetadata::minted(t, &minting.indexed.utxo)
            })
            .collect()
    };
    assert_eq!(expected.len(), GENESIS_TOKENS.len());
    assert_eq!(served.iter().map(|t| (**t).clone()).collect::<Vec<_>>(), expected);
    for token in &served {
        assert!(!eip4(token).name.is_empty(), "a genesis token is EIP-4 by its name");
    }
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

/// Each token found reads its minting box once, and none is read again once kept; an unknown
/// id has no box to read.
#[tokio::test]
async fn each_token_found_reads_its_minting_box_once() {
    let (sim, tokens) = sim();
    let asked = joined(&[token_id("nft-a"), unknown(), token_id("nft-b")]);
    tokens.metadata(Some(&asked)).await.unwrap();
    assert_eq!(sim.calls(), [Endpoint::Tokens, Endpoint::IndexedBox, Endpoint::IndexedBox]);

    tokens.metadata(Some(&asked)).await.unwrap();
    assert_eq!(sim.calls()[3..], [Endpoint::Tokens], "only the unknown id is asked again");
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
    assert_eq!(served[0].minting_tx_id, tx.id);
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

/// Answers every minting box read with the recorded box `name`, whatever its id.
async fn minting_boxes(server: &MockServer, name: &str) {
    Mock::given(method("GET"))
        .and(path_regex("^/blockchain/box/byId/"))
        .respond_with(
            ResponseTemplate::new(fixtures::status(name))
                .set_body_raw(fixtures::text(name), "application/json"),
        )
        .mount(server)
        .await;
}

/// The recorded EIP-4 mints: the picture NFT with its type and artwork from its minting box,
/// the plain token with none.
#[tokio::test]
async fn the_recorded_mints_are_served_with_their_assets() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    let manifest = fixtures::manifest();
    let fixtures = &manifest["fixtures"];
    Mock::given(path("/blockchain/tokens"))
        .and(body_json(&fixtures["tokens_eip4"]["body"]))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(fixtures::text("tokens_eip4"), "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;
    for name in ["minting_box_picture", "minting_box_plain"] {
        Mock::given(method("GET"))
            .and(path(fixtures[name]["path"].as_str().unwrap()))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(fixtures::text(name), "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    let ids: Vec<TokenId> =
        serde_json::from_value(fixtures["tokens_eip4"]["body"].clone()).unwrap();
    let served = tokens.metadata(Some(&joined(&ids))).await.unwrap();
    assert_eq!(ids_of(&served), ids);
    for (token, name) in served
        .iter()
        .zip(["minting_box_picture", "minting_box_plain"])
    {
        let minting: Value = load(name);
        assert_eq!(token.minting_tx_id.to_string(), minting["transactionId"].as_str().unwrap());
        assert_eq!(token.standard.as_str(), "eip-4", "{name}");
    }

    let picture = eip4(&served[0])
        .asset
        .as_deref()
        .expect("the picture NFT has an asset");
    assert_eq!(picture.asset_type, AssetType::NftPicture);
    assert!(picture.artwork_hash.is_some());
    assert!(
        picture
            .artwork_url
            .as_deref()
            .unwrap()
            .starts_with("https://")
    );
    assert!(
        picture
            .artwork_cover
            .as_deref()
            .unwrap()
            .starts_with("https://")
    );
    assert_eq!(eip4(&served[1]).asset, None, "the plain token has none");

    tokens.metadata(Some(&joined(&ids))).await.unwrap();
}

/// A token whose minting box the indexer no longer knows (a reorg since its listing) has no
/// mint tx to serve: it is left out and not kept, so it is asked again.
#[tokio::test]
async fn a_token_whose_minting_box_is_unknown_is_left_out_and_not_kept() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    let nft = sample("nft");
    let recorded: Vec<Value> = load("tokens");
    Mock::given(path("/blockchain/tokens"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([recorded[0]])))
        .expect(2)
        .mount(&server)
        .await;
    minting_boxes(&server, "box_by_id_unknown").await;

    for _ in 0..2 {
        assert!(tokens.metadata(Some(&nft)).await.unwrap().is_empty());
    }
}

/// A `503` reading a minting box fails the request; a token whose box was read before it is
/// kept, so a retry asks only for the rest.
#[tokio::test]
async fn a_503_reading_a_minting_box_fails_the_request_and_keeps_the_reads_done() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    let recorded: Vec<Value> = load("tokens");
    let [nft, small] = ["nft", "smallSupplyToken"].map(sample);
    Mock::given(path("/blockchain/tokens"))
        .and(body_json(json!([nft, small])))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([recorded[0], recorded[1]])))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/blockchain/tokens"))
        .and(body_json(json!([nft])))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([recorded[0]])))
        .expect(1)
        .mount(&server)
        .await;
    let read = format!("/blockchain/box/byId/{}", recorded[1]["boxId"].as_str().unwrap());
    Mock::given(path(read))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(fixtures::text("minting_box_plain"), "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;
    // Answered after the read that succeeds, so that one is done when this one fails.
    Mock::given(path_regex("^/blockchain/box/byId/"))
        .respond_with(
            ResponseTemplate::new(503)
                .insert_header("Retry-After", "3")
                .set_delay(Duration::from_millis(100)),
        )
        .mount(&server)
        .await;

    let both = format!("{nft},{small}");
    let error = tokens.metadata(Some(&both)).await.unwrap_err();
    assert_eq!(error, ApiError::NodeUnavailable { retry_after: Some(Duration::from_secs(3)) });
    let [nft_id, small_id]: [TokenId; 2] = [&nft, &small].map(|id| id.parse().unwrap());
    assert!(tokens.cache.get(&nft_id).is_none());
    assert!(tokens.cache.get(&small_id).is_some(), "the read done before the failure is kept");

    tokens.metadata(Some(&both)).await.unwrap_err();
}

/// The node is asked only for the ids not kept, once each, in request order.
#[tokio::test]
async fn only_missing_ids_reach_the_node() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    let recorded: Vec<Value> = load("tokens");
    let [nft, small, large] = ["nft", "smallSupplyToken", "largeSupplyToken"].map(sample);
    minting_boxes(&server, "minting_box_plain").await;
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
    let names: Vec<&str> = served.iter().map(|t| &*eip4(t).name).collect();
    assert_eq!(names, ["ERGUSD-PT", "Emission Contract NFT", "Reemission Token"]);
}

/// The recorded batch, its unknown id left out.
#[tokio::test]
async fn the_recorded_batch_is_served() {
    let server = MockServer::start().await;
    let tokens = node(&server).await;
    let requested = fixtures::manifest()["fixtures"]["tokens"]["body"].clone();
    minting_boxes(&server, "minting_box_plain").await;
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

/// A token weighs its strings and its asset's, so the budget bounds tokens with long
/// descriptions too.
#[test]
fn the_cache_stays_within_its_budget() {
    let with = |n: u8, description: usize, asset: Option<Asset>| TokenMetadata {
        id: crate::sim::testing::digest(n),
        minting_tx_id: crate::sim::testing::digest(n),
        emission: 1,
        standard: Standard::Eip4(Eip4 {
            name: "n".into(),
            description: "d".repeat(description).into(),
            decimals: 0,
            asset: asset.map(Box::new),
        }),
    };
    let token = |n, description| with(n, description, None);
    assert_eq!(weight(&token(0, 1000)) - weight(&token(0, 0)), 1000);
    let nft = with(
        0,
        0,
        Some(Asset {
            asset_type: AssetType::NftPicture,
            artwork_hash: None,
            artwork_url: Some("u".repeat(1000).into()),
            artwork_cover: None,
        }),
    );
    assert!(weight(&nft) - weight(&token(0, 0)) > 1000, "an asset's links are weighed");

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
