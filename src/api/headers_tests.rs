//! The header service over the node client: the recorded header served byte for byte, and
//! its refusals.

use std::sync::Mutex;

use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

use super::*;
use crate::{
    adapters::HeaderCache,
    api::wire::HeadersBody,
    clients::NodeClient,
    engine::{Config, Engine, Event},
    fixtures,
    sim::testing::ready,
    types::{address::Network, ergo::IndexedBlock},
};

/// An engine at the recorded block, and a reader whose node serves the recorded header.
async fn recorded() -> (MockServer, Mutex<Engine>, Headers<NodeClient>) {
    let server = MockServer::start().await;
    let id = fixtures::sample("headerId");
    Mock::given(path(format!("/blocks/modifier/{id}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(fixtures::text("header_by_id"), "application/json"),
        )
        .mount(&server)
        .await;
    let engine = Mutex::new(Engine::new(Config::default()));
    let block: IndexedBlock = fixtures::load("indexed_block");
    ready(engine.apply(Event::BlockApplied(block)))
        .unwrap()
        .unwrap();
    let node = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
    (server, engine, Headers::new(HeaderReader::new(node, HeaderCache::new())))
}

#[tokio::test]
async fn the_node_json_is_served_as_it_came_without_whitespace() {
    let (_server, engine, headers) = recorded().await;
    let page = headers.last(&engine, Some(1)).await.unwrap();
    let body = serde_json::to_string(&HeadersBody(&page)).unwrap();
    // The values hold no whitespace, so dropping every space is the compacted text.
    let header: String = fixtures::text("header_by_id")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let id = fixtures::sample("headerId");
    let expected =
        format!(r#"{{"asOf":{{"height":1915472,"headerId":"{id}"}},"items":[{header}]}}"#);
    assert_eq!(body, expected);
}

#[tokio::test]
async fn a_header_the_node_lacks_is_a_node_error() {
    // Only the tip is recorded, so its parent is a 404.
    let (_server, engine, headers) = recorded().await;
    let error = headers.last(&engine, Some(2)).await.unwrap_err();
    assert_eq!(error.code(), "node_error");
}

#[tokio::test]
async fn limits_outside_one_to_ten_are_refused_and_no_tip_is_not_ready() {
    let (_server, engine, headers) = recorded().await;
    for limit in [0, MAX_LIMIT + 1] {
        let error = headers.last(&engine, Some(limit)).await.unwrap_err();
        assert_eq!(error, ApiError::InvalidLimit { max: 10 });
    }
    let empty = Mutex::new(Engine::new(Config::default()));
    let error = headers.last(&empty, None).await.unwrap_err();
    assert_eq!(error.code(), "not_ready");
}
