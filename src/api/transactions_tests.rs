//! The transaction service: each refusal's reason from the node's own message, the body
//! checks that cost no node call, and the node's answers through the node client.

use std::time::Duration;

use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

use super::*;
use crate::{
    clients::NodeClient,
    fixtures::{self, manifest, signed_tx, status},
    sim::{Chain, Endpoint, SimChain, SimConfig, testing::ready},
    types::address::Network,
};

/// The reason each recorded or hand-written refusal must have, by its fixture's name.
fn expected_reason(name: &str) -> Refusal {
    let reason = name
        .trim_start_matches("tx_send_")
        .trim_start_matches("tx_check_");
    match reason {
        "unparseable" => Refusal::Unparseable,
        "invalid" => Refusal::Invalid,
        "double_spend" => Refusal::DoubleSpend,
        "fee_too_low" => Refusal::FeeTooLow,
        "already_in_pool" => Refusal::AlreadyInPool,
        "pool_full" | "pool_outbid" => Refusal::PoolFull,
        r if r.starts_with("policy_") => Refusal::Policy,
        other => panic!("no reason for {other}"),
    }
}

fn detail(name: &str) -> String {
    let error: crate::clients::node::ApiError = fixtures::load(name);
    error.detail.expect("a refusal has a message")
}

#[test]
fn every_node_refusal_has_its_reason() {
    let fixtures = manifest()["fixtures"].as_object().unwrap().clone();
    let mut reasons = std::collections::BTreeSet::new();
    for name in fixtures
        .keys()
        .filter(|n| n.starts_with("tx_") && status(n) == 400)
    {
        let reason = Refusal::of(&detail(name));
        assert_eq!(reason, expected_reason(name), "{name}");
        reasons.insert(reason.code());
    }
    // `too_large` can't be recorded: the relay refuses such a body before the node sees it.
    // The node writes the tx out in the middle of the message; the tx here is a stand-in.
    let too_large = "Transaction ErgoTransaction(id: bf43e456, inputs: Vector(Input(6f008c45, \
                     ProverResult(...))), outputs: Vector(...), size: 98305) has too large size \
                     98305";
    assert_eq!(Refusal::of(too_large), Refusal::TooLarge);
    reasons.insert(Refusal::of(too_large).code());
    assert_eq!(reasons.len(), 8, "{reasons:?}");
}

/// Messages no reason names fall back to `invalid`, and a reason's text inside a validation
/// error does not count: matches are anchored.
#[test]
fn other_messages_are_invalid() {
    let id = "bf43e4564063c180a4d1eb2ba3b889659e96a85d7cae23811c88299a7f1f244c";
    for message in [
        // `ErgoMemPool.process`, both unlisted.
        "not all utxos in place yet".to_owned(),
        format!("Pool can not accept transaction {id}"),
        // Another reason's text inside a validation error.
        "Malformed transaction: Double spending attempt".to_owned(),
        "Malformed transaction: Transaction x has too large size 5".to_owned(),
        "Malformed transaction: Min fee not met: 1 ergs required, 0 ergs given".to_owned(),
        "Transaction pays less than any other in the pool being full, twice".to_owned(),
        "Transaction x has too large size".to_owned(),
        "a message from a newer node".to_owned(),
        String::new(),
    ] {
        assert_eq!(Refusal::of(&message), Refusal::Invalid, "{message:?}");
    }
}

/// A service over a simulated node, which takes no tx: whatever reaches it is a `501`.
fn simulated() -> (SimChain, Transactions<SimChain>) {
    let sim = SimChain::new(Chain::new(SimConfig::default()));
    (sim.clone(), Transactions::new(sim))
}

fn reason(result: Result<TxId, ApiError>) -> &'static str {
    match result {
        Err(ApiError::TransactionRejected { reason, message }) => {
            assert!(!message.is_empty());
            reason.code()
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_body_that_is_not_hex_or_too_long_costs_no_node_call() {
    let (sim, transactions) = simulated();
    for body in ["", "  \n", "abc", "zz", "0x00", "00 11", "é0", "\"\"", "\"00", "\"\"00\"\""] {
        assert_eq!(reason(ready(transactions.submit(body, true))), "unparseable", "{body:?}");
    }
    let over = "00".repeat(MAX_TX_BYTES + 1);
    assert_eq!(reason(ready(transactions.submit(&over, false))), "too_large");
    assert!(sim.calls().is_empty(), "{:?}", sim.calls());
}

/// The cap is inclusive, as the node's (`tx.size > maxTransactionSize`), and whitespace
/// around the hex is left out, in any case of hex.
#[test]
fn a_tx_of_the_largest_size_and_whitespace_around_hex_reach_the_node() {
    let (sim, transactions) = simulated();
    let largest = "aB".repeat(MAX_TX_BYTES);
    let quoted = format!("\"{}\"", signed_tx());
    for body in [largest, format!("\n {}\r\n", signed_tx()), quoted] {
        let error = ready(transactions.submit(&body, true)).unwrap_err();
        assert_eq!(error.code(), "node_error");
    }
    assert_eq!(sim.calls(), [Endpoint::SubmitTransaction; 3]);
}

/// A service whose node answers `route` with `response`.
async fn answering(
    route: &str,
    response: ResponseTemplate,
) -> (MockServer, Transactions<NodeClient>) {
    let server = MockServer::start().await;
    Mock::given(path(route))
        .respond_with(response)
        .mount(&server)
        .await;
    let node = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
    (server, Transactions::new(node))
}

fn recorded(name: &str) -> ResponseTemplate {
    ResponseTemplate::new(status(name)).set_body_raw(fixtures::text(name), "application/json")
}

#[tokio::test]
async fn the_node_takes_the_signed_tx_and_answers_its_id() {
    let id: TxId = fixtures::load("tx_check_signed");
    let (_server, transactions) =
        answering("/transactions/checkBytes", recorded("tx_check_signed")).await;
    assert_eq!(transactions.submit(&signed_tx(), false).await, Ok(id));
    let (_server, transactions) =
        answering("/transactions/bytes", recorded("tx_send_signed")).await;
    assert_eq!(transactions.submit(&signed_tx(), true).await, Ok(id));
}

/// The node's message is passed on, with its reason, from either route.
#[tokio::test]
async fn a_refusal_carries_the_nodes_message_and_its_reason() {
    for (route, name) in [
        ("/transactions/bytes", "tx_send_invalid"),
        ("/transactions/checkBytes", "tx_check_unparseable"),
        ("/transactions/bytes", "tx_send_double_spend"),
    ] {
        let (_server, transactions) = answering(route, recorded(name)).await;
        let error = transactions
            .submit(&signed_tx(), route.ends_with("bytes"))
            .await;
        let expected =
            ApiError::TransactionRejected { reason: expected_reason(name), message: detail(name) };
        assert_eq!(error, Err(expected), "{name}");
    }
}

#[tokio::test]
async fn other_node_answers_map_as_on_every_route() {
    let null_detail = ResponseTemplate::new(400)
        .set_body_raw(r#"{"error":400,"reason":"bad.request","detail":null}"#, "application/json");
    let (_server, transactions) = answering("/transactions/bytes", null_detail).await;
    assert_eq!(reason(transactions.submit(&signed_tx(), true).await), "invalid");

    let busy = ResponseTemplate::new(503).insert_header("Retry-After", "7");
    let (_server, transactions) = answering("/transactions/bytes", busy).await;
    let error = transactions.submit(&signed_tx(), true).await.unwrap_err();
    assert_eq!(error, ApiError::NodeUnavailable { retry_after: Some(Duration::from_secs(7)) });

    let (_server, transactions) =
        answering("/transactions/bytes", ResponseTemplate::new(500)).await;
    let error = transactions.submit(&signed_tx(), true).await.unwrap_err();
    assert_eq!(error.code(), "node_error");
}
