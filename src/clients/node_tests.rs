//! Recorded node responses: their shapes, and `NodeClient` reading them over HTTP.

use serde_json::Value;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, body_string, header, method, path, query_param},
};

use super::*;
use crate::{
    engine::BlockRef,
    fixtures::{self, load, manifest, sample, status},
    types::{HashDigest, HexBytes, address::Address, ergo::BoxAssets},
};

fn holds(b: &IndexedBox, token_id: &str) -> Option<u64> {
    b.utxo
        .tokens
        .iter()
        .find(|t| t.id.to_string() == token_id)
        .map(|t| t.amount)
}

fn digest(hex: &str) -> HashDigest {
    hex.parse().unwrap()
}

fn sample_tree() -> ErgoTree {
    HexBytes::from(hex::decode(sample("ergoTree")).unwrap())
}

fn assert_not_found(name: &str) {
    assert_eq!(status(name), 404);
    let err: ApiError = load(name);
    assert_eq!(err.reason, "not-found");
}

mod shapes {
    use super::*;

    #[test]
    fn every_manifest_entry_has_a_file() {
        let manifest = manifest();
        let fixtures = manifest["fixtures"].as_object().expect("fixtures map");
        assert!(!fixtures.is_empty());
        for name in fixtures.keys() {
            assert!(fixtures::path(name).exists(), "{name}.json missing");
        }
    }

    #[test]
    fn info() {
        let info: InfoResponse = load("info");
        assert!(info.last_mempool_update > 0);
        assert_eq!(info.network.parse::<Network>().unwrap(), Network::Mainnet);
        let raw: Value = load("info");
        assert_eq!(
            info.best_full_header_id.map(|id| id.to_string()).as_deref(),
            raw["bestFullHeaderId"].as_str()
        );
        assert_eq!(info.full_height.map(u64::from), raw["fullHeight"].as_u64());
    }

    #[test]
    fn info_before_the_first_full_block() {
        // The node drops null fields, so both are absent until a full block is applied.
        let info: InfoResponse =
            serde_json::from_str(r#"{"lastMemPoolUpdateTime": 1, "network": "mainnet"}"#).unwrap();
        assert_eq!((info.best_full_header_id, info.full_height), (None, None));
    }

    #[test]
    fn indexed_height() {
        let h: IndexedHeightResponse = load("indexed_height");
        assert_eq!(h.indexed_height, h.full_height);
    }

    #[test]
    fn indexed_block() {
        let block: IndexedBlock = load("indexed_block");
        assert_eq!(block.header.id, digest(&sample("headerId")));
        assert!(!block.transactions.is_empty());

        let mut spends_own_output = false;
        for (i, tx) in block.transactions.iter().enumerate() {
            assert!(!tx.inputs.is_empty());
            for input in &tx.inputs {
                assert!(input.inclusion_height <= block.header.height);
                if input.inclusion_height == block.header.height {
                    let creator = input.utxo.transaction_id;
                    assert!(block.transactions[..i].iter().any(|t| t.id == creator));
                    spends_own_output = true;
                }
            }
            for output in &tx.outputs {
                assert_eq!(output.utxo.transaction_id, tx.id);
                assert_eq!(output.inclusion_height, block.header.height);
            }
        }
        assert!(spends_own_output, "sample block has no box created and spent in it");
    }

    #[test]
    fn indexed_block_unknown() {
        assert_not_found("indexed_block_unknown");
    }

    #[test]
    fn header_by_id() {
        let header: BlockHeader = load("header_by_id");
        assert_eq!(header.id, digest(&sample("headerId")));
        let block: IndexedBlock = load("indexed_block");
        assert_eq!(BlockRef::from(&header), BlockRef::from(&block.header));
        assert_eq!(header.parent_id, block.header.parent_id);
    }

    #[test]
    fn header_by_id_unknown() {
        assert_not_found("header_by_id_unknown");
    }

    #[test]
    fn mempool_keeps_resolved_transactions() {
        let txs: Vec<MempoolTransactionResponse> = load("mempool");
        let count = txs.len();
        assert!(count > 0);
        assert_eq!(filter_out_unresolved(txs).len(), count);
    }

    #[test]
    fn mempool_drops_transactions_with_unresolved_inputs() {
        let txs: Vec<MempoolTransactionResponse> = load("mempool_unresolved_input");
        assert!(txs[0].inputs[0].utxo.is_none());
        let dropped = txs[0].id;
        let count = txs.len();

        let kept = filter_out_unresolved(txs);
        assert_eq!(kept.len(), count - 1);
        assert!(kept.iter().all(|tx| tx.id != dropped));
    }

    /// A child and grandchild of an unresolved tx resolve their inputs from its outputs (the
    /// node keeps it in its pool), but neither can be valid, so both go with it.
    #[test]
    fn mempool_drops_the_descendants_of_unresolved_transactions() {
        let txs: Vec<MempoolTransactionResponse> = load("mempool");
        assert!(txs.len() >= 2);
        let derive = |parent: &MempoolTransactionResponse, n: u8| {
            let spent = parent.outputs[0].clone();
            let mut output = spent.clone();
            output.id = digest(&format!("{n:02x}").repeat(32));
            output.transaction_id = digest(&format!("{:02x}", n + 1).repeat(32));
            MempoolTransactionResponse {
                id: output.transaction_id,
                inputs: vec![MempoolTransactionInput { utxo: Some(spent) }],
                outputs: vec![output],
            }
        };
        let mut parent = txs[0].clone();
        parent.inputs[0].utxo = None;
        let child = derive(&parent, 0xa0);
        let grandchild = derive(&child, 0xb0);
        let unrelated = txs[1].clone();
        let unrelated_id = unrelated.id;

        // In any order.
        let kept = filter_out_unresolved(vec![grandchild, unrelated, child, parent]);
        let ids: Vec<TxId> = kept.iter().map(|tx| tx.id).collect();
        assert_eq!(ids, vec![unrelated_id]);
    }

    #[test]
    fn unspent_by_ergo_tree() {
        let boxes: Vec<IndexedBox> = load("unspent_by_ergo_tree");
        assert!(boxes.len() >= 2);
        let address = Address::decode(&sample("address")).unwrap();
        assert_eq!(address.ergo_tree(), &sample_tree());
        assert!(boxes.iter().all(|b| b.utxo.ergo_tree == sample_tree()));
        assert!(
            boxes
                .windows(2)
                .all(|w| w[0].inclusion_height <= w[1].inclusion_height)
        );
    }

    #[test]
    fn unspent_by_ergo_tree_empty_is_200_not_404() {
        assert_eq!(status("unspent_by_ergo_tree_empty"), 200);
        let boxes: Vec<IndexedBox> = load("unspent_by_ergo_tree_empty");
        assert!(boxes.is_empty());
    }

    #[test]
    fn balance() {
        let balance: BalanceResponse = load("balance");
        assert!(balance.confirmed.nano_ergs > 0);

        let empty: BalanceResponse = load("balance_empty");
        assert_eq!(empty.confirmed.nano_ergs, 0);
        assert!(empty.confirmed.tokens.is_empty());
    }

    #[test]
    fn box_count_counts_spent_boxes_and_is_0_for_an_unknown_address() {
        let count = |name| load::<BoxCountResponse>(name).total;
        assert!(count("box_count_by_address") > 0);
        // No box left (`balance_empty`), but some received.
        assert!(count("box_count_by_address_empty") > 0);
        assert_eq!(status("box_count_by_address_unused"), 200);
        assert_eq!(count("box_count_by_address_unused"), 0);
        for name in
            ["box_count_by_address", "box_count_by_address_empty", "box_count_by_address_unused"]
        {
            let raw: Value = load(name);
            assert_eq!(raw["items"], Value::Array(Vec::new()), "{name} lists a box");
        }
    }

    #[test]
    fn token_info() {
        let nft: TokenInfo = load("token_nft");
        assert_eq!(nft.emission_amount, 1);

        let small: TokenInfo = load("token_small_supply");
        assert!((2..=100).contains(&small.emission_amount));

        let large: TokenInfo = load("token_large_supply");
        assert!(large.emission_amount > 100);

        assert_not_found("token_unknown");
    }

    /// The batch lists the known tokens in request order, leaving out the unknown id, each as
    /// its own `/token/byId` answers it.
    #[test]
    fn tokens_leave_out_unknown_ids() {
        let requested: Vec<TokenId> =
            serde_json::from_value(manifest()["fixtures"]["tokens"]["body"].clone()).unwrap();
        assert_eq!(requested[2], digest(&"ab".repeat(32)));
        let tokens: Vec<TokenMetadata> = load("tokens");
        let ids: Vec<TokenId> = tokens.iter().map(|t| t.id).collect();
        assert_eq!(ids, [requested[0], requested[1], requested[3]]);
        for (token, alone) in
            tokens
                .iter()
                .zip(["token_nft", "token_small_supply", "token_large_supply"])
        {
            assert_eq!(*token, load::<TokenMetadata>(alone), "{alone}");
        }
        let large = &tokens[2];
        assert!(large.emission > 1 << 53, "read exactly, past a JavaScript number");
        assert_ne!(large.minting_box_id, large.id, "the minting box is not the token id");
    }

    #[test]
    fn unspent_by_token_nft_is_one_box() {
        let boxes: Vec<IndexedBox> = load("unspent_by_token_nft");
        assert_eq!(boxes.len(), 1);
        assert_eq!(holds(&boxes[0], &sample("nft")), Some(1));
    }

    #[test]
    fn unspent_by_token_small_supply_is_bounded_by_emission() {
        let boxes: Vec<IndexedBox> = load("unspent_by_token_small_supply");
        let info: TokenInfo = load("token_small_supply");
        let token = sample("smallSupplyToken");

        assert!(boxes.len() >= 2);
        assert!(boxes.len() as u64 <= info.emission_amount);
        let total: u64 = boxes
            .iter()
            .map(|b| holds(b, &token).expect("box holds the token"))
            .sum();
        assert!(total <= info.emission_amount);
    }

    #[test]
    fn unspent_by_token_unknown_is_200_not_404() {
        assert_eq!(status("unspent_by_token_unknown"), 200);
        let boxes: Vec<IndexedBox> = load("unspent_by_token_unknown");
        assert!(boxes.is_empty());
    }

    #[test]
    fn box_by_id_spent() {
        let _: IndexedBox = load("box_by_id_spent");
        let raw: Value = load("box_by_id_spent");
        assert!(raw["spentTransactionId"].is_string());
        assert!(raw.get("spendingHeight").is_none(), "node now sends spendingHeight");
    }

    #[test]
    fn box_by_id_unspent() {
        let found: BoxById = load("box_by_id_unspent");
        assert!(!found.spent);
        let found: BoxById = load("box_by_id_spent");
        assert!(found.spent);
    }

    /// Every refusal, recorded or hand-written, is the node's `400` with a message in
    /// `detail`; an accepted tx is its id as a JSON string.
    #[test]
    fn tx_answers() {
        let id: TxId = load("tx_check_signed");
        assert_eq!(load::<TxId>("tx_send_signed"), id);
        let fixtures = manifest()["fixtures"].as_object().unwrap().clone();
        let refusals: Vec<_> = fixtures
            .keys()
            .filter(|n| n.starts_with("tx_") && status(n) == 400)
            .collect();
        assert_eq!(refusals.len(), 12);
        for name in refusals {
            let error: ApiError = load(name);
            assert_eq!(error.reason, "bad.request", "{name}");
            assert!(error.detail.is_some_and(|d| !d.is_empty()), "{name}");
        }
    }

    /// A pool tx's input is the box when the node resolved it, `None` when it sent only
    /// `boxId` and `spendingProof`, and a malformed field fails the list, as an output's does.
    #[test]
    fn mempool_inputs_resolved_unresolved_and_malformed() {
        let mut txs: Vec<Value> = load("mempool");
        let tx = txs
            .iter_mut()
            .find(|t| !t["inputs"].as_array().unwrap().is_empty())
            .unwrap();
        let input = tx["inputs"][0].clone();
        let read = |input: &Value| {
            serde_json::from_value::<MempoolTransactionInput>(input.clone()).map(|i| i.utxo)
        };
        let utxo: UTxO = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(read(&input).unwrap(), Some(utxo));
        let bare = serde_json::json!({
            "boxId": input["boxId"],
            "spendingProof": input["spendingProof"],
        });
        assert_eq!(read(&bare).unwrap(), None);
        let mut malformed = input;
        malformed["value"] = "lots".into();
        assert!(read(&malformed).is_err());
        tx["inputs"][0] = malformed;
        let text = serde_json::to_string(&txs).unwrap();
        assert!(serde_json::from_str::<Vec<MempoolTransactionResponse>>(&text).is_err());
    }

    #[test]
    fn box_by_id_unknown() {
        assert_not_found("box_by_id_unknown");
    }

    #[test]
    fn with_pool_omits_missing_ids() {
        let requested = manifest()["fixtures"]["with_pool_partial"]["body"].clone();
        let existing = requested[1].as_str().expect("second id exists").to_string();

        let boxes: Vec<UTxO> = load("with_pool_partial");
        assert_eq!(boxes.len(), 1);
        assert_eq!(boxes[0].id.to_string(), existing);

        assert_eq!(status("with_pool_none"), 200);
        let none: Vec<UTxO> = load("with_pool_none");
        assert!(none.is_empty());
    }

    /// Newest first, offset from the newest end: a read at offset 1 is the tail of one at
    /// offset 0, with the same total. The anchored cursor rests on this.
    #[test]
    fn address_transactions_are_newest_first_offset_from_the_newest() {
        let first: AddressTransactions = load("transactions_by_address");
        let later: AddressTransactions = load("transactions_by_address_offset");
        assert_eq!(first.items.len(), 3);
        assert_eq!(first.total, later.total);
        assert!(first.total > 3);
        assert_eq!(first.items[1..], later.items[..]);
        let heights: Vec<u32> = first.items.iter().map(|t| t.inclusion_height).collect();
        assert!(heights.is_sorted_by(|a, b| a >= b), "newest first: {heights:?}");

        let tree = sample_tree();
        for tx in &first.items {
            assert!(tx.timestamp > 1_500_000_000_000, "milliseconds");
            let touches = |boxes: &[BoxAssets]| boxes.iter().any(|b| b.ergo_tree == tree);
            assert!(touches(&tx.inputs) || touches(&tx.outputs), "{} touches the address", tx.id);
        }
        // Every input is a full box: the node resolves them all for indexed txs.
        let raw: Value = load("transactions_by_address");
        let inputs = raw["items"][0]["inputs"].as_array().unwrap();
        assert_eq!(inputs.len(), first.items[0].inputs.len());
        assert!(inputs.iter().all(|i| i["value"].is_u64()));
    }

    #[test]
    fn an_unused_address_has_an_empty_history() {
        assert_eq!(status("transactions_by_address_empty"), 200);
        let empty: AddressTransactions = load("transactions_by_address_empty");
        assert_eq!(empty, AddressTransactions { items: Vec::new(), total: 0 });
    }
}

mod source {
    use super::*;

    async fn node() -> (MockServer, NodeClient) {
        let server = MockServer::start().await;
        let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
        (server, client)
    }

    /// Replies with a fixture and the status it was recorded with.
    fn recorded(name: &str) -> ResponseTemplate {
        ResponseTemplate::new(status(name)).set_body_raw(fixtures::text(name), "application/json")
    }

    fn json(status: u16, body: Value) -> ResponseTemplate {
        ResponseTemplate::new(status).set_body_json(body)
    }

    fn not_found() -> ResponseTemplate {
        recorded("token_unknown")
    }

    fn page(offset: u32, limit: u32) -> Page {
        Page { offset, limit }
    }

    #[tokio::test]
    async fn connect_reads_the_network() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/info"))
            .respond_with(recorded("info"))
            .expect(1)
            .mount(&server)
            .await;
        let client = NodeClient::connect(reqwest::Client::new(), &server.uri())
            .await
            .unwrap();
        assert_eq!(client.network(), Network::Mainnet);
    }

    #[tokio::test]
    async fn connect_rejects_an_unknown_network() {
        let server = MockServer::start().await;
        Mock::given(path("/info"))
            .respond_with(json(
                200,
                serde_json::json!({"lastMemPoolUpdateTime": 1, "network": "devnet"}),
            ))
            .mount(&server)
            .await;
        let result = NodeClient::connect(reqwest::Client::new(), &server.uri()).await;
        assert!(matches!(result, Err(NodeError::UnknownNetwork(_))));
    }

    #[tokio::test]
    async fn indexed_height() {
        let (server, node) = node().await;
        Mock::given(method("GET"))
            .and(path("/blockchain/indexedHeight"))
            .respond_with(recorded("indexed_height"))
            .expect(1)
            .mount(&server)
            .await;

        let height = node.indexed_height().await.unwrap();
        assert_eq!(height.indexed_height, height.full_height);
    }

    #[tokio::test]
    async fn indexed_block_or_none() {
        let (server, node) = node().await;
        let id = digest(&sample("headerId"));
        let unknown = digest(&"ab".repeat(32));
        Mock::given(method("GET"))
            .and(path(format!("/blockchain/block/byHeaderId/{id}")))
            .respond_with(recorded("indexed_block"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/blockchain/block/byHeaderId/{unknown}")))
            .respond_with(recorded("indexed_block_unknown"))
            .expect(1)
            .mount(&server)
            .await;

        assert_eq!(node.indexed_block(&id).await.unwrap().unwrap().header.id, id);
        assert_eq!(node.indexed_block(&unknown).await.unwrap(), None);
    }

    #[tokio::test]
    async fn header_or_none() {
        let (server, node) = node().await;
        let id = digest(&sample("headerId"));
        let unknown = digest(&"ab".repeat(32));
        Mock::given(method("GET"))
            .and(path(format!("/blocks/modifier/{id}")))
            .respond_with(recorded("header_by_id"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/blocks/modifier/{unknown}")))
            .respond_with(recorded("header_by_id_unknown"))
            .expect(1)
            .mount(&server)
            .await;

        let header = node.header(&id).await.unwrap().unwrap();
        assert_eq!(header.id, id);
        assert_eq!(header.json, load::<BlockHeader>("header_by_id").json);
        assert_eq!(node.header(&unknown).await.unwrap(), None);
    }

    #[tokio::test]
    async fn mempool_drops_unresolved_transactions() {
        let (server, node) = node().await;
        Mock::given(method("GET"))
            .and(path("/transactions/unconfirmed"))
            .respond_with(recorded("mempool_unresolved_input"))
            .expect(1)
            .mount(&server)
            .await;

        let all: Vec<Value> = load("mempool_unresolved_input");
        let txs = node.mempool().await.unwrap();
        assert_eq!(txs.len(), all.len() - 1);
        assert!(txs.iter().all(|tx| !tx.inputs.is_empty()));
    }

    #[tokio::test]
    async fn unspent_by_ergo_tree_posts_the_tree_and_pages_oldest_first() {
        let (server, node) = node().await;
        let tree = sample_tree();
        Mock::given(method("POST"))
            .and(path("/blockchain/box/unspent/byErgoTree"))
            .and(body_json(sample("ergoTree")))
            .and(query_param("offset", "10"))
            .and(query_param("limit", "5"))
            .and(query_param("sortDirection", "asc"))
            .respond_with(recorded("unspent_by_ergo_tree"))
            .expect(1)
            .mount(&server)
            .await;

        let boxes = node.unspent_by_ergo_tree(&tree, page(10, 5)).await.unwrap();
        assert!(!boxes.is_empty());
        assert!(boxes.iter().all(|b| b.utxo.ergo_tree == tree));
    }

    #[tokio::test]
    async fn unspent_empty_or_404_is_an_empty_set() {
        let (server, node) = node().await;
        let empty = HexBytes::from(vec![0x01]);
        let missing = HexBytes::from(vec![0x02]);
        let token = digest(&"ab".repeat(32));
        Mock::given(body_json("01"))
            .respond_with(recorded("unspent_by_ergo_tree_empty"))
            .mount(&server)
            .await;
        Mock::given(body_json("02"))
            .respond_with(not_found())
            .mount(&server)
            .await;
        Mock::given(path(format!("/blockchain/box/unspent/byTokenId/{token}")))
            .respond_with(not_found())
            .mount(&server)
            .await;

        assert!(
            node.unspent_by_ergo_tree(&empty, page(0, 5))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            node.unspent_by_ergo_tree(&missing, page(0, 5))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            node.unspent_by_token(&token, page(0, 5))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn unspent_by_token() {
        let (server, node) = node().await;
        let token = digest(&sample("smallSupplyToken"));
        Mock::given(method("GET"))
            .and(path(format!("/blockchain/box/unspent/byTokenId/{token}")))
            .and(query_param("offset", "0"))
            .and(query_param("limit", "100"))
            .and(query_param("sortDirection", "asc"))
            .respond_with(recorded("unspent_by_token_small_supply"))
            .expect(1)
            .mount(&server)
            .await;

        let boxes = node.unspent_by_token(&token, page(0, 100)).await.unwrap();
        assert!(boxes.len() >= 2);
    }

    #[tokio::test]
    async fn token_info_or_none() {
        let (server, node) = node().await;
        let nft = digest(&sample("nft"));
        let unknown = digest(&"ab".repeat(32));
        Mock::given(path(format!("/blockchain/token/byId/{nft}")))
            .respond_with(recorded("token_nft"))
            .mount(&server)
            .await;
        Mock::given(path(format!("/blockchain/token/byId/{unknown}")))
            .respond_with(recorded("token_unknown"))
            .mount(&server)
            .await;

        assert_eq!(
            node.token_info(&nft)
                .await
                .unwrap()
                .unwrap()
                .emission_amount,
            1
        );
        assert_eq!(node.token_info(&unknown).await.unwrap(), None);
    }

    #[tokio::test]
    async fn tokens_posts_ids() {
        let (server, node) = node().await;
        let requested = manifest()["fixtures"]["tokens"]["body"].clone();
        let ids: Vec<TokenId> = serde_json::from_value(requested.clone()).unwrap();
        Mock::given(method("POST"))
            .and(path("/blockchain/tokens"))
            .and(body_json(requested))
            .respond_with(recorded("tokens"))
            .expect(1)
            .mount(&server)
            .await;

        let tokens = node.tokens(&ids).await.unwrap();
        assert_eq!(tokens, load::<Vec<TokenMetadata>>("tokens"));
    }

    #[tokio::test]
    async fn boxes_with_pool_posts_ids() {
        let (server, node) = node().await;
        let requested = manifest()["fixtures"]["with_pool_partial"]["body"].clone();
        let ids: Vec<BoxId> = serde_json::from_value(requested.clone()).unwrap();
        Mock::given(method("POST"))
            .and(path("/utxo/withPool/byIds"))
            .and(body_json(requested))
            .respond_with(recorded("with_pool_partial"))
            .expect(1)
            .mount(&server)
            .await;

        let boxes = node.boxes_with_pool(&ids).await.unwrap();
        assert_eq!(boxes.len(), 1);
        assert_eq!(boxes[0].id, ids[1]);
    }

    #[tokio::test]
    async fn indexed_box_or_none() {
        let (server, node) = node().await;
        let spent = manifest()["fixtures"]["box_by_id_spent"]["path"]
            .as_str()
            .unwrap()
            .rsplit('/')
            .next()
            .map(digest)
            .unwrap();
        let unknown = digest(&"ab".repeat(32));
        Mock::given(path(format!("/blockchain/box/byId/{spent}")))
            .respond_with(recorded("box_by_id_spent"))
            .mount(&server)
            .await;
        Mock::given(path(format!("/blockchain/box/byId/{unknown}")))
            .respond_with(recorded("box_by_id_unknown"))
            .mount(&server)
            .await;

        let unspent = manifest()["fixtures"]["box_by_id_unspent"]["path"]
            .as_str()
            .unwrap()
            .rsplit('/')
            .next()
            .map(digest)
            .unwrap();
        Mock::given(path(format!("/blockchain/box/byId/{unspent}")))
            .respond_with(recorded("box_by_id_unspent"))
            .mount(&server)
            .await;

        let found = node.indexed_box(&spent).await.unwrap().unwrap();
        assert_eq!((found.indexed.utxo.id, found.spent), (spent, true));
        let found = node.indexed_box(&unspent).await.unwrap().unwrap();
        assert_eq!((found.indexed.utxo.id, found.spent), (unspent, false));
        assert_eq!(node.indexed_box(&unknown).await.unwrap(), None);
    }

    /// The hex goes as it is, as `text/plain`, to the route `broadcast` picks.
    async fn submitted(route: &str, fixture: &str, broadcast: bool) -> Result<TxId, NodeError> {
        let (server, node) = node().await;
        let hex = fixtures::signed_tx();
        Mock::given(method("POST"))
            .and(path(route))
            .and(header("content-type", "text/plain"))
            .and(body_string(hex.clone()))
            .respond_with(recorded(fixture))
            .expect(1)
            .mount(&server)
            .await;
        node.submit_transaction(&hex, broadcast).await
    }

    #[tokio::test]
    async fn a_tx_is_checked_or_sent_and_its_id_read() {
        let id: TxId = load("tx_check_signed");
        let checked = submitted("/transactions/checkBytes", "tx_check_signed", false).await;
        assert_eq!(checked.unwrap(), id);
        let sent = submitted("/transactions/bytes", "tx_send_signed", true).await;
        assert_eq!(sent.unwrap(), id);
    }

    #[tokio::test]
    async fn a_refused_tx_is_a_400_with_the_nodes_message() {
        let error = submitted("/transactions/bytes", "tx_send_invalid", true).await;
        let Err(NodeError::Api { status, detail: Some(detail), .. }) = error else {
            panic!("{error:?}")
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(detail.starts_with("Malformed transaction: "), "{detail}");
    }

    #[tokio::test]
    async fn confirmed_balance_posts_the_trees_address() {
        let (server, node) = node().await;
        Mock::given(method("POST"))
            .and(path("/blockchain/balance"))
            .and(body_json(sample("address")))
            .respond_with(recorded("balance"))
            .expect(1)
            .mount(&server)
            .await;

        let balance = node.confirmed_balance(&sample_tree()).await.unwrap();
        assert!(balance.nano_ergs > 0);
    }

    #[tokio::test]
    async fn box_count_posts_the_trees_address_and_asks_for_no_box() {
        let (server, node) = node().await;
        for name in
            ["box_count_by_address", "box_count_by_address_empty", "box_count_by_address_unused"]
        {
            let address = manifest()["fixtures"][name]["body"].clone();
            Mock::given(method("POST"))
                .and(path("/blockchain/box/byAddress"))
                .and(query_param("limit", "0"))
                .and(body_json(address.clone()))
                .respond_with(recorded(name))
                .expect(1)
                .mount(&server)
                .await;
            let tree = Address::decode(address.as_str().unwrap())
                .unwrap()
                .into_ergo_tree();
            let total = load::<BoxCountResponse>(name).total;
            assert_eq!(node.box_count(&tree).await.unwrap(), total, "{name}");
        }
    }

    #[tokio::test]
    async fn address_transactions_post_the_trees_address_with_offset_and_limit() {
        let (server, node) = node().await;
        Mock::given(method("POST"))
            .and(path("/blockchain/transaction/byAddress"))
            .and(body_json(sample("address")))
            .and(query_param("offset", "1"))
            .and(query_param("limit", "2"))
            .respond_with(recorded("transactions_by_address_offset"))
            .expect(1)
            .mount(&server)
            .await;

        let page = node
            .address_transactions(&sample_tree(), 1, 2)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert!(page.total > 2);
    }

    /// The history call waits past the client's own timeout, which still bounds the rest.
    #[tokio::test]
    async fn address_transactions_wait_past_the_clients_timeout() {
        let server = MockServer::start().await;
        let slow = |name: &str| recorded(name).set_delay(std::time::Duration::from_millis(400));
        Mock::given(path("/blockchain/transaction/byAddress"))
            .respond_with(slow("transactions_by_address_offset"))
            .mount(&server)
            .await;
        Mock::given(path("/info"))
            .respond_with(slow("info"))
            .mount(&server)
            .await;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();
        let node = NodeClient::new(client, &server.uri(), Network::Mainnet);

        assert!(node.info().await.is_err(), "the client's timeout");
        let page = node.address_transactions(&sample_tree(), 1, 2).await;
        assert_eq!(page.unwrap().items.len(), 2);
    }

    #[tokio::test]
    async fn unavailable_carries_retry_after() {
        let (server, node) = node().await;
        Mock::given(path("/blockchain/indexedHeight"))
            .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "2"))
            .mount(&server)
            .await;
        Mock::given(path("/blockchain/token/byId/".to_string() + &"ab".repeat(32)))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        assert!(matches!(
            node.indexed_height().await,
            Err(NodeError::Unavailable { retry_after: Some(d) }) if d == Duration::from_secs(2)
        ));
        assert!(matches!(
            node.token_info(&digest(&"ab".repeat(32))).await,
            Err(NodeError::Unavailable { retry_after: None })
        ));
    }

    #[tokio::test]
    async fn other_errors_carry_the_reason() {
        let (server, node) = node().await;
        let body = serde_json::json!({
            "error": 400,
            "reason": "bad.request",
            "detail": "No more than 16384 boxes can be requested"
        });
        Mock::given(path("/blockchain/box/unspent/byErgoTree"))
            .respond_with(json(400, body))
            .mount(&server)
            .await;
        Mock::given(path("/info"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let err = node
            .unspent_by_ergo_tree(&sample_tree(), page(0, 20000))
            .await
            .unwrap_err();
        let NodeError::Api { status, reason, detail } = err else { panic!("{err:?}") };
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(reason, "bad.request");
        assert_eq!(detail.as_deref(), Some("No more than 16384 boxes can be requested"));

        let err = node.info().await.unwrap_err();
        assert!(matches!(err, NodeError::Api { reason, .. } if reason == "boom"));
    }
}
