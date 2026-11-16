//! Layer 7: smoke tests against a real indexed node (DESIGN.md, "Layer 7"). Ignored by
//! default; run them with
//!
//! ```sh
//! ERGO_NODE_URL=http://127.0.0.1:9053 cargo test --test smoke -- --ignored
//! ```
//!
//! The keys are the recorded fixtures' (`tests/fixtures/manifest.json`), on mainnet.
//! `HERGMES_SMOKE_ADDRESS` names another address, for a testnet node. Only `ERGO_NODE_URL` is
//! read of the relay's settings: the tests build the relay's parts themselves, so the keys an
//! operator configured in the environment are not loaded.
//!
//! These are not the main source of correctness, which is the simulator. They check what it
//! cannot: that the relay's view of real keys is the node's own, read through other code
//! paths of the node than the ones the relay loads from.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use hergmes::{
    adapters::{BoxCache, BoxFetcher, Follower, TokenChecker, loader::Loader},
    api::{ApiError, Balances, History, Pages, pages},
    clients::NodeClient,
    engine::{BlockRef, Engine, StoredBox, Totals, TrackingKey},
    runtime::{self, Handle},
    types::{
        address::Address,
        ergo::{BoxId, TokenId, TxId},
    },
};
use serde_json::Value;
use tokio::{task::JoinHandle, time::sleep};

/// A block can land between any two reads, and the indexer applies it a moment after the
/// node: a comparison across a moved tip is done again, this many times.
const ATTEMPTS: u32 = 10;
/// The node's cap on a page of `/blockchain/box/byAddress` (node 6.0.8, `c1a69f65`,
/// `BlockchainApiRoute.MaxItems`, checked in `validateAndGetBoxesByAddress`).
const NODE_PAGE: usize = 16_384;

/// The relay's parts over a real node, as `main` wires them, with no configured keys and no
/// HTTP: the tests call the services the routes call.
struct Relay {
    http: reqwest::Client,
    url: String,
    engine: Handle,
    pages: Pages<NodeClient>,
    balances: Balances<NodeClient>,
    history: History<NodeClient>,
    _follower: JoinHandle<()>,
}

/// The node's tip as a first load reads it: the best full block and the indexer's height.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeTip {
    header_id: String,
    full_height: u64,
    indexed_height: u64,
}

impl NodeTip {
    /// Whether the node, its indexer and the relay stand at one block.
    fn is(&self, block: &BlockRef) -> bool {
        self.full_height == u64::from(block.height)
            && self.indexed_height == self.full_height
            && self.header_id == block.header_id.to_string()
    }
}

impl Relay {
    async fn start() -> Self {
        let url =
            std::env::var("ERGO_NODE_URL").expect("ERGO_NODE_URL names the node to test against");
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        let node = NodeClient::connect(http.clone(), &url)
            .await
            .expect("the node answers");
        let (engine, _) = runtime::spawn(Engine::new(Default::default()));
        let follower = Follower::new(node.clone(), Default::default());
        let mut synced = follower.synced();
        let follower = tokio::spawn({
            let engine = engine.clone();
            async move {
                follower.run(&engine).await;
            }
        });
        synced
            .wait_for(|synced| *synced)
            .await
            .expect("the follower runs");

        let cache = BoxCache::new(64 << 20);
        let loader = Arc::new(Loader::new(node.clone(), Default::default()).seeding(cache.clone()));
        let tokens = Arc::new(TokenChecker::new(node.clone(), 100));
        Self {
            http,
            url: url.trim_end_matches('/').to_owned(),
            engine,
            pages: Pages::new(loader, tokens, BoxFetcher::new(node.clone(), cache)),
            balances: Balances::new(node.clone(), Default::default()),
            history: History::new(node, Default::default()),
            _follower: follower,
        }
    }

    async fn get(&self, path: &str) -> Value {
        let response = self.http.get(format!("{}{path}", self.url)).send().await;
        response
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    /// A `POST` with the address as the body, as the node's `/blockchain` routes take it.
    async fn post(&self, path: &str, address: &str) -> Value {
        let request = self
            .http
            .post(format!("{}{path}", self.url))
            .body(address.to_owned());
        let response = request.send().await;
        response
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn node_tip(&self) -> NodeTip {
        let info = self.get("/info").await;
        let indexed = self.get("/blockchain/indexedHeight").await;
        NodeTip {
            header_id: info["bestFullHeaderId"].as_str().unwrap().to_owned(),
            full_height: info["fullHeight"].as_u64().unwrap(),
            indexed_height: indexed["indexedHeight"].as_u64().unwrap(),
        }
    }

    /// Every box of `key`, paged at one block, loading the key if it is not tracked. `None`
    /// if a page was not ready or its block left the chain meanwhile: the caller starts
    /// again.
    async fn boxes(&self, key: &TrackingKey) -> Option<(BlockRef, Vec<StoredBox>)> {
        let mut boxes = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .pages
                .page(&self.engine, key, cursor.as_deref(), Some(pages::MAX_LIMIT))
                .await;
            let page = match page {
                Ok(page) => page,
                Err(ApiError::NotReady { .. } | ApiError::CursorExpired) => return None,
                Err(error) => panic!("{key:?}: {error}"),
            };
            boxes.extend(page.items);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return Some((page.as_of, boxes)),
            }
        }
    }

    /// The node's confirmed balance of `address`, through `/blockchain/balance`.
    async fn node_balance(&self, address: &str) -> Totals {
        let balance = self.post("/blockchain/balance", address).await;
        let confirmed = &balance["confirmed"];
        let tokens = confirmed["tokens"].as_array().unwrap().iter().map(|t| {
            let id: TokenId = t["tokenId"].as_str().unwrap().parse().unwrap();
            (id, t["amount"].as_u64().unwrap())
        });
        Totals { nano_ergs: confirmed["nanoErgs"].as_u64().unwrap(), tokens: tokens.collect() }
    }
}

fn manifest() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/manifest.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// The address under test, and its key.
fn address() -> (String, TrackingKey) {
    let address = std::env::var("HERGMES_SMOKE_ADDRESS").unwrap_or_else(|_| {
        manifest()["samples"]["address"]
            .as_str()
            .unwrap()
            .to_owned()
    });
    let tree = Address::decode(&address)
        .expect("a valid address")
        .ergo_tree()
        .clone();
    (address, TrackingKey::Tree(tree))
}

fn token(sample: &str) -> TrackingKey {
    TrackingKey::Token(
        manifest()["samples"][sample]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
    )
}

fn totals(boxes: &[StoredBox]) -> Totals {
    let mut totals = Totals::default();
    for b in boxes {
        totals.add(&b.utxo);
    }
    totals
}

/// The boxes are the key's, each once, in `(inclusionHeight, boxId)` order.
fn check_boxes(key: &TrackingKey, boxes: &[StoredBox]) {
    for b in boxes {
        assert!(key.matches(&b.utxo), "{key:?}: box {} is not the key's", b.utxo.id);
    }
    let order: Vec<(u32, BoxId)> = boxes
        .iter()
        .map(|b| (b.inclusion_height, b.utxo.id))
        .collect();
    assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{key:?}: keyset order, no box twice");
}

/// An address, an NFT and a small-supply token are each loaded and served: the boxes are the
/// key's, and for the address the balance the relay keeps is the sum of the boxes it serves.
#[tokio::test]
#[ignore = "needs a real node: set ERGO_NODE_URL"]
async fn an_address_an_nft_and_a_small_supply_token_are_tracked_and_served() {
    let relay = Relay::start().await;
    let (_, address) = address();
    for key in [address.clone(), token("nft"), token("smallSupplyToken")] {
        let (as_of, boxes) = relay.boxes(&key).await.expect("a quiet first load");
        check_boxes(&key, &boxes);
        println!("{key:?}: {} boxes at {}", boxes.len(), as_of.height);
        match &key {
            TrackingKey::Token(_) => {
                assert!((1..=100).contains(&boxes.len()), "at most the supply")
            }
            TrackingKey::Tree(_) => assert!(!boxes.is_empty(), "the address holds boxes"),
        }
        let view = relay.pages.with_pool(&relay.engine, &key).await.unwrap();
        assert!(view.total >= view.pending.len(), "the view with the pool is served");
    }
    assert_eq!(boxes_of(&relay, &token("nft")).await.len(), 1, "an NFT is one box");

    let TrackingKey::Tree(tree) = &address else { unreachable!() };
    for _ in 0..ATTEMPTS {
        let Some((as_of, boxes)) = relay.boxes(&address).await else { continue };
        let balance = relay.balances.balance(&relay.engine, tree).await.unwrap();
        if balance.height != as_of.height {
            continue;
        }
        assert_eq!(balance.confirmed, totals(&boxes), "running totals against the boxes served");
        assert!(balance.used);
        return;
    }
    panic!("the tip moved between the pages and the balance {ATTEMPTS} times");
}

async fn boxes_of(relay: &Relay, key: &TrackingKey) -> Vec<StoredBox> {
    relay.boxes(key).await.expect("a quiet read").1
}

/// At the indexer's tip, the relay's first load and running totals against two other code
/// paths of the node: `/blockchain/box/byAddress` (every box the address ever had, kept when
/// it has no `spentTransactionId`; the relay loads from `unspent/byErgoTree`) and
/// `/blockchain/balance`. The tip and the indexed height are read before and after, as a
/// first load does, and a comparison a block landed in is done again.
#[tokio::test]
#[ignore = "needs a real node: set ERGO_NODE_URL"]
async fn a_first_load_is_the_indexers_unspent_boxes_and_balance() {
    let relay = Relay::start().await;
    let (address, key) = address();
    let TrackingKey::Tree(tree) = &key else { unreachable!() };
    for attempt in 1..=ATTEMPTS {
        let before = relay.node_tip().await;
        let Some((as_of, boxes)) = relay.boxes(&key).await else { continue };
        let relay_balance = relay.balances.balance(&relay.engine, tree).await.unwrap();

        let mut unspent = BTreeSet::new();
        let mut ever = 0;
        loop {
            let path = format!("/blockchain/box/byAddress?offset={ever}&limit={NODE_PAGE}");
            let page = relay.post(&path, &address).await;
            let items = page["items"].as_array().unwrap();
            ever += items.len();
            for b in items
                .iter()
                .filter(|b| b.get("spentTransactionId").is_none_or(Value::is_null))
            {
                unspent.insert(b["boxId"].as_str().unwrap().parse::<BoxId>().unwrap());
            }
            if items.is_empty() || ever as u64 >= page["total"].as_u64().unwrap() {
                break;
            }
        }
        let node_balance = relay.node_balance(&address).await;
        let after = relay.node_tip().await;

        if before != after || !after.is(&as_of) || relay_balance.height != as_of.height {
            println!(
                "attempt {attempt}: the tip moved ({before:?} to {after:?}, relay at {as_of:?})"
            );
            sleep(Duration::from_secs(3)).await;
            continue;
        }
        check_boxes(&key, &boxes);
        let loaded: BTreeSet<BoxId> = boxes.iter().map(|b| b.utxo.id).collect();
        println!(
            "{address} at {}: {} unspent of {ever} boxes ever, {} nanoERG",
            as_of.height,
            loaded.len(),
            node_balance.nano_ergs
        );
        assert_eq!(loaded, unspent, "the first load against box/byAddress");
        assert_eq!(
            relay_balance.confirmed, node_balance,
            "running totals against /blockchain/balance"
        );
        assert_eq!(totals(&boxes), node_balance, "the boxes served against /blockchain/balance");
        return;
    }
    panic!("the node's tip moved during each of {ATTEMPTS} comparisons");
}

/// An address's whole history, paged to its end: no tx twice, as many confirmed txs as the
/// indexer lists, and their changes summing to the address's confirmed balance. A scroll a
/// block landed in is done again: a new tx changes both the count and the balance.
#[tokio::test]
#[ignore = "needs a real node: set ERGO_NODE_URL"]
async fn an_addresss_whole_history_pages_to_its_balance() {
    let relay = Relay::start().await;
    let (address, key) = address();
    let TrackingKey::Tree(tree) = &key else { unreachable!() };
    'attempt: for attempt in 1..=ATTEMPTS {
        let before = relay.node_tip().await;
        let mut confirmed: Vec<TxId> = Vec::new();
        let mut sum = (0i128, BTreeMap::<TokenId, i128>::new());
        let (mut cursor, mut pages, mut stalled) = (None::<String>, 0, 0);
        loop {
            let limit = Some(hergmes::api::history::MAX_LIMIT);
            let page = match relay
                .history
                .page(&relay.engine, tree, cursor.as_deref(), limit)
                .await
            {
                Ok(page) => page,
                Err(error @ (ApiError::NotReady { .. } | ApiError::CursorExpired)) => {
                    println!("attempt {attempt}: {error}");
                    sleep(Duration::from_secs(3)).await;
                    continue 'attempt;
                }
                Err(error) => panic!("{error}"),
            };
            pages += 1;
            // A page can serve nothing and keep its cursor while the list moves under it,
            // but a scroll that stops advancing would never end: it is a failure.
            let served = page.items.iter().any(|item| item.included.is_some());
            stalled = if served { 0 } else { stalled + 1 };
            assert!(stalled < 3, "three pages in a row served no confirmed tx");
            for item in page.items.iter().filter(|item| item.included.is_some()) {
                confirmed.push(item.tx_id);
                sum.0 += item.delta.nano_ergs;
                for (id, amount) in &item.delta.tokens {
                    *sum.1.entry(*id).or_default() += amount;
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let listed = relay
            .post("/blockchain/transaction/byAddress?limit=1", &address)
            .await;
        let balance = relay.node_balance(&address).await;
        let after = relay.node_tip().await;
        if before != after || after.indexed_height != after.full_height {
            println!("attempt {attempt}: the tip moved ({before:?} to {after:?})");
            sleep(Duration::from_secs(3)).await;
            continue;
        }

        println!("{address}: {} txs over {pages} pages", confirmed.len());
        assert!(pages >= 3, "the history spans pages: {pages}");
        let distinct: HashSet<&TxId> = confirmed.iter().collect();
        assert_eq!(distinct.len(), confirmed.len(), "no tx twice");
        assert_eq!(confirmed.len() as u64, listed["total"].as_u64().unwrap(), "every tx listed");
        sum.1.retain(|_, amount| *amount != 0);
        let expected = balance
            .tokens
            .iter()
            .map(|(id, amount)| (*id, i128::from(*amount)));
        assert_eq!(
            sum,
            (i128::from(balance.nano_ergs), expected.collect()),
            "changes sum to the balance"
        );
        return;
    }
    panic!("the node's tip moved during each of {ATTEMPTS} scrolls");
}
