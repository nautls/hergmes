//! The balance reader against `wiremock`: the height it reports, and an indexer moving under
//! it.

use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use super::*;
use crate::{
    clients::NodeClient,
    fixtures::{self, sample},
    types::{HexBytes, address::Network},
};

fn height(indexed: u32) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(json!({ "indexedHeight": indexed, "fullHeight": indexed }))
}

/// Serves the indexed heights in order, then the recorded balance.
async fn reader(heights: &[u32]) -> (MockServer, BalanceReader<NodeClient>) {
    let server = MockServer::start().await;
    for &h in heights {
        Mock::given(method("GET"))
            .and(path("/blockchain/indexedHeight"))
            .respond_with(height(h))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/blockchain/balance"))
        .respond_with(
            ResponseTemplate::new(fixtures::status("balance"))
                .set_body_raw(fixtures::text("balance"), "application/json"),
        )
        .mount(&server)
        .await;
    let client = NodeClient::new(reqwest::Client::new(), &server.uri(), Network::Mainnet);
    (server, BalanceReader::new(client))
}

fn tree() -> ErgoTree {
    HexBytes(hex::decode(sample("ergoTree")).unwrap())
}

#[tokio::test]
async fn a_balance_comes_with_the_height_it_was_read_at() {
    let (_server, reader) = reader(&[10, 10]).await;
    let read = reader.read(&tree()).await.unwrap().unwrap();
    assert_eq!(read.height, 10);
    assert_eq!(read.confirmed.nano_ergs, 361_490_634_617);
}

#[tokio::test]
async fn an_indexer_moving_during_the_call_is_read_again() {
    let (server, reader) = reader(&[10, 11, 11, 11]).await;
    assert_eq!(reader.read(&tree()).await.unwrap().unwrap().height, 11);
    let balance_calls = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/blockchain/balance")
        .count();
    assert_eq!(balance_calls, 2);
}

#[tokio::test]
async fn an_indexer_that_keeps_moving_gives_no_balance() {
    let (_server, reader) = reader(&[10, 11, 12, 13, 14, 15]).await;
    assert_eq!(reader.read(&tree()).await.unwrap(), None);
}

mod track {
    use std::sync::Mutex;

    use super::super::*;
    use crate::{
        engine::{Config as EngineConfig, Engine, Query, Reply, TrackingKey},
        sim::{
            Endpoint, SimChain,
            testing::{new_chain, ready},
        },
    };

    fn synced_engine(chain: &crate::sim::Chain) -> Mutex<Engine> {
        let mut engine = Engine::new(EngineConfig::default());
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
            .unwrap();
        Mutex::new(engine)
    }

    fn count(sim: &SimChain, endpoint: Endpoint) -> usize {
        sim.calls().iter().filter(|e| **e == endpoint).count()
    }

    /// The engine's balance of `tree`, if it tracks it.
    fn tracked(engine: &Mutex<Engine>, tree: &ErgoTree) -> Option<Totals> {
        let query = Query::Balance(TrackingKey::Tree(tree.clone()));
        match engine.lock().unwrap().query(query) {
            Reply::Balance(balance) => balance.map(|b| b.confirmed),
            other => unreachable!("{other:?}"),
        }
    }

    #[test]
    fn an_address_is_tracked_from_its_balance_at_the_tip() {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let reader = BalanceReader::new(sim.clone());
        assert!(ready(reader.try_track(&engine, &tree)).unwrap());
        let expected = {
            let chain = sim.lock();
            chain
                .oracle()
                .totals(&TrackingKey::Tree(tree.clone()), chain.height())
        };
        assert_eq!(tracked(&engine, &tree), Some(expected));
        assert_eq!(count(&sim, Endpoint::UnspentByErgoTree), 0, "no boxes read");
    }

    /// The engine took the tip's block from elsewhere and the indexer has not reached it: its
    /// balance would be the block before's.
    #[test]
    fn an_indexer_behind_the_tip_is_not_read() {
        let mut chain = new_chain();
        chain.set_indexer_lag(1);
        let mut engine = Engine::new(EngineConfig::default());
        let tip = chain.mine(0, 1, 1);
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(tip.height)))
            .unwrap();
        let engine = Mutex::new(engine);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let reader = BalanceReader::new(sim.clone());
        assert!(!ready(reader.try_track(&engine, &tree)).unwrap());
        assert_eq!(count(&sim, Endpoint::ConfirmedBalance), 0);
        assert_eq!(tracked(&engine, &tree), None);
    }

    /// A block during the balance read: the balance may be either block's, so it is dropped.
    #[test]
    fn a_block_during_the_read_drops_it() {
        let chain = new_chain();
        let engine = synced_engine(&chain);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        sim.on_call(move |endpoint, chain| {
            if endpoint == Endpoint::ConfirmedBalance {
                chain.mine(0, 1, 1);
            }
        });
        let reader = BalanceReader::new(sim.clone());
        assert!(!ready(reader.try_track(&engine, &tree)).unwrap());
        assert_eq!(tracked(&engine, &tree), None);
    }

    /// The engine moves after its tip was read, the node's tip staying: it rejects the load,
    /// and the attempt is not taken as tracked.
    #[test]
    fn a_load_the_engine_rejects_is_not_tracked() {
        let chain = new_chain();
        let engine = std::sync::Arc::new(synced_engine(&chain));
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let moved = engine.clone();
        sim.on_call(move |endpoint, _| {
            if endpoint == Endpoint::ConfirmedBalance {
                let mut engine = moved.lock().unwrap();
                let tip = engine.tip().unwrap();
                engine.apply(Event::BlockReverted(tip)).unwrap();
            }
        });
        let reader = BalanceReader::new(sim.clone());
        assert!(!ready(reader.try_track(&*engine, &tree)).unwrap());
        assert_eq!(tracked(&engine, &tree), None);
    }

    /// A balance-only key counts as one against the cap on tracked boxes: at a full cap with
    /// no idle key to evict, it is refused, and nothing is tracked.
    #[test]
    fn a_full_cap_refuses_a_balance_load() {
        let chain = new_chain();
        let mut engine = Engine::new(EngineConfig { max_tracked_boxes: 0, ..Default::default() });
        engine
            .apply(Event::BlockApplied(chain.oracle().indexed_block(chain.height())))
            .unwrap();
        let engine = Mutex::new(engine);
        let sim = SimChain::new(chain);
        let tree = sim.lock().trees()[0].clone();
        let reader = BalanceReader::new(sim.clone());
        let refused = ready(reader.try_track(&engine, &tree));
        assert!(
            matches!(refused, Err(TrackError::CapacityExceeded { tracked: 0, evictable: 0 })),
            "{refused:?}"
        );
        assert_eq!(tracked(&engine, &tree), None);
    }
}
