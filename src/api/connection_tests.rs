//! A connection over in-memory channels, against a real engine task and `SimChain`, on a
//! paused clock: the handshake, the stream following the chain through reorgs, refusals,
//! replace and unsubscribe, backpressure and keepalive.
//!
//! The client here keeps a view of each key from the messages, as the SDK will, and checks it
//! against the oracle: a block must extend the block before it, a rollback must undo the tip.

use std::{collections::BTreeMap, time::Duration};

use serde_json::{Value, json};
use tokio::{task::JoinHandle, time::sleep};

use super::*;
use crate::{
    adapters::{BoxCache, BoxFetcher, EngineHandle, Follower, Loader, TokenChecker},
    api::{Balances, KeyPolicy, Pages, ws::Closed},
    engine::{self, Engine, Event, Query, Reply},
    runtime::{Settled, settling, spawn},
    sim::{Chain, Endpoint, SimChain, SimConfig, token_id},
    types::{
        address::{self, Network},
        ergo::{BoxId, HeaderId},
    },
};

const CONFIG: engine::Config = engine::Config { window: 8, grace: 3, ..engine::Config::DEFAULT };
/// How long a client waits for the next message before it takes the connection as quiet.
const QUIET: Duration = Duration::from_millis(100);

struct Fixture {
    sim: SimChain,
    engine: Handle,
    _task: JoinHandle<()>,
    follower: Follower<SimChain>,
    api: Arc<Api<SimChain, Handle>>,
}

impl Fixture {
    async fn new() -> Self {
        Self::waiting_for(Settled::now()).await
    }

    /// Loading no key for a client until `settled` is.
    async fn waiting_for(settled: Settled) -> Self {
        Self::with(CONFIG, settled).await
    }

    async fn with(config: engine::Config, settled: Settled) -> Self {
        let mut chain = Chain::new(SimConfig { recovery_reads: 0, ..SimConfig::default() });
        chain.mine(0, 2, 1);
        let sim = SimChain::new(chain);
        let (engine, task) = spawn(Engine::new(config));
        let cache = BoxCache::new(1 << 20);
        let follower = Follower::new(sim.clone(), Default::default());
        let api = Arc::new(Api {
            engine: engine.clone(),
            pages: Pages::new(
                Arc::new(Loader::new(sim.clone(), Default::default()).seeding(cache.clone())),
                Arc::new(TokenChecker::new(sim.clone(), 100)),
                BoxFetcher::new(sim.clone(), cache),
            )
            .waiting_for(settled),
            balances: Balances::new(sim.clone(), Default::default()),
            network: Network::Mainnet,
            policy: KeyPolicy::default(),
            synced: follower.synced(),
        });
        let mut f = Self { sim, engine, _task: task, follower, api };
        f.tick().await;
        f
    }

    /// One follower tick: the engine reaches the node's tip, and takes its mempool.
    async fn tick(&mut self) {
        self.follower
            .tick(&self.engine, Instant::now())
            .await
            .unwrap();
    }

    /// Mines a block with a few fresh txs and follows it.
    async fn mine(&mut self, seed: u64) {
        self.sim.lock().mine(0, 3, seed);
        self.tick().await;
    }

    fn connect(&self) -> Client {
        self.connect_with(Config::default(), 1024)
    }

    /// A client whose socket holds `buffer` messages the client has not read.
    fn connect_with(&self, config: Config, buffer: usize) -> Client {
        let (to_server, incoming) = mpsc::channel(16);
        let (outgoing, from_server) = mpsc::channel(buffer);
        let socket = TestSocket { incoming, outgoing };
        let server = tokio::spawn(serve(self.api.clone(), config, socket));
        Client {
            to_server: Some(to_server),
            from_server,
            server,
            answers_pings: true,
            pings: 0,
            closed: None,
        }
    }

    fn address(&self, index: usize) -> String {
        address::encode(&self.sim.lock().trees()[index].0, Network::Mainnet)
    }

    fn tree_key(&self, index: usize) -> TrackingKey {
        TrackingKey::Tree(self.sim.lock().trees()[index].clone())
    }

    /// A client's snapshot through the page service, which loads the key: its boxes and the
    /// block they are as of.
    async fn snapshot(&self, key: &TrackingKey) -> View {
        let mut view = View::default();
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .api
                .pages
                .page(&self.engine, key, cursor.as_deref(), Some(2))
                .await
                .unwrap();
            view.tip = Some(page.as_of);
            for b in &page.items {
                view.boxes.insert(b.utxo.id, b.inclusion_height);
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return view,
            }
        }
    }

    /// Whether the engine tracks `key`. Not a read the grace period counts: a capacity query.
    async fn tracked(&self, key: &TrackingKey) -> bool {
        match self
            .engine
            .query(Query::Capacity(key.clone()))
            .await
            .unwrap()
        {
            Reply::Capacity(room) => room.is_none(),
            other => panic!("{other:?}"),
        }
    }

    /// Checks `view` holds the oracle's set for `key` at the node's tip.
    fn check(&self, key: &TrackingKey, view: &View) {
        let chain = self.sim.lock();
        let tip = chain.tip();
        assert_eq!(view.tip, Some(tip), "the view is at the tip");
        let expected: BTreeMap<BoxId, u32> = chain
            .oracle()
            .unspent(key, tip.height)
            .into_keys()
            .map(|k| (k.box_id, k.inclusion_height))
            .collect();
        assert_eq!(view.boxes, expected, "{key:?}'s boxes, with their heights");
    }
}

struct TestSocket {
    incoming: mpsc::Receiver<Message>,
    outgoing: mpsc::Sender<Message>,
}

impl Socket for TestSocket {
    async fn recv(&mut self) -> Option<Result<Message, RecvError>> {
        self.incoming.recv().await.map(Ok)
    }

    async fn send(&mut self, message: Message) -> Result<(), Closed> {
        self.outgoing.send(message).await.map_err(|_| Closed)
    }
}

struct Client {
    /// `None` once the client has hung up.
    to_server: Option<mpsc::Sender<Message>>,
    from_server: mpsc::Receiver<Message>,
    server: JoinHandle<()>,
    answers_pings: bool,
    pings: usize,
    /// The close frame's code and reason, or `None` in the code when the connection ended
    /// without one.
    closed: Option<(Option<u16>, String)>,
}

impl Client {
    async fn send_text(&self, text: String) {
        let to_server = self.to_server.as_ref().expect("connected");
        to_server.send(Message::Text(text.into())).await.unwrap();
    }

    async fn send(&self, message: Value) {
        self.send_text(message.to_string()).await;
    }

    async fn subscribe(&self, key: Value, from: Option<BlockRef>) {
        self.send(subscribe_message(key, from)).await;
    }

    /// A subscribe with an `id`, echoed in its answer.
    async fn subscribe_as(&self, id: u32, key: Value, from: Option<BlockRef>) {
        let mut message = subscribe_message(key, from);
        message["id"] = id.into();
        self.send(message).await;
    }

    async fn unsubscribe(&self, key: Value) {
        self.send(json!({ "op": "unsubscribe", "key": key })).await;
    }

    /// Every text message until the connection is quiet or closed, answering pings.
    async fn texts(&mut self) -> Vec<Value> {
        let mut texts = Vec::new();
        while self.closed.is_none() {
            let Ok(message) = timeout(QUIET, self.from_server.recv()).await else {
                break;
            };
            match message {
                None => self.closed = Some((None, String::new())),
                Some(Message::Text(text)) => texts.push(serde_json::from_str(&text).unwrap()),
                Some(Message::Ping(payload)) => {
                    self.pings += 1;
                    if self.answers_pings
                        && let Some(to_server) = &self.to_server
                    {
                        let _ = to_server.send(Message::Pong(payload)).await;
                    }
                }
                Some(Message::Close(frame)) => {
                    self.closed = Some(match frame {
                        Some(f) => (Some(f.code), f.reason.to_string()),
                        None => (None, String::new()),
                    });
                }
                Some(other) => panic!("unexpected {other:?}"),
            }
        }
        texts
    }

    /// Hangs up, and waits for the server to end the connection.
    async fn hang_up(mut self) {
        self.to_server = None;
        drop(self.from_server);
        self.server.await.unwrap();
    }
}

fn subscribe_message(key: Value, from: Option<BlockRef>) -> Value {
    let mut message = json!({ "op": "subscribe", "key": key });
    if let Some(from) = from {
        message["from"] = json!({ "height": from.height, "headerId": from.header_id });
    }
    message
}

/// A client's view of one key, from its snapshot and the messages since.
#[derive(Debug, Default)]
struct View {
    tip: Option<BlockRef>,
    /// Box ids and the heights that included them.
    boxes: BTreeMap<BoxId, u32>,
    mempool: Option<Value>,
    balance: Option<Value>,
}

fn id<T: std::str::FromStr>(value: &Value) -> T
where
    T::Err: std::fmt::Debug,
{
    value.as_str().expect("a hex id").parse().unwrap()
}

fn block_ref(height: &Value, header_id: &Value) -> BlockRef {
    BlockRef { height: height.as_u64().unwrap() as u32, header_id: id(header_id) }
}

impl View {
    /// Applies a message, checking what the SDK checks.
    fn apply(&mut self, m: &Value) {
        match m["type"].as_str().unwrap() {
            "block" => {
                let tip = self.tip.expect("a box view has a tip");
                let block = block_ref(&m["height"], &m["headerId"]);
                assert_eq!(id::<HeaderId>(&m["parentId"]), tip.header_id, "{block:?} skips");
                assert_eq!(block.height, tip.height + 1);
                for s in m["spent"].as_array().unwrap() {
                    assert!(self.boxes.remove(&id(&s["boxId"])).is_some(), "spends unknown");
                }
                for b in m["created"].as_array().unwrap() {
                    let height = b["inclusionHeight"].as_u64().unwrap() as u32;
                    assert_eq!(height, block.height);
                    assert!(self.boxes.insert(id(&b["boxId"]), height).is_none(), "creates held");
                }
                self.tip = Some(block);
            }
            "rollback" => {
                let block = block_ref(&m["height"], &m["headerId"]);
                assert_eq!(Some(block), self.tip, "rolls back a block the view is not at");
                for s in m["spent"].as_array().unwrap() {
                    assert!(self.boxes.remove(&id(&s["boxId"])).is_some(), "undoes unknown");
                }
                for b in m["created"].as_array().unwrap() {
                    let height = b["inclusionHeight"].as_u64().unwrap() as u32;
                    assert!(height < block.height, "a restored box is older than the block");
                    assert!(self.boxes.insert(id(&b["boxId"]), height).is_none(), "restores held");
                }
                self.tip =
                    Some(BlockRef { height: block.height - 1, header_id: id(&m["parentId"]) });
            }
            "mempool" => {
                let tip = self.tip.expect("a box view has a tip");
                assert_eq!(m["tipHeight"].as_u64(), Some(tip.height.into()));
                self.mempool = Some(m["pending"].clone());
            }
            "balance" => self.balance = Some(m.clone()),
            // The start of the subscription; the view is already at its `from`.
            "subscribed" => {}
            other => panic!("{other} does not apply to a view: {m}"),
        }
    }
}

fn types(messages: &[Value]) -> Vec<&str> {
    messages
        .iter()
        .map(|m| m["type"].as_str().unwrap())
        .collect()
}

fn token_key() -> (TrackingKey, Value) {
    let id = token_id("small-100");
    (TrackingKey::Token(id), json!({ "tokenId": id.to_string() }))
}

// --- The handshake and the stream ---

#[tokio::test(start_paused = true)]
async fn a_box_subscription_replays_from_its_snapshot_then_follows_the_chain() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(0);
    let echo = json!({ "address": f.address(0) });
    let mut view = f.snapshot(&key).await;
    let from = view.tip;
    for seed in 0..2 {
        f.mine(seed).await;
    }
    f.sim.lock().mempool_add(7);
    f.tick().await;

    let mut client = f.connect();
    client.subscribe(echo.clone(), from).await;
    let catch_up = client.texts().await;
    assert_eq!(types(&catch_up), ["subscribed", "block", "block", "mempool", "balance"]);
    assert!(catch_up.iter().all(|m| m["key"] == echo), "the key as sent");
    for m in &catch_up {
        view.apply(m);
    }
    f.check(&key, &view);

    for seed in 2..6 {
        f.mine(seed).await;
        let live = client.texts().await;
        assert_eq!(live[0]["type"], "block", "a block first, then its overlay and balance");
        for m in &live {
            view.apply(m);
        }
        f.check(&key, &view);
    }
    assert!(client.closed.is_none());
}

/// The Done-when case: the client's block is orphaned and the engine has reverted it, but
/// not yet applied the new branch, when the subscribe arrives. The catch-up rolls the client
/// back, and the new branch follows live.
#[tokio::test(start_paused = true)]
async fn a_subscribe_in_the_middle_of_a_reorg_rolls_back_then_follows_the_new_branch() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(0);
    f.snapshot(&key).await;
    // A block that moves one of the key's boxes, so its rollback restores it.
    let mine_touching = |f: &mut Fixture| {
        let mut chain = f.sim.lock();
        let source = crate::sim::testing::box_of(&chain, 0);
        let to = chain.trees()[1].clone();
        let tx = chain.transfer(&[source], &to);
        chain.mine_txs(vec![tx]);
    };
    mine_touching(&mut f);
    f.tick().await;
    let mut view = f.snapshot(&key).await;
    let orphaned = view.tip.unwrap();

    f.sim.lock().reorg(1, 1, 99);
    let reverted = f
        .engine
        .apply(Event::BlockReverted(orphaned))
        .await
        .unwrap();
    assert!(reverted.is_ok(), "the engine is at the orphaned block's parent");

    let mut client = f.connect();
    client
        .subscribe(json!({ "address": f.address(0) }), Some(orphaned))
        .await;
    let catch_up = client.texts().await;
    assert_eq!(types(&catch_up), ["subscribed", "rollback", "mempool", "balance"]);
    assert!(!catch_up[1]["created"].as_array().unwrap().is_empty(), "a box restored");
    for m in &catch_up {
        view.apply(m);
    }

    f.tick().await;
    let live = client.texts().await;
    assert_eq!(types(&live)[..2], ["block", "block"], "the new branch");
    for m in &live {
        view.apply(m);
    }
    f.check(&key, &view);
}

/// A live subscription through a reorg: the rollback, then the new branch.
#[tokio::test(start_paused = true)]
async fn a_live_subscription_follows_a_reorg() {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let mut view = f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo, view.tip).await;
    for m in &client.texts().await {
        view.apply(m);
    }
    for seed in 0..3 {
        f.mine(seed).await;
    }
    f.sim.lock().reorg(2, 1, 42);
    f.tick().await;
    let messages = client.texts().await;
    assert!(types(&messages).contains(&"rollback"));
    for m in &messages {
        view.apply(m);
    }
    f.check(&key, &view);
    assert!(!messages.iter().any(|m| m["type"] == "balance"), "a token key has no balance");
}

#[tokio::test(start_paused = true)]
async fn a_balance_only_subscription_loads_the_key_and_gets_only_balances() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(2);
    assert!(!f.tracked(&key).await);
    let mut client = f.connect();
    client
        .subscribe(json!({ "address": f.address(2) }), None)
        .await;
    let catch_up = client.texts().await;
    assert_eq!(types(&catch_up), ["subscribed", "balance"]);
    assert!(f.tracked(&key).await, "loaded");
    let tip = f.sim.lock().tip();
    assert_eq!(catch_up[1]["asOf"], json!({ "height": tip.height, "headerId": tip.header_id }));

    // A block that moves its box: a new balance, and nothing else.
    {
        let mut chain = f.sim.lock();
        let source = crate::sim::testing::box_of(&chain, 2);
        let to = chain.trees()[3].clone();
        let tx = chain.transfer(&[source], &to);
        chain.mine_txs(vec![tx]);
    }
    f.tick().await;
    assert_eq!(types(&client.texts().await), ["balance"]);
    // A block that leaves it alone sends nothing.
    f.sim.lock().mine_txs(vec![]);
    f.tick().await;
    assert!(client.texts().await.is_empty());
}

/// The engine drops a key whose load block is orphaned: the subscription ends with its
/// `resync_required`, which is not a slow consumer, so the connection stays and the client
/// subscribes again.
#[tokio::test(start_paused = true)]
async fn a_routed_resync_ends_the_subscription_and_keeps_the_connection() {
    let mut f = Fixture::new().await;
    let echo = json!({ "address": f.address(2) });
    let mut client = f.connect();
    client.subscribe(echo.clone(), None).await;
    assert_eq!(types(&client.texts().await), ["subscribed", "balance"]);

    f.sim.lock().reorg(1, 1, 5);
    f.tick().await;
    let messages = client.texts().await;
    assert_eq!(
        messages,
        [json!({ "type": "resync_required", "key": echo, "reason": "fork_beyond_window" })]
    );
    f.mine(6).await;
    assert!(client.texts().await.is_empty(), "nothing more for the ended subscription");
    assert!(client.closed.is_none());

    client.subscribe(echo, None).await;
    assert_eq!(types(&client.texts().await), ["subscribed", "balance"]);
}

/// With `from`, an untracked key is not loaded: its deltas since `from` don't exist.
#[tokio::test(start_paused = true)]
async fn a_subscription_with_from_to_an_untracked_key_is_not_tracked() {
    let f = Fixture::new().await;
    let tip = f.sim.lock().tip();
    let mut client = f.connect();
    client
        .subscribe_as(4, json!({ "address": f.address(1) }), Some(tip))
        .await;
    let messages = client.texts().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["type"], "resync_required");
    assert_eq!(messages[0]["reason"], "not_tracked");
    assert_eq!(messages[0]["id"], 4);
    assert!(!f.tracked(&f.tree_key(1)).await, "not loaded");
}

// --- Reconnecting (the Done-when sim cases) ---

/// Leaves after following a key, then comes back `blocks` blocks later and subscribes from
/// the last block it applied. Returns the messages, and the node calls made meanwhile.
async fn reconnect_after(blocks: u32) -> (Fixture, View, Vec<Value>, Vec<Endpoint>) {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let mut view = f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo.clone(), view.tip).await;
    f.mine(0).await;
    for m in &client.texts().await {
        view.apply(m);
    }
    client.hang_up().await;

    for seed in 1..=blocks {
        f.mine(seed.into()).await;
    }
    let before = f.sim.calls().len();
    let mut client = f.connect();
    client.subscribe(echo, view.tip).await;
    let messages = client.texts().await;
    let calls = f.sim.calls()[before..].to_vec();
    (f, view, messages, calls)
}

#[tokio::test(start_paused = true)]
async fn a_reconnect_within_the_grace_period_replays_without_a_reload() {
    let (f, mut view, messages, calls) = reconnect_after(CONFIG.grace - 1).await;
    let replayed = CONFIG.grace as usize - 1;
    assert_eq!(types(&messages)[0], "subscribed");
    assert_eq!(types(&messages)[1..=replayed], vec!["block"; replayed][..]);
    for m in &messages {
        view.apply(m);
    }
    f.check(&token_key().0, &view);
    assert!(calls.is_empty(), "no node call: {calls:?}");
}

#[tokio::test(start_paused = true)]
async fn a_reconnect_after_the_grace_period_gets_resync_required() {
    let (f, _, messages, _) = reconnect_after(CONFIG.grace + 1).await;
    assert_eq!(types(&messages), ["resync_required"]);
    assert_eq!(messages[0]["reason"], "not_tracked");
    assert!(!f.tracked(&token_key().0).await);
}

// --- Refusals ---

#[tokio::test(start_paused = true)]
async fn refused_keys_are_rejected_and_the_connection_carries_on() {
    let f = Fixture::new().await;
    let mut client = f.connect();
    let tip = f.sim.lock().tip();
    let cases = [
        (json!({ "address": "nope" }), Some(tip), "invalid_address"),
        (json!({ "tokenId": "ab" }), Some(tip), "invalid_token_id"),
        (token_key().1, None, "from_required"),
        (json!({ "tokenId": token_id("large").to_string() }), None, "from_required"),
    ];
    for (key, from, reason) in cases {
        client.subscribe(key.clone(), from).await;
        let messages = client.texts().await;
        assert_eq!(messages, [json!({ "type": "rejected", "key": key, "reason": reason })]);
    }
    assert!(client.closed.is_none());
}

/// A load that fails for now is `rejected` with the wait: here, the configured keys have not
/// settled.
#[tokio::test(start_paused = true)]
async fn a_load_that_fails_for_now_is_rejected_with_retry_after() {
    let (settled, _pending) = settling(1);
    let f = Fixture::waiting_for(settled).await;
    let mut client = f.connect();
    client
        .subscribe_as(3, json!({ "address": f.address(0) }), None)
        .await;
    let messages = client.texts().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["reason"], "not_ready");
    assert_eq!(messages[0]["retryAfter"], 1);
    assert_eq!(messages[0]["id"], 3, "a refusal after the load carries the id");
    assert!(!f.tracked(&f.tree_key(0)).await);
}

#[tokio::test(start_paused = true)]
async fn the_cap_refuses_one_subscription_more_but_not_a_replace() {
    let f = Fixture::new().await;
    let mut client = f.connect_with(Config { max_subscriptions: 2 }, 1024);
    for i in 0..2 {
        client
            .subscribe(json!({ "address": f.address(i) }), None)
            .await;
    }
    let both = ["subscribed", "balance", "subscribed", "balance"];
    assert_eq!(types(&client.texts().await), both);
    client
        .subscribe(json!({ "address": f.address(2) }), None)
        .await;
    let refused = client.texts().await;
    assert_eq!(refused[0]["reason"], "too_many_subscriptions");
    assert!(refused[0].get("retryAfter").is_none());
    // A replace adds none; an unsubscribe makes room.
    client
        .subscribe(json!({ "address": f.address(1) }), None)
        .await;
    assert_eq!(types(&client.texts().await), ["subscribed", "balance"]);
    client.unsubscribe(json!({ "address": f.address(1) })).await;
    client
        .subscribe(json!({ "address": f.address(2) }), None)
        .await;
    assert_eq!(types(&client.texts().await), ["subscribed", "balance"]);
}

#[tokio::test(start_paused = true)]
async fn a_malformed_message_closes_the_connection_with_its_reason() {
    let f = Fixture::new().await;
    let mut client = f.connect();
    client
        .send(json!({ "op": "subscibe", "key": { "address": "x" } }))
        .await;
    assert!(client.texts().await.is_empty());
    let (code, reason) = client.closed.clone().expect("closed");
    assert_eq!(code, Some(close_code::POLICY));
    assert!(reason.contains("subscibe"), "{reason}");

    // A reason longer than a close frame takes is cut at a character boundary.
    let mut client = f.connect();
    client
        .send_text(format!(r#"{{"op":"{}"}}"#, "é".repeat(200)))
        .await;
    client.texts().await;
    let (_, reason) = client.closed.clone().expect("closed");
    assert!(reason.len() <= MAX_CLOSE_REASON && reason.len() > 100, "{}", reason.len());

    let mut client = f.connect();
    let to_server = client.to_server.clone().unwrap();
    to_server
        .send(Message::Binary(vec![1, 2].into()))
        .await
        .unwrap();
    client.texts().await;
    assert_eq!(client.closed.unwrap().0, Some(close_code::POLICY));
}

#[test]
fn truncation_keeps_whole_characters() {
    assert_eq!(truncate("abc", 2), "ab");
    assert_eq!(truncate("aé", 2), "a");
    assert_eq!(truncate("aé", 3), "aé");
    assert_eq!(truncate("", 5), "");
}

// --- Replace and unsubscribe ---

/// Subscribing again replaces: a box subscription after a balance-only one gets every
/// message once, and the key's count is one, so it goes idle once the client leaves.
#[tokio::test(start_paused = true)]
async fn subscribing_again_replaces_the_subscription() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(0);
    let mut client = f.connect();
    // Two spellings of one tree are one subscription.
    let p2s = address::encode_as_p2s(&f.sim.lock().trees()[0].0, Network::Mainnet);
    client.subscribe(json!({ "address": p2s }), None).await;
    assert_eq!(types(&client.texts().await), ["subscribed", "balance"]);
    let mut view = f.snapshot(&key).await;
    let echo = json!({ "address": f.address(0) });
    client.subscribe(echo.clone(), view.tip).await;
    for m in &client.texts().await {
        view.apply(m);
    }
    for seed in 0..3 {
        f.mine(seed).await;
        let messages = client.texts().await;
        assert_eq!(types(&messages).iter().filter(|t| **t == "block").count(), 1, "once");
        assert!(messages.iter().all(|m| m["key"] == echo), "the latest spelling");
        for m in &messages {
            view.apply(m);
        }
    }
    f.check(&key, &view);

    client.hang_up().await;
    for seed in 3..3 + u64::from(CONFIG.grace) + 1 {
        f.mine(seed).await;
    }
    assert!(!f.tracked(&key).await, "no count left behind");
}

/// A replace while the old subscription's messages are still on their way: the client takes
/// a new snapshot and subscribes from it without reading what came before. Everything up to
/// `subscribed` belongs to the old subscription, which does not apply to the new snapshot;
/// what follows does.
#[tokio::test(start_paused = true)]
async fn subscribed_marks_where_a_replacing_subscription_starts() {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let first = f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo.clone(), first.tip).await;
    for seed in 0..3 {
        f.mine(seed).await;
    }
    // Unread: the old subscription's catch-up and three blocks.
    let mut view = f.snapshot(&key).await;
    client.subscribe(echo.clone(), view.tip).await;
    let messages = client.texts().await;
    // The new subscription's start is the last `subscribed`; the first is the old one's.
    let start = messages
        .iter()
        .rposition(|m| m["type"] == "subscribed")
        .expect("subscribed");
    let old = &messages[..start];
    assert_eq!(types(old).iter().filter(|t| **t == "block").count(), 3, "{:?}", types(old));
    assert_eq!(old[0]["type"], "subscribed", "the old subscription's own start");
    let stale_block = old.iter().find(|m| m["type"] == "block").unwrap();
    assert!(
        stale_block["height"].as_u64().unwrap() <= view.tip.unwrap().height.into(),
        "an old block would not extend the new snapshot"
    );

    assert_eq!(types(&messages[start..]), ["subscribed", "mempool"]);
    for m in &messages[start..] {
        view.apply(m);
    }
    f.mine(3).await;
    for m in &client.texts().await {
        view.apply(m);
    }
    f.check(&key, &view);
}

/// A subscribe refused on the spot ends the key's old subscription, as one refused after a
/// load does: after `rejected` the client holds nothing for the key.
#[tokio::test(start_paused = true)]
async fn a_refused_replace_ends_the_old_subscription() {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let view = f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo.clone(), view.tip).await;
    client.texts().await;
    client.subscribe(echo.clone(), None).await;
    assert_eq!(
        client.texts().await,
        [json!({ "type": "rejected", "key": echo, "reason": "from_required" })]
    );
    f.mine(0).await;
    assert!(client.texts().await.is_empty(), "the old subscription sends nothing");
    for seed in 1..=u64::from(CONFIG.grace) {
        f.mine(seed).await;
    }
    assert!(!f.tracked(&key).await, "and no longer counts");
    assert!(client.closed.is_none());
}

/// A connection built by hand, for the cases the simulator can't time.
fn connection<K: Socket>(f: &Fixture, socket: K) -> Connection<SimChain, K> {
    Connection {
        api: f.api.clone(),
        config: Config::default(),
        socket,
        keys: HashMap::new(),
        feeds: StreamMap::new(),
        pending: JoinSet::new(),
        next_seq: 0,
        unanswered: 0,
    }
}

/// A socket with nothing to read, as long as the client's end is held, and room for
/// everything written.
fn idle_socket() -> (TestSocket, mpsc::Sender<Message>, mpsc::Receiver<Message>) {
    let (to_server, incoming) = mpsc::channel(1);
    let (outgoing, from_server) = mpsc::channel(1024);
    (TestSocket { incoming, outgoing }, to_server, from_server)
}

/// A replace keeps the old subscription registered until the engine has answered the new
/// one, so the key never has no subscriber: with no grace, a block in that gap would untrack
/// it, and a `from` subscribe would then be `not_tracked`. Here the gap is held open by
/// never running the new subscribe.
#[tokio::test(start_paused = true)]
async fn a_replace_keeps_the_key_tracked_until_the_new_subscribe_is_answered() {
    let config = engine::Config { grace: 0, ..CONFIG };
    let mut f = Fixture::with(config, Settled::now()).await;
    let key = f.tree_key(0);
    let echo = ClientKey::Address(f.address(0));
    let (socket, _to_server, _from_server) = idle_socket();
    let mut connection = connection(&f, socket);
    let subscribe = |from| ClientMessage::Subscribe { key: echo.clone(), from, id: None };
    connection.on_message(subscribe(None)).await.unwrap();
    let joined = connection.pending.join_next().await.unwrap();
    connection.on_opened(joined).await.unwrap();
    assert!(matches!(connection.keys[&key].state, State::Live(_)));

    let tip = f.sim.lock().tip();
    connection.on_message(subscribe(Some(tip))).await.unwrap();
    connection.pending.abort_all();
    f.mine(0).await;
    assert!(f.tracked(&key).await, "the replaced subscription still counts");

    // A newer message for the key passes the old subscription on, and an unsubscribe ends it.
    connection.on_message(subscribe(None)).await.unwrap();
    connection.pending.abort_all();
    f.mine(1).await;
    assert!(f.tracked(&key).await, "passed on to the newest subscribe");
    connection
        .on_message(ClientMessage::Unsubscribe { key: echo.clone() })
        .await
        .unwrap();
    f.mine(2).await;
    assert!(!f.tracked(&key).await, "ended by the unsubscribe");
}

/// A socket that records the order of its reads and writes, with `reads` pongs ready to read
/// at once, then nothing.
struct RecordingSocket {
    reads: usize,
    log: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Socket for RecordingSocket {
    async fn recv(&mut self) -> Option<Result<Message, RecvError>> {
        if self.reads == 0 {
            return std::future::pending().await;
        }
        self.reads -= 1;
        self.log.lock().unwrap().push("read".into());
        Some(Ok(Message::Pong(Default::default())))
    }

    async fn send(&mut self, message: Message) -> Result<(), Closed> {
        let Message::Text(text) = message else { panic!("unexpected {message:?}") };
        let text: Value = serde_json::from_str(&text).unwrap();
        self.log
            .lock()
            .unwrap()
            .push(text["type"].as_str().unwrap().into());
        Ok(())
    }
}

/// A client that keeps its socket readable does not hold up the connection's other work: a
/// finished subscribe and a live output are written before the reads queued with them.
#[tokio::test(start_paused = true)]
async fn reads_do_not_starve_outputs_or_finished_subscribes() {
    let f = Fixture::new().await;
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut connection = connection(&f, RecordingSocket { reads: 8, log: log.clone() });

    // A live subscription with an output waiting.
    let live = f.tree_key(1);
    f.snapshot(&live).await;
    let (sink, feed) = mpsc::channel(4);
    let Ok(Subscription::Live { id, .. }) =
        f.engine.subscribe(live.clone(), None, sink.clone()).await
    else {
        panic!("a tracked key subscribes");
    };
    let output = Output::ResyncRequired { key: live.clone(), reason: ResyncReason::OutsideWindow };
    sink.try_send(Arc::new(Routed::new(output))).unwrap();
    connection
        .feeds
        .insert(live.clone(), StreamNotifyClose::new(ReceiverStream::new(feed)));
    let echo = ClientKey::Address(f.address(1));
    connection
        .keys
        .insert(live, Entry { echo, state: State::Live(id) });

    // A subscribe that has finished.
    let opening = f.tree_key(0);
    let abort = connection.pending.spawn(std::future::ready((
        opening.clone(),
        0,
        Opened::Resync(ResyncReason::NotTracked),
    )));
    let echo = ClientKey::Address(f.address(0));
    connection.keys.insert(
        opening,
        Entry { echo, state: State::Pending { seq: 0, id: None, abort, replaced: None } },
    );
    connection.next_seq = 1;
    tokio::task::yield_now().await;

    let _ = timeout(Duration::from_secs(1), connection.run()).await;
    let log = log.lock().unwrap().clone();
    assert_eq!(log[..2], ["resync_required", "resync_required"], "{log:?}");
    assert_eq!(log[2..], ["read"; 8]);
    drop(sink);
}

/// Every answer to a subscribe carries its id, refusals on the spot included, and a
/// subscribe without one gets answers without one.
#[tokio::test(start_paused = true)]
async fn answers_carry_the_subscribes_id() {
    let f = Fixture::new().await;
    let (_, token) = token_key();
    let address = json!({ "address": f.address(0) });
    let mut client = f.connect();
    client.subscribe_as(1, address.clone(), None).await;
    client.subscribe_as(2, token.clone(), None).await;
    client
        .subscribe_as(3, json!({ "address": "x" }), None)
        .await;
    let messages = client.texts().await;
    let answers: Vec<(&str, &Value)> = messages
        .iter()
        .filter(|m| m["type"] != "balance")
        .map(|m| (m["type"].as_str().unwrap(), &m["id"]))
        .collect();
    assert_eq!(answers.len(), 3, "{messages:?}");
    for (kind, id) in [("subscribed", 1), ("rejected", 2), ("rejected", 3)] {
        assert!(answers.contains(&(kind, &json!(id))), "{kind} {id} in {answers:?}");
    }

    client.subscribe(address, None).await;
    let messages = client.texts().await;
    assert_eq!(types(&messages), ["subscribed", "balance"]);
    assert!(messages[0].get("id").is_none(), "no id asked, none sent");
    assert!(client.closed.is_none());
}

/// A client that does not wait for answers: a subscribe answered after the client sent a
/// newer message for the key, here a replace and an unsubscribe with a subscribe after it.
/// Reading as it goes, the client drops the key's messages until the answer with its latest
/// id, and what follows applies to its snapshot. Without ids, the first `subscribed` would
/// have passed for the newest subscribe's start.
#[tokio::test(start_paused = true)]
async fn ids_tell_an_older_subscribes_answer_from_the_newest() {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let mut view = f.snapshot(&key).await;
    for seed in 0..2 {
        f.mine(seed).await;
    }
    let mut client = f.connect();
    // Each subscribe is answered, unread, before the client's next message.
    client.subscribe_as(1, echo.clone(), view.tip).await;
    sleep(QUIET).await;
    client.unsubscribe(echo.clone()).await;
    client.subscribe_as(2, echo.clone(), view.tip).await;
    sleep(QUIET).await;
    client.subscribe_as(3, echo.clone(), view.tip).await;

    let mut started = false;
    let mut skipped = Vec::new();
    for m in client.texts().await {
        if !started {
            if m["type"] == "subscribed" && m["id"] == 3 {
                started = true;
            } else if m["type"] == "subscribed" {
                skipped.push(m["id"].clone());
            }
            if !started {
                continue;
            }
        }
        view.apply(&m);
    }
    assert!(started, "the newest subscribe was answered");
    assert_eq!(skipped, [json!(1), json!(2)], "the older answers came first, each with its id");
    f.mine(2).await;
    for m in &client.texts().await {
        view.apply(m);
    }
    f.check(&key, &view);
}

#[tokio::test(start_paused = true)]
async fn an_unsubscribe_stops_the_messages_at_once() {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let view = f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo.clone(), view.tip).await;
    client.texts().await;
    client.unsubscribe(echo).await;
    f.mine(0).await;
    assert!(client.texts().await.is_empty());
    for seed in 1..=u64::from(CONFIG.grace) {
        f.mine(seed).await;
    }
    assert!(!f.tracked(&key).await, "idle once unsubscribed, while the client stays");
    assert!(client.closed.is_none());
}

/// A subscribe and an unsubscribe of one key read back to back: the subscribe's task has
/// not run when the unsubscribe cancels it, so nothing is sent and no count is left.
#[tokio::test(start_paused = true)]
async fn an_unsubscribe_cancels_a_pending_subscribe() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(3);
    let echo = json!({ "address": f.address(3) });
    f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo.clone(), None).await;
    client.unsubscribe(echo).await;
    assert!(client.texts().await.is_empty());
    for seed in 0..=u64::from(CONFIG.grace) {
        f.mine(seed).await;
    }
    assert!(!f.tracked(&key).await);
}

/// A subscribe's result that comes after a newer message for its key: here the key's
/// newest subscribe is still pending as `seq` 1 when results of the one before arrive.
/// None of them is written or installed, and the pending one keeps its place. The simulator
/// can't time this: the result must be queued when the newer message is read.
#[tokio::test(start_paused = true)]
async fn a_result_for_a_key_that_moved_on_is_discarded() {
    let f = Fixture::new().await;
    let key = f.tree_key(0);
    f.snapshot(&key).await;
    let (socket, _to_server, mut from_server) = idle_socket();
    let mut connection = connection(&f, socket);
    connection.next_seq = 2;
    let abort = connection.pending.spawn(std::future::pending());
    let echo = ClientKey::Address(f.address(0));
    connection.keys.insert(
        key.clone(),
        Entry { echo, state: State::Pending { seq: 1, id: None, abort, replaced: None } },
    );

    let live = subscribe(&f.api, &key, None).await;
    assert!(matches!(live, Opened::Live { .. }));
    for stale in [
        Opened::Resync(ResyncReason::NotTracked),
        Opened::Rejected(ApiError::CapacityExceeded),
        live,
    ] {
        connection
            .on_opened(Ok((key.clone(), 0, stale)))
            .await
            .unwrap();
    }
    assert!(from_server.try_recv().is_err(), "nothing written");
    assert!(connection.feeds.is_empty(), "nothing installed");
    assert!(matches!(connection.keys[&key].state, State::Pending { seq: 1, .. }));
}

// --- Backpressure and keepalive ---

/// The Done-when case: a client that stops reading. The socket fills, then the sink, and the
/// engine task ends the subscription; once the client reads again it gets what was queued,
/// `resync_required` (`slow_consumer`) and the close.
#[tokio::test(start_paused = true)]
async fn a_slow_consumer_gets_resync_required_and_is_closed() {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let view = f.snapshot(&key).await;
    let mut client = f.connect_with(Config::default(), 4);
    client.subscribe(echo.clone(), view.tip).await;
    // Stalled: blocks keep coming while the client reads nothing.
    for seed in 0..(SINK_CAPACITY as u64 + 10) {
        f.mine(seed).await;
    }
    let messages = client.texts().await;
    let last = messages.last().unwrap();
    assert_eq!(last, &json!({ "type": "resync_required", "key": echo, "reason": "slow_consumer" }));
    assert_eq!(client.closed.clone().unwrap().0, Some(close_code::POLICY));
    assert!(messages.len() < SINK_CAPACITY * 3, "bounded: {}", messages.len());
    client.server.await.unwrap();
}

/// A client that never reads at all: the write waits [`WRITE_TIMEOUT`], then the
/// connection is dropped, and with it the subscription.
#[tokio::test(start_paused = true)]
async fn a_client_that_never_reads_is_dropped_after_the_write_timeout() {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let view = f.snapshot(&key).await;
    let client = f.connect_with(Config::default(), 1);
    client.subscribe(echo, view.tip).await;
    for seed in 0..3 {
        f.mine(seed).await;
    }
    sleep(WRITE_TIMEOUT - Duration::from_secs(1)).await;
    assert!(!client.server.is_finished(), "still waiting on the write");
    sleep(Duration::from_secs(2)).await;
    assert!(client.server.is_finished(), "dropped");
    for seed in 3..4 + u64::from(CONFIG.grace) {
        f.mine(seed).await;
    }
    assert!(!f.tracked(&key).await, "its subscription ended with it");
}

/// A client on a slow link that keeps draining: a catch-up that takes longer than the idle
/// limit to write is not taken for silence, since no ping went out during it.
#[tokio::test(start_paused = true)]
async fn a_slow_reader_is_not_dropped_during_a_long_catch_up() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(0);
    let echo = json!({ "address": f.address(0) });
    let view = f.snapshot(&key).await;
    // Another client holds the key while this one is away.
    let mut holder = f.connect();
    holder.subscribe(echo.clone(), None).await;
    for seed in 0..6 {
        f.mine(seed).await;
    }
    holder.texts().await;

    let mut client = f.connect_with(Config::default(), 1);
    client.subscribe(echo, view.tip).await;
    // One message every 15 s: the catch-up's 9 messages (`subscribed`, 6 blocks, the overlay
    // and the balance) take over two minutes, more than the idle limit's 90 s.
    let mut types = Vec::new();
    for _ in 0..20 {
        if types.len() == 9 {
            break;
        }
        sleep(Duration::from_secs(15)).await;
        match timeout(QUIET, client.from_server.recv()).await {
            Ok(Some(Message::Text(text))) => {
                let message: Value = serde_json::from_str(&text).unwrap();
                types.push(message["type"].as_str().unwrap().to_owned());
            }
            Ok(Some(Message::Ping(payload))) => {
                let to_server = client.to_server.as_ref().unwrap();
                to_server.send(Message::Pong(payload)).await.unwrap();
            }
            Ok(other) => panic!("the connection ended after {types:?}: {other:?}"),
            Err(_) => {}
        }
    }
    assert_eq!(types[7..], ["mempool", "balance"]);
    assert!(!client.server.is_finished());
}

#[tokio::test(start_paused = true)]
async fn the_server_pings_and_drops_a_client_that_stays_silent() {
    let f = Fixture::new().await;
    let mut client = f.connect();
    client.answers_pings = false;
    for pings in 1..=MISSED_PINGS as usize {
        sleep(PING_INTERVAL).await;
        client.texts().await;
        assert_eq!(client.pings, pings);
        assert!(client.closed.is_none(), "kept after {pings} unanswered");
    }
    sleep(PING_INTERVAL).await;
    client.texts().await;
    let (code, _) = client
        .closed
        .clone()
        .expect("closed at the ping after two unanswered");
    assert_eq!(code, Some(close_code::POLICY));
    // Silent, it never answers the close either: dropped once the wait for its answer ends.
    assert!(!client.server.is_finished(), "waiting for the client's close");
    sleep(CLOSE_TIMEOUT + Duration::from_secs(1)).await;
    assert!(client.server.is_finished());
}

#[tokio::test(start_paused = true)]
async fn a_client_that_answers_pings_stays_connected() {
    let f = Fixture::new().await;
    let mut client = f.connect();
    for _ in 0..10 {
        sleep(PING_INTERVAL).await;
        client.texts().await;
    }
    assert!(client.pings >= 9, "{}", client.pings);
    assert!(client.closed.is_none());
}
