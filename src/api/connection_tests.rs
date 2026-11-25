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
    adapters::{BoxCache, BoxFetcher, EngineHandle, Follower, HeaderReader, Loader, TokenChecker},
    api::{
        Balances, Boxes, Headers, History, KeyPolicy, Pages, Tokens, Transactions,
        wire::ParametersBody, ws::Closed,
    },
    engine::{self, Engine, Event, Query, Reply},
    runtime::{Clock, ManualClock, Settled, settling, spawn_with_clock},
    sim::{
        Chain, Endpoint, MAINNET_PARAMETERS, SimChain, SimConfig, script_template,
        testing::{BLOCK_TIME, blocks},
        token_id,
    },
    types::{
        address::{self, Network},
        ergo::{BoxId, Parameters},
    },
};

/// The grace period, in blocks of [`BLOCK_TIME`]: each mined block moves the clock by one.
/// It counts from when the engine sees a client go. A hang-up is seen at once, but a
/// client's own unsubscribe can reach the engine after the next block moved the clock, so a
/// test waiting that out mines one block more than the grace period.
const GRACE_BLOCKS: u32 = 3;
const CONFIG: engine::Config =
    engine::Config { window: 8, grace: blocks(GRACE_BLOCKS as u64), ..engine::Config::DEFAULT };
/// How long a client waits for the next message before it takes the connection as quiet.
const QUIET: Duration = Duration::from_millis(100);

struct Fixture {
    sim: SimChain,
    clock: ManualClock,
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
        let clock = ManualClock::default();
        let (engine, task) = spawn_with_clock(Engine::new(config), Clock::Manual(clock.clone()));
        let cache = BoxCache::new(1 << 20);
        let follower = Follower::new(sim.clone(), Default::default());
        let api = Arc::new(Api {
            engine: engine.clone(),
            pages: Pages::new(
                Arc::new(Loader::new(sim.clone(), Default::default()).seeding(cache.clone())),
                Arc::new(TokenChecker::new(sim.clone(), 100)),
                BoxFetcher::new(sim.clone(), cache),
            )
            .waiting_for(settled.clone()),
            balances: Balances::new(sim.clone(), Default::default()).waiting_for(settled),
            history: History::new(sim.clone(), Default::default()),
            headers: Headers::new(HeaderReader::new(sim.clone(), follower.headers())),
            boxes: Boxes::new(sim.clone()),
            tokens: Tokens::new(sim.clone()),
            transactions: Transactions::new(sim.clone()),
            network: Network::Mainnet,
            policy: KeyPolicy::default(),
            started: follower.started(),
            parameters: follower.parameters(),
            metrics: Default::default(),
        });
        let mut f = Self { sim, clock, engine, _task: task, follower, api };
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

    /// Mines a block with a few fresh txs and follows it, a block's time later.
    async fn mine(&mut self, seed: u64) {
        self.clock.advance(BLOCK_TIME);
        self.sim.lock().mine(0, 3, seed);
        self.tick().await;
    }

    /// Mines a block that moves one of tree `tree`'s boxes back to it, so the key changes:
    /// only a block that changes a key is sent to it.
    async fn mine_changing(&mut self, tree: usize) {
        self.clock.advance(BLOCK_TIME);
        {
            let mut chain = self.sim.lock();
            let source = crate::sim::testing::box_of(&chain, tree);
            let to = chain.trees()[tree].clone();
            let tx = chain.transfer(&[source], &to);
            chain.mine_txs(vec![tx]);
        }
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
            tips: Vec::new(),
            keep_tips: false,
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
            view.known_at = Some(page.as_of);
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
            .query(Query::Capacity { key: key.clone(), up_to: 0 })
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
        let oracle = chain.oracle();
        // The key's last change: at or above the oracle's last change of its boxes (the key's
        // load block, if they have not changed since), on the main chain.
        let last = view.last.expect("a message named the key's last change");
        assert_eq!(chain.header_at(last.height), Some(last), "the last change is on the chain");
        let changed = (1..=tip.height)
            .rev()
            .find(|&h| oracle.unspent(key, h) != oracle.unspent(key, h - 1));
        assert!(
            changed.is_none_or(|h| last.height >= h),
            "{last:?} misses a change at {changed:?}"
        );
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
    /// Every `tip` received, in order.
    tips: Vec<BlockRef>,
    /// Whether [`Client::texts`] returns `tip` messages too; it always records them.
    keep_tips: bool,
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

    /// Every text message until the connection is quiet or closed, answering pings. A `tip`
    /// is recorded in `tips`, and left out unless `keep_tips`; every other message must come
    /// after one, since the engine has a tip before any client connects.
    async fn texts(&mut self) -> Vec<Value> {
        let mut texts = Vec::new();
        while self.closed.is_none() {
            let Ok(message) = timeout(QUIET, self.from_server.recv()).await else {
                break;
            };
            match message {
                None => self.closed = Some((None, String::new())),
                Some(Message::Text(text)) => {
                    let message: Value = serde_json::from_str(&text).unwrap();
                    if message["type"] == "tip" {
                        let fields = 3 + usize::from(message.get("parameters").is_some());
                        assert_eq!(message.as_object().unwrap().len(), fields, "{message}");
                        self.tips
                            .push(block_ref(&message["height"], &message["headerId"]));
                        if !self.keep_tips {
                            continue;
                        }
                    } else {
                        assert!(!self.tips.is_empty(), "{message} came before the first tip");
                    }
                    texts.push(message);
                }
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
    /// The block of the key's last change, once a message names it.
    last: Option<BlockRef>,
    /// Until then, the block the boxes are known at: the snapshot's, or the `from` of the
    /// last subscribe. The key's last change is at or below it.
    known_at: Option<BlockRef>,
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
    /// Where a subscribe resumes from: the key's last change, or where the boxes are known.
    fn from(&self) -> Option<BlockRef> {
        self.last.or(self.known_at)
    }

    /// Applies a message, checking what the SDK checks.
    fn apply(&mut self, m: &Value) {
        match m["type"].as_str().unwrap() {
            "block" => {
                let block = block_ref(&m["height"], &m["headerId"]);
                let prev = block_ref(&m["prev"]["height"], &m["prev"]["headerId"]);
                self.chain(prev, "block");
                assert!(prev.height < block.height);
                assert!(m.get("parentId").is_none(), "no parentId on the wire");
                let (spent, created) = (m["spent"].as_array().unwrap(), m["created"].as_array());
                assert!(!spent.is_empty() || !created.unwrap().is_empty(), "an empty block");
                for s in spent {
                    assert!(self.boxes.remove(&id(&s["boxId"])).is_some(), "spends unknown");
                }
                for b in created.unwrap() {
                    let height = b["inclusionHeight"].as_u64().unwrap() as u32;
                    assert_eq!(height, block.height);
                    assert!(self.boxes.insert(id(&b["boxId"]), height).is_none(), "creates held");
                }
                self.last = Some(block);
            }
            "rollback" => {
                let block = block_ref(&m["height"], &m["headerId"]);
                self.chain(block, "rollback");
                for s in m["spent"].as_array().unwrap() {
                    assert!(self.boxes.remove(&id(&s["boxId"])).is_some(), "undoes unknown");
                }
                for b in m["created"].as_array().unwrap() {
                    let height = b["inclusionHeight"].as_u64().unwrap() as u32;
                    assert!(height < block.height, "a restored box is older than the block");
                    assert!(self.boxes.insert(id(&b["boxId"]), height).is_none(), "restores held");
                }
                self.last = Some(block_ref(&m["prev"]["height"], &m["prev"]["headerId"]));
            }
            "mempool" => {
                let last = &m["lastChange"];
                self.chain(block_ref(&last["height"], &last["headerId"]), "mempool");
                self.mempool = Some(m["pending"].clone());
            }
            "balance" => {
                let last = &m["lastChange"];
                if self.known_at.is_some() {
                    self.chain(block_ref(&last["height"], &last["headerId"]), "balance");
                }
                self.balance = Some(m.clone());
            }
            // The start of the subscription: the replay chains from its `from`, or below it.
            "subscribed" => {
                self.known_at = self.from();
                self.last = None;
            }
            other => panic!("{other} does not apply to a view: {m}"),
        }
    }

    /// A message naming the key's last change: the view's, or before any message named it,
    /// one at or below where the boxes are known.
    fn chain(&mut self, named: BlockRef, kind: &str) {
        match self.last {
            Some(last) => assert_eq!(named, last, "{kind} off the key's chain of changes"),
            None => {
                let known_at = self.known_at.expect("a box view starts at a block");
                assert!(named.height <= known_at.height, "{kind} above {known_at:?}");
                if named.height == known_at.height {
                    assert_eq!(named, known_at, "{kind} on another fork");
                }
                self.last = Some(named);
            }
        }
    }
}

fn types(messages: &[Value]) -> Vec<&str> {
    messages
        .iter()
        .map(|m| m["type"].as_str().unwrap())
        .collect()
}

fn template_key() -> (TrackingKey, Value) {
    let template = script_template();
    let spelling = json!({ "template": template.to_string() });
    (TrackingKey::Template(template), spelling)
}

fn token_key() -> (TrackingKey, Value) {
    let id = token_id("small-100");
    (TrackingKey::Token(id), json!({ "tokenId": id.to_string() }))
}

#[tokio::test(start_paused = true)]
async fn a_box_subscription_replays_from_its_snapshot_then_follows_the_chain() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(0);
    let echo = json!({ "address": f.address(0) });
    let mut view = f.snapshot(&key).await;
    let from = view.from();
    for _ in 0..2 {
        f.mine_changing(0).await;
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
        let blocks = live.iter().take_while(|m| m["type"] == "block").count();
        assert!(live[blocks..].iter().all(|m| m["type"] != "block"), "blocks come first");
        for m in &live {
            view.apply(m);
        }
        f.check(&key, &view);
        f.mine_changing(0).await;
        let live = client.texts().await;
        assert_eq!(live[0]["type"], "block", "a block first, then its overlay and balance");
        for m in &live {
            view.apply(m);
        }
        f.check(&key, &view);
    }
    assert!(client.closed.is_none());
}

/// The client's block is orphaned and the engine has reverted it, but not yet applied the new
/// branch, when the subscribe arrives. The catch-up rolls the client back, and the new branch
/// follows live.
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
    let orphaned = view.known_at.unwrap();

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
    assert_eq!(live[0]["type"], "block", "the new branch changes the key");
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
    client.subscribe(echo, view.from()).await;
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

/// A balance-only subscription tracks the address from one read of its balance, with no
/// boxes: no page of the node's, so an address of any size can be followed.
#[tokio::test(start_paused = true)]
async fn a_balance_only_subscription_loads_the_key_and_gets_only_balances() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(2);
    assert!(!f.tracked(&key).await);
    let mut client = f.connect();
    let before = f.sim.calls().len();
    client
        .subscribe(json!({ "address": f.address(2) }), None)
        .await;
    let catch_up = client.texts().await;
    assert_eq!(types(&catch_up), ["subscribed", "balance"]);
    assert!(f.tracked(&key).await, "loaded");
    let calls = f.sim.calls()[before..].to_vec();
    assert!(calls.contains(&Endpoint::ConfirmedBalance), "{calls:?}");
    assert!(!calls.contains(&Endpoint::UnspentByErgoTree), "no page: {calls:?}");
    let expected = {
        let chain = f.sim.lock();
        chain.oracle().totals(&key, chain.height())
    };
    assert_eq!(catch_up[1]["confirmed"]["nanoErgs"], json!(expected.nano_ergs.to_string()));
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
    assert_eq!(resyncs(&f, "fork_beyond_window"), 1, "a routed one");
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

/// The parameters ride on a connection's first tip and, after that, only on the tip after they
/// change.
#[tokio::test(start_paused = true)]
async fn the_parameters_ride_on_the_first_tip_and_after_each_change() {
    let mut f = Fixture::new().await;
    let mut client = f.connect();
    client.keep_tips = true;
    let first = client.texts().await;
    assert_eq!(types(&first), ["tip"]);
    let rest = serde_json::to_value(ParametersBody(&MAINNET_PARAMETERS)).unwrap();
    assert_eq!(first[0]["parameters"], rest);

    f.mine(0).await;
    let next = client.texts().await;
    assert_eq!(types(&next), ["tip"]);
    assert!(next[0].get("parameters").is_none(), "unchanged: left out");

    // A new epoch: the node's parameters change with the block that opens it.
    let epoch = Parameters { height: MAINNET_PARAMETERS.height + 1024, ..MAINNET_PARAMETERS };
    f.sim.lock().set_parameters(epoch);
    f.mine(1).await;
    let changed = client.texts().await;
    assert_eq!(changed[0]["parameters"]["height"], epoch.height);
    f.mine(2).await;
    assert!(client.texts().await[0].get("parameters").is_none());

    // A new connection gets them on its first tip, changed or not.
    let mut late = f.connect();
    late.keep_tips = true;
    assert_eq!(late.texts().await[0]["parameters"]["height"], epoch.height);
}

/// A connection's first message is the tip, subscribed or not, and each block brings the new
/// one; a burst of blocks may bring only the last.
#[tokio::test(start_paused = true)]
async fn every_connection_gets_the_tip_first_and_each_new_one() {
    let mut f = Fixture::new().await;
    let mut client = f.connect();
    client.keep_tips = true;
    let opening = client.texts().await;
    assert_eq!(types(&opening), ["tip"], "nothing subscribed, the tip all the same");
    assert_eq!(client.tips, [f.sim.lock().tip()]);
    for seed in 0..3 {
        f.mine(seed).await;
        assert_eq!(types(&client.texts().await), ["tip"]);
        assert_eq!(client.tips.last(), Some(&f.sim.lock().tip()));
    }
    // An overlay alone moves no tip.
    f.sim.lock().mempool_add(7);
    f.tick().await;
    assert_eq!(client.texts().await, Vec::<Value>::new());
    f.sim.lock().reorg(2, 1, 9);
    f.tick().await;
    client.texts().await;
    assert!(client.tips.len() > 4, "the reorg sent a tip");
    assert_eq!(client.tips.last(), Some(&f.sim.lock().tip()), "after a reorg");
}

/// The tip goes out ahead of the outputs of the block that moved it, so a client's height is
/// never older than the boxes it just got.
#[tokio::test(start_paused = true)]
async fn the_new_tip_comes_before_the_blocks_outputs() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(0);
    let mut view = f.snapshot(&key).await;
    let mut client = f.connect();
    client.keep_tips = true;
    client
        .subscribe(json!({ "address": f.address(0) }), view.from())
        .await;
    let opening = client.texts().await;
    assert_eq!(types(&opening), ["tip", "subscribed", "mempool", "balance"]);

    for seed in 0..3 {
        f.mine(seed).await;
        let live = client.texts().await;
        assert_eq!(types(&live)[..2], ["tip", "block"]);
        assert_eq!(live[0]["headerId"], live[1]["headerId"]);
        for m in &live[1..] {
            view.apply(m);
        }
        f.check(&key, &view);
    }

    // Through a reorg, every rollback and block comes after a tip, and the last tip is the
    // block the client ends at.
    f.sim.lock().reorg(2, 1, 9);
    f.tick().await;
    let live = client.texts().await;
    assert_eq!(live[0]["type"], "tip", "{:?}", types(&live));
    for m in live.iter().filter(|m| m["type"] != "tip") {
        view.apply(m);
    }
    f.check(&key, &view);
    assert_eq!(client.tips.last().copied(), Some(f.sim.lock().tip()));
}

/// Leaves after following a key, then comes back `blocks` blocks later and subscribes from
/// the last block it applied. Returns the messages, and the node calls made meanwhile.
async fn reconnect_after(blocks: u32) -> (Fixture, View, Vec<Value>, Vec<Endpoint>) {
    let mut f = Fixture::new().await;
    let (key, echo) = token_key();
    let mut view = f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo.clone(), view.from()).await;
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
    client.subscribe(echo, view.from()).await;
    let messages = client.texts().await;
    let calls = f.sim.calls()[before..].to_vec();
    (f, view, messages, calls)
}

#[tokio::test(start_paused = true)]
async fn a_reconnect_within_the_grace_period_replays_without_a_reload() {
    let (f, mut view, messages, calls) = reconnect_after(GRACE_BLOCKS - 1).await;
    let replayed = GRACE_BLOCKS as usize - 1;
    assert_eq!(types(&messages)[0], "subscribed");
    assert_eq!(types(&messages)[1..=replayed], vec!["block"; replayed][..]);
    for m in &messages {
        view.apply(m);
    }
    f.check(&token_key().0, &view);
    assert!(calls.is_empty(), "no node call: {calls:?}");
}

/// A hang-up ends the connection's subscriptions at once, not at the next block's sweep of
/// closed sinks: its key is idle from then, so a load at the cap can evict it before any
/// block comes.
#[tokio::test(start_paused = true)]
async fn a_hang_up_leaves_its_keys_evictable_before_the_next_block() {
    let f = Fixture::new().await;
    let (key, echo) = token_key();
    let view = f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo, view.from()).await;
    client.texts().await;
    let stats = || async {
        match f.engine.query(Query::Stats).await.unwrap() {
            Reply::Stats(stats) => stats,
            other => panic!("{other:?}"),
        }
    };
    f.clock.advance(CONFIG.evict_after);
    assert_eq!(stats().await.evictable_boxes, 0, "subscribed");

    client.hang_up().await;
    f.clock.advance(CONFIG.evict_after);
    let stats = stats().await;
    assert!(stats.tracked_boxes > 0);
    assert_eq!(stats.evictable_boxes, stats.tracked_boxes, "idle since the hang-up");
}

#[tokio::test(start_paused = true)]
async fn a_reconnect_after_the_grace_period_gets_resync_required() {
    let (f, _, messages, _) = reconnect_after(GRACE_BLOCKS + 1).await;
    assert_eq!(types(&messages), ["resync_required"]);
    assert_eq!(messages[0]["reason"], "not_tracked");
    assert_eq!(resyncs(&f, "not_tracked"), 1, "a subscribe's answer");
    assert!(!f.tracked(&token_key().0).await);
}

/// The `resync_required` messages counted under `reason`.
fn resyncs(f: &Fixture, reason: &str) -> u64 {
    f.api
        .metrics
        .count(&format!("hergmes_resyncs_total{{reason=\"{reason}\"}}"))
}

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
        (json!({ "template": "a" }), Some(tip), "invalid_template"),
        (template_key().1, None, "from_required"),
    ];
    for (key, from, reason) in cases {
        client.subscribe(key.clone(), from).await;
        let messages = client.texts().await;
        assert_eq!(messages, [json!({ "type": "rejected", "key": key, "reason": reason })]);
    }
    assert!(client.closed.is_none());
    // Each is counted under its code, apart from REST's refusals.
    let count = |transport: &str, code: &str| {
        f.api.metrics.count(&format!(
            "hergmes_rejections_total{{transport=\"{transport}\",code=\"{code}\"}}"
        ))
    };
    assert_eq!(count("ws", "invalid_address"), 1);
    assert_eq!(count("ws", "from_required"), 3);
    assert_eq!(count("rest", "invalid_address"), 0);
}

/// A load that fails for now is `rejected` with the wait: here, the engine stays a block
/// behind the node, so no balance is read at its tip.
#[tokio::test(start_paused = true)]
async fn a_load_that_fails_for_now_is_rejected_with_retry_after() {
    let f = Fixture::new().await;
    f.sim.lock().mine(0, 1, 1);
    let mut client = f.connect();
    client
        .subscribe_as(3, json!({ "address": f.address(0) }), None)
        .await;
    // Its attempts, a second apart.
    sleep(Duration::from_secs(3)).await;
    let messages = client.texts().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["reason"], "not_ready");
    assert_eq!(messages[0]["retryAfter"], 1);
    assert_eq!(messages[0]["id"], 3, "a refusal after the load carries the id");
    assert!(!f.tracked(&f.tree_key(0)).await);
}

/// The configured keys go first at startup so they take the cap on tracked units before
/// clients can. A balance-only key counts its charge, so a balance subscription waits for
/// them too: `not_ready` until they settle, tracked once they have.
#[tokio::test(start_paused = true)]
async fn a_balance_subscription_waits_for_the_configured_keys() {
    let (settled, pending) = settling(1);
    let f = Fixture::waiting_for(settled).await;
    let mut client = f.connect();
    let address = json!({ "address": f.address(0) });
    client.subscribe(address.clone(), None).await;
    let messages = client.texts().await;
    assert_eq!(types(&messages), ["rejected"]);
    assert_eq!(messages[0]["reason"], "not_ready");
    assert!(!f.tracked(&f.tree_key(0)).await);

    drop(pending);
    client.subscribe(address, None).await;
    assert_eq!(types(&client.texts().await), ["subscribed", "balance"]);
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
    client.subscribe(echo.clone(), view.from()).await;
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
    for seed in 3..3 + u64::from(GRACE_BLOCKS) + 1 {
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
    client.subscribe(echo.clone(), first.from()).await;
    for seed in 0..3 {
        f.mine(seed).await;
    }
    // Unread: the old subscription's catch-up and three blocks.
    let mut view = f.snapshot(&key).await;
    client.subscribe(echo.clone(), view.from()).await;
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
        stale_block["height"].as_u64().unwrap() <= view.known_at.unwrap().height.into(),
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
    client.subscribe(echo.clone(), view.from()).await;
    client.texts().await;
    client.subscribe(echo.clone(), None).await;
    assert_eq!(
        client.texts().await,
        [json!({ "type": "rejected", "key": echo, "reason": "from_required" })]
    );
    f.mine(0).await;
    assert!(client.texts().await.is_empty(), "the old subscription sends nothing");
    for seed in 1..=u64::from(GRACE_BLOCKS) {
        f.mine(seed).await;
    }
    assert!(!f.tracked(&key).await, "and no longer counts");
    assert!(client.closed.is_none());
}

/// A connection built by hand, for the cases the simulator can't time, with the tip and the
/// parameters as they are taken as sent; `tip.mark_changed()` makes the tip unsent.
fn connection<K: Socket>(f: &Fixture, socket: K) -> Connection<SimChain, K> {
    let mut tip = f.engine.tip();
    tip.borrow_and_update();
    let mut parameters = f.api.parameters.clone();
    parameters.borrow_and_update();
    Connection {
        api: f.api.clone(),
        tip,
        parameters,
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
    let config = engine::Config { grace: Duration::ZERO, ..CONFIG };
    let mut f = Fixture::with(config, Settled::now()).await;
    let key = f.tree_key(0);
    let echo = protocol::ClientKey::Address(f.address(0));
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

/// Closing a connection ends what the engine still has registered for it, not only its live
/// subscriptions: a replaced one held while its replacement runs, and a subscribe the
/// engine answered that the connection never joined. Every key is then idle from the close.
#[tokio::test(start_paused = true)]
async fn a_closed_connection_ends_held_and_unjoined_subscriptions() {
    let f = Fixture::new().await;
    let (replaced, unjoined) = (f.tree_key(0), f.tree_key(1));
    let echo = |i| protocol::ClientKey::Address(f.address(i));
    let (socket, _to_server, _from_server) = idle_socket();
    let mut connection = connection(&f, socket);
    let subscribe = |i, from| ClientMessage::Subscribe { key: echo(i), from, id: None };

    // Key 0: live, then replaced by a subscribe that never runs.
    connection.on_message(subscribe(0, None)).await.unwrap();
    let joined = connection.pending.join_next().await.unwrap();
    connection.on_opened(joined).await.unwrap();
    let tip = f.sim.lock().tip();
    connection
        .on_message(subscribe(0, Some(tip)))
        .await
        .unwrap();
    connection.pending.abort_all();
    while connection.pending.join_next().await.is_some() {}
    // Key 1: subscribed by the engine, its answer never joined.
    connection.on_message(subscribe(1, None)).await.unwrap();
    sleep(QUIET).await;
    assert!(f.tracked(&replaced).await && f.tracked(&unjoined).await);

    connection.close().await;
    f.clock.advance(CONFIG.evict_after);
    let Reply::Stats(stats) = f.engine.query(Query::Stats).await.unwrap() else {
        panic!("a stats reply");
    };
    assert_eq!(stats.evictable_boxes, stats.tracked_boxes, "no subscription left behind");
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
/// finished subscribe and a live output are written before the reads queued with them, and
/// the tip before them all.
#[tokio::test(start_paused = true)]
async fn reads_do_not_starve_outputs_or_finished_subscribes() {
    let f = Fixture::new().await;
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut connection = connection(&f, RecordingSocket { reads: 8, log: log.clone() });
    connection.tip.mark_changed();

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
    let echo = protocol::ClientKey::Address(f.address(1));
    connection
        .keys
        .insert(live, Entry { echo: echo.echo(), state: State::Live(id) });

    // A subscribe that has finished.
    let opening = f.tree_key(0);
    connection.pending.spawn(std::future::ready((
        opening.clone(),
        0,
        Opened::Resync(ResyncReason::NotTracked),
    )));
    let echo = protocol::ClientKey::Address(f.address(0));
    connection.keys.insert(
        opening,
        Entry { echo: echo.echo(), state: State::Pending { seq: 0, id: None, replaced: None } },
    );
    connection.next_seq = 1;
    tokio::task::yield_now().await;

    let _ = timeout(Duration::from_secs(1), connection.run()).await;
    let log = log.lock().unwrap().clone();
    assert_eq!(log[..3], ["tip", "resync_required", "resync_required"], "{log:?}");
    assert_eq!(log[3..], ["read"; 8]);
    drop(sink);
}

/// On the multi-threaded runtime the engine task can publish a tip and route its outputs
/// after the loop found the tip unchanged and before it took an output or a finished
/// subscribe. The handlers are called here as the loop would call them then, with the tip
/// changed and not yet sent: each sends the tip first.
#[tokio::test(start_paused = true)]
async fn an_output_or_an_answer_taken_before_a_changed_tip_sends_the_tip_first() {
    let mut f = Fixture::new().await;
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut connection = connection(&f, RecordingSocket { reads: 0, log: log.clone() });
    connection.tip.mark_changed();

    // An output, with the tip changed.
    let key = f.tree_key(0);
    let echo = protocol::ClientKey::Address(f.address(0)).echo();
    connection.keys.insert(
        key.clone(),
        Entry { echo: echo.clone(), state: State::Pending { seq: 0, id: None, replaced: None } },
    );
    let output = Output::ResyncRequired { key: key.clone(), reason: ResyncReason::OutsideWindow };
    connection
        .on_output(key.clone(), Some(Arc::new(Routed::new(output))))
        .await
        .unwrap();
    assert_eq!(*log.lock().unwrap(), ["tip", "resync_required"]);

    // A finished subscribe, after a block moved the tip.
    f.mine(1).await;
    connection.keys.insert(
        key.clone(),
        Entry { echo, state: State::Pending { seq: 1, id: None, replaced: None } },
    );
    let joined = Ok((key, 1, Opened::Resync(ResyncReason::NotTracked)));
    connection.on_opened(joined).await.unwrap();
    assert_eq!(log.lock().unwrap()[2..], ["tip", "resync_required"]);

    // With the tip sent, nothing more goes ahead of an output.
    let key = f.tree_key(1);
    let echo = protocol::ClientKey::Address(f.address(1)).echo();
    connection.keys.insert(
        key.clone(),
        Entry { echo, state: State::Pending { seq: 2, id: None, replaced: None } },
    );
    let output = Output::ResyncRequired { key: key.clone(), reason: ResyncReason::OutsideWindow };
    connection
        .on_output(key, Some(Arc::new(Routed::new(output))))
        .await
        .unwrap();
    assert_eq!(log.lock().unwrap()[4..], ["resync_required"]);
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
    client.subscribe_as(1, echo.clone(), view.from()).await;
    sleep(QUIET).await;
    client.unsubscribe(echo.clone()).await;
    client.subscribe_as(2, echo.clone(), view.from()).await;
    sleep(QUIET).await;
    client.subscribe_as(3, echo.clone(), view.from()).await;

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
    client.subscribe(echo.clone(), view.from()).await;
    client.texts().await;
    client.unsubscribe(echo).await;
    f.mine(0).await;
    assert!(client.texts().await.is_empty());
    for seed in 1..=u64::from(GRACE_BLOCKS) + 1 {
        f.mine(seed).await;
    }
    assert!(!f.tracked(&key).await, "idle once unsubscribed, while the client stays");
    assert!(client.closed.is_none());
}

/// A subscribe and an unsubscribe of one key read back to back: the subscribe runs on, but
/// nothing is sent and no count is left.
#[tokio::test(start_paused = true)]
async fn an_unsubscribe_discards_a_pending_subscribe() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(3);
    let echo = json!({ "address": f.address(3) });
    f.snapshot(&key).await;
    let mut client = f.connect();
    client.subscribe(echo.clone(), None).await;
    client.unsubscribe(echo).await;
    assert!(client.texts().await.is_empty());
    for seed in 0..=u64::from(GRACE_BLOCKS) {
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
    connection.pending.spawn(std::future::pending());
    let echo = protocol::ClientKey::Address(f.address(0));
    connection.keys.insert(
        key.clone(),
        Entry { echo: echo.echo(), state: State::Pending { seq: 1, id: None, replaced: None } },
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

fn count(f: &Fixture, endpoint: Endpoint) -> usize {
    f.sim.calls().iter().filter(|e| **e == endpoint).count()
}

/// Every message the connection has written so far.
fn written(from_server: &mut mpsc::Receiver<Message>) -> Vec<Value> {
    let mut messages = Vec::new();
    while let Ok(Message::Text(text)) = from_server.try_recv() {
        messages.push(serde_json::from_str(&text).unwrap());
    }
    messages
}

/// Joins the connection's subscribes as they finish, until none is left.
async fn finish(connection: &mut Connection<SimChain, TestSocket>) {
    while let Some(joined) = connection.pending.join_next().await {
        connection.on_opened(joined).await.unwrap();
    }
}

/// A replace while the key is loading does not restart the load: the older subscribe runs
/// on, its answer discarded, and the newer one waits for the same load. Cancelled, the load
/// started over, so a client that subscribed again faster than a large key loads kept it from
/// ever finishing, paging the node each time.
#[tokio::test(start_paused = true)]
async fn a_replace_during_a_load_does_not_restart_it() {
    let f = Fixture::new().await;
    let key = f.tree_key(0);
    let echo = protocol::ClientKey::Address(f.address(0));
    let (socket, _to_server, mut from_server) = idle_socket();
    let mut connection = connection(&f, socket);
    let subscribe = |id| ClientMessage::Subscribe { key: echo.clone(), from: None, id: Some(id) };

    f.sim
        .set_latency(Endpoint::ConfirmedBalance, Duration::from_secs(10));
    connection.on_message(subscribe(1)).await.unwrap();
    while count(&f, Endpoint::ConfirmedBalance) == 0 {
        tokio::task::yield_now().await;
    }
    // The load is reading the balance.
    connection.on_message(subscribe(2)).await.unwrap();
    finish(&mut connection).await;

    assert_eq!(count(&f, Endpoint::ConfirmedBalance), 1, "one load");
    let messages = written(&mut from_server);
    assert_eq!(types(&messages), ["subscribed", "balance"]);
    assert_eq!(messages[0]["id"], 2, "only the newest subscribe is answered");
    assert!(matches!(connection.keys[&key].state, State::Live(_)));
}

/// Subscribes the client has replaced or unsubscribed run on, so they count against the cap
/// until they finish: if they didn't, a client could start loads without bound by cancelling
/// them. A replace of a pending subscribe adds one, and when refused it ends the key's
/// subscribe, as every refusal does.
#[tokio::test(start_paused = true)]
async fn cancelled_subscribes_count_against_the_cap_until_they_finish() {
    let f = Fixture::new().await;
    let (socket, _to_server, mut from_server) = idle_socket();
    let mut connection = connection(&f, socket);
    connection.config = Config { max_subscriptions: 2 };
    let echo = |i| protocol::ClientKey::Address(f.address(i));
    let subscribe = |i, id| ClientMessage::Subscribe { key: echo(i), from: None, id: Some(id) };

    // Read back to back: no subscribe has run yet.
    connection.on_message(subscribe(0, 1)).await.unwrap();
    let unsubscribe = ClientMessage::Unsubscribe { key: echo(0) };
    connection.on_message(unsubscribe).await.unwrap();
    connection.on_message(subscribe(1, 2)).await.unwrap();
    connection.on_message(subscribe(1, 3)).await.unwrap();
    connection.on_message(subscribe(2, 4)).await.unwrap();
    let refused = written(&mut from_server);
    assert_eq!(types(&refused), ["rejected", "rejected"]);
    for (message, id) in refused.iter().zip([3, 4]) {
        assert_eq!(message["reason"], "too_many_subscriptions");
        assert_eq!(message["id"], id);
    }
    assert!(connection.keys.is_empty(), "the refused replace ended the key's subscribe");

    finish(&mut connection).await;
    assert!(written(&mut from_server).is_empty(), "cancelled subscribes are not answered");
    assert!(connection.feeds.is_empty());
    // Finished, they make room.
    connection.on_message(subscribe(2, 5)).await.unwrap();
    finish(&mut connection).await;
    assert_eq!(types(&written(&mut from_server)), ["subscribed", "balance"]);
}

/// A client that stops reading. The socket fills, then the sink, and the engine task ends the
/// subscription; once the client reads again it gets what was queued, `resync_required`
/// (`slow_consumer`) and the close.
#[tokio::test(start_paused = true)]
async fn a_slow_consumer_gets_resync_required_and_is_closed() {
    let mut f = Fixture::new().await;
    let key = f.tree_key(0);
    let echo = json!({ "address": f.address(0) });
    let view = f.snapshot(&key).await;
    let mut client = f.connect_with(Config::default(), 4);
    client.subscribe(echo.clone(), view.from()).await;
    // Stalled: blocks that change the key keep coming while the client reads nothing.
    for _ in 0..(SINK_CAPACITY + 10) {
        f.mine_changing(0).await;
    }
    let messages = client.texts().await;
    let last = messages.last().unwrap();
    assert_eq!(last, &json!({ "type": "resync_required", "key": echo, "reason": "slow_consumer" }));
    assert_eq!(client.closed.clone().unwrap().0, Some(close_code::POLICY));
    assert!(messages.len() < SINK_CAPACITY * 3, "bounded: {}", messages.len());
    assert_eq!(resyncs(&f, "slow_consumer"), 1);
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
    client.subscribe(echo, view.from()).await;
    for seed in 0..3 {
        f.mine(seed).await;
    }
    sleep(WRITE_TIMEOUT - Duration::from_secs(1)).await;
    assert!(!client.server.is_finished(), "still waiting on the write");
    sleep(Duration::from_secs(2)).await;
    assert!(client.server.is_finished(), "dropped");
    for seed in 3..4 + u64::from(GRACE_BLOCKS) {
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
    for _ in 0..6 {
        f.mine_changing(0).await;
    }
    holder.texts().await;

    let mut client = f.connect_with(Config::default(), 1);
    client.subscribe(echo, view.from()).await;
    // One message every 15 s: the tip and the catch-up's 9 messages (`subscribed`, 6 blocks,
    // the overlay and the balance) take over two minutes, more than the idle limit's 90 s.
    let mut types = Vec::new();
    for _ in 0..20 {
        if types.len() == 10 {
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
    assert_eq!(types[..2], ["tip", "subscribed"]);
    assert_eq!(types[8..], ["mempool", "balance"]);
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
