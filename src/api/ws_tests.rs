//! The route over a real socket, on real time: what only the upgrade shows. The connection's
//! behaviour is tested over in-memory channels in `connection_tests.rs`.

use axum::{body::Body, http::Request};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{self, protocol::frame::coding::CloseCode},
};
use tower::ServiceExt;

use super::*;
use crate::{
    adapters::{BoxCache, BoxFetcher, Follower, Loader, TokenChecker},
    api::{Balances, KeyPolicy, Pages},
    engine::{Config as EngineConfig, Engine},
    runtime::spawn,
    sim::{Chain, SimChain, SimConfig},
    types::address::{self, Network},
};

type Stream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct Server {
    sim: SimChain,
    router: Router,
}

async fn server() -> Server {
    let sim = SimChain::new(Chain::new(SimConfig::default()));
    let (engine, _task) = spawn(Engine::new(EngineConfig::default()));
    let mut follower = Follower::new(sim.clone(), Default::default());
    follower
        .tick(&engine, tokio::time::Instant::now())
        .await
        .unwrap();
    let cache = BoxCache::new(1 << 20);
    let api = Arc::new(Api {
        engine,
        pages: Pages::new(
            Arc::new(Loader::new(sim.clone(), Default::default()).seeding(cache.clone())),
            Arc::new(TokenChecker::new(sim.clone(), 100)),
            BoxFetcher::new(sim.clone(), cache),
        ),
        balances: Balances::new(sim.clone(), Default::default()),
        network: Network::Mainnet,
        policy: KeyPolicy::default(),
        synced: follower.synced(),
    });
    Server { sim, router: router(api, Config::default()) }
}

async fn connect(server: &Server) -> Stream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, server.router.clone()).into_future());
    let (stream, _) = connect_async(format!("ws://{addr}/v1/ws")).await.unwrap();
    stream
}

/// The next message, skipping pings; `None` once the stream ends.
async fn next(stream: &mut Stream) -> Option<tungstenite::Message> {
    loop {
        let message = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("an answer within 5 s")?;
        match message {
            Ok(tungstenite::Message::Ping(_)) => continue,
            Ok(message) => return Some(message),
            Err(_) => return None,
        }
    }
}

async fn send(stream: &mut Stream, message: String) {
    stream
        .send(tungstenite::Message::Text(message.into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_subscription_runs_over_a_real_socket() {
    let server = server().await;
    let address = address::encode(&server.sim.lock().trees()[0].0, Network::Mainnet);
    let mut stream = connect(&server).await;
    let key = json!({ "address": address });
    send(&mut stream, json!({ "op": "subscribe", "key": key }).to_string()).await;
    for expected in ["subscribed", "balance"] {
        let Some(tungstenite::Message::Text(text)) = next(&mut stream).await else {
            panic!("a text message");
        };
        let message: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(message["type"], expected);
        assert_eq!(message["key"], key);
    }
}

/// A message up to [`MAX_MESSAGE`] is read: a long address is refused per key. One byte
/// more closes the connection with 1009, which the relay sends itself.
#[tokio::test]
async fn a_message_above_the_limit_closes_the_connection_with_1009() {
    let server = server().await;
    let mut stream = connect(&server).await;
    let message = |len: usize| {
        let envelope = json!({ "op": "subscribe", "key": { "address": "" } }).to_string();
        let address = "9".repeat(len - envelope.len());
        json!({ "op": "subscribe", "key": { "address": address } }).to_string()
    };
    assert_eq!(message(MAX_MESSAGE).len(), MAX_MESSAGE);

    send(&mut stream, message(MAX_MESSAGE)).await;
    let Some(tungstenite::Message::Text(text)) = next(&mut stream).await else {
        panic!("a text message");
    };
    let rejected: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(rejected["reason"], "invalid_address");

    send(&mut stream, message(MAX_MESSAGE + 1)).await;
    match next(&mut stream).await {
        Some(tungstenite::Message::Close(Some(frame))) => assert_eq!(frame.code, CloseCode::Size),
        other => panic!("expected a 1009 close, got {other:?}"),
    }
}

/// Far above the limit, the close still reaches the client: tungstenite refuses a frame by
/// its header, before reading the payload.
#[tokio::test]
async fn a_message_far_above_the_limit_is_closed_with_1009_too() {
    let server = server().await;
    let mut stream = connect(&server).await;
    let _ = stream
        .send(tungstenite::Message::Text("x".repeat(1 << 20).into()))
        .await;
    match next(&mut stream).await {
        Some(tungstenite::Message::Close(Some(frame))) => assert_eq!(frame.code, CloseCode::Size),
        other => panic!("expected a 1009 close, got {other:?}"),
    }
}

#[tokio::test]
async fn a_request_without_an_upgrade_is_refused() {
    let server = server().await;
    let request = Request::get("/v1/ws").body(Body::empty()).unwrap();
    let response = server.router.oneshot(request).await.unwrap();
    assert!(response.status().is_client_error(), "{}", response.status());
}
