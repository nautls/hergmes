//! The WebSocket at `GET /v1/ws`: the upgrade, its limits, and the socket a connection
//! talks through.
//!
//! A connection's work is in [`connection`], written against [`Socket`] rather than axum's
//! WebSocket, so tests drive it over in-memory channels on a paused clock.

use std::{future::Future, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::Response,
    routing::get,
};

use crate::{
    api::{connection, http::Api},
    clients::NodeSource,
    runtime::Handle,
};

/// A subscription's sink, in messages. A sink fills only once the kernel's send buffer and
/// tungstenite's write buffer have, so it is the last buffer, not the main one: it covers a
/// burst of about 20 blocks in one tick (`block`, `mempool` and `balance` each). A slot is a
/// pointer: outputs are shared between subscribers.
pub const SINK_CAPACITY: usize = 64;
/// How long one write may wait for the socket before the connection is dropped. Without it, a
/// client that keeps its connection open and never reads would hold the task in `send`.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// How often the server pings: under Cloudflare's 100 s and nginx's 60 s idle limits, which
/// a key's `block` every 2 minutes would not keep.
pub const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Pings a connection may leave unanswered, in a row, before it is dropped at the next one:
/// 60 to 90 s of silence. A client gone without a FIN would otherwise keep its keys tracked
/// until TCP gives up. Counted in pings, not time, because no ping goes out while a write
/// waits on a slow link: a client draining a long catch-up has had nothing to answer.
pub const MISSED_PINGS: u32 = 2;
/// How long a closing connection waits for the client's half of the close handshake before it
/// drops the socket. A client that never answers is gone or broken, and is dropped anyway.
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// The largest message or frame a client may send. The longest valid one, a subscribe with a
/// P2S address of about 5,600 characters and a `from`, is about 6 KB.
pub const MAX_MESSAGE: usize = 16 * 1024;
/// tungstenite's read buffer, per connection, held for the connection's life and zero-filled
/// on its first read, so resident whether the client sends anything or not. Its default,
/// 128 KiB, is for heavy inbound traffic; a client here sends a few subscribes and a pong
/// every [`PING_INTERVAL`], and the default made an idle connection cost about 131 KiB
/// (`examples/membench.rs`, `connections`), against about 8 KiB with this. A frame larger than
/// the buffer still arrives whole: the buffer grows to fit it, up to [`MAX_MESSAGE`]
/// (tungstenite 0.29.0, `protocol/frame/mod.rs`, `FrameCodec::read_frame` and `read_in`).
pub const READ_BUFFER: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// The most subscriptions one connection holds, live and pending, counting subscribes
    /// the client cancelled until they finish.
    pub max_subscriptions: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { max_subscriptions: 100 }
    }
}

struct Ws<S> {
    api: Arc<Api<S, Handle>>,
    config: Config,
}

/// The WebSocket route. It needs the engine task's [`Handle`], which subscribes, so it is
/// kept apart from the REST routes, which run over any engine handle.
pub fn router<S: NodeSource + 'static>(api: Arc<Api<S, Handle>>, config: Config) -> Router {
    Router::new()
        .route("/v1/ws", get(upgrade::<S>))
        .with_state(Arc::new(Ws { api, config }))
}

async fn upgrade<S: NodeSource + 'static>(
    State(ws): State<Arc<Ws<S>>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let (api, config) = (ws.api.clone(), ws.config);
    upgrade
        .max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .read_buffer_size(READ_BUFFER)
        .on_upgrade(move |socket| connection::serve(api, config, socket))
}

/// Why a read failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// A message or frame above [`MAX_MESSAGE`]. tungstenite fails the read without closing
    /// (0.29.0, `protocol/mod.rs`, `read_message_frame`), so the connection sends the 1009.
    TooLarge,
    /// The connection failed.
    Broken,
}

/// The socket could not take a message: the connection is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

/// A WebSocket as a connection uses it. `recv` must be cancel-safe: the connection waits on
/// it next to its subscriptions' outputs and drops it when one comes first.
pub trait Socket: Send + 'static {
    /// The next message, or `None` once the client has gone.
    fn recv(&mut self) -> impl Future<Output = Option<Result<Message, RecvError>>> + Send;
    /// Sends and flushes one message.
    fn send(&mut self, message: Message) -> impl Future<Output = Result<(), Closed>> + Send;
}

impl Socket for WebSocket {
    async fn recv(&mut self) -> Option<Result<Message, RecvError>> {
        // `WebSocket::recv` is `StreamExt::next`, which is cancel-safe.
        let read = WebSocket::recv(self).await?;
        Some(read.map_err(|error| {
            match error
                .into_inner()
                .downcast::<tungstenite::Error>()
                .map(|e| *e)
            {
                Ok(tungstenite::Error::Capacity(_)) => RecvError::TooLarge,
                _ => RecvError::Broken,
            }
        }))
    }

    async fn send(&mut self, message: Message) -> Result<(), Closed> {
        WebSocket::send(self, message).await.map_err(|_| Closed)
    }
}

#[cfg(test)]
#[path = "ws_tests.rs"]
mod tests;
