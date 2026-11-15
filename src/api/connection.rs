//! One WebSocket connection: it reads the client's messages, runs its subscribes, and
//! forwards its subscriptions' outputs (DESIGN.md, "Connections").
//!
//! Forwarding never waits on a subscribe: a connection that stopped forwarding during a load
//! would let its other sinks fill, and the engine task would end them as slow consumers. So
//! subscribes run as tasks, in parallel across keys, with one current operation per key. A
//! newer `subscribe` or `unsubscribe` for a key makes that key's pending one stale, and its
//! result is discarded. A stale subscribe still runs to its end, never aborted: aborted, it
//! drops the key's load midway, and the next caller starts it over, so a client subscribing
//! again faster than a key loads would keep it from ever finishing. Order is kept per key, the
//! only order the protocol promises.

use std::{collections::HashMap, sync::Arc};

use axum::extract::ws::{CloseFrame, Message, close_code};
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior, interval_at, sleep, timeout},
};
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose, wrappers::ReceiverStream};
use tracing::{debug, error, warn};

use crate::{
    adapters::EngineGone,
    api::{
        ApiError,
        http::Api,
        protocol::{self, ClientKey, ClientMessage, Reject, ServerMessage, SubscribeId},
        ws::{
            CLOSE_TIMEOUT, Config, MISSED_PINGS, PING_INTERVAL, RecvError, SINK_CAPACITY, Socket,
            WRITE_TIMEOUT,
        },
    },
    clients::NodeSource,
    engine::{BlockRef, Output, ResyncReason, TrackingKey},
    runtime::{Handle, Routed, Subscription, SubscriptionId},
};

/// The longest close reason a frame carries (RFC 6455, 5.5: a control frame's payload is at
/// most 125 bytes, two of them the code).
const MAX_CLOSE_REASON: usize = 123;

/// Serves one connection until it ends.
pub async fn serve<S, K>(api: Arc<Api<S, Handle>>, config: Config, socket: K)
where
    S: NodeSource + 'static,
    K: Socket,
{
    let mut connection = Connection {
        api,
        config,
        socket,
        keys: HashMap::new(),
        feeds: StreamMap::new(),
        pending: JoinSet::new(),
        next_seq: 0,
        unanswered: 0,
    };
    let end = connection.run().await;
    debug!(?end, "WebSocket connection ended");
    // Dropping the connection aborts its pending subscribes and drops its sinks' receivers.
    // The engine task ends a subscription with a closed sink when it next sends to it, and
    // sweeps closed sinks before every block, the only event that untracks idle keys, so an
    // unsubscribe per subscription here would change nothing.
}

/// Why a connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// The client closed, or the connection broke.
    Left,
    /// A message that does not parse, or not text.
    Malformed,
    TooLarge,
    /// A subscription's sink filled up.
    SlowConsumer,
    /// A write waited [`WRITE_TIMEOUT`].
    WriteTimedOut,
    /// [`MISSED_PINGS`] pings in a row unanswered.
    Idle,
    /// The engine stopped; the process is exiting.
    EngineGone,
    /// A subscribe task panicked.
    Panicked,
}

type Step = Result<(), End>;

/// A key's subscription on this connection, and the key as the client last spelled it.
struct Entry {
    echo: ClientKey,
    state: State,
}

enum State {
    /// Subscribing; `seq` tells its result from one a newer message made stale, and `id`
    /// is the client's name for the subscribe, for its answer.
    Pending {
        seq: u64,
        id: Option<SubscribeId>,
        replaced: Option<Replaced>,
    },
    Live(SubscriptionId),
}

/// The live subscription a pending subscribe replaces, out of the merge so it sends nothing
/// more, but still registered until the engine has answered the new subscribe. Ended first,
/// it would leave the key with no subscriber for a moment, and a block in that moment could
/// untrack the key and turn the new subscribe into `not_tracked`. Its receiver is kept for
/// the same reason: a dropped one is a closed sink, which the engine task ends before the
/// next block.
struct Replaced {
    id: SubscriptionId,
    _feed: Feed,
}

/// A subscription's outputs, then `None` when the engine task ends it: [`StreamMap`] drops
/// a finished stream without a word, and the end is the slow consumer's signal.
type Feed = StreamNotifyClose<ReceiverStream<Arc<Routed>>>;

/// How a subscribe task ended.
enum Opened {
    Live { id: SubscriptionId, catch_up: Vec<Output>, feed: mpsc::Receiver<Arc<Routed>> },
    Resync(ResyncReason),
    Rejected(ApiError),
    EngineGone,
}

struct Connection<S, K> {
    api: Arc<Api<S, Handle>>,
    config: Config,
    socket: K,
    keys: HashMap<TrackingKey, Entry>,
    /// The live subscriptions' outputs, by key.
    feeds: StreamMap<TrackingKey, Feed>,
    /// Every subscribe not yet joined, stale ones included.
    pending: JoinSet<(TrackingKey, u64, Opened)>,
    next_seq: u64,
    /// Pings sent since the client last sent anything.
    unanswered: u32,
}

impl<S: NodeSource + 'static, K: Socket> Connection<S, K> {
    async fn run(&mut self) -> End {
        let mut ping = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            // Each branch only picks the next step; writes happen in the handlers, so a
            // half-finished write is never dropped for another branch. The order is what no
            // branch can starve: finished subscribes come first, as each comes from a read;
            // then outputs, which come at the engine's pace and end in `slow_consumer` if
            // they outrun the socket; then reads, which a client can keep ready for as long
            // as it likes; and the ping last. Reads before the ping, so a pong that arrived
            // during a long write is counted before the next tick.
            let step = tokio::select! {
                biased;
                Some(joined) = self.pending.join_next(), if !self.pending.is_empty() => {
                    self.on_opened(joined).await
                }
                Some((key, item)) = self.feeds.next(), if !self.feeds.is_empty() => {
                    self.on_output(key, item).await
                }
                read = self.socket.recv() => self.on_read(read).await,
                _ = ping.tick() => self.on_ping().await,
            };
            if let Err(end) = step {
                return end;
            }
        }
    }

    async fn on_read(&mut self, read: Option<Result<Message, RecvError>>) -> Step {
        let message = match read {
            None | Some(Err(RecvError::Broken)) => return Err(End::Left),
            Some(Err(RecvError::TooLarge)) => {
                // The failed read ended tungstenite's stream, so the rest of the message can't
                // be drained and the drop resets the socket. Held a while first, so the client
                // reads the close frame before the reset can discard it.
                if self.send_close(close_code::SIZE, "message too big").await {
                    sleep(CLOSE_TIMEOUT).await;
                }
                return Err(End::TooLarge);
            }
            Some(Ok(message)) => message,
        };
        self.unanswered = 0;
        match message {
            Message::Text(text) => match ClientMessage::parse(&text) {
                Ok(message) => self.on_message(message).await,
                Err(reason) => {
                    debug!(%reason, "Malformed WebSocket message");
                    self.close_with(close_code::POLICY, &reason).await;
                    Err(End::Malformed)
                }
            },
            Message::Binary(_) => {
                self.close_with(close_code::POLICY, "messages are JSON text")
                    .await;
                Err(End::Malformed)
            }
            // tungstenite answers pings itself.
            Message::Ping(_) | Message::Pong(_) => Ok(()),
            Message::Close(_) => {
                // tungstenite has queued the reply, and the next read sends it.
                self.drain().await;
                Err(End::Left)
            }
        }
    }

    async fn on_message(&mut self, message: ClientMessage) -> Step {
        let (network, policy) = (self.api.network, &self.api.policy);
        match message {
            ClientMessage::Subscribe { key: echo, from, id } => {
                let key = match echo.resolve(network, policy) {
                    Ok(key) => key,
                    Err(error) => return self.reject(&echo, Reject::Api(error), id).await,
                };
                // A subscribe ends the key's old subscription whatever comes of it, so after
                // `rejected` the client holds none, however the refusal came.
                let replaced = self.take(&key);
                let refusal = if matches!(key, TrackingKey::Token(_)) && from.is_none() {
                    Some(Reject::FromRequired)
                } else if self.held() >= self.config.max_subscriptions {
                    Some(Reject::TooManySubscriptions)
                } else {
                    None
                };
                if let Some(reject) = refusal {
                    if let Some(replaced) = replaced {
                        unsubscribe(&self.api.engine, replaced.id).await?;
                    }
                    return self.reject(&echo, reject, id).await;
                }
                self.open(key, echo, from, id, replaced);
                Ok(())
            }
            // A key that does not resolve was never subscribed.
            ClientMessage::Unsubscribe { key } => match key.resolve(network, policy) {
                Ok(key) => self.end(&key).await,
                Err(_) => Ok(()),
            },
        }
    }

    /// Starts subscribing to `key` in a task of its own.
    fn open(
        &mut self,
        key: TrackingKey,
        echo: ClientKey,
        from: Option<BlockRef>,
        id: Option<SubscribeId>,
        replaced: Option<Replaced>,
    ) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let api = self.api.clone();
        let task_key = key.clone();
        self.pending.spawn(async move {
            let opened = subscribe(&api, &task_key, from).await;
            (task_key, seq, opened)
        });
        self.keys
            .insert(key, Entry { echo, state: State::Pending { seq, id, replaced } });
    }

    /// What counts against the cap: live subscriptions and unfinished subscribes, stale ones
    /// included, since each may be loading. A replaced subscription waiting on its successor
    /// is out of the merge and not counted, so a replace of a live subscription adds none.
    fn held(&self) -> usize {
        self.feeds.len() + self.pending.len()
    }

    /// Takes `key`'s subscription out of the connection, for a subscribe that replaces it:
    /// a pending one is left to finish as stale, and the live one, its own or the one it was
    /// replacing, passes to the new subscribe.
    fn take(&mut self, key: &TrackingKey) -> Option<Replaced> {
        match self.keys.remove(key)?.state {
            State::Pending { replaced, .. } => replaced,
            State::Live(id) => {
                let feed = self.feeds.remove(key).expect("a live key's feed is merged");
                Some(Replaced { id, _feed: feed })
            }
        }
    }

    /// Ends this connection's subscription to `key`, live or pending.
    async fn end(&mut self, key: &TrackingKey) -> Step {
        match self.take(key) {
            Some(replaced) => unsubscribe(&self.api.engine, replaced.id).await,
            None => Ok(()),
        }
    }

    async fn on_opened(&mut self, joined: Result<(TrackingKey, u64, Opened), JoinError>) -> Step {
        let (key, seq, opened) = match joined {
            Ok(joined) => joined,
            // Subscribes are aborted only when the connection drops, which never joins them;
            // hand-driven tests abort them to hold a gap open.
            Err(error) if error.is_cancelled() => return Ok(()),
            Err(error) => {
                error!(%error, "A WebSocket subscribe failed, closing the connection");
                return Err(End::Panicked);
            }
        };
        let current = matches!(
            self.keys.get(&key),
            Some(Entry { state: State::Pending { seq: s, .. }, .. }) if *s == seq
        );
        if !current {
            // A newer message for the key came while the subscribe ran.
            return match opened {
                Opened::Live { id, .. } => unsubscribe(&self.api.engine, id).await,
                Opened::EngineGone => Err(End::EngineGone),
                Opened::Resync(_) | Opened::Rejected(_) => Ok(()),
            };
        }
        let entry = self.keys.get_mut(&key).expect("current");
        let State::Pending { replaced, id, .. } = &mut entry.state else {
            unreachable!("a current result is for a pending key")
        };
        let (replaced, subscribe_id) = (replaced.take(), *id);
        // The engine has answered the new subscribe, and counts it if it took effect, so the
        // subscription it replaces can end without leaving the key unsubscribed.
        if let Some(replaced) = replaced {
            unsubscribe(&self.api.engine, replaced.id).await?;
        }
        match opened {
            Opened::Live { id, catch_up, feed } => {
                let entry = self.keys.get_mut(&key).expect("current");
                // Live before the catch-up is written, so a failed write still unsubscribes.
                entry.state = State::Live(id);
                // Once per subscribe; each live output borrows the echo instead.
                let echo = entry.echo.clone();
                self.write(&ServerMessage::Subscribed { key: &echo, id: subscribe_id })
                    .await?;
                for output in &catch_up {
                    self.write(&ServerMessage::Output { key: &echo, output })
                        .await?;
                }
                // Live outputs only after the catch-up: the sink has held them meanwhile.
                self.feeds
                    .insert(key, StreamNotifyClose::new(ReceiverStream::new(feed)));
                Ok(())
            }
            Opened::Resync(reason) => {
                let echo = self.keys.remove(&key).expect("current").echo;
                self.write(&ServerMessage::Resync { key: &echo, reason, id: subscribe_id })
                    .await
            }
            Opened::Rejected(error) => {
                let echo = self.keys.remove(&key).expect("current").echo;
                self.reject(&echo, Reject::Api(error), subscribe_id).await
            }
            Opened::EngineGone => Err(End::EngineGone),
        }
    }

    async fn on_output(&mut self, key: TrackingKey, item: Option<Arc<Routed>>) -> Step {
        let Some(routed) = item else {
            // The engine task ended the subscription with no `resync_required`: its sink was
            // full. The socket is shared, so the connection's other keys are behind too.
            let echo = self.keys.remove(&key).expect("a live key").echo;
            self.feeds.remove(&key);
            let reason = ResyncReason::SlowConsumer;
            self.write(&ServerMessage::Resync { key: &echo, reason, id: None })
                .await?;
            self.close_with(close_code::POLICY, "slow consumer").await;
            return Err(End::SlowConsumer);
        };
        let keyless = routed
            .encoded
            .get_or_init(|| protocol::keyless(&routed.output));
        let text = protocol::with_key(keyless, &self.keys[&key].echo);
        self.send(Message::Text(text.into())).await?;
        if matches!(routed.output, Output::ResyncRequired { .. }) {
            // The engine ended the subscription and took it off the key's count.
            self.keys.remove(&key);
            self.feeds.remove(&key);
        }
        Ok(())
    }

    async fn on_ping(&mut self) -> Step {
        if self.unanswered >= MISSED_PINGS {
            self.close_with(close_code::POLICY, "pings unanswered")
                .await;
            return Err(End::Idle);
        }
        self.unanswered += 1;
        self.send(Message::Ping(Default::default())).await
    }

    /// Logged as REST logs its errors: a node failure's details at `warn`, since the client
    /// gets only the code, and other refusals at `debug`.
    async fn reject(&mut self, echo: &ClientKey, reject: Reject, id: Option<SubscribeId>) -> Step {
        match &reject {
            Reject::Api(ApiError::EngineGone(_)) => return Err(End::EngineGone),
            Reject::Api(error @ ApiError::NodeError(_)) => {
                warn!(code = reject.code(), %error, "WebSocket subscribe failed");
            }
            _ => debug!(code = reject.code(), "WebSocket subscribe refused"),
        }
        self.write(&ServerMessage::Rejected { key: echo, reject: &reject, id })
            .await
    }

    async fn write(&mut self, message: &ServerMessage<'_>) -> Step {
        self.send(Message::Text(message.to_text().into())).await
    }

    async fn send(&mut self, message: Message) -> Step {
        match timeout(WRITE_TIMEOUT, self.socket.send(message)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(End::Left),
            Err(_) => Err(End::WriteTimedOut),
        }
    }

    /// Closes the connection with a reason: sends a close frame, if the socket takes it, then
    /// waits for the client's. The connection ends either way.
    async fn close_with(&mut self, code: u16, reason: &str) {
        if self.send_close(code, reason).await {
            self.drain().await;
        }
    }

    /// Sends a close frame, and tells whether the socket took it.
    async fn send_close(&mut self, code: u16, reason: &str) -> bool {
        let reason = truncate(reason, MAX_CLOSE_REASON);
        let frame = CloseFrame { code, reason: reason.into() };
        self.send(Message::Close(Some(frame))).await.is_ok()
    }

    /// Reads, discarding, until the client's side ends, for at most [`CLOSE_TIMEOUT`]. A read is
    /// what sends tungstenite's reply to a client's close frame, and the end of the stream is
    /// the client's reply to ours. A socket dropped with the client's messages still unread is
    /// reset, and a reset can discard the close frame before the client reads it: its reason
    /// is lost, and the client sees code 1006.
    async fn drain(&mut self) {
        let _ =
            timeout(CLOSE_TIMEOUT, async { while let Some(Ok(_)) = self.socket.recv().await {} })
                .await;
    }
}

/// Ends a subscription. A free function, not a method: a `&self` held across the await would
/// make the connection need a `Sync` socket, which axum's is not.
async fn unsubscribe(engine: &Handle, id: SubscriptionId) -> Step {
    engine
        .unsubscribe(id)
        .await
        .map_err(|EngineGone| End::EngineGone)
}

/// Subscribes to `key` from `from`. A balance-only subscription loads the key if it is not
/// tracked, then subscribes again; one with `from` never loads: its catch-up needs deltas
/// only a key tracked since `from` has, so the client redoes its snapshot, which loads.
async fn subscribe<S: NodeSource>(
    api: &Api<S, Handle>,
    key: &TrackingKey,
    from: Option<BlockRef>,
) -> Opened {
    let once = || async {
        let (sink, feed) = mpsc::channel(SINK_CAPACITY);
        match api.engine.subscribe(key.clone(), from, sink).await {
            Ok(Subscription::Live { id, catch_up }) => Opened::Live { id, catch_up, feed },
            Ok(Subscription::Resync(reason)) => Opened::Resync(reason),
            Err(EngineGone) => Opened::EngineGone,
        }
    };
    match once().await {
        Opened::Resync(ResyncReason::NotTracked) if from.is_none() => {}
        opened => return opened,
    }
    match api.pages.load(&api.engine, key).await {
        Ok(()) => once().await,
        Err(ApiError::EngineGone(_)) => Opened::EngineGone,
        Err(error) => Opened::Rejected(error),
    }
}

/// At most `max` bytes of `s`, cut at a character boundary.
fn truncate(s: &str, max: usize) -> &str {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
