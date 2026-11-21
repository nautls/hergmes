//! The memory benchmark: three runs, each checking one estimate against the process's
//! resident memory.
//!
//! ```sh
//! cargo run --release --example membench -- engine <keys> <boxes-per-key>
//! cargo run --release --example membench -- balance <keys> [<tokens-per-key> [<tree-bytes>]]
//! cargo run --release --example membench -- cache <boxes>
//! cargo run --release --features sim --example membench -- connections <connections> [<keys>]
//! ```
//!
//! `engine` loads synthetic address keys straight into an [`Engine`], with no adapters and
//! no cache, and reports the growth per tracked box: the id state, last measured at about 135
//! bytes, and 600 a key. `cache` fills a [`BoxCache`] with small boxes and reports the growth
//! against the cache's own weighted size, which is what the budget bounds.
//!
//! `engine` also prints what a load and a scrape's `Query::Stats` take on average, in
//! microseconds: both must stay flat, or nearly, as the keys grow.
//!
//! `balance` tracks synthetic addresses balance-only, each holding ERG and `<tokens-per-key>`
//! distinct tokens (default two), and reports the growth per key and what an empty block then
//! takes, in microseconds: a block walks only the keys with boxes, so with balance-only keys
//! alone that time should stay near zero however many there are. A key's running totals keep
//! one entry per token it holds, so runs at several token counts give the cost of a key with
//! none and the cost of each token. Its trees are P2PK ones (36 bytes) unless `<tree-bytes>`
//! asks for longer ones, as a contract's (P2S) are: the engine shares one copy of a key's
//! tree among its maps, so a byte of tree should cost about a byte.
//!
//! `connections` opens WebSocket connections to the relay over a `SimChain`, served in this
//! process, and reports what each one costs the relay; with `<keys>`, each connection then
//! subscribes that many keys with no boxes, and it reports what a subscription costs besides
//! its boxes. Its clients are blocking sockets with WebSocket framing written by hand, so
//! they hold no buffers of their own. Besides resident
//! memory it reports glibc's live allocations: a buffer allocated whole but never written,
//! such as a fresh connection's read buffer, is live without being resident, and becomes
//! resident as the connection uses it or once it reuses memory freed earlier, as it does in a
//! relay that has run for a while.
//!
//! One run is one process, so nothing but the structure under test grows. A run's figure
//! still holds what the allocator keeps of freed memory (a load's full boxes are freed once
//! the engine has their ids), so run each at two sizes and take the slope: with the boxes per
//! key fixed, that share is the same at both sizes and cancels.
//!
//! An example rather than a test: the test harness's threads and other tests' allocations
//! would count in resident memory.

use std::sync::Arc;

use hergmes::{
    adapters::{BoxCache, cache::weight},
    api::metrics::resident_memory,
    engine::{BlockRef, Config, Engine, Event, LoadedBox, Query, Reply, TrackingKey, key_charge},
    types::{
        HexBytes,
        ergo::{BlockHeader, IndexedBlock, UnconfirmedTransaction},
    },
};
use support::{digest, plain_box, tree};

// Shared with the other examples, which use more of it.
#[allow(dead_code)]
mod support;

/// Pending txs in the benchmark's mempool snapshot: a busy pool, a fifth of the node's cap.
const POOL_TXS: u64 = 200;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let number = |i: usize| -> usize {
        let arg = args.get(i).unwrap_or_else(|| usage());
        arg.parse().unwrap_or_else(|_| usage())
    };
    match args.first().map(String::as_str) {
        Some("engine") => engine(number(1), number(2)),
        Some("balance") => balance(
            number(1),
            args.get(2).map_or(2, |_| number(2)),
            args.get(3).map_or(0, |_| number(3)),
        ),
        Some("cache") => cache(number(1)),
        Some("connections") => connections(number(1), args.get(2).map_or(0, |_| number(2))),
        _ => usage(),
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: membench engine <keys> <boxes-per-key> | membench balance <keys> [tokens-per-key [tree-bytes]] \
         | membench cache <boxes> \
         | membench connections <connections> [keys-per-connection] (with --features sim)"
    );
    std::process::exit(2)
}

fn resident() -> u64 {
    resident_memory().expect("resident memory is read from /proc, on Linux")
}

fn engine(keys: usize, boxes_per_key: usize) {
    let boxes = keys * boxes_per_key;
    // Room for every box and key: the cap counts each key's charge besides its boxes.
    let charge = key_charge(&TrackingKey::Tree(tree(0)), 0);
    let mut engine =
        Engine::new(Config { max_tracked_units: boxes + keys * charge, ..Config::DEFAULT });
    let tip = BlockRef { height: 1_000_000, header_id: digest(u64::MAX) };
    let header = BlockHeader {
        id: tip.header_id,
        parent_id: digest(u64::MAX - 1),
        height: tip.height,
        json: "{}".into(),
    };
    engine
        .apply(Event::BlockApplied(IndexedBlock { header, transactions: Vec::new() }))
        .expect("the first block");

    // A pool as a busy node holds, paying no tracked key: every load decides the snapshot
    // again and reads the new key's overlay from it, as it does in the relay.
    let stranger = tree(u64::MAX);
    let txs: Vec<UnconfirmedTransaction> = (0..POOL_TXS)
        .map(|t| UnconfirmedTransaction {
            id: digest(u64::MAX - 2 - 3 * t),
            inputs: vec![plain_box(u64::MAX - 3 - 3 * t, &stranger)],
            outputs: vec![plain_box(u64::MAX - 4 - 3 * t, &stranger)],
        })
        .collect();
    engine
        .apply(Event::MempoolSnapshot { txs: txs.into(), observed_tip: tip })
        .expect("a snapshot is never an error");

    let before = resident();
    let loading = std::time::Instant::now();
    for k in 0..keys {
        let tree = tree(k as u64);
        // Inclusion heights spread below the tip, as a real key's are.
        let loaded: Vec<LoadedBox> = (0..boxes_per_key)
            .map(|b| LoadedBox {
                utxo: Arc::new(plain_box((k * boxes_per_key + b) as u64, &tree)),
                inclusion_height: tip.height - (b % 100_000) as u32,
            })
            .collect();
        let outputs = engine
            .apply(Event::KeyLoaded { key: TrackingKey::Tree(tree), as_of: tip, boxes: loaded })
            .expect("a load is never an error");
        assert!(outputs.len() <= 1, "tracked, with at most its first balance: {outputs:?}");
    }
    let after = resident();
    let load_micros = loading.elapsed().as_micros() / keys as u128;

    // What a scrape costs the engine task: the walk of the key index, timed over a few runs.
    let started = std::time::Instant::now();
    for _ in 0..9 {
        engine.query(Query::Stats);
    }
    let stats_micros = started.elapsed().as_micros() / 9;
    let Reply::Stats(stats) = engine.query(Query::Stats) else { unreachable!() };
    assert_eq!((stats.address_keys, stats.tracked_boxes), (keys, boxes), "every key was loaded");
    let grown = after.saturating_sub(before);
    println!(
        "engine keys={keys} boxes_per_key={boxes_per_key} boxes={boxes} rss_before={before} \
         rss_after={after} grown={grown} bytes_per_box={:.1} load_micros={load_micros} stats_micros={stats_micros}",
        grown as f64 / boxes as f64
    );
}

fn balance(keys: usize, tokens: usize, tree_bytes: usize) {
    use hergmes::engine::Totals;

    // A P2PK tree, padded to `tree_bytes` with bytes that keep every key's tree distinct.
    let tree = |k: u64| {
        let mut bytes = tree(k).0.to_vec();
        let mut n = 0;
        while bytes.len() < tree_bytes {
            bytes.extend_from_slice(&digest(k ^ (n << 48)).0);
            n += 1;
        }
        bytes.truncate(tree_bytes.max(36));
        HexBytes::from(bytes)
    };

    // Room for every key: the cap counts each one's charge.
    let charge = key_charge(&TrackingKey::Tree(tree(0)), tokens);
    let mut engine = Engine::new(Config { max_tracked_units: keys * charge, ..Config::DEFAULT });
    let header = |height: u32| BlockHeader {
        id: digest(u64::MAX - u64::from(1_000_000 - height)),
        parent_id: digest(u64::MAX - u64::from(1_000_000 - height) + 1),
        height,
        json: "{}".into(),
    };
    let mut height = 1_000_000;
    engine
        .apply(Event::BlockApplied(IndexedBlock { header: header(height), transactions: vec![] }))
        .expect("the first block");
    let tip = engine.tip().expect("a tip");

    let before = resident();
    let loading = std::time::Instant::now();
    for k in 0..keys {
        let mut confirmed = Totals { nano_ergs: 1_000_000_000 + k as u64, ..Totals::default() };
        for t in 0..tokens {
            confirmed
                .tokens
                .insert(digest((t as u64 + 1) << 40 | k as u64), 5 + t as u64);
        }
        let event = Event::BalanceLoaded { tree: tree(k as u64), as_of: tip, confirmed };
        let outputs = engine.apply(event).expect("a load is never an error");
        assert!(outputs.is_empty(), "tracked: {outputs:?}");
    }
    let after = resident();
    let load_micros = loading.elapsed().as_micros() / keys as u128;
    let Reply::Stats(stats) = engine.query(Query::Stats) else { unreachable!() };
    assert_eq!(stats.balance_keys, keys, "every key was loaded");

    // Empty blocks: what every block pays for the keys, with no change to any.
    let blocks = 20;
    let started = std::time::Instant::now();
    for _ in 0..blocks {
        height += 1;
        let mut h = header(height);
        h.parent_id = engine.tip().expect("a tip").header_id;
        engine
            .apply(Event::BlockApplied(IndexedBlock { header: h, transactions: vec![] }))
            .expect("extends the tip");
    }
    let block_micros = started.elapsed().as_micros() / blocks;
    let grown = after.saturating_sub(before);
    println!(
        "balance keys={keys} tokens_per_key={tokens} tree_bytes={} rss_before={before} rss_after={after} grown={grown} \
         bytes_per_key={:.1} load_micros={load_micros} empty_block_micros={block_micros}",
        tree(0).0.len(),
        grown as f64 / keys as f64
    );
}

fn cache(boxes: usize) {
    let tree = tree(0);
    let each = u64::from(weight(&plain_box(0, &tree)));
    // Room for every box and a little more, so nothing is evicted: the run measures what the
    // cache holds, not what it turned away.
    let budget = each * boxes as u64 + each * 16;
    let cache = BoxCache::new(budget);

    let before = resident();
    for n in 0..boxes {
        cache.insert(Arc::new(plain_box(n as u64, &tree)));
    }
    let stats = cache.stats();
    let after = resident();

    assert_eq!(stats.entries, boxes as u64, "nothing was evicted");
    let grown = after.saturating_sub(before);
    println!(
        "cache boxes={boxes} weight_per_box={each} weighted_bytes={} rss_before={before} \
         rss_after={after} grown={grown} bytes_per_box={:.1} grown_over_weighted={:.2}",
        stats.weighted_bytes,
        grown as f64 / boxes as f64,
        grown as f64 / stats.weighted_bytes as f64
    );
}

/// Connections opened, and dropped, before the first reading: the relay's first connections
/// set up what every later one shares (the server's accept path, the tip's sender, the
/// allocator's arenas), and that is not a connection's cost.
#[cfg(feature = "sim")]
const WARM_UP: usize = 16;

#[cfg(feature = "sim")]
fn connections(count: usize, keys: usize) {
    use std::{net::SocketAddr, time::Duration};

    use hergmes::{
        sim::relay::{self, SimRelay},
        types::address::{Network, encode},
    };

    // The relay's runtime, as `main` builds it: one worker per core, so the allocator has as
    // many arenas as it does in the relay.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let address: SocketAddr = runtime.block_on(async {
        let (router, relay, _engine_task) = SimRelay::start(relay::chain()).await;
        // Kept for the whole run: dropping it would not stop the relay, but nothing here
        // needs to wonder.
        std::mem::forget(relay);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port on localhost");
        let address = listener.local_addr().expect("a bound listener's address");
        tokio::spawn(async move { axum::serve(listener, router).await });
        address
    });
    // Long enough for the relay's tasks to finish what a step started (a connection's opening,
    // a subscribe's catch-up) and to drop what a closed connection held.
    let settle = || std::thread::sleep(Duration::from_millis(500));
    let reading = || (resident(), glibc::in_use());
    let started = std::time::Instant::now();

    // REST's client, made and used before the first reading so its pool is not counted.
    let http = reqwest::Client::new();
    let address_url = |a: &str| format!("http://{address}/v1/addresses/{a}/boxes");
    let warm = encode(&tree(u64::MAX).0, Network::Mainnet);
    runtime.block_on(snapshot(&http, address_url(&warm)));
    drop(
        (0..WARM_UP)
            .map(|_| client::open(address))
            .collect::<Vec<_>>(),
    );
    settle();

    let before = reading();
    let mut streams: Vec<_> = (0..count).map(|_| client::open(address)).collect();
    settle();
    let opened = reading();

    // Every key a different P2PK address with no boxes: what a subscription costs besides its
    // boxes, which `engine` measures. A key is subscribed as the SDK does it: its first page
    // over REST, which loads it, then a subscribe from that page's block. Every answer must be
    // `subscribed`.
    for (c, stream) in streams.iter_mut().enumerate() {
        for k in 0..keys {
            let address = encode(&tree((c * keys + k) as u64).0, Network::Mainnet);
            let from = runtime.block_on(snapshot(&http, address_url(&address)));
            client::send_text(
                stream,
                &format!(
                    r#"{{"op":"subscribe","key":{{"address":"{address}"}},"from":{from},"id":{k}}}"#
                ),
            );
        }
        client::await_subscribed(stream, keys);
    }
    settle();
    let subscribed = reading();
    let elapsed = started.elapsed();

    let per = |from: u64, to: u64, n: usize| to.saturating_sub(from) as f64 / n.max(1) as f64;
    let heap = |from: (u64, Option<u64>), to: (u64, Option<u64>), n: usize| match (from.1, to.1) {
        (Some(a), Some(b)) => format!("{:.0}", per(a, b, n)),
        _ => "unknown".into(),
    };
    println!(
        "connections={count} keys_per_connection={keys} rss_before={} rss_after={} \
         rss_per_connection={:.0} heap_per_connection={} rss_per_subscription={:.0} \
         heap_per_subscription={} seconds={:.1}",
        before.0,
        subscribed.0,
        per(before.0, opened.0, count),
        heap(before, opened, count),
        per(opened.0, subscribed.0, count * keys),
        heap(opened, subscribed, count * keys),
        elapsed.as_secs_f64(),
    );
    // The relay drops a connection that leaves two pings unanswered, 60 to 90 s after its
    // last answer; a reading after that would miss connections.
    assert!(elapsed < Duration::from_secs(55), "too slow to trust the readings: fewer keys");
    drop(streams);
    runtime.shutdown_background();
}

/// A key's first page of boxes, which loads it, and the block it is pinned to, as a
/// subscribe's `from` spells it.
#[cfg(feature = "sim")]
async fn snapshot(http: &reqwest::Client, url: String) -> String {
    let page: serde_json::Value = http
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .expect("the first page is served")
        .json()
        .await
        .expect("a JSON page");
    page["asOf"].to_string()
}

#[cfg(not(feature = "sim"))]
fn connections(_: usize, _: usize) {
    eprintln!("membench connections runs the relay over a SimChain: add --features sim");
    std::process::exit(2)
}

/// A WebSocket client on a blocking socket, framed by hand: it holds no buffers of its own,
/// so what the process grows by is the relay's. It speaks only what the benchmark needs.
#[cfg(feature = "sim")]
mod client {
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpStream},
    };

    use serde_json::Value;

    const TEXT: u8 = 0x1;
    const CLOSE: u8 = 0x8;
    const PING: u8 = 0x9;
    const PONG: u8 = 0xa;

    /// A connection whose upgrade the relay answered. What the relay sends next is left in
    /// the kernel's socket buffers until read, which resident memory doesn't count.
    pub fn open(address: SocketAddr) -> TcpStream {
        let mut stream = TcpStream::connect(address).unwrap_or_else(|e| {
            panic!("connect: {e} (two sockets per connection: raise ulimit -n)")
        });
        stream
            .write_all(
                b"GET /v1/ws HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n\
                  Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                  Sec-WebSocket-Version: 13\r\n\r\n",
            )
            .expect("the upgrade request is written");
        let mut response = Vec::new();
        let mut byte = [0];
        while !response.ends_with(b"\r\n\r\n") {
            stream
                .read_exact(&mut byte)
                .expect("the upgrade response is read");
            response.push(byte[0]);
        }
        assert!(
            response.starts_with(b"HTTP/1.1 101"),
            "the relay upgrades: {}",
            String::from_utf8_lossy(&response)
        );
        stream
    }

    /// One frame from the client: masked, as RFC 6455 requires, with a zero key, which
    /// leaves the payload as it is.
    fn send(stream: &mut TcpStream, opcode: u8, payload: &[u8]) {
        let mut frame = vec![0x80 | opcode];
        match payload.len() {
            n if n < 126 => frame.push(0x80 | n as u8),
            n if n <= usize::from(u16::MAX) => {
                frame.push(0x80 | 126);
                frame.extend_from_slice(&(n as u16).to_be_bytes());
            }
            n => {
                frame.push(0x80 | 127);
                frame.extend_from_slice(&(n as u64).to_be_bytes());
            }
        }
        frame.extend_from_slice(&[0; 4]);
        frame.extend_from_slice(payload);
        stream.write_all(&frame).expect("a frame is written");
    }

    pub fn send_text(stream: &mut TcpStream, text: &str) {
        send(stream, TEXT, text.as_bytes());
    }

    /// The next text message, answering pings on the way. The relay sends whole frames.
    fn next_text(stream: &mut TcpStream) -> Value {
        loop {
            let mut head = [0; 2];
            stream.read_exact(&mut head).expect("a frame's header");
            let mut len = u64::from(head[1] & 0x7f);
            if len == 126 {
                let mut ext = [0; 2];
                stream.read_exact(&mut ext).expect("a frame's length");
                len = u64::from(u16::from_be_bytes(ext));
            } else if len == 127 {
                let mut ext = [0; 8];
                stream.read_exact(&mut ext).expect("a frame's length");
                len = u64::from_be_bytes(ext);
            }
            let mut payload = vec![0; usize::try_from(len).expect("a frame that fits")];
            stream.read_exact(&mut payload).expect("a frame's payload");
            match head[0] & 0x0f {
                TEXT => return serde_json::from_slice(&payload).expect("a JSON message"),
                PING => send(stream, PONG, &payload),
                CLOSE => panic!("the relay closed: {}", String::from_utf8_lossy(&payload)),
                _ => {}
            }
        }
    }

    /// Reads until `keys` subscribes are answered, each with `subscribed`.
    pub fn await_subscribed(stream: &mut TcpStream, keys: usize) {
        let mut answered = 0;
        while answered < keys {
            let message = next_text(stream);
            match message["type"].as_str() {
                Some("subscribed") => answered += 1,
                Some("resync_required" | "rejected") => panic!("not subscribed: {message}"),
                _ => {}
            }
        }
    }
}

/// glibc's count of live allocations, which resident memory doesn't show for memory
/// allocated but never written.
#[cfg(feature = "sim")]
mod glibc {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    pub fn in_use() -> Option<u64> {
        /// glibc's `struct mallinfo2` (`malloc.h`, glibc 2.33 and later), every field a
        /// `size_t`.
        #[repr(C)]
        struct Mallinfo2 {
            arena: usize,
            ordblks: usize,
            smblks: usize,
            hblks: usize,
            hblkhd: usize,
            usmblks: usize,
            fsmblks: usize,
            uordblks: usize,
            fordblks: usize,
            keepcost: usize,
        }
        unsafe extern "C" {
            fn mallinfo2() -> Mallinfo2;
        }
        // SAFETY: `mallinfo2` takes no arguments and returns a struct by value; it only reads
        // the allocator's counters, under the arenas' locks.
        let info = unsafe { mallinfo2() };
        // Live chunks in every arena, and allocations served by their own `mmap`.
        Some((info.uordblks + info.hblkhd) as u64)
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    pub fn in_use() -> Option<u64> {
        None
    }
}
