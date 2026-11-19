# hergmes

A realtime, mempool-aware relay for the [Ergo](https://ergoplatform.org) blockchain.

hergmes sits in front of an indexed Ergo node and lets clients track the boxes of **addresses**
and **small-supply tokens** (NFTs and tokens with an emission of at most 100). A client gets a
key's confirmed boxes and an address's balance, then receives live updates: new boxes, spent
boxes, pending mempool activity, balance changes and reorgs. Pending activity shows in the
balance as a signed change on top of the confirmed amount ("10 ERG incoming", "10 ERG leaving").

> **Status:** early development. The relay follows the node, tracks the keys clients ask for
> and the ones named in its configuration, and serves the REST API and the WebSocket stream.
> The TypeScript client SDK is in [`sdk/`](sdk/README.md), not published yet.

## How it works

The relay uses a **snapshot + deltas** sync model:

- **REST** serves the confirmed box set of a key, paginated (keyset) and pinned to a block.
- **WebSocket** streams deltas from that block onward: confirmed block changes, rollbacks, and a
  per-key mempool overlay.

Every snapshot and block event carries both `height` and `headerId`, so a client resuming after a
reorg is either rolled back to the fork point or told to resync, never left silently diverged.

## Requirements

- An Ergo node with the **extra indexer** enabled (`extraIndex = true`, for the `/blockchain/*`
  endpoints) and a UTXO state (`stateType = "utxo"`, the default): in digest mode the node
  resolves no mempool inputs and serves no `/utxo/withPool` boxes.
- **Optional:** Node.js 23.6+, to re-record the test fixtures.

## Getting started

```sh
ERGO_NODE_URL=http://127.0.0.1:9053 cargo run
```

Every other setting has a default. To change one, copy
[`hergmes.example.toml`](hergmes.example.toml) to `hergmes.toml`, which the relay reads from the
directory it starts in, or pass another file with `--config <path>`. The startup log names the
file read, or says there is none. A key the relay does not know stops it at startup.

Three settings depend on where the relay runs, so a container can set them as environment
variables instead, without a file: `ERGO_NODE_URL`, `HERGMES_LISTEN_ADDR` and
`HERGMES_METRICS_ADDR`. Each is set in the file or as its variable, never both.

| Key                                | Default        | Description                                                                       |
| ---------------------------------- | -------------- | --------------------------------------------------------------------------------- |
| `node_url`                         |                | URL of an indexed Ergo node (required); or `ERGO_NODE_URL`                        |
| `window_blocks`                    | 720            | Blocks of deltas kept for replays, rollbacks and pinned pages                     |
| `grace_secs`                       | 600            | Seconds a key with no subscribers stays tracked after its last read (at least 60) |
| `poll_interval_ms`                 | 1000           | How often the node's status is read to spot a new block or mempool change         |
| `mempool_refresh_ms`               | 10000          | Most time between full mempool fetches, even if the node's status shows no change |
| `token_supply_limit`               | 100            | Largest token emission that can be tracked                                        |
| `track_addresses`                  | `[]`           | Addresses tracked for the life of the process                                     |
| `track_tokens`                     | `[]`           | Token ids tracked for the life of the process                                     |
| `allow_addresses`                  | omitted: any   | Addresses clients may name: `"p2pk"`, `"p2sh"`, `"p2s"` and addresses             |
| `deny_addresses`                   | `[]`           | Addresses clients may not name; deny wins over allow                              |
| `allow_tokens`                     | omitted: any   | Token ids clients may name                                                        |
| `box_cache_mb`                     | 128            | Memory for full boxes kept to serve pages, in MiB; 0 disables it                  |
| `max_boxes_per_key`                | 50000          | Most boxes an address or token can have to be tracked                             |
| `max_tracked_boxes`                | 2000000        | Most boxes tracked over all keys; about 860 MiB resident when full                |
| `listen_addr`                      | 127.0.0.1:8080 | IP and port of the REST API and the WebSocket; or `HERGMES_LISTEN_ADDR`           |
| `allow_origins`                    | `["*"]`        | Browser pages and extensions that may use the API: origins and `"scheme://*"`     |
| `max_subscriptions_per_connection` | 100            | Most subscriptions one WebSocket connection holds                                 |
| `metrics_addr`                     |                | IP and port of `/metrics`, omitted: none; or `HERGMES_METRICS_ADDR`               |

By default, with the allow lists omitted, the relay serves any address and any token within the
supply limit. The allow and deny lists narrow that, for an instance run for one dapp or one
wallet; an empty allow list (`[]`) allows nothing. A refused key is `403 key_not_allowed`, on
every route that takes a key (a box id and a tx are not keys). Keys in `track_addresses` and
`track_tokens` are always allowed. Denying one, or denying an address the allow list names, stops
the relay at startup, and the startup log shows the policy in force. For example:

```toml
# a dapp: its own contracts and tokens, with a higher supply limit
allow_addresses = ["<contract>", "<contract>"]
allow_tokens = ["<token-id>", "<token-id>"]
# at least the largest emission among the allowed tokens
token_supply_limit = 1000000

# a wallet: keys and scripts it signs for, plus a few contracts it relies on
allow_addresses = ["p2pk", "p2sh", "<contract>"]

# a general-purpose instance, without busy contracts no client needs
deny_addresses = ["<miner-fee-contract>"]
```

`allow_origins` limits which browser pages and extensions may use the relay, by their `Origin`
header, on every REST route and on the WebSocket; a refused origin is `403 origin_not_allowed`.
An entry is an exact origin (`https://app.example`) or a whole scheme (`chrome-extension://*`).
A request without an `Origin` is always served.

The API listens on loopback by default, so a fresh install is not exposed until you choose. Put
it behind a reverse proxy for TLS and rate limiting, or set `HERGMES_LISTEN_ADDR=0.0.0.0:8080` in
a container. The relay does not compress responses: Cloudflare does it on its own, and behind
nginx or Caddy turn it on there (`gzip on; gzip_types application/json;`, or `encode gzip`); a
500-box page shrinks about five times. Addresses travel in the URL path, and the longest P2S
address is about 5,600 characters: that fits nginx's default 8 KB request line and Cloudflare's
16 KB URL limit, so keep any lower proxy limit above it. A tx body is at most 192 KiB of hex,
under nginx's default `client_max_body_size` of 1 MB.

Logging uses `tracing`; set `RUST_LOG` (e.g. `RUST_LOG=hergmes=debug`) to adjust verbosity.

On glibc the relay caps malloc arenas at 4 at startup, which held its memory about 30% lower at
saturation for a small throughput cost under heavy bursts. `MALLOC_ARENA_MAX` (or
`glibc.malloc.arena_max` in `GLIBC_TUNABLES`) overrides it. Each WebSocket connection holds a
file descriptor, so the relay raises its soft open-file limit to the hard one at startup (under
systemd, from 1,024 to 524,288 by default) and warns if that is below 4,096; set `LimitNOFILE` then.
An unlimited hard limit leaves the soft one as it is.

Set `metrics_addr` (e.g. `"127.0.0.1:9100"`) to serve Prometheus metrics on `GET /metrics`,
on an address of its own and never on the public one: tracked keys by kind, tracked boxes, the
ring buffer's and the box cache's sizes, cache hits and misses, the boxes idle keys hold and the
keys evicted, resident memory, and refusals and resyncs by reason. They are counts only and
name no address or token.

Every read and event goes through one engine task, one message at a time. Two histograms show
whether that task keeps up: `hergmes_engine_inbox_wait_seconds`, how long messages wait for it,
and `hergmes_engine_message_seconds`, how long it spends on each, by kind (`apply_block`,
`apply_load`, `query_page`, `subscribe_boxes`, …). A long wait comes from the slow kinds ahead
of it.

## Sizing

The defaults fit a relay that shares a machine with its node. Full at the default
`max_tracked_boxes`, with about 100k keys on 1,000 connections, a mainnet relay held 855 to 861
MiB resident over 30 hours, about 450 B per tracked box all told. Memory grows about linearly
with what is tracked and connected:

| What                         | Live memory   | Bounded by                                               |
| ---------------------------- | ------------- | -------------------------------------------------------- |
| A tracked box, with its key  | about 170 B   | `max_tracked_boxes`, and `max_boxes_per_key` for one key |
| The box cache                | its budget    | `box_cache_mb`                                           |
| A subscription               | about 2.5 KiB | `max_subscriptions_per_connection`, per connection       |
| An idle WebSocket connection | about 8 KiB   | the open-file limit                                      |

Resident memory sits above these live figures, by what the allocator keeps.

At the cap, a load first evicts keys with no subscribers that have been idle for a minute, least
recently active first, and is refused with `capacity_exceeded` only if all of them together
can't make room. Idle keys would leave after `grace_secs` anyway, so they never stand in the way
of active ones, and the minute keeps a client's key between requests that belong together, such
as the pages of one walk. `hergmes_evictable_boxes` shows the room they hold, and
`hergmes_evicted_keys_total` how many went.

To track more, raise `max_tracked_boxes` and `box_cache_mb` together. At the defaults the cache
holds about a tenth of the tracked boxes and answers about 99% of lookups. A larger cap with the
same cache means more misses, and each miss is a call to the node. Keep the cache at about the
same share of tracked boxes, then watch `hergmes_box_cache_hits_total` and
`hergmes_box_cache_misses_total` and adjust.

The relay has not been run above the default cap, so a higher one is not yet tested.

## REST API

| Route                                         | What                                              |
| --------------------------------------------- | ------------------------------------------------- |
| `GET /v1/addresses/{address}/boxes`           | A page of an address's unspent boxes              |
| `GET /v1/tokens/{tokenId}/boxes`              | A page of a small-supply token's unspent boxes    |
| `GET /v1/addresses/{address}/boxes/with-pool` | An address's boxes with the mempool applied       |
| `GET /v1/tokens/{tokenId}/boxes/with-pool`    | A token's boxes with the mempool applied          |
| `GET /v1/addresses/{address}/balance`         | An address's balance, pending change and `used`   |
| `GET /v1/addresses/{address}/transactions`    | An address's txs, newest first, pending on top    |
| `GET /v1/headers`                             | The last block headers up to the tip, for signing |
| `GET /v1/boxes/{boxId}`                       | One box, if a tx can spend it now                 |
| `GET /v1/tokens/metadata?ids={tokenId},...`   | Tokens' names, decimals and emissions             |
| `POST /v1/transactions`                       | Send a signed tx, or only check it                |
| `GET /v1/status`                              | Version, network, tip and readiness               |

Box pages take `limit` (default 500, at most 1000) and the `cursor` from the previous page's
`nextCursor`; every page of a snapshot is pinned to the first page's block (`asOf`). Boxes are in
Fleet SDK's `Box<string>` shape plus `inclusionHeight`, and every amount is a base-10 integer
string. Errors are `{ "error": "<code>", "message": "..." }` (a refused tx adds a `reason`), and
a `503` carries `Retry-After`.

`/boxes/with-pool` is for clients that fetch without keeping state: the key's confirmed boxes
minus those pending txs spend, then the boxes pending txs create (these have no
`inclusionHeight`), all at one tip and in one response with no cursor. It holds at most 1000
confirmed boxes and every pending one; `truncated` says whether confirmed boxes were left out,
and `total` is the whole view's size. Its `asOf` is the height only: the view can't be
followed, so to stream a key, take the snapshot from `/boxes`. "Pending" means this relay's
node's mempool.

`/balance` is `{ "asOf": { "height" }, "confirmed", "pending", "used" }`. `pending` is the
signed net change the mempool's txs would make, per asset. `used` says whether the address ever
received a box, confirmed or pending, so a wallet can find its used addresses (a used address
can hold nothing). A pending payment counts, so `used` can go back to false if that tx leaves
the pool unconfirmed. It is not streamed: on a subscription, the first non-zero `balance` means
used, but a box received and spent within one block, or a pending output spent by another
pending tx, never shows as one; read `/balance` again to settle it.

`/transactions` is an address's history, read from the node's indexer: each tx's `txId`,
`height`, `timestamp` (milliseconds) and `delta`, its signed net change per asset for the
address. The first page lists the pending txs on top, without `height` or `timestamp`. It
takes `limit` (default 25, at most 100) and the previous page's `nextCursor`; a page can be
short and still have one, and `null` means the end. History is not streamed: when a
subscription says the address changed, read the first page again.

`/headers` is the last `limit` block headers (default 10, at most 10) up to the relay's tip,
newest first, for a transaction's signing context: `{ "asOf": { "height", "headerId" },
"items": [...] }`, the first item being the tip. Each item is the node's header JSON as it
came, Fleet SDK's `BlockHeader`. The tip is the best full block the relay serves boxes at, not
the node's best header, which `/blocks/lastHeaders` follows and which can run ahead of it.

`/boxes/{boxId}` is one box, as one item of `/boxes/with-pool`: confirmed with its
`inclusionHeight`, or created by a pending tx, without one. A box that is spent, by a block or
by a pending tx, or unknown is `404 box_not_found`. A box id is not a key, so the key policy
does not apply.

`/tokens/metadata?ids=` is the metadata of up to 100 tokens, comma-separated: `{ "items": [{
"tokenId", "mintingBoxId", "name", "description", "decimals", "emission" }] }`, in the order asked
and each once. A token the indexer does not know is left out, so a request is never refused for one
unknown id. `name`, `description` and `decimals` are what the minter wrote in the minting box's
R4 to R6 (EIP-4), read by the node: anyone can mint a token named like another, so they say
nothing about who issued it. `mintingBoxId` is the first output of the minting tx holding the
token, not the token id. No `ids`, or more than 100, is `400 invalid_ids`. Any token can be
named: the key policy and the supply limit do not apply.

`POST /v1/transactions` takes a signed tx serialized to hex (Fleet SDK's `serializeTransaction`) as
a `text/plain` body (a JSON string is taken too, as by the node), at most 196,608 characters (the
node's default 96 KiB limit), and forwards it to the node, which validates it and broadcasts it: `{
"transactionId": "..." }`. With `?broadcast=false` the node only validates it. A refusal is `400 {
"error": "transaction_rejected", "reason", "message" }`, `message` being the node's own: `reason` is
one of `unparseable`, `too_large`, `invalid` (also any message the relay does not know),
`double_spend`, `fee_too_low`, `already_in_pool`, `pool_full` and `policy`. A check covers less than
a send: it never answers the last five, so a tx can pass it and still be refused when sent. The tx
reaches the relay's mempool view at its next poll, like any other.

## WebSocket

`GET /v1/ws` streams the relay's tip, and what happens to a key after its snapshot. Every
connection gets `{ "type": "tip", "height": ..., "headerId": ... }` when it opens and whenever
the tip changes, before the messages of the block that changed it; a burst of blocks may bring
only the last tip. Clients send JSON text:

```jsonc
{ "op": "subscribe", "key": { "address": "9f..." }, "from": { "height": 1234567, "headerId": "..." } }
{ "op": "subscribe", "key": { "tokenId": "..." }, "from": { "height": 1234567, "headerId": "..." } }
{ "op": "subscribe", "key": { "address": "9f..." }, "id": 7 }   // balance only; `id` is optional
{ "op": "unsubscribe", "key": { "address": "9f..." } }
```

A subscribe that takes effect is answered with `subscribed`. A subscription with `from` (a page's
`asOf`) then replays the blocks since, or rolls back to the fork point, and sends the current
`mempool` overlay and, for an address, its `balance`; `block`, `rollback`, `mempool` and `balance`
messages follow live. One without `from` gets only `balance` messages, and loads the address if no
one tracks it. Subscribing again to a key replaces its subscription, even if the new subscribe is
refused; after a replace or an unsubscribe, a key's messages before the next `subscribed` belong to
the old subscription. A subscribe's answer (`subscribed`, `rejected` or `resync_required`) carries
its `id`, an integer the client picks: an older subscribe's answer can arrive after the client sent
a newer message for the key, so a client drops the key's messages until the answer with its latest
`id` (or, sending no ids, waits for a key's answer before its next message).

A refused subscribe gets `rejected`, with REST's error codes, `retryAfter` (seconds) where REST
would send `Retry-After`, and two codes of its own: `too_many_subscriptions` and `from_required`
(a token has no balance). `resync_required` means: take a new snapshot, or for a balance-only
subscription, subscribe again.

The server pings every 30 s, so proxies' idle timeouts never close a quiet subscription, and
closes a connection that leaves two pings unanswered (close code 1008, `pings unanswered`). A
client that falls behind gets `resync_required` (`slow_consumer`) and is disconnected; one that
sends malformed JSON is disconnected with close code 1008 and the reason. Messages from clients
are limited to 16 KiB; a larger one closes the connection with 1009. The subscription cap counts
a subscribe the client replaced or unsubscribed until it finishes, since it may still be loading
the key.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- --deny warnings
cargo test
```

- The `sim` feature exposes the `sim` module (a simulated chain and oracle) to integration tests.
- Node client tests run against recorded responses in `tests/fixtures/`, served with `wiremock`.
  To re-record them from a live node:

    ```sh
    node scripts/record-fixtures.ts --node http://127.0.0.1:9053
    ```

    See the header of `scripts/record-fixtures.ts` for all options.

- The smoke tests check the relay against a real indexed node. They are ignored by default:

    ```sh
    ERGO_NODE_URL=http://127.0.0.1:9053 cargo test --test smoke -- --ignored
    ```

- `examples/membench.rs` measures the memory a tracked box and a cached box take; its header
  has the commands.

## Project layout

| Path              | Contents                                                  |
| ----------------- | --------------------------------------------------------- |
| `src/clients/`    | Ergo node HTTP client and the `NodeSource` trait          |
| `src/types/`      | Node types and the address codec                          |
| `src/engine/`     | The engine: a pure state machine with no I/O and no clock |
| `src/adapters/`   | Node I/O feeding the engine                               |
| `src/runtime/`    | The engine task, output routing, configured keys          |
| `src/config.rs`   | Configuration from `hergmes.toml` and `ERGO_NODE_URL`     |
| `src/api/`        | REST routes and their services, the WebSocket, metrics    |
| `src/sim/`        | Simulated chain and oracle for tests (`sim` feature)      |
| `tests/smoke.rs`  | Smoke tests against a real node                           |
| `tests/fixtures/` | Recorded node responses                                   |
| `scripts/`        | The fixture recorder                                      |
| `examples/`       | The memory benchmark                                      |
