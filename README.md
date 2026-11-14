# hergmes

A realtime, mempool-aware relay for the [Ergo](https://ergoplatform.org) blockchain.

hergmes sits in front of an indexed Ergo node and lets clients track the boxes of **addresses**
and **small-supply tokens** (NFTs and tokens with an emission of at most 100). A client gets a
key's current boxes and an address's balance, then receives live updates: new boxes, spent
boxes, pending mempool activity, balance changes and reorgs. Pending activity shows in the balance as a signed
change on top of the confirmed amount ("10 ERG incoming", "10 ERG leaving").

> **Status:** early development. The relay follows the node, tracks the keys named in its
> configuration and serves the REST API. The WebSocket layer is still being built.

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
  resolves no mempool inputs and serves no `/utxo/withPool` boxes
- **Optional:** Node.js 23.6+, if you want to re-record test fixtures

## Getting started

```sh
cp .env.example .env
# set ERGO_NODE_URL to your indexed node, e.g. http://127.0.0.1:9053
cargo run
```

| Variable                     | Default        | Description                                                                       |
| ---------------------------- | -------------- | --------------------------------------------------------------------------------- |
| `ERGO_NODE_URL`              |                | URL of an indexed Ergo node (required)                                            |
| `HERGMES_WINDOW_BLOCKS`      | 720            | Blocks of deltas kept for replays, rollbacks and pinned pages                     |
| `HERGMES_GRACE_BLOCKS`       | 5              | Blocks a key with no subscribers stays tracked                                    |
| `HERGMES_POLL_INTERVAL_MS`   | 1000           | How often the node's status is read to spot a new block or mempool change         |
| `HERGMES_MEMPOOL_REFRESH_MS` | 10000          | Most time between full mempool fetches, even if the node's status shows no change |
| `HERGMES_TOKEN_SUPPLY_LIMIT` | 100            | Largest token emission that can be tracked                                        |
| `HERGMES_TRACK_ADDRESSES`    |                | Addresses tracked for the life of the process, comma-separated                    |
| `HERGMES_TRACK_TOKENS`       |                | Token ids tracked for the life of the process, comma-separated                    |
| `HERGMES_ALLOW_ADDRESSES`    | any            | Addresses clients may name: `p2pk`, `p2sh`, `p2s`, addresses or `none`            |
| `HERGMES_DENY_ADDRESSES`     |                | Addresses clients may not name; deny wins over allow                              |
| `HERGMES_ALLOW_TOKENS`       | any            | Token ids clients may name, or `none`                                             |
| `HERGMES_BOX_CACHE_MB`       | 128            | Memory for full boxes kept to serve pages, in MiB; 0 disables it                  |
| `HERGMES_MAX_BOXES_PER_KEY`  | 50000          | Most boxes an address or token can have to be tracked                             |
| `HERGMES_MAX_TRACKED_BOXES`  | 2000000        | Most boxes tracked over all keys (about 110 bytes each)                           |
| `HERGMES_LISTEN_ADDR`        | 127.0.0.1:8080 | IP and port of the REST API                                                       |

By default (`any`), the relay serves any address and any token within the supply limit. The allow and deny
lists narrow that, for an instance run for one dapp or one wallet; a refused key is
`403 key_not_allowed`, on every route. Keys in `HERGMES_TRACK_*` are always allowed. Denying one,
or denying an address the allow list names, stops the relay at startup. The startup log shows the
policy in force, so a misspelled variable shows up as `any`. For example:

```sh
# a dapp: its own contracts and tokens, with a higher supply limit
HERGMES_ALLOW_ADDRESSES=<contract>,<contract>
HERGMES_ALLOW_TOKENS=<token-id>,<token-id>
# at least the largest emission among the allowed tokens
HERGMES_TOKEN_SUPPLY_LIMIT=1000000

# a wallet: keys and scripts it signs for, plus a few contracts it relies on
HERGMES_ALLOW_ADDRESSES=p2pk,p2sh,<contract>

# a general-purpose instance, without busy contracts no client needs
HERGMES_DENY_ADDRESSES=<miner-fee-contract>
```

The API listens on loopback by default, so a fresh install is not exposed until you choose. Put
it behind a reverse proxy for TLS and rate limiting, or set `HERGMES_LISTEN_ADDR=0.0.0.0:8080` in
a container. The relay does not compress responses: Cloudflare does it on its own, and behind
nginx or Caddy turn it on there (`gzip on; gzip_types application/json;`, or `encode gzip`); a
500-box page shrinks about five times. Addresses travel in the URL path, and the longest P2S address is about 5,600
characters: that fits nginx's default 8 KB request line and Cloudflare's 16 KB URL limit, so keep
any lower proxy limit above it.

Logging uses `tracing`; set `RUST_LOG` (e.g. `RUST_LOG=hergmes=debug`) to adjust verbosity.

## REST API

| Route                                 | What                                              |
| ------------------------------------- | ------------------------------------------------- |
| `GET /v1/addresses/{address}/boxes`   | A page of an address's unspent boxes              |
| `GET /v1/tokens/{tokenId}/boxes`      | A page of a small-supply token's unspent boxes    |
| `GET /v1/addresses/{address}/balance` | An address's confirmed balance and pending change |
| `GET /v1/status`                      | Version, network, tip and readiness               |

Box pages take `limit` (default 500, at most 1000) and the `cursor` from the previous page's
`nextCursor`; every page of a snapshot is pinned to the first page's block (`asOf`). Boxes are in
Fleet SDK's `Box<string>` shape plus `inclusionHeight`, and every amount is a base-10 integer
string. Errors are `{ "error": "<code>", "message": "..." }`, and a `503` carries `Retry-After`.

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

## Project layout

| Path              | Contents                                                  |
| ----------------- | --------------------------------------------------------- |
| `src/clients/`    | Ergo node HTTP client and the `NodeSource` trait          |
| `src/types/`      | Wire types and the address codec                          |
| `src/engine/`     | The engine: a pure state machine with no I/O and no clock |
| `src/adapters/`   | Node I/O feeding the engine                               |
| `src/runtime/`    | The engine task, output routing, configured keys          |
| `src/config.rs`   | Configuration from environment variables                  |
| `src/api/`        | REST routes and their services; WebSocket (in progress)   |
| `src/sim/`        | Simulated chain for tests (`sim` feature)                 |
| `tests/fixtures/` | Recorded node responses                                   |
