# hergmes

A realtime, mempool-aware relay for the [Ergo](https://ergoplatform.org) blockchain.

hergmes sits in front of an indexed Ergo node and lets clients follow three kinds of **keys**:

- **addresses**,
- **small-supply tokens** (NFTs and other tokens with a small emission, set by
  `token_supply_limit`),
- **contract templates** (every instance of a contract, whatever its constants).

A client reads a key's confirmed boxes (and an address's balance) over REST, then gets live
updates over a WebSocket: new and spent boxes, pending mempool activity, balance changes and
reorgs. Pending activity shows in the balance as a signed change on top of the confirmed amount
("10 ERG incoming", "10 ERG leaving").

## Requirements

An Ergo node with:

- the **extra indexer** enabled (`extraIndex = true`), for the `/blockchain/*` endpoints;
- a **UTXO state** (`stateType = "utxo"`, the default): in digest mode the node resolves no
  mempool inputs.

## Getting started

```sh
ERGO_NODE_URL=http://127.0.0.1:9053 cargo run
```

The API then listens on `127.0.0.1:8080`. Check it with `curl localhost:8080/v1/status`.

## Configuration

Every setting but the node URL has a default. To change one, copy
[`hergmes.example.toml`](hergmes.example.toml) to `hergmes.toml` in the directory the relay starts
in, or pass `--config <path>`. An unknown key stops the relay at startup.

`ERGO_NODE_URL`, `HERGMES_LISTEN_ADDR` and `HERGMES_METRICS_ADDR` can replace `node_url`,
`listen_addr` and `metrics_addr`, for containers (set each in one place, not both).

| Key                                | Default        | Description                                                           |
| ---------------------------------- | -------------- | --------------------------------------------------------------------- |
| `node_url`                         |                | URL of an indexed Ergo node (required)                                |
| `listen_addr`                      | 127.0.0.1:8080 | Address of the REST API and the WebSocket                             |
| `metrics_addr`                     | none           | Address of the Prometheus `/metrics` endpoint                         |
| `allow_origins`                    | `["*"]`        | Browser origins allowed, exact or `"scheme://*"`                      |
| `track_addresses`                  | `[]`           | Addresses always tracked                                              |
| `track_tokens`                     | `[]`           | Token ids always tracked                                              |
| `track_templates`                  | `[]`           | Templates (hex) always tracked                                        |
| `allow_addresses`                  | any            | Addresses clients may use; also `"p2pk"`, `"p2sh"`, `"p2s"`           |
| `deny_addresses`                   | `[]`           | Addresses clients may not use; deny wins over allow                   |
| `allow_tokens`                     | any            | Token ids clients may use                                             |
| `allow_templates`                  | any            | Templates (hex) clients may use                                       |
| `token_supply_limit`               | 100            | Largest token emission that can be tracked                            |
| `window_blocks`                    | 720            | Blocks of history kept for resumes and rollbacks                      |
| `grace_secs`                       | 3600           | How long a key stays tracked after its last client leaves (min. 60)   |
| `max_boxes_per_key`                | 16383          | Most boxes a key can hold to be tracked                               |
| `max_tracked_units`                | 2000000        | Total tracking budget, about one unit per box (see [Sizing](#sizing)) |
| `box_cache_mb`                     | 128            | Memory for cached full boxes, in MiB                                  |
| `max_subscriptions_per_connection` | 100            | Most subscriptions per WebSocket connection                           |
| `poll_interval_ms`                 | 1000           | How often the node is polled for a new block or mempool change        |
| `mempool_refresh_ms`               | 10000          | Longest time between full mempool fetches                             |

### Restricting keys

With no allow lists the relay serves any address, any token within the supply limit and any
template. Allow and deny lists narrow that for an instance run for one dapp or wallet; an empty
allow list (`[]`) allows nothing. A refused key is `403 key_not_allowed`. Keys in the `track_*`
lists are always allowed.

```toml
# a dapp: its own contracts and tokens, with a higher supply limit
allow_addresses = ["<contract>", "<contract>"]
allow_tokens = ["<token-id>", "<token-id>"]
token_supply_limit = 1000000

# a wallet: keys and scripts it signs for, plus a few contracts it relies on
allow_addresses = ["p2pk", "p2sh", "<contract>"]

# a general-purpose instance, without busy contracts no client needs
deny_addresses = ["<miner-fee-contract>"]
```

## Deployment

- **Exposure.** The API listens on loopback by default. Put it behind a reverse proxy for TLS
  and rate limiting.
- **Compression.** The relay does not compress responses; enable gzip in the proxy (a box page
  shrinks about five times).
- **URL length.** Addresses and templates travel in the URL path. Long P2S addresses fit
  default proxy limits, but the longest templates exceed nginx's default request line: raise it
  with `large_client_header_buffers 4 16k;`.
- **File descriptors.** Each WebSocket holds one. The relay raises its soft limit to the hard
  one and warns below 4,096; under systemd, set `LimitNOFILE` if needed.
- **systemd.** Use `Type=notify`: the relay reports ready once `/v1/status` would, so units
  ordered `After=` it start once it has reached the node's tip and loaded its configured keys.
  Loading those from a cold node can take minutes, so raise `TimeoutStartSec` (e.g. `10min`).
- **Logging.** `RUST_LOG` sets the verbosity (default `info`; e.g. `RUST_LOG=hergmes=debug`).
- **Metrics.** Set `metrics_addr` (e.g. `"127.0.0.1:9100"`) to serve Prometheus metrics on a
  private address: tracked keys and boxes, cache hit rate, memory, refusals and resyncs.

## Sizing

The defaults fit a relay sharing a machine with its node: full at the default
`max_tracked_units`, with about 62k tracked keys, a mainnet relay held about 710 MiB resident.
Memory grows about linearly with what is tracked.

When the budget is full, keys with no subscribers that have been idle for a minute are evicted to
make room; a load is refused with `capacity_exceeded` only if that isn't enough.

To track more, raise `max_tracked_units` and `box_cache_mb` together, then watch
`hergmes_box_cache_hits_total` and `hergmes_box_cache_misses_total` (each miss is a node call).

## REST API

| Route                                         | Query parameters  | Returns                                           |
| --------------------------------------------- | ----------------- | ------------------------------------------------- |
| `GET /v1/addresses/{address}/boxes`           | `limit`, `cursor` | A page of an address's unspent boxes              |
| `GET /v1/tokens/{tokenId}/boxes`              | `limit`, `cursor` | A page of a small-supply token's unspent boxes    |
| `GET /v1/templates/{template}/boxes`          | `limit`, `cursor` | A page of a contract template's unspent boxes     |
| `GET /v1/addresses/{address}/boxes/spendable` |                   | An address's spendable boxes, mempool applied     |
| `GET /v1/addresses/{address}/balance`         |                   | Confirmed balance, pending change, and `used`     |
| `GET /v1/addresses/{address}/transactions`    | `limit`, `cursor` | Tx history, newest first, pending on top          |
| `GET /v1/boxes/{boxId}`                       |                   | One box, if a tx can spend it now                 |
| `GET /v1/headers`                             | `limit`           | The last block headers, newest first, for signing |
| `GET /v1/tokens/metadata`                     | `ids` (required)  | Token EIP-4 metadata: name, decimals, emission…   |
| `POST /v1/transactions`                       | `broadcast`       | Sends a signed tx, given as hex in the body       |
| `GET /v1/status`                              |                   | Version, network, tip and readiness               |

Parameters:

- **`limit`** is how many items a page holds:

  | Route           | Default | Max  |
  | --------------- | ------- | ---- |
  | box pages       | 500     | 1000 |
  | `/transactions` | 25      | 100  |
  | `/headers`      | 10      | 10   |

- **`cursor`** fetches the next page: pass the previous response's `nextCursor`. Leave it out for
  the first page; `nextCursor: null` means there are no more (a short page can still have a
  cursor). All pages of one box walk show the same block as the first (`asOf`).
- **`ids`** is a comma-separated list of up to 100 token ids:
  `/v1/tokens/metadata?ids=<id1>,<id2>`.
- **`broadcast=false`** makes the node only validate the tx, without sending it. Default `true`.

Responses:

- Boxes use Fleet SDK's `Box<string>` shape plus `inclusionHeight`; amounts are integer strings.
- `/boxes/spendable` is a one-shot view for stateless clients: confirmed boxes minus those
  pending txs spend, plus those they create. To follow an address live, use `/boxes` and the
  WebSocket.
- A template is an ErgoTree's bytes after its header and constants, in hex. Only trees with
  segregated constants are supported; others are `422 unsupported_template`.
- Errors are `{ "error": "<code>", "message": "..." }`; a `503` carries `Retry-After`. A
  refused tx is `400 transaction_rejected` with a `reason`.

## WebSocket

`GET /v1/ws` streams the relay's tip and the changes to subscribed keys. The
[SDK](sdk/README.md) handles the protocol below; use it unless you need a raw client.

```jsonc
// follow a key's boxes from a REST page's `asOf`
{ "op": "subscribe", "key": { "address": "9f..." }, "from": { "height": 1234567, "headerId": "..." }, "id": 1 }
{ "op": "subscribe", "key": { "tokenId": "..." },  "from": { ... } }
{ "op": "subscribe", "key": { "template": "..." }, "from": { ... } }
// an address's balance only, without its boxes
{ "op": "subscribe", "key": { "address": "9f..." } }
{ "op": "unsubscribe", "key": { "address": "9f..." } }
```

The server sends:

- `tip` on connect and whenever the tip changes;
- `subscribed`, `rejected` or `resync_required` in answer to a subscribe, echoing its `id`;
- `block` and `rollback` when a block changes the key's boxes, then `mempool` and `balance`
  updates.

Each message links to the key's previous change (`prev` on `block`/`rollback`, `lastChange` on
`mempool`/`balance`). If a link doesn't match what the client last saw, a change was missed:
take a new snapshot. `resync_required` means the same.

The server pings every 30 s. Slow consumers are sent `resync_required` (`slow_consumer`) and
disconnected; client messages are limited to 16 KiB.

## Development

See [AGENTS.md](AGENTS.md) for the commands CI runs, the architecture and the testing approach.
The smoke tests run against a real indexed node and are ignored by default:

```sh
ERGO_NODE_URL=http://127.0.0.1:9053 cargo test --test smoke -- --ignored
```
