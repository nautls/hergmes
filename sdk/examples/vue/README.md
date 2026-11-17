# hergmes Vue example

A page that follows addresses and tokens through a hergmes relay: each key's phase, balance,
spendable boxes and pending txs, and an address's history, as blocks and mempool txs arrive.
It uses the README's `useHandle` helper as it is there.

It builds against the SDK's source, so it needs no SDK build. From `sdk/`:

```sh
pnpm install
pnpm --dir examples/vue dev
```

Open the printed URL, enter the relay's root (`http://127.0.0.1:8080` by default, or
`?relay=<url>` in the page's URL) and an address or a token id. Token ids are 64 hex digits;
anything else is taken for an address. "Balance only" follows an address's balance without its
boxes. The relay URL and the tracked keys are kept in the browser's storage.

## Against the simulator

`sim-relay`, the relay over a simulated chain that the SDK's tests use, prints its port and
moves only when driven. From the repo's root:

```sh
cargo run --features sim --bin sim-relay      # prints {"port": …}
RELAY=http://127.0.0.1:<port>
curl -s $RELAY/sim/keys                       # its addresses and token ids
# Random blocks, mempool txs and reorgs, one per second:
for i in $(seq 1 1000); do
  curl -s -XPOST -H 'content-type: application/json' -d "{\"seed\":\"$i\"}" $RELAY/sim/random
  curl -s -XPOST $RELAY/sim/tick; echo; sleep 1
done
```
