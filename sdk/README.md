# hergmes SDK

The TypeScript client for the hergmes relay: an address's or a small-supply token's boxes and an
address's balance, kept current over one WebSocket with the mempool applied, its tx history, and
the relay's REST routes.

## Install

```sh
npm install @hergmes/sdk
```

## Usage

```ts
import { Relay } from "@hergmes/sdk";

// The relay's root, under which `/v1` is served; it may have a path.
const relay = new Relay("https://relay.example.com");

const wallet = relay.subscribe({ address: "9f…" });
wallet.subscribe(({ phase, boxes, balance }) => {
  if (phase !== "live") return; // `boxes` and `balance` are their last live values, stale
  console.log(boxes?.spendable.length, balance?.confirmed.nanoErgs, balance?.pending.nanoErgs);
});

// Later: ends this hold, and the key's subscription with the last one.
wallet.close();
// Or everything: the socket, every subscription and history.
relay.close();
```

### Subscriptions

`relay.subscribe(key, { boxes })` answers a `Subscription`: a hold on the key. The key is
`{ address }`, `{ tokenId }` or `{ template }`: a template in hex, a constant-segregated
ErgoTree's bytes after its constants, for the boxes of every instance of a contract whatever its
constants.

- With boxes (the default) the SDK reads the key's confirmed boxes, subscribes from that block
  and applies every block, rollback and mempool change that touches the key. Its `state.boxes` has
  the confirmed boxes, the pending txs as they affect the key, and `spendable`, what a tx can
  spend now: confirmed boxes and pending outputs no pending tx spends. For an address, its total
  is the balance's `confirmed + pending`; for any key, `utxoSum(boxes.spendable)` from
  `@fleet-sdk/common` totals it. `boxes.lastChange` is the block of
  the key's last change: the relay sends a key only the blocks that change it, so the height to
  show is the relay's, `relay.tip`.
- With `{ boxes: false }`, an address's balance alone: no snapshot, the lightest subscription.
  Token and template keys have no balance and always follow boxes.

`state.balance` is the address's balance: `confirmed`, and `pending`, the signed change the
mempool makes to it. Amounts are `bigint`s, tokens a `Map` from token id to amount.

`state.phase` says where the subscription is, and whether `boxes` and `balance` are current:

| Phase          | Meaning                                                                      |
| -------------- | ---------------------------------------------------------------------------- |
| `snapshot`     | reading the key's boxes                                                      |
| `catching_up`  | subscribed, replaying to the relay's tip                                     |
| `live`         | following the chain: `boxes` and `balance` are current                       |
| `waiting`      | refused or failed for now; `error.retryAfter` says when it starts again      |
| `rejected`     | refused or failed for good, `error` says why; a later subscribe tries again  |
| `disconnected` | the socket closed; it resumes from the key's last change once it opens again |

Outside `live`, `boxes` and `balance` keep their last live values, to show as stale. Recovery is
the SDK's: after a closed socket it reconnects with a jittered backoff and each key resumes from
its last change, and when the relay asks for a resync, or a message doesn't fit its boxes, the key reads
a new snapshot.

`state` is replaced whole on every change. `subscribe(listener)` calls the listener at once with
the current state, then once for the messages that arrived together, with the latest state. It
answers a function that removes the listener.

Several holds on one key share its subscription, so components can each open their own. A box
hold replaces a balance-only subscription; the key is unsubscribed when its last hold closes.

`relay.tip` is the relay's tip, the block every box and balance it serves is at, and
`relay.onTip(listener)` sees each new one.

### History

`relay.history(address, { refresh, limit })` answers a `TxHistory`: the address's txs, newest
first, pending ones on top, each with its signed net change to the address. The first page is
read at once; `more()` reads the next older one.

With `refresh: true` the first page is read again whenever the address's balance changes. This
holds the address as a balance-only subscription does, sharing the key's subscription if the
app has one, so close the history when done. Older pages are not checked again: when the list
no longer joins them, it is read again from the top and `state.resets` goes up.

```ts
const history = relay.history(address, { refresh: true });
history.subscribe(({ items, end, loading, error }) => render(items));
await history.more();
history.close();
```

### REST

`relay.rest` is a `RestClient`, also usable alone as `new RestClient(url)`. Each method answers
the route's JSON as it came, amounts as base-10 strings:

| Method                                 | Route                                         |
| -------------------------------------- | --------------------------------------------- |
| `status()`                             | `GET /v1/status`                              |
| `parameters()`                         | `GET /v1/parameters`                          |
| `boxes(key, { cursor, limit })`        | `GET /v1/{addresses,tokens}/{key}/boxes`      |
| `spendable(address)`                   | `GET /v1/addresses/{address}/boxes/spendable` |
| `balance(address)`                     | `GET /v1/addresses/{address}/balance`         |
| `history(address, { cursor, limit })`  | `GET /v1/addresses/{address}/transactions`    |
| `headers({ limit })`                   | `GET /v1/headers`                             |
| `box(boxId)`                           | `GET /v1/boxes/{boxId}`                       |
| `tokens(ids)`                          | `GET /v1/tokens/metadata?ids=...`             |
| `submitTransaction(tx, { broadcast })` | `POST /v1/transactions`                       |

`tokens` answers the metadata of the tokens the relay's node knows among `ids`, leaving out the
others; more than 100 ids take a request per 100, one after another.

```ts
const { items } = await relay.rest.tokens(tokenIds);
// A token of no standard has no metadata, so its amounts have no decimals.
const decimals = new Map(
  items.map((t) => [t.tokenId, t.standard === "eip-4" ? t.metadata.decimals : 0]),
);
```

`submitTransaction` takes a signed tx in hex, or as Fleet's `SignedTransaction`, which the SDK
serializes. With `broadcast: false` the node only checks it.

```ts
const { transactionId } = await relay.rest.submitTransaction(signedTx);
```

Every method takes a `signal` to cancel it. A response other than 2xx throws a `RelayError`
with its `status`, `code`, `reason` (why the node refused a tx) and `retryAfter`. A `503` is
retried after its `Retry-After`, up to `maxAttempts` in all, except
`capacity_exceeded`, which frees up only as keys go idle.

```ts
try {
  await relay.rest.submitTransaction(signedTx);
} catch (error) {
  if (error instanceof RelayError && error.reason === "fee_too_low") {
    // …
  }
}
```

### Options

`new Relay(url, options)`:

- `backoff: { firstMs, maxMs }`: the waits before reconnecting and before retrying a key, each
  drawn from 0 to a bound that doubles from `firstMs` to `maxMs`; 1 s and 30 s by default.
- `pageSize`: boxes per snapshot page, at most 1000 (the default).
- `maxAttempts`: attempts in all for a request answered `503`; 3 by default.
- `debug`: sees every message the relay sends, as text, and every new snapshot taken, for
  logging.
- `fetch`, `WebSocket`: the implementations to use, the global ones by default.

## Using the SDK with UI frameworks

The SDK hands out two kinds of handle, with one shape:

- `relay.subscribe(key)`, a key's boxes: the confirmed set, the mempool's overlay and what is
  spendable now, plus an address's balance;
- `relay.subscribe({ address }, { boxes: false })`, an address's balance alone;
- `relay.history(address, { refresh: true })`, an address's txs, the first page kept fresh.

Each has a `state` replaced whole on every change, `subscribe(listener)` (a Svelte store's
contract: the current state at once, then each change) and `close()`. So one
helper per framework serves all of them. Close a handle when the component that opened it goes
away: a subscription, and a history with `refresh`, hold the key at the relay until then.

### React

```tsx
import { useCallback, useEffect, useState, useSyncExternalStore, type DependencyList } from "react";

interface Handle<S> {
  readonly state: S;
  subscribe(listener: (state: S) => void): () => void;
  close(): void;
}

/** Opens a handle while the component is mounted, again when `deps` change. */
export function useHandle<H extends Handle<unknown>>(open: () => H, deps: DependencyList) {
  const [handle, setHandle] = useState<H>();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    const h = open();
    setHandle(h);
    return () => h.close();
  }, deps);
  const subscribe = useCallback(
    (onChange: () => void) => handle?.subscribe(onChange) ?? (() => undefined),
    [handle],
  );
  const state = useSyncExternalStore(subscribe, () => handle?.state as H["state"] | undefined);
  return { handle, state };
}
```

### Vue

`examples/vue` is a runnable page built on this helper. `state` is frozen, so a `shallowRef` holds it. The handle opens again when what `open` reads
changes (a prop, a ref):

```ts
import { shallowRef, watchEffect } from "vue";

interface Handle<S> {
  readonly state: S;
  subscribe(listener: (state: S) => void): () => void;
  close(): void;
}

export function useHandle<H extends Handle<unknown>>(open: () => H) {
  const handle = shallowRef<H>();
  const state = shallowRef<H["state"]>();
  watchEffect((onCleanup) => {
    const h = open();
    handle.value = h;
    const off = h.subscribe((s) => (state.value = s));
    onCleanup(() => {
      off();
      h.close();
    });
  });
  return { handle, state };
}
```

### Svelte

A handle is a store: `$handle` is its state. Wrap the component in `{#key address}` where the
address can change.

```svelte
<script lang="ts">
  import { onDestroy } from "svelte";
  import type { Relay } from "@hergmes/sdk";

  let { relay, address }: { relay: Relay; address: string } = $props();
  const wallet = relay.subscribe({ address });
  const history = relay.history(address, { refresh: true });
  onDestroy(() => {
    wallet.close();
    history.close();
  });
</script>

<p>{$wallet.phase}: {$wallet.boxes?.spendable.length ?? "…"} boxes spendable</p>
```

### What each state holds

With React (Vue's `state` is a ref, `state.value` in a script):

```tsx
// An address's balance alone: no snapshot, the lightest subscription.
const { state: balance } = useHandle(
  () => relay.subscribe({ address }, { boxes: false }),
  [relay, address],
);
// balance?.balance?.confirmed.nanoErgs, balance?.balance?.pending.nanoErgs (signed)

// Boxes, for building txs: what can be spent now.
const { state: wallet } = useHandle(() => relay.subscribe({ address }), [relay, address]);
// wallet?.boxes?.spendable; its totals: utxoSum(wallet.boxes.spendable) from @fleet-sdk/common

// History: the first page kept fresh, older pages on demand.
const { state: txs, handle: history } = useHandle(
  () => relay.history(address, { refresh: true }),
  [relay, address],
);
// txs?.items, then history?.more() at the bottom of the list
```

- `phase` says whether `boxes` and `balance` are current: only `live` is. While the
  subscription recovers (`snapshot`, `catching_up`, `disconnected`, `waiting`) they keep their
  last live values, to show as stale; `rejected` is final until the next subscribe, and
  `error` says why.
- Several handles on one key share its subscription: a balance-only one and a history with
  `refresh` on an address the app already follows cost nothing at the relay.
- A history's `resets` goes up when the list was read again from its first page, its older
  pages dropped: scroll back to the top. A history item without `height` is pending.
