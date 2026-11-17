## Using the SDK with UI frameworks

The SDK in `sdk/` hands out two kinds of handle, with one shape:

- `relay.subscribe(key)`, a key's boxes: the confirmed set, the mempool's overlay and what is
  spendable now, plus an address's balance;
- `relay.subscribe({ address }, { boxes: false })`, an address's balance alone;
- `relay.history(address, { refresh: true })`, an address's txs, the first page kept fresh.

Each has a `state` replaced whole on every change, `onChange(listener)`, `subscribe(listener)`
(a Svelte store's contract: the current state at once, then each change) and `close()`. So one
helper per framework serves all of them. Close a handle when the component that opened it goes
away: a subscription, and a history with `refresh`, hold the key at the relay until then.

### React

```tsx
import { useCallback, useEffect, useState, useSyncExternalStore, type DependencyList } from "react";

interface Handle<S> {
  readonly state: S;
  onChange(listener: (state: S) => void): () => void;
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
    (onChange: () => void) => handle?.onChange(onChange) ?? (() => undefined),
    [handle],
  );
  const state = useSyncExternalStore(subscribe, () => handle?.state as H["state"] | undefined);
  return { handle, state };
}
```

### Vue

`state` is frozen, so a `shallowRef` holds it. The handle opens again when what `open` reads
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
  import type { Relay } from "hergmes-sdk";

  let { relay, address }: { relay: Relay; address: string } = $props();
  const wallet = relay.subscribe({ address });
  const history = relay.history(address, { refresh: true });
  onDestroy(() => {
    wallet.close();
    history.close();
  });
</script>

<p>{$wallet.phase}: {$wallet.view?.spendableTotals.nanoErgs ?? "…"} nanoERG spendable</p>
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

// Boxes, for building txs: what can be spent now, and its totals.
const { state: wallet } = useHandle(() => relay.subscribe({ address }), [relay, address]);
// wallet?.view?.spendable, wallet?.view?.spendableTotals

// History: the first page kept fresh, older pages on demand.
const { state: txs, handle: history } = useHandle(
  () => relay.history(address, { refresh: true }),
  [relay, address],
);
// txs?.items, then history?.more() at the bottom of the list
```

- `phase` says whether `view` and `balance` are current: only `live` is. While the
  subscription recovers (`snapshot`, `catching_up`, `disconnected`, `waiting`) they keep their
  last live values, to show as stale; `rejected` is final until the next subscribe, and
  `error` says why.
- Several handles on one key share its subscription: a balance-only one and a history with
  `refresh` on an address the app already follows cost nothing at the relay.
- A history's `resets` goes up when the list was read again from its first page, its older
  pages dropped: scroll back to the top. A history item without `height` is pending.
