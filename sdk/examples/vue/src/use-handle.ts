import { shallowRef, watchEffect } from "vue";

// The helper from the SDK's README, as it is there.

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
