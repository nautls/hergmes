/**
 * Calls an app's function. What it throws is the app's: rethrown on a task of its own, where
 * it surfaces as any uncaught error does, so the SDK's own work and the other listeners carry
 * on.
 */
export function call<T>(listener: ((value: T) => void) | undefined, value: T): void {
  try {
    listener?.(value);
  } catch (error) {
    setTimeout(() => {
      throw error;
    }, 0);
  }
}

/**
 * A Svelte store's `subscribe` over a handle's `state` and `onChange`: `listener` gets the
 * current state at once, then each change, never the same state twice in a row (a change
 * made before the batched call would otherwise come again).
 */
export function store<T>(
  handle: { readonly state: T; onChange(listener: (state: T) => void): () => void },
  listener: (state: T) => void,
): () => void {
  let last = handle.state;
  listener(last);
  return handle.onChange((state) => {
    if (state === last) return;
    last = state;
    listener(state);
  });
}
