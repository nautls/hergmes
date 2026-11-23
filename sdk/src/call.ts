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
 * A Svelte store's `subscribe` over a handle's state and the listeners its batched calls go
 * to: `listener` gets the current state at once, then each change, never the same state twice
 * in a row (a change made before the batched call would otherwise come again).
 */
export function store<T>(
  current: T,
  listeners: Set<(state: T) => void>,
  listener: (state: T) => void,
): () => void {
  let last = current;
  call(listener, last);
  const changed = (state: T) => {
    if (state === last) return;
    last = state;
    listener(state);
  };
  listeners.add(changed);
  return () => {
    listeners.delete(changed);
  };
}
