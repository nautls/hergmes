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
