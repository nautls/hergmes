import { afterEach, expect, test, vi } from "vitest";
import { Relay, type SubscriptionState } from "../src/index.ts";

/** A socket the test feeds by hand; it opens at once and records what is sent. */
class FakeSocket extends EventTarget {
  static readonly OPEN = 1;
  static last: FakeSocket | undefined;
  readyState = 0;
  readonly url: string;
  readonly sent: unknown[] = [];

  constructor(url: string) {
    super();
    this.url = url;
    FakeSocket.last = this;
    queueMicrotask(() => {
      this.readyState = FakeSocket.OPEN;
      this.dispatchEvent(new Event("open"));
    });
  }

  send(text: string): void {
    this.sent.push(JSON.parse(text));
  }

  close(): void {
    this.readyState = 3;
  }

  /** Delivers messages one after another within the caller's task. */
  deliver(...messages: unknown[]): void {
    for (const m of messages) {
      const data = typeof m === "string" ? m : JSON.stringify(m);
      this.dispatchEvent(new MessageEvent("message", { data }));
    }
  }

  /** The socket closed under the relay. */
  drop(): void {
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
}

const key = { address: "9fAddress" };
const at = (height: number) => ({ height, headerId: height.toString(16).padStart(64, "0") });
const balance = (height: number, nanoErgs: number) => ({
  type: "balance",
  key,
  asOf: at(height),
  confirmed: { nanoErgs: String(nanoErgs), tokens: [] },
  pending: { nanoErgs: "0", tokens: [] },
});
const task = () => new Promise((r) => setTimeout(r, 0));
const Socket = FakeSocket as unknown as typeof WebSocket;
/** A `fetch` that never answers: snapshots stay in progress. */
const pending: typeof fetch = () => new Promise<Response>(() => undefined);

/** A relay on a fake socket, with `key` live balance-only. */
async function liveBalance(relay: Relay) {
  const sub = relay.subscribe(key, { boxes: false });
  await task();
  const socket = FakeSocket.last;
  if (!socket) throw new Error("no socket");
  socket.deliver({ type: "subscribed", id: 0, key }, balance(10, 1));
  return { sub, socket };
}

afterEach(() => {
  vi.useRealTimers();
});

test("listeners are called once for the messages that arrived together, with the last state", async () => {
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const sub = relay.subscribe(key, { boxes: false });
  const calls: SubscriptionState[] = [];
  sub.onChange((state) => calls.push(state));
  await task();
  const socket = FakeSocket.last;
  expect(socket?.sent).toEqual([{ op: "subscribe", key, id: 0 }]);
  // The subscribe's own phase change was notified on its own.
  calls.length = 0;

  socket?.deliver({ type: "subscribed", id: 0, key }, balance(10, 1), balance(11, 2));
  // The state is current at once; the listeners hear of it after the task.
  expect(sub.state.balance?.confirmed.nanoErgs).toBe(2n);
  expect(calls).toEqual([]);
  await task();
  expect(calls).toHaveLength(1);
  expect(calls[0]).toBe(sub.state);
  expect(calls[0]?.phase).toBe("live");

  socket?.deliver(balance(12, 3));
  await task();
  expect(calls.map((s) => s.balance?.confirmed.nanoErgs)).toEqual([2n, 3n]);

  // Nothing changed: no call.
  socket?.deliver({ type: "tip", ...at(12) });
  await task();
  expect(calls).toHaveLength(2);
  relay.close();
});

test("a subscription that ended starts again on the next subscribe", async () => {
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const { sub, socket } = await liveBalance(relay);
  socket.drop();
  expect(sub.state.phase).toBe("disconnected");
  const again = relay.subscribe(key, { boxes: false });
  await task();
  const next = FakeSocket.last;
  expect(next).not.toBe(socket);
  expect(next?.sent).toEqual([{ op: "subscribe", key, id: 1 }]);
  // The last balance stays while it starts again.
  expect(again.state).toMatchObject({ phase: "catching_up", balance: { asOf: at(10) } });
  expect(sub.state).toBe(again.state);

  next?.deliver({ type: "rejected", id: 1, key, reason: "not_ready", retryAfter: 1 });
  expect(sub.state.phase).toBe("rejected");
  relay.subscribe(key, { boxes: false });
  expect(next?.sent).toEqual([
    { op: "subscribe", key, id: 1 },
    { op: "subscribe", key, id: 2 },
  ]);
  relay.close();
});

test("a box subscription replacing a balance-only one keeps its balance meanwhile", async () => {
  const relay = new Relay("http://relay", { WebSocket: Socket, fetch: pending });
  const { sub } = await liveBalance(relay);
  relay.subscribe(key);
  expect(sub.state).toMatchObject({ phase: "snapshot", balance: { asOf: at(10) } });
  relay.close();
});

test("a listener that throws stops neither the others nor the SDK", async () => {
  vi.useFakeTimers();
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const other = { address: "9fOther" };
  const a = relay.subscribe(key, { boxes: false });
  const b = relay.subscribe(other, { boxes: false });
  await vi.advanceTimersByTimeAsync(0);
  const seen: string[] = [];
  a.onChange(() => {
    throw new Error("app bug");
  });
  a.onChange(() => seen.push("a"));
  b.onChange(() => seen.push("b"));
  relay.onTip(() => {
    throw new Error("tip bug");
  });
  FakeSocket.last?.deliver(
    { type: "subscribed", id: 0, key },
    { type: "tip", ...at(10) },
    balance(10, 1),
    { type: "subscribed", id: 1, key: other },
    { ...balance(10, 2), key: other },
  );
  expect(relay.tip).toEqual(at(10));
  expect(b.state.balance?.confirmed.nanoErgs).toBe(2n);
  // The listeners run on a timer; each error is thrown on a timer of its own.
  const errors: string[] = [];
  while (vi.getTimerCount() > 0) {
    try {
      vi.advanceTimersToNextTimer();
    } catch (error) {
      errors.push((error as Error).message);
    }
  }
  expect(seen).toEqual(["a", "b"]);
  expect(errors.sort()).toEqual(["app bug", "tip bug"]);
  relay.close();
});

test("a frame that is not JSON is ignored", async () => {
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const { sub, socket } = await liveBalance(relay);
  socket.deliver("ping", "null", "[1]", balance(11, 5));
  expect(sub.state.balance?.confirmed.nanoErgs).toBe(5n);
  relay.close();
});

test("the socket is under the same root as the routes", () => {
  const relay = new Relay("https://host/api?token=x", { WebSocket: Socket });
  relay.subscribe(key, { boxes: false });
  expect(FakeSocket.last?.url).toBe("wss://host/api/v1/ws");
  relay.close();
});

test("closing the relay forgets its tip", async () => {
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const { socket } = await liveBalance(relay);
  socket.deliver({ type: "tip", ...at(10) });
  expect(relay.tip).toEqual(at(10));
  relay.close();
  expect(relay.tip).toBeUndefined();
});
