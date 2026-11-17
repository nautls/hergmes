import { afterEach, expect, test, vi } from "vitest";
import { Relay, type SubscriptionState } from "../src/index.ts";
import { FakeSocket } from "./fake-socket.ts";

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
const Socket = FakeSocket.class;
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
  vi.restoreAllMocks();
  FakeSocket.opens = true;
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

test("a dropped socket opens again after the backoff, and each key subscribes again", async () => {
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  const relay = new Relay("http://relay", {
    WebSocket: Socket,
    backoff: { firstMs: 1000, maxMs: 4000 },
  });
  const { sub, socket } = await liveBalance(relay);
  vi.useFakeTimers();
  socket.drop();
  expect(sub.state).toMatchObject({ phase: "disconnected", balance: { asOf: at(10) } });
  // A subscribe meanwhile waits for the backoff too.
  const again = relay.subscribe(key, { boxes: false });
  const other = { address: "9fOther" };
  relay.subscribe(other, { boxes: false });
  expect(FakeSocket.last).toBe(socket);
  await vi.advanceTimersByTimeAsync(499);
  expect(FakeSocket.last).toBe(socket);
  await vi.advanceTimersByTimeAsync(1);
  const next = FakeSocket.last;
  expect(next).not.toBe(socket);
  expect(next?.sent).toEqual([
    { op: "subscribe", key, id: 1 },
    { op: "subscribe", key: other, id: 2 },
  ]);
  expect(again.state).toBe(sub.state);
  next?.deliver({ type: "subscribed", id: 1, key }, balance(12, 3));
  expect(sub.state).toMatchObject({ phase: "live", balance: { asOf: at(12) } });
  relay.close();
});

test("the reconnect's wait is drawn under a doubling bound, which starts over once a key is live", async () => {
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  const relay = new Relay("http://relay", {
    WebSocket: Socket,
    backoff: { firstMs: 1000, maxMs: 4000 },
  });
  const { socket } = await liveBalance(relay);
  vi.useFakeTimers();
  const waits: number[] = [];
  /** Drops the current socket and measures the wait until the next one opens. */
  const dropAndWait = async () => {
    const dropped = FakeSocket.last;
    dropped?.drop();
    let waited = 0;
    while (FakeSocket.last === dropped) {
      await vi.advanceTimersByTimeAsync(100);
      waited += 100;
    }
    waits.push(waited);
  };
  expect(FakeSocket.last).toBe(socket);
  await dropAndWait();
  await dropAndWait();
  await dropAndWait();
  await dropAndWait();
  // A relay that accepts and sends its tip, then closes, is not back.
  FakeSocket.last?.deliver({ type: "tip", ...at(11) });
  await dropAndWait();
  // A key live again is.
  const last = FakeSocket.last;
  const { id } = last?.sent.at(-1) as { id: number };
  last?.deliver({ type: "subscribed", id, key }, balance(11, 2));
  await dropAndWait();
  // Each wait is half its bound, with `random` at 0.5: 1, 2, 4, then 4 s at the cap.
  expect(waits).toEqual([500, 1000, 2000, 2000, 2000, 500]);
  relay.close();
});

test("keys stay disconnected until the new socket opens", async () => {
  vi.spyOn(Math, "random").mockReturnValue(0);
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const { sub, socket } = await liveBalance(relay);
  vi.useFakeTimers();
  FakeSocket.opens = false;
  socket.drop();
  await vi.advanceTimersByTimeAsync(0);
  const connecting = FakeSocket.last;
  expect(connecting).not.toBe(socket);
  expect(sub.state.phase).toBe("disconnected");
  expect(connecting?.sent).toEqual([]);
  // It opens after all.
  if (!connecting) throw new Error("no socket");
  connecting.readyState = FakeSocket.OPEN;
  connecting.dispatchEvent(new Event("open"));
  expect(sub.state.phase).toBe("catching_up");
  expect(connecting.sent).toEqual([{ op: "subscribe", key, id: 1 }]);
  relay.close();
});

test("closing the relay or the last hold stops the reconnect", async () => {
  for (const end of ["relay", "hold"] as const) {
    const relay = new Relay("http://relay", { WebSocket: Socket });
    const { sub, socket } = await liveBalance(relay);
    await task();
    vi.useFakeTimers();
    socket.drop();
    if (end === "relay") relay.close();
    else sub.close();
    // Only the listeners' call is left.
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(FakeSocket.last).toBe(socket);
    vi.useRealTimers();
  }
});

test("a frame from a socket already dropped is ignored", async () => {
  vi.spyOn(Math, "random").mockReturnValue(0);
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const { sub, socket } = await liveBalance(relay);
  vi.useFakeTimers();
  socket.drop();
  await vi.advanceTimersByTimeAsync(0);
  const next = FakeSocket.last;
  expect(next?.sent).toEqual([{ op: "subscribe", key, id: 1 }]);
  // Late frames of the old socket, one even answering the new subscribe's id.
  socket.deliver({ type: "subscribed", id: 1, key }, balance(11, 9), { type: "tip", ...at(11) });
  expect(sub.state.phase).toBe("catching_up");
  expect(relay.tip).toBeUndefined();
  relay.close();
});

test("a refusal for now waits its retryAfter; closing the hold or the relay cancels it", async () => {
  vi.useFakeTimers();
  for (const end of ["none", "hold", "relay"] as const) {
    const relay = new Relay("http://relay", { WebSocket: Socket });
    const sub = relay.subscribe(key, { boxes: false });
    await vi.advanceTimersByTimeAsync(0);
    const socket = FakeSocket.last;
    if (!socket) throw new Error("no socket");
    const id = (socket.sent[0] as { id: number }).id;
    socket.deliver({ type: "rejected", id, key, reason: "not_ready", retryAfter: 2 });
    expect(sub.state).toMatchObject({ phase: "waiting", error: { code: "not_ready" } });
    if (end === "hold") sub.close();
    if (end === "relay") relay.close();
    // The listeners' call: then only the retry, if any, is left.
    await vi.advanceTimersByTimeAsync(1);
    expect(vi.getTimerCount()).toBe(end === "none" ? 1 : 0);
    const sent = socket.sent.length;
    await vi.advanceTimersByTimeAsync(1998);
    expect(socket.sent).toHaveLength(sent);
    await vi.advanceTimersByTimeAsync(1);
    if (end === "none") {
      expect(socket.sent.at(-1)).toEqual({ op: "subscribe", key, id: id + 1 });
      expect(sub.state.phase).toBe("catching_up");
    } else {
      expect(socket.sent).toHaveLength(sent);
    }
    relay.close();
  }
});

test("a snapshot that fails for now is read again after the backoff; a 4xx is final", async () => {
  // Only timeouts: reading a response's body goes through the others.
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  const reads: string[] = [];
  let answer: () => Promise<Response> = () => Promise.reject(new TypeError("fetch failed"));
  const fetch: typeof globalThis.fetch = (input) => {
    reads.push((input as string | URL).toString());
    return answer();
  };
  const relay = new Relay("http://relay", { WebSocket: Socket, fetch, maxAttempts: 1 });
  const sub = relay.subscribe(key);
  await vi.advanceTimersByTimeAsync(0);
  expect(sub.state).toMatchObject({
    phase: "waiting",
    error: { code: "network", retryAfter: 0.5 },
  });
  expect(reads).toHaveLength(1);
  // A proxy's rate limit, then its page in place of the relay's answer: both for now.
  answer = () => Promise.resolve(new Response("slow down", { status: 429 }));
  await vi.advanceTimersByTimeAsync(500);
  expect(reads).toHaveLength(2);
  expect(sub.state).toMatchObject({
    phase: "waiting",
    error: { code: "http_429", status: 429, retryAfter: 1 },
  });
  answer = () => Promise.resolve(new Response("<html>", { status: 200 }));
  await vi.advanceTimersByTimeAsync(1000);
  expect(reads).toHaveLength(3);
  expect(sub.state).toMatchObject({ phase: "waiting", error: { code: "bad_response" } });
  answer = () =>
    Promise.resolve(Response.json({ error: "key_not_allowed", message: "no" }, { status: 403 }));
  await vi.advanceTimersByTimeAsync(2000);
  expect(reads).toHaveLength(4);
  expect(sub.state).toMatchObject({
    phase: "rejected",
    error: { code: "key_not_allowed", status: 403 },
  });
  // The listeners' call; then nothing is left to retry.
  await vi.runOnlyPendingTimersAsync();
  expect(vi.getTimerCount()).toBe(0);
  // The app's next subscribe tries again.
  relay.subscribe(key);
  expect(reads).toHaveLength(5);
  relay.close();
});

test("a snapshot read stops when its hold or its relay closes", async () => {
  const signals: (AbortSignal | undefined)[] = [];
  const fetch: typeof globalThis.fetch = (_, init) => {
    signals.push(init?.signal ?? undefined);
    return new Promise<Response>(() => undefined);
  };
  const relay = new Relay("http://relay", { WebSocket: Socket, fetch });
  const sub = relay.subscribe(key);
  relay.subscribe({ address: "9fOther" });
  await task();
  expect(signals.map((s) => s?.aborted)).toEqual([false, false]);
  sub.close();
  expect(signals.map((s) => s?.aborted)).toEqual([true, false]);
  relay.close();
  expect(signals.map((s) => s?.aborted)).toEqual([true, true]);
});

test("a key refused for good starts again on the next subscribe", async () => {
  const relay = new Relay("http://relay", { WebSocket: Socket });
  const sub = relay.subscribe(key, { boxes: false });
  await task();
  const socket = FakeSocket.last;
  socket?.deliver({ type: "rejected", id: 0, key, reason: "too_many_subscriptions" });
  expect(sub.state.phase).toBe("rejected");
  relay.subscribe(key, { boxes: false });
  expect(socket?.sent).toEqual([
    { op: "subscribe", key, id: 0 },
    { op: "subscribe", key, id: 1 },
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
