import { afterEach, expect, test, vi } from "vitest";
import { Relay, type HistoryState, type WireHistoryItem } from "../src/index.ts";
import { FakeSocket } from "./fake-socket.ts";

const address = "9fAddress";
const key = { address };
const at = (height: number) => ({ height, headerId: height.toString(16).padStart(64, "0") });
const balance = (height: number, nanoErgs: number) => ({
  type: "balance",
  key,
  asOf: at(height),
  confirmed: { nanoErgs: String(nanoErgs), tokens: [] },
  pending: { nanoErgs: "0", tokens: [] },
});
const task = () => new Promise((r) => setTimeout(r, 0));

/** A confirmed tx at `height`, or a pending one without it. */
function tx(txId: string, height?: number): WireHistoryItem {
  const delta = { nanoErgs: "1", tokens: [] };
  return height === undefined ? { txId, delta } : { txId, height, timestamp: height, delta };
}

const page = (items: WireHistoryItem[], nextCursor: string | null = null) =>
  Response.json({ items, nextCursor });
const expired = () =>
  Response.json({ error: "cursor_expired", message: "expired" }, { status: 410 });

/** The ids and heights of a history's items, `id@height` or `id` while pending. */
const ids = (state: HistoryState) =>
  state.items.map((i) => (i.height === undefined ? i.txId : `${i.txId}@${String(i.height)}`));

/**
 * A relay route serving history pages from `answer`, called with each read's cursor (`null` for
 * the first page). Records the cursors read, in order.
 */
function served(answer: (cursor: string | null) => Response | Promise<Response>) {
  const reads: (string | null)[] = [];
  const fetch: typeof globalThis.fetch = (input) => {
    const url = new URL((input as string | URL).toString());
    expect(url.pathname).toBe(`/v1/addresses/${address}/transactions`);
    const cursor = url.searchParams.get("cursor");
    reads.push(cursor);
    return Promise.resolve(answer(cursor));
  };
  return { fetch, reads, firsts: () => reads.filter((c) => c === null).length };
}

/** A promise settled from outside, for a read the test answers when it chooses. */
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test("more() appends older pages until the end, and reads one page at a time", async () => {
  const pages: Record<string, Response | undefined> = {};
  const route = served((cursor) => {
    if (cursor === null) return page([tx("p"), tx("c", 3), tx("b", 2)], "c1");
    if (cursor === "c1") return page([tx("a", 1)], null);
    return pages[cursor] ?? page([]);
  });
  const relay = new Relay("http://relay", { fetch: route.fetch });
  const history = relay.history(address, { limit: 2 });
  expect(history.state.loading).toBe(true);
  // A read in flight: `more()` answers it and reads nothing more.
  await history.more();
  expect(route.reads).toEqual([null]);
  expect(ids(history.state)).toEqual(["p", "c@3", "b@2"]);
  expect(history.state).toMatchObject({ end: false, loading: false, resets: 0 });

  const first = history.more();
  expect(history.more()).toBe(first);
  await first;
  expect(route.reads).toEqual([null, "c1"]);
  expect(ids(history.state)).toEqual(["p", "c@3", "b@2", "a@1"]);
  expect(history.state.end).toBe(true);
  // At the end: nothing to read.
  await history.more();
  expect(route.reads).toHaveLength(2);
  relay.close();
});

test("empty pages with a cursor are read through; three in a row are an error", async () => {
  let empties = 2;
  let n = 0;
  const route = served((cursor) => {
    if (cursor === null) return page([tx("b", 2)], "c0");
    n++;
    if (n <= empties) return page([], `c${String(n)}`);
    return page([tx("a", 1)], null);
  });
  const relay = new Relay("http://relay", { fetch: route.fetch });
  const history = relay.history(address);
  await history.more();
  await history.more();
  expect(route.reads).toEqual([null, "c0", "c1", "c2"]);
  expect(ids(history.state)).toEqual(["b@2", "a@1"]);
  expect(history.state).toMatchObject({ end: true, error: undefined });

  empties = 6;
  n = 0;
  const again = relay.history(address);
  await again.more();
  await again.more();
  expect(again.state).toMatchObject({ end: false, error: { code: "empty_pages" } });
  expect(ids(again.state)).toEqual(["b@2"]);
  // The next call goes on from the last empty page's cursor.
  await again.more();
  expect(route.reads.slice(-3)).toEqual(["c3", "c4", "c5"]);
  expect(again.state).toMatchObject({ error: { code: "empty_pages" } });
  await again.more();
  expect(route.reads.at(-1)).toBe("c6");
  expect(ids(again.state)).toEqual(["b@2", "a@1"]);
  expect(again.state).toMatchObject({ end: true, error: undefined });

  // A third empty page that is the last: the end, not an error.
  const last = new Relay("http://relay", {
    fetch: served((cursor) =>
      cursor === null
        ? page([tx("b", 2)], "e0")
        : page([], cursor === "e2" ? null : `e${String(Number(cursor.slice(1)) + 1)}`),
    ).fetch,
  });
  const ending = last.history(address);
  await ending.more();
  await ending.more();
  expect(ending.state).toMatchObject({ end: true, error: undefined });
  expect(ids(ending.state)).toEqual(["b@2"]);
  last.close();
  relay.close();
});

test("an expired cursor reads the first page again, drops the older pages and counts a reset", async () => {
  let firsts = 0;
  const route = served((cursor) => {
    if (cursor === null) {
      firsts++;
      if (firsts === 1) return page([tx("d", 4), tx("c", 3)], "c1");
      // The same first page as before: it would join the older pages, which the reorg reached.
      return firsts === 2 ? page([tx("d", 4), tx("c", 3)], "c9") : page([tx("e", 5)], "c9");
    }
    if (cursor === "c1") return page([tx("b", 2)], "c2");
    if (cursor === "c2") return expired();
    return page([tx("a", 1)]);
  });
  const relay = new Relay("http://relay", { fetch: route.fetch });
  const history = relay.history(address);
  await history.more();
  await history.more();
  expect(ids(history.state)).toEqual(["d@4", "c@3", "b@2"]);
  await history.more();
  expect(route.reads).toEqual([null, "c1", "c2", null]);
  expect(ids(history.state)).toEqual(["d@4", "c@3"]);
  expect(history.state).toMatchObject({ end: false, error: undefined, resets: 1 });
  // It scrolls on from the new first page.
  await history.more();
  expect(route.reads.at(-1)).toBe("c9");
  expect(ids(history.state)).toEqual(["d@4", "c@3", "a@1"]);
  relay.close();
});

/**
 * A refreshed first page against a list scrolled to `[p, d@4, c@3 | b@2, a@1]`, `p` pending:
 * the history's items after it, and its resets.
 */
async function refreshed(first: WireHistoryItem[], nextCursor: string | null = "c9") {
  let firsts = 0;
  const route = served((cursor) => {
    if (cursor !== null) return page([tx("b", 2), tx("a", 1)], "c2");
    firsts++;
    return firsts === 1 ? page([tx("p"), tx("d", 4), tx("c", 3)], "c1") : page(first, nextCursor);
  });
  const relay = new Relay("http://relay", { fetch: route.fetch, WebSocket: FakeSocket.class });
  const history = relay.history(address, { refresh: true });
  await history.more();
  await history.more();
  expect(ids(history.state)).toEqual(["p", "d@4", "c@3", "b@2", "a@1"]);
  await task();
  const socket = FakeSocket.last;
  socket?.deliver({ type: "subscribed", id: 0, key }, balance(5, 1));
  // The listeners' call starts the read.
  await task();
  while (history.state.loading) await task();
  relay.close();
  return { items: ids(history.state), resets: history.state.resets };
}

test("a refreshed first page joins the older pages at its oldest tx", async () => {
  expect(await refreshed([tx("q"), tx("p"), tx("e", 5), tx("d", 4)])).toEqual({
    items: ["q", "p", "e@5", "d@4", "c@3", "b@2", "a@1"],
    resets: 0,
  });
  // The pending tx confirmed: listed once, as confirmed.
  expect(await refreshed([tx("p", 5), tx("d", 4)])).toEqual({
    items: ["p@5", "d@4", "c@3", "b@2", "a@1"],
    resets: 0,
  });
  // It left the pool unconfirmed: gone.
  expect(await refreshed([tx("e", 5), tx("d", 4)])).toEqual({
    items: ["e@5", "d@4", "c@3", "b@2", "a@1"],
    resets: 0,
  });
});

test("a refreshed first page that does not join replaces the list, and counts a reset", async () => {
  // A page or more of txs arrived since.
  expect(await refreshed([tx("g", 7), tx("f", 6)])).toEqual({ items: ["g@7", "f@6"], resets: 1 });
  // A reorg put the joining tx at another height.
  expect(await refreshed([tx("e", 5), tx("c", 4)])).toEqual({ items: ["e@5", "c@4"], resets: 1 });
  // A reorg mined the page's txs in another order: joining at `c` would list `b` twice.
  expect(await refreshed([tx("b", 4), tx("c", 3)])).toEqual({ items: ["b@4", "c@3"], resets: 1 });
  // The whole list in one page: the older pages are dropped, with nothing left to scroll.
  expect(await refreshed([tx("e", 5), tx("d", 4), tx("a", 1)], null)).toEqual({
    items: ["e@5", "d@4", "a@1"],
    resets: 0,
  });
});

test("a refresh reads the first page once per burst, and once more for balances during a read", async () => {
  const answers: ReturnType<typeof deferred<Response>>[] = [];
  const route = served(() => {
    const answer = deferred<Response>();
    answers.push(answer);
    return answer.promise;
  });
  const relay = new Relay("http://relay", { fetch: route.fetch, WebSocket: FakeSocket.class });
  const history = relay.history(address, { refresh: true });
  await task();
  const socket = FakeSocket.last;
  if (!socket) throw new Error("no socket");
  expect(socket.sent).toEqual([{ op: "subscribe", key, id: 0 }]);
  // A key not held before: the first read waits for its first balance, and is the only one.
  expect(route.firsts()).toBe(0);
  socket.deliver({ type: "subscribed", id: 0, key }, balance(10, 1));
  await task();
  expect(route.firsts()).toBe(1);
  // Balances during the read: one more after it.
  socket.deliver(balance(10, 2));
  await task();
  socket.deliver(balance(11, 3));
  await task();
  expect(route.firsts()).toBe(1);
  answers[0]?.resolve(page([tx("a", 10)]));
  await task();
  expect(route.firsts()).toBe(2);
  answers[1]?.resolve(page([tx("b", 11), tx("a", 10)]));
  await task();
  expect(route.firsts()).toBe(2);
  expect(history.state).toMatchObject({ loading: false });
  expect(ids(history.state)).toEqual(["b@11", "a@10"]);

  // A burst: one read.
  socket.deliver(balance(12, 4), balance(12, 5));
  await task();
  expect(route.firsts()).toBe(3);
  answers[2]?.resolve(page([]));
  await task();
  // A message that changes no balance reads nothing.
  socket.deliver({ type: "tip", ...at(13) });
  await task();
  expect(route.firsts()).toBe(3);
  relay.close();
});

test("a key already live reads nothing more until its balance changes", async () => {
  const route = served(() => page([]));
  const relay = new Relay("http://relay", { fetch: route.fetch, WebSocket: FakeSocket.class });
  const sub = relay.subscribe(key, { boxes: false });
  await task();
  FakeSocket.last?.deliver({ type: "subscribed", id: 0, key }, balance(10, 1));
  expect(sub.state.phase).toBe("live");
  const history = relay.history(address, { refresh: true });
  await history.more();
  await task();
  expect(route.firsts()).toBe(1);
  // The history's hold is one more on the key's subscription: nothing new on the wire.
  expect(FakeSocket.last?.sent).toEqual([{ op: "subscribe", key, id: 0 }]);
  FakeSocket.last?.deliver(balance(11, 2));
  await task();
  expect(route.firsts()).toBe(2);
  // Closing it leaves the app's subscription alone.
  history.close();
  expect(FakeSocket.last?.sent).toHaveLength(1);
  expect(sub.state.phase).toBe("live");
  relay.close();
});

test("a first page that fails for now is read again after its wait; a balance reads at once", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  let answer: () => Response | Promise<Response> = () =>
    Response.json(
      { error: "not_ready", message: "later" },
      { status: 503, headers: { "retry-after": "2" } },
    );
  const route = served(() => answer());
  const relay = new Relay("http://relay", {
    fetch: route.fetch,
    WebSocket: FakeSocket.class,
    maxAttempts: 1,
    backoff: { firstMs: 1000, maxMs: 4000 },
  });
  const history = relay.history(address, { refresh: true });
  await vi.advanceTimersByTimeAsync(0);
  const socket = FakeSocket.last;
  socket?.deliver({ type: "subscribed", id: 0, key }, balance(10, 1));
  await vi.advanceTimersByTimeAsync(0);
  expect(history.state).toMatchObject({
    loading: false,
    error: { code: "not_ready", retryAfter: 2 },
  });
  await vi.advanceTimersByTimeAsync(1999);
  expect(route.firsts()).toBe(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(route.firsts()).toBe(2);
  // A network error: the backoff, half of its first bound with `random` at 0.5.
  answer = () => Promise.reject(new TypeError("fetch failed"));
  await vi.advanceTimersByTimeAsync(2000);
  expect(route.firsts()).toBe(3);
  expect(history.state.error).toMatchObject({ code: "network", retryAfter: 0.5 });
  // A balance during the wait reads at once, and the wait is over.
  socket?.deliver(balance(10, 2));
  answer = () => page([tx("a", 10)]);
  await vi.advanceTimersByTimeAsync(0);
  expect(route.firsts()).toBe(4);
  expect(history.state).toMatchObject({ error: undefined, loading: false });
  await vi.advanceTimersByTimeAsync(60_000);
  expect(route.firsts()).toBe(4);
  // A 4xx is final until the next balance.
  answer = () => Response.json({ error: "key_not_allowed", message: "no" }, { status: 403 });
  socket?.deliver(balance(11, 2));
  await vi.advanceTimersByTimeAsync(0);
  expect(route.firsts()).toBe(5);
  expect(history.state.error).toMatchObject({ code: "key_not_allowed", status: 403 });
  await vi.advanceTimersByTimeAsync(60_000);
  expect(route.firsts()).toBe(5);
  answer = () => page([tx("b", 12)]);
  socket?.deliver(balance(12, 3));
  await vi.advanceTimersByTimeAsync(0);
  expect(route.firsts()).toBe(6);
  expect(ids(history.state)).toEqual(["b@12"]);
  relay.close();
});

test("an older page that fails is read again only when the app asks", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  let fail = true;
  const route = served((cursor) => {
    if (cursor === null) return page([tx("b", 2)], "c1");
    return fail ? Promise.reject(new TypeError("fetch failed")) : page([tx("a", 1)]);
  });
  const relay = new Relay("http://relay", { fetch: route.fetch });
  const history = relay.history(address);
  await history.more();
  await history.more();
  expect(history.state).toMatchObject({ error: { code: "network" }, loading: false });
  expect(history.state.error?.retryAfter).toBeUndefined();
  await vi.advanceTimersByTimeAsync(60_000);
  expect(route.reads).toEqual([null, "c1"]);
  fail = false;
  await history.more();
  expect(route.reads).toEqual([null, "c1", "c1"]);
  expect(ids(history.state)).toEqual(["b@2", "a@1"]);
  expect(history.state.error).toBeUndefined();
  relay.close();
});

test("closing a history or its relay stops its reads and its retry, and releases its hold", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  for (const end of ["history", "relay"] as const) {
    const signals: AbortSignal[] = [];
    const fetch: typeof globalThis.fetch = (_, init) => {
      if (init?.signal) signals.push(init.signal);
      return Promise.resolve(Response.json({ error: "x", message: "x" }, { status: 503 }));
    };
    const relay = new Relay("http://relay", { fetch, WebSocket: FakeSocket.class, maxAttempts: 1 });
    const history = relay.history(address, { refresh: true });
    await vi.advanceTimersByTimeAsync(0);
    const socket = FakeSocket.last;
    socket?.deliver({ type: "subscribed", id: 0, key }, balance(10, 1));
    await vi.advanceTimersByTimeAsync(0);
    expect(history.state.error?.code).toBe("x");
    const reading = relay.history(address, { refresh: true });
    expect(signals.at(-1)?.aborted).toBe(false);
    if (end === "history") {
      history.close();
      reading.close();
      expect(socket?.sent.at(-1)).toEqual({ op: "unsubscribe", key });
    } else {
      relay.close();
    }
    expect(signals.at(-1)?.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  }
});

test("an older page read keeps a first page's failure, and a first page read cancels its retry", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  let failing = false;
  const route = served((cursor) => {
    if (cursor === null) {
      return failing ? Promise.reject(new TypeError("fetch failed")) : page([tx("b", 2)], "c1");
    }
    return cursor === "c1" ? page([tx("a", 1)], "c2") : expired();
  });
  const relay = new Relay("http://relay", {
    fetch: route.fetch,
    WebSocket: FakeSocket.class,
    backoff: { firstMs: 1000, maxMs: 4000 },
  });
  const history = relay.history(address, { refresh: true });
  await vi.advanceTimersByTimeAsync(0);
  const socket = FakeSocket.last;
  socket?.deliver({ type: "subscribed", id: 0, key }, balance(10, 1));
  await vi.advanceTimersByTimeAsync(0);
  expect(ids(history.state)).toEqual(["b@2"]);
  // A refresh fails for now: its retry waits 0.5 s.
  failing = true;
  socket?.deliver(balance(11, 2));
  await vi.advanceTimersByTimeAsync(0);
  expect(history.state.error).toMatchObject({ code: "network", retryAfter: 0.5 });
  // An older page read meanwhile: the top is still stale, so the error stays.
  await history.more();
  expect(ids(history.state)).toEqual(["b@2", "a@1"]);
  expect(history.state.error).toMatchObject({ code: "network" });
  // A first page read after an expired cursor: the retry has nothing left to do.
  failing = false;
  await history.more();
  const reads = route.firsts();
  expect(ids(history.state)).toEqual(["b@2"]);
  expect(history.state.error).toBeUndefined();
  await vi.advanceTimersByTimeAsync(10_000);
  expect(route.firsts()).toBe(reads);
  relay.close();
});

test("subscribe calls at once with the state, then on changes", async () => {
  const route = served(() => page([tx("a", 1)]));
  const relay = new Relay("http://relay", { fetch: route.fetch });
  const history = relay.history(address);
  const seen: HistoryState[] = [];
  const off = history.subscribe((state) => seen.push(state));
  expect(seen).toEqual([history.state]);
  expect(seen[0]?.loading).toBe(true);
  await history.more();
  await task();
  expect(seen.map(ids)).toEqual([[], ["a@1"]]);
  off();
  relay.close();
});
