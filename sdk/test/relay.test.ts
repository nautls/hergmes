import { ensureUTxOBigInt } from "@fleet-sdk/common";
import { isDeepStrictEqual } from "node:util";
import { afterAll, afterEach, beforeAll, expect, test, vi } from "vitest";
import { Relay, type BlockRef, type Subscription } from "../src/index.ts";
import { SimRelay, until } from "./sim.ts";

let sim: SimRelay;
let relay: Relay;

beforeAll(async () => {
  sim = await SimRelay.start();
});

afterAll(() => {
  sim.stop();
});

afterEach(() => {
  relay.close();
  vi.restoreAllMocks();
});

/**
 * Waits until `sub` is live with the oracle's boxes at `tip`, the relay's tip. Only blocks that
 * change a key reach it, so a view's last change says nothing of the tip: its boxes do.
 */
async function liveAt(sub: Subscription, tip: BlockRef, what: string): Promise<void> {
  const want = (await sim.expected(sub.key)).boxes.map((b) => ensureUTxOBigInt(b));
  await until(
    () =>
      sub.state.phase === "live" &&
      relay.tip?.headerId === tip.headerId &&
      isDeepStrictEqual(sub.state.view?.confirmed, want),
    what,
  );
}

/** A block that moves boxes, then a settle: the new tip. */
async function mine(seed: number): Promise<BlockRef> {
  await sim.ops([{ op: "mine", fromPool: 0, fresh: 3, seed: String(seed) }]);
  return sim.settle();
}

async function address(n: number): Promise<string> {
  const { addresses } = await sim.keys();
  const address = addresses[n];
  if (!address) throw new Error(`no address ${String(n)}`);
  return address;
}

test("holds on one key share its subscription, which ends with the last", async () => {
  relay = new Relay(sim.url);
  const key = { address: await address(0) };
  const first = relay.subscribe(key);
  const second = relay.subscribe(key);
  let tip = await sim.settle();
  await liveAt(second, tip, "the key live");
  expect(first.state).toBe(second.state);

  const changes: string[] = [];
  second.onChange((state) => changes.push(state.phase));
  first.close();
  first.close();
  tip = await mine(1);
  await liveAt(second, tip, "the remaining hold to follow the next block");
  // Listeners hear of it on the next timer, after `state` changed.
  await until(() => changes.length > 0, "the remaining hold's listener called");
  expect(second.state.view?.confirmed).toEqual(
    (await sim.expected(key)).boxes.map((b) => ensureUTxOBigInt(b)),
  );

  second.close();
  expect(second.state.phase).toBe("disconnected");
});

test("a box subscription replaces a balance-only one on the same key", async () => {
  relay = new Relay(sim.url);
  const key = { address: await address(1) };
  const balanceOnly = relay.subscribe(key, { boxes: false });
  await sim.settle();
  await until(() => balanceOnly.state.phase === "live", "the balance");
  expect(balanceOnly.state.view).toBeUndefined();

  const phases: string[] = [];
  balanceOnly.onChange((state) => phases.push(state.phase));
  const boxes = relay.subscribe(key);
  const tip = await mine(2);
  await liveAt(boxes, tip, "the box subscription live");
  // The balance-only hold's listeners hear of the replacement too.
  await until(() => phases.at(-1) === "live", "the balance-only hold notified");
  expect(balanceOnly.state).toBe(boxes.state);
  expect(boxes.state.view?.confirmed).toEqual(
    (await sim.expected(key)).boxes.map((b) => ensureUTxOBigInt(b)),
  );
  // A balance-only hold added later keeps the boxes.
  relay.subscribe(key, { boxes: false });
  expect(boxes.state.view).toBeDefined();
});

test("a token key always follows boxes", async () => {
  relay = new Relay(sim.url);
  const { tokens } = await sim.keys();
  const token = tokens.find((t) => BigInt(t.emission) <= 100n);
  if (!token) throw new Error("no token under the supply limit");
  const sub = relay.subscribe({ tokenId: token.tokenId }, { boxes: false });
  const tip = await sim.settle();
  await liveAt(sub, tip, "the token live");
  expect(sub.state.balance).toBeUndefined();
});

test("the tip follows the relay's", async () => {
  relay = new Relay(sim.url);
  const tips: BlockRef[] = [];
  relay.onTip((tip) => tips.push(tip));
  relay.subscribe({ address: await address(2) }, { boxes: false });
  const tip = await mine(3);
  await until(() => relay.tip?.headerId === tip.headerId, "the tip");
  expect(tips.at(-1)).toEqual(tip);
});

test("closing the relay ends every subscription", async () => {
  relay = new Relay(sim.url);
  const sub = relay.subscribe({ address: await address(3) });
  const tip = await sim.settle();
  await liveAt(sub, tip, "the key live");
  relay.close();
  expect(sub.state.phase).toBe("disconnected");
});

/** The global `WebSocket`, counting the sockets opened. */
function counting() {
  const opened: { count: number; last?: WebSocket } = { count: 0 };
  class Counting extends WebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols);
      opened.count++;
      opened.last = this;
    }
  }
  return { WebSocket: Counting, opened };
}

test("a hold closed after its relay opens no socket to unsubscribe", async () => {
  const { WebSocket, opened } = counting();
  relay = new Relay(sim.url, { WebSocket });
  const sub = relay.subscribe({ address: await address(4) }, { boxes: false });
  await sim.settle();
  await until(() => sub.state.phase === "live", "the balance");
  relay.close();
  sub.close();
  await new Promise((r) => setTimeout(r, 50));
  expect(opened.count).toBe(1);
});

test("a hold of an ended subscription does not end the key's next one", async () => {
  relay = new Relay(sim.url);
  const key = { address: await address(0) };
  const old = relay.subscribe(key);
  let tip = await sim.settle();
  await liveAt(old, tip, "the key live");
  relay.close();
  const current = relay.subscribe(key);
  old.close();
  tip = await mine(4);
  await liveAt(current, tip, "the new subscription to follow the next block");
});

test("a hold closed after its socket dropped opens no socket to unsubscribe", async () => {
  const { WebSocket, opened } = counting();
  relay = new Relay(sim.url, { WebSocket });
  const sub = relay.subscribe({ address: await address(4) }, { boxes: false });
  await sim.settle();
  await until(() => sub.state.phase === "live", "the balance");
  opened.last?.close();
  await until(() => sub.state.phase === "disconnected", "the drop");
  sub.close();
  await new Promise((r) => setTimeout(r, 50));
  expect(opened.count).toBe(1);
});

test("a box subscription resumes by itself after its socket dropped, with no snapshot", async () => {
  const { WebSocket, opened } = counting();
  let reads = 0;
  const fetch: typeof globalThis.fetch = (input, init) => {
    if ((input as string | URL).toString().includes("/boxes")) reads++;
    return globalThis.fetch(input, init);
  };
  // The first wait drawn as its longest, 200 ms, for the test to act while the key is cut off.
  vi.spyOn(Math, "random").mockReturnValue(1);
  relay = new Relay(sim.url, { WebSocket, fetch, backoff: { firstMs: 200, maxMs: 400 } });
  const key = { address: await address(2) };
  const sub = relay.subscribe(key);
  let tip = await sim.settle();
  await liveAt(sub, tip, "the key live");
  const snapshots = reads;
  opened.last?.close();
  await until(() => sub.state.phase === "disconnected", "the drop");
  // A subscribe to the key while it is cut off waits for the reconnect too.
  relay.subscribe(key);
  expect(opened.count).toBe(1);
  await until(() => opened.count === 2, "the reconnect");
  tip = await mine(5);
  await liveAt(sub, tip, "the key live again");
  expect(sub.state.view?.confirmed).toEqual(
    (await sim.expected(key)).boxes.map((b) => ensureUTxOBigInt(b)),
  );
  expect(reads).toBe(snapshots);
  expect(opened.count).toBe(2);
});
