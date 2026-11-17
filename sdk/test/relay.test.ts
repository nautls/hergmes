import { ensureUTxOBigInt } from "@fleet-sdk/common";
import { afterAll, afterEach, beforeAll, expect, test } from "vitest";
import { Relay, type BlockRef, type Subscription } from "../src/index.ts";
import { SimRelay } from "./sim.ts";

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
});

/** Waits until `done` holds, or fails after 10 s: far past the longest wait measured. */
async function until(done: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!done()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

const liveAt = (sub: Subscription, tip: BlockRef) => () =>
  sub.state.phase === "live" && sub.state.view?.at.headerId === tip.headerId;

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
  await until(liveAt(second, tip), "the key live");
  expect(first.state).toBe(second.state);

  const changes: string[] = [];
  second.onChange((state) => changes.push(state.phase));
  first.close();
  first.close();
  tip = await mine(1);
  await until(liveAt(second, tip), "the remaining hold to follow the next block");
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
  await until(liveAt(boxes, tip), "the box subscription live");
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
  await until(
    () => sub.state.phase === "live" && sub.state.view?.at.headerId === tip.headerId,
    "the token live",
  );
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
  await until(liveAt(sub, tip), "the key live");
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
  await until(liveAt(old, tip), "the key live");
  relay.close();
  const current = relay.subscribe(key);
  old.close();
  tip = await mine(4);
  await until(liveAt(current, tip), "the new subscription to follow the next block");
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

test("a box subscription comes back after its socket dropped, once subscribed again", async () => {
  const { WebSocket, opened } = counting();
  relay = new Relay(sim.url, { WebSocket });
  const key = { address: await address(2) };
  const sub = relay.subscribe(key);
  let tip = await sim.settle();
  await until(liveAt(sub, tip), "the key live");
  opened.last?.close();
  await until(() => sub.state.phase === "disconnected", "the drop");
  const view = sub.state.view;
  const again = relay.subscribe(key);
  expect(again.state.view).toBe(view);
  tip = await mine(5);
  await until(liveAt(again, tip), "the key live again");
  expect(again.state.view?.confirmed).toEqual(
    (await sim.expected(key)).boxes.map((b) => ensureUTxOBigInt(b)),
  );
  expect(opened.count).toBe(2);
});
