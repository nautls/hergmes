import { isDeepStrictEqual } from "node:util";
import { ensureUTxOBigInt } from "@fleet-sdk/common";
import { afterAll, afterEach, beforeAll, expect, test } from "vitest";
import { Relay, type BlockRef, type Key, type Resync, type Subscription } from "../src/index.ts";
import { Faults, type Verdict } from "./faults.ts";
import { type SimOp, SimRelay, until } from "./sim.ts";

// A relay bug's faults, one frame at a time, and a real refusal for now. Its own process: the
// first test needs an address no one tracks yet.
let sim: SimRelay;
const relays: Relay[] = [];

beforeAll(async () => {
  sim = await SimRelay.start();
});

afterAll(() => {
  sim.stop();
});

afterEach(() => {
  for (const relay of relays.splice(0)) relay.close();
});

async function address(n: number): Promise<string> {
  const { addresses } = await sim.keys();
  const address = addresses[n];
  if (!address) throw new Error(`no address ${String(n)}`);
  return address;
}

let seed = 0;
const mine = (): SimOp => ({ op: "mine", fromPool: 1, fresh: 2, seed: String(++seed) });

/** A relay on the sim through `faults`, with short backoffs and its resyncs recorded. */
function relayWith(faults: Faults): { relay: Relay; resyncs: Resync[] } {
  const resyncs: Resync[] = [];
  const relay = new Relay(sim.url, {
    WebSocket: faults.WebSocket,
    backoff: { firstMs: 10, maxMs: 50 },
    debug: (event) => {
      if ("resync" in event) resyncs.push(event.resync);
    },
  });
  relays.push(relay);
  return { relay, resyncs };
}

/** A frame filter giving `verdict` to the first frame of `type` for `key`, once armed. */
function once(key: Key, type: string, verdict: Verdict) {
  const name = JSON.stringify(key);
  const filter = {
    armed: false,
    hit: false,
    frame: (text: string): Verdict => {
      if (!filter.armed || filter.hit) return "pass";
      const message = JSON.parse(text) as { type: string; key?: Key };
      if (message.type !== type || JSON.stringify(message.key) !== name) return "pass";
      filter.hit = true;
      return verdict;
    },
  };
  return filter;
}

/** Waits until the subscription is live at `tip` and holds the oracle's state there. */
async function converged(sub: Subscription, tip: BlockRef): Promise<void> {
  const expected = await sim.expected(sub.key);
  const want = {
    confirmed: expected.boxes.map((b) => ensureUTxOBigInt(b)),
    pending: expected.pending.map((tx) => tx.txId),
    balance: expected.balance && {
      confirmed: BigInt(expected.balance.confirmed.nanoErgs),
      pending: BigInt(expected.balance.pending.nanoErgs),
    },
  };
  const held = () => ({
    confirmed: sub.state.boxes?.confirmed,
    pending: sub.state.boxes?.pending.map((tx) => tx.txId),
    balance: sub.state.balance && {
      confirmed: sub.state.balance.confirmed.nanoErgs,
      pending: sub.state.balance.pending.nanoErgs,
    },
  });
  const done = () =>
    sub.state.phase === "live" &&
    (sub.state.boxes?.lastChange.height ?? 0) <= tip.height &&
    isDeepStrictEqual(held(), want);
  await until(done, `the oracle's state at ${String(tip.height)}`).catch((e: unknown) => {
    expect(held()).toEqual(want);
    throw e;
  });
}

test("a balance refused for now waits its retryAfter, then goes live", async () => {
  // The engine lags behind the node, so loading an untracked address gives up as `not_ready`.
  await sim.ops([{ op: "indexerLag", lag: 2 }, mine(), mine(), mine()]);
  expect((await sim.tick()).synced).toBe(false);
  const { relay } = relayWith(new Faults());
  const sub = relay.subscribe({ address: await address(0) }, { boxes: false });
  await until(() => sub.state.phase === "waiting", "the refusal");
  expect(sub.state.error).toMatchObject({ code: "not_ready", retryAfter: 1 });
  const tip = await sim.settle();
  await until(() => sub.state.phase === "live", "the retry");
  expect(sub.state.balance?.asOf).toEqual(tip);
});

test.each<[string, Verdict, SimOp]>([
  ["block", "drop", mine()],
  ["block", "duplicate", mine()],
  ["rollback", "drop", { op: "reorg", depth: 1, extra: 0, seed: "77" }],
  ["rollback", "duplicate", { op: "reorg", depth: 1, extra: 0, seed: "78" }],
])("a %s the relay sends, then %s, is found and the key recovers", async (type, verdict, op) => {
  const key = { address: await address(1) };
  const filter = once(key, type, verdict);
  const { relay, resyncs } = relayWith(new Faults({ frame: filter.frame }));
  const sub = relay.subscribe(key);
  let tip = await sim.settle();
  await converged(sub, tip);
  // A block above the key's load block that changes the key, so a reorg of one block has
  // something of the key's to undo. Mined until one does: which boxes a block spends follows
  // the simulator's box ids, which any change to its txs moves.
  for (let blocks = 1; ; blocks++) {
    await sim.ops([mine()]);
    tip = await sim.settle();
    await converged(sub, tip);
    if (sub.state.boxes?.lastChange.height === tip.height) break;
    if (blocks === 20) throw new Error("no block changed the key");
  }

  filter.armed = true;
  await sim.ops([op]);
  await sim.settle();
  // Another block, for a lost one to be noticed by.
  await sim.ops([mine()]);
  tip = await sim.settle();
  await converged(sub, tip);
  expect(filter.hit).toBe(true);
  expect(resyncs.filter((r) => r.by === "sdk").length).toBeGreaterThan(0);
});

test("a duplicated subscribed, mempool or balance changes nothing", async () => {
  const key = { address: await address(2) };
  const filters = (["subscribed", "mempool", "balance"] as const).map((type) => {
    const filter = once(key, type, "duplicate");
    filter.armed = true;
    return filter;
  });
  const frame = (text: string): Verdict =>
    filters.map((f) => f.frame(text)).find((v) => v !== "pass") ?? "pass";
  const { relay, resyncs } = relayWith(new Faults({ frame }));
  const sub = relay.subscribe(key);
  await sim.ops([{ op: "mempoolAdd", seed: "5" }, mine()]);
  const tip = await sim.settle();
  await converged(sub, tip);
  expect(filters.map((f) => f.hit)).toEqual([true, true, true]);
  expect(resyncs).toEqual([]);
});
