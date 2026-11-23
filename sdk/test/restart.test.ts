import { isDeepStrictEqual } from "node:util";
import { ensureUTxOBigInt } from "@fleet-sdk/common";
import { afterAll, expect, test } from "vitest";
import { Relay, type Resync, type Subscription } from "../src/index.ts";
import { Faults } from "./faults.ts";
import { SimRelay, until } from "./sim.ts";

// A relay restart: its process killed, nothing listening for a while, then a new process, whose
// chain is a new one. The SDK reaches both through one URL, routed to whichever is up.
const ROOT = "http://relay.test";
const sims: SimRelay[] = [];

afterAll(() => {
  for (const sim of sims) sim.stop();
});

test("after a restart the SDK reconnects through a dead port and converges on the new chain", async () => {
  let up = await SimRelay.start();
  sims.push(up);
  let target = up.url;
  let sockets = 0;
  const faults = new Faults({
    route: (url) => {
      sockets++;
      return url.replace(ROOT.replace("http", "ws"), target.replace("http", "ws"));
    },
  });
  const resyncs: Resync[] = [];
  const relay = new Relay(ROOT, {
    WebSocket: faults.WebSocket,
    fetch: (input, init) => fetch((input as string | URL).toString().replace(ROOT, target), init),
    backoff: { firstMs: 10, maxMs: 50 },
    debug: (event) => {
      if ("resync" in event) resyncs.push(event.resync);
    },
  });
  const { addresses, tokens } = await up.keys();
  const [first, second, third] = addresses;
  const token = tokens.find((t) => BigInt(t.emission) <= 100n);
  if (!first || !second || !third || !token) throw new Error("too few keys");
  const subs: Subscription[] = [
    relay.subscribe({ address: first }),
    relay.subscribe({ tokenId: token.tokenId }),
    relay.subscribe({ address: second }, { boxes: false }),
  ];
  await up.ops([{ op: "mine", fromPool: 0, fresh: 3, seed: "1" }]);
  let tip = await up.settle();
  await until(
    () => subs.every((s) => s.state.phase === "live" && s.state.balance?.asOf.height !== 0),
    "every key live",
  );

  // The process dies; its port answers nothing while the SDK backs off and tries again.
  up.stop();
  await until(() => subs.every((s) => s.state.phase === "disconnected"), "the drop");
  const before = sockets;
  await until(() => sockets >= before + 3, "reconnects to the dead port");
  // A snapshot meanwhile fails to connect, and waits to try again.
  const late = relay.subscribe({ address: third });
  await until(() => late.state.phase === "waiting", "the snapshot's retry");
  expect(late.state.error?.code).toBe("network");

  up = await SimRelay.start();
  sims.push(up);
  target = up.url;
  tip = await up.settle();
  subs.push(late);
  for (const sub of subs) {
    const expected = await up.expected(sub.key);
    const want = {
      confirmed: sub === subs[2] ? undefined : expected.boxes.map((b) => ensureUTxOBigInt(b)),
      balance: expected.balance && BigInt(expected.balance.confirmed.nanoErgs),
    };
    const held = () => ({
      confirmed: sub.state.boxes?.confirmed,
      balance: sub.state.balance?.confirmed.nanoErgs,
    });
    await until(
      () =>
        sub.state.phase === "live" &&
        (sub === subs[2] || (sub.state.boxes?.lastChange.height ?? Infinity) <= tip.height) &&
        isDeepStrictEqual(held(), want),
      `${JSON.stringify(sub.key)} on the new chain`,
    ).catch((e: unknown) => {
      expect(held()).toEqual(want);
      throw e;
    });
  }
  // The new process tracks nothing: each box key resumed, was told so, and took a snapshot.
  expect(resyncs.filter((r) => r.by === "sdk")).toEqual([]);
  const resumed = resyncs.filter((r) => r.reason === "not_tracked").map((r) => r.key);
  expect(resumed).toHaveLength(2);
  expect(resumed).toEqual(expect.arrayContaining([subs[0]?.key, subs[1]?.key]));
  relay.close();
});
