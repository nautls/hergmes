import { describe, expect, test } from "vitest";
import { KeyMachine, type Action, type Resync } from "../src/machine.ts";
import type { ServerMessage } from "../src/protocol.ts";
import type { BlockRef, Key, PoolBox, WireConfirmedBox } from "../src/index.ts";

const key: Key = { address: "9fAddress" };
const hex = (n: number) => n.toString(16).padStart(64, "0");
const block = (height: number): BlockRef => ({ height, headerId: hex(1000 + height) });

function box(n: number, height?: number, value = 1000, tokens: [string, number][] = []): PoolBox {
  const b: PoolBox = {
    boxId: hex(n),
    transactionId: hex(5000 + n),
    index: 0,
    ergoTree: "0008cd",
    creationHeight: 1,
    value: String(value),
    assets: tokens.map(([tokenId, amount]) => ({ tokenId, amount: String(amount) })),
    additionalRegisters: {},
  };
  if (height !== undefined) b.inclusionHeight = height;
  return b;
}
const confirmed = (n: number, height: number, value?: number) =>
  box(n, height, value) as WireConfirmedBox;

function delta(
  type: "block" | "rollback",
  height: number,
  created: WireConfirmedBox[],
  spent: number[],
  parent = block(height - 1).headerId,
): ServerMessage {
  const spends = spent.map((n) => ({ boxId: hex(n), txId: hex(9000 + n) }));
  const at = block(height);
  return { type, key, ...at, parentId: parent, created, spent: spends };
}

const mempool = (tipHeight: number, pending: [string, PoolBox[], number[]][] = []) =>
  ({
    type: "mempool",
    key,
    tipHeight,
    pending: pending.map(([txId, created, spent]) => ({ txId, created, spent: spent.map(hex) })),
  }) as ServerMessage;

const balance = (at: BlockRef, nanoErgs: number, pending = 0): ServerMessage => ({
  type: "balance",
  key,
  asOf: at,
  confirmed: { nanoErgs: String(nanoErgs), tokens: [] },
  pending: { nanoErgs: String(pending), tokens: [] },
});

/** A machine with its ids and resyncs recorded. */
function machine(boxes = true) {
  let id = 0;
  const resyncs: Resync[] = [];
  const m = new KeyMachine(
    key,
    boxes,
    () => id++,
    (r) => resyncs.push(r),
  );
  return { m, resyncs };
}

function snapshotOf(actions: Action[]): number {
  const [action] = actions;
  if (action?.kind !== "snapshot") throw new Error(`no snapshot in ${JSON.stringify(actions)}`);
  return action.generation;
}

function subscribeOf(actions: Action[]) {
  const [action] = actions;
  if (action?.kind !== "send" || action.message.op !== "subscribe") {
    throw new Error(`no subscribe in ${JSON.stringify(actions)}`);
  }
  const { id } = action.message;
  if (id === undefined) throw new Error("a subscribe without an id");
  return { ...action.message, id };
}

/** A box subscription live at block 10 with boxes 1 (height 9) and 2 (height 10). */
function live() {
  const t = machine();
  const generation = snapshotOf(t.m.start());
  const sub = subscribeOf(
    t.m.snapshotLoaded(generation, block(10), [confirmed(1, 9), confirmed(2, 10)]),
  );
  t.m.receive({ type: "subscribed", id: sub.id, key });
  t.m.receive(mempool(10));
  return t;
}

describe("a box subscription", () => {
  test("snapshots, subscribes from it, catches up and goes live at the mempool", () => {
    const { m, resyncs } = machine();
    expect(m.state.phase).toBe("snapshot");
    const generation = snapshotOf(m.start());
    const sub = subscribeOf(m.snapshotLoaded(generation, block(10), [confirmed(1, 9)]));
    expect(sub).toEqual({ op: "subscribe", key, from: block(10), id: 0 });
    expect(m.state.phase).toBe("catching_up");

    m.receive({ type: "subscribed", id: 0, key });
    m.receive(delta("block", 11, [confirmed(2, 11)], [1]));
    expect(m.state.phase).toBe("catching_up");
    expect(m.state.view).toBeUndefined();
    m.receive(mempool(11, [["tx", [box(3)], [2]]]));
    m.receive(balance(block(11), 1000, 0));

    expect(m.state.phase).toBe("live");
    const view = m.state.view;
    expect(view?.at).toEqual(block(11));
    expect(view?.confirmed.map((b) => b.boxId)).toEqual([hex(2)]);
    expect(view?.confirmed[0]?.value).toBe(1000n);
    expect(view?.spendable.map((b) => b.boxId)).toEqual([hex(3)]);
    expect(m.state.balance?.confirmed.nanoErgs).toBe(1000n);
    expect(resyncs).toEqual([]);
  });

  test("drops the key's messages until the answer with its subscribe's id", () => {
    const { m } = machine();
    const generation = snapshotOf(m.start());
    m.snapshotLoaded(generation, block(10), [confirmed(1, 9)]);
    // An older subscription's messages, and an older subscribe's answers.
    m.receive(delta("block", 11, [], [7]));
    m.receive({ type: "subscribed", key });
    m.receive({ type: "rejected", id: 99, key, reason: "not_ready", retryAfter: 1 });
    m.receive({ type: "resync_required", id: 98, key, reason: "not_tracked" });
    m.receive(mempool(3));
    expect(m.state.phase).toBe("catching_up");
    m.receive({ type: "subscribed", id: 0, key });
    m.receive(mempool(10));
    expect(m.state.phase).toBe("live");
  });

  test("follows blocks and rollbacks", () => {
    const { m, resyncs } = live();
    m.receive(delta("block", 11, [confirmed(3, 11)], [1]));
    m.receive(delta("rollback", 11, [confirmed(1, 9)], [3]));
    expect(m.state.view?.at).toEqual(block(10));
    expect(m.state.view?.confirmed.map((b) => b.boxId)).toEqual([hex(1), hex(2)]);
    m.receive(delta("block", 11, [], [2], block(10).headerId));
    expect(m.state.view?.confirmed.map((b) => b.boxId)).toEqual([hex(1)]);
    expect(resyncs).toEqual([]);
  });

  test.each<[string, ServerMessage, Resync["by"]?]>([
    ["block_does_not_chain", delta("block", 11, [], [], hex(4242))],
    ["block_does_not_chain", delta("block", 12, [], [], block(10).headerId)],
    ["spends_unknown_box", delta("block", 11, [], [7])],
    ["creates_held_box", delta("block", 11, [confirmed(1, 11)], [])],
    ["creates_held_box", delta("block", 11, [confirmed(3, 10)], [])],
    ["rollback_of_another_block", delta("rollback", 9, [], [])],
    ["undoes_unknown_box", delta("rollback", 10, [], [7])],
    ["restores_held_box", delta("rollback", 10, [confirmed(2, 9)], [])],
    ["restores_held_box", delta("rollback", 10, [confirmed(3, 10)], [])],
    ["mempool_at_another_height", mempool(9)],
    ["balance_at_another_block", balance(block(9), 1)],
    ["fork_beyond_window", { type: "resync_required", key, reason: "fork_beyond_window" }, "relay"],
  ])("takes a new snapshot on %s", (reason, message, by = "sdk") => {
    const { m, resyncs } = live();
    const view = m.state.view;
    const generation = snapshotOf(m.receive(message));
    expect(resyncs).toEqual([{ key, by, reason }]);
    // The last live view stays, marked by the phase as no longer current.
    expect(m.state).toMatchObject({ phase: "snapshot", view });
    // Messages of the old subscription are ignored until the new one starts.
    m.receive(delta("block", 11, [], []));
    expect(m.state.view).toBe(view);
    const sub = subscribeOf(m.snapshotLoaded(generation, block(12), [confirmed(5, 12)]));
    m.receive({ type: "subscribed", id: sub.id, key });
    m.receive(mempool(12));
    expect(m.state.phase).toBe("live");
    expect(m.state.view?.confirmed.map((b) => b.boxId)).toEqual([hex(5)]);
  });

  test("takes a new snapshot when the subscribe's answer is resync_required", () => {
    const { m, resyncs } = machine();
    const generation = snapshotOf(m.start());
    m.snapshotLoaded(generation, block(10), []);
    snapshotOf(m.receive({ type: "resync_required", id: 0, key, reason: "outside_window" }));
    expect(resyncs).toEqual([{ key, by: "relay", reason: "outside_window" }]);
  });

  test("starts the snapshot again when its cursor expired, ignoring the old one's result", () => {
    const { m, resyncs } = machine();
    const first = snapshotOf(m.start());
    const second = snapshotOf(
      m.snapshotFailed(first, { code: "cursor_expired", message: "expired" }),
    );
    expect(second).not.toBe(first);
    expect(m.snapshotLoaded(first, block(10), [])).toEqual([]);
    expect(subscribeOf(m.snapshotLoaded(second, block(11), [])).from).toEqual(block(11));
    expect(resyncs).toEqual([{ key, by: "relay", reason: "cursor_expired" }]);
  });

  test("is rejected when the snapshot fails otherwise, or the relay refuses it", () => {
    const failed = machine().m;
    const generation = snapshotOf(failed.start());
    failed.snapshotFailed(generation, { code: "unknown_token", message: "unknown" });
    expect(failed.state).toMatchObject({ phase: "rejected", error: { code: "unknown_token" } });

    const refused = machine().m;
    refused.snapshotLoaded(snapshotOf(refused.start()), block(10), []);
    refused.receive({ type: "rejected", id: 0, key, reason: "capacity_exceeded", retryAfter: 120 });
    expect(refused.state).toMatchObject({
      phase: "rejected",
      error: { code: "capacity_exceeded", retryAfter: 120 },
    });
    expect(refused.receive(mempool(10))).toEqual([]);
    expect(refused.state.phase).toBe("rejected");
  });

  test("ignores a message type it does not know", () => {
    const { m } = live();
    const state = m.state;
    expect(m.receive({ type: "future", key } as unknown as ServerMessage)).toEqual([]);
    expect(m.state).toBe(state);
  });

  test("unsubscribes when stopped, and ignores what still arrives", () => {
    const { m } = live();
    expect(m.stop()).toEqual([{ kind: "send", message: { op: "unsubscribe", key } }]);
    expect(m.state.phase).toBe("disconnected");
    m.receive(delta("block", 11, [], []));
    expect(m.state.view?.at).toEqual(block(10));
  });
});

describe("a block that leaves the key untouched", () => {
  test("reuses the view's boxes and spendable set, moving only its block", () => {
    const { m } = live();
    m.receive(mempool(10, [["a", [box(3)], [1]]]));
    const before = m.state.view;
    m.receive(delta("block", 11, [], []));
    m.receive(mempool(11, [["a", [box(3)], [1]]]));
    const after = m.state.view;
    expect(after?.at).toEqual(block(11));
    expect(after?.confirmed).toBe(before?.confirmed);
    m.receive(delta("block", 12, [], []));
    expect(m.state.view?.spendable).toBe(after?.spendable);
  });

  test("a block that moves boxes keeps the keyset order without sorting them all", () => {
    const { m } = live();
    m.receive(delta("block", 11, [confirmed(9, 11), confirmed(4, 11)], [1]));
    expect(m.state.view?.confirmed.map((b) => b.boxId)).toEqual([hex(2), hex(4), hex(9)]);
    m.receive(delta("rollback", 11, [confirmed(1, 9)], [9, 4]));
    expect(m.state.view?.confirmed.map((b) => b.boxId)).toEqual([hex(1), hex(2)]);
  });
});

describe("spendable now", () => {
  test("is the confirmed boxes and pending outputs no pending tx spends", () => {
    const { m } = live();
    const t = "f".repeat(64);
    m.receive(
      mempool(10, [
        ["a", [box(3, undefined, 400, [[t, 5]]), box(4, undefined, 300)], [1]],
        // Chained on `a`: spends one of its outputs.
        ["b", [box(6, undefined, 50)], [4]],
      ]),
    );
    const view = m.state.view;
    expect(view?.spendable.map((b) => b.boxId)).toEqual([hex(2), hex(3), hex(6)]);
    expect(view?.spendableTotals).toEqual({ nanoErgs: 1450n, tokens: new Map([[t, 5n]]) });
  });

  test("lists a box once while a block confirms a pending output before the next mempool", () => {
    const { m } = live();
    m.receive(mempool(10, [["a", [box(3)], [1]]]));
    m.receive(delta("block", 11, [confirmed(3, 11)], [1]));
    expect(m.state.view?.spendable.map((b) => b.boxId)).toEqual([hex(2), hex(3)]);
  });
});

describe("a balance-only subscription", () => {
  test("subscribes without a snapshot and goes live at its balance", () => {
    const { m } = machine(false);
    expect(subscribeOf(m.start())).toEqual({ op: "subscribe", key, id: 0 });
    m.receive({ type: "subscribed", id: 0, key });
    expect(m.state.phase).toBe("catching_up");
    m.receive(balance(block(10), 5, -2));
    expect(m.state).toMatchObject({ phase: "live", view: undefined });
    expect(m.state.balance?.pending.nanoErgs).toBe(-2n);
    m.receive(balance(block(12), 7));
    expect(m.state.balance?.asOf).toEqual(block(12));
  });

  test("subscribes again on resync_required", () => {
    const { m, resyncs } = machine(false);
    m.start();
    m.receive({ type: "subscribed", id: 0, key });
    m.receive(balance(block(10), 5));
    const sub = subscribeOf(m.receive({ type: "resync_required", key, reason: "slow_consumer" }));
    expect(sub).toEqual({ op: "subscribe", key, id: 1 });
    expect(resyncs).toEqual([{ key, by: "relay", reason: "slow_consumer" }]);
    m.receive(balance(block(11), 9));
    expect(m.state.balance?.asOf).toEqual(block(10));
  });
});
