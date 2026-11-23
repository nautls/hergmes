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

/** A block or rollback at `height`, chained to the key's change at `prev`. */
function delta(
  type: "block" | "rollback",
  height: number,
  created: WireConfirmedBox[],
  spent: number[],
  prev = block(height - 1),
): ServerMessage {
  const spends = spent.map((n) => ({ boxId: hex(n), txId: hex(9000 + n) }));
  const at = block(height);
  return { type, key, ...at, prev, created, spent: spends };
}

/** An overlay naming the key's last change at `lastChange`. */
const mempool = (lastChange: number, pending: [string, PoolBox[], number[]][] = []) =>
  ({
    type: "mempool",
    key,
    lastChange: block(lastChange),
    pending: pending.map(([txId, created, spent]) => ({ txId, created, spent: spent.map(hex) })),
  }) as ServerMessage;

const balance = (at: BlockRef, nanoErgs: number, pending = 0, lastChange = at): ServerMessage => ({
  type: "balance",
  key,
  asOf: at,
  lastChange,
  confirmed: { nanoErgs: String(nanoErgs), tokens: [] },
  pending: { nanoErgs: String(pending), tokens: [] },
});

/** A machine with its ids and resyncs recorded; the n-th backoff is n + 1 seconds. */
function machine(boxes = true, online = true) {
  let id = 0;
  const resyncs: Resync[] = [];
  const m = new KeyMachine(
    key,
    boxes,
    {
      nextId: () => id++,
      onResync: (r) => resyncs.push(r),
      backoff: (attempt) => (attempt + 1) * 1000,
    },
    online,
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
    expect(m.state.boxes).toBeUndefined();
    m.receive(mempool(11, [["tx", [box(3)], [2]]]));
    m.receive(balance(block(11), 1000, 0));

    expect(m.state.phase).toBe("live");
    const view = m.state.boxes;
    expect(view?.lastChange).toEqual(block(11));
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
    expect(m.state.boxes?.lastChange).toEqual(block(10));
    expect(m.state.boxes?.confirmed.map((b) => b.boxId)).toEqual([hex(1), hex(2)]);
    m.receive(delta("block", 11, [], [2]));
    expect(m.state.boxes?.confirmed.map((b) => b.boxId)).toEqual([hex(1)]);
    expect(resyncs).toEqual([]);
  });

  test("chains a change across the blocks that left the key alone, which are not sent", () => {
    const { m, resyncs } = live();
    m.receive(delta("block", 14, [confirmed(3, 14)], [1], block(10)));
    m.receive(mempool(14));
    m.receive(balance(block(15), 1000, 0, block(14)));
    expect(m.state.boxes?.lastChange).toEqual(block(14));
    m.receive(delta("rollback", 14, [confirmed(1, 9)], [3], block(10)));
    expect(m.state.boxes?.lastChange).toEqual(block(10));
    expect(resyncs).toEqual([]);
  });

  test("refuses a first change at the snapshot's height on another fork", () => {
    const { m, resyncs } = machine();
    const sub = subscribeOf(m.snapshotLoaded(snapshotOf(m.start()), block(10), [confirmed(1, 9)]));
    m.receive({ type: "subscribed", id: sub.id, key });
    const fork = { height: 10, headerId: hex(4242) };
    m.receive({ type: "mempool", key, lastChange: fork, pending: [] });
    expect(resyncs).toEqual([{ key, by: "sdk", reason: "mempool_at_another_change" }]);
  });

  test.each<[string, ServerMessage, Resync["by"]?]>([
    ["block_does_not_chain", delta("block", 11, [], [7], { height: 10, headerId: hex(4242) })],
    ["block_does_not_chain", delta("block", 12, [], [7], block(11))],
    ["block_does_not_chain", delta("block", 10, [], [7], block(10))],
    ["spends_unknown_box", delta("block", 11, [], [7])],
    ["creates_held_box", delta("block", 11, [confirmed(1, 11)], [])],
    ["creates_held_box", delta("block", 11, [confirmed(3, 10)], [])],
    ["rollback_of_another_block", delta("rollback", 9, [], [])],
    ["undoes_unknown_box", delta("rollback", 10, [], [7])],
    ["restores_held_box", delta("rollback", 10, [confirmed(2, 9)], [])],
    ["restores_held_box", delta("rollback", 10, [confirmed(3, 10)], [])],
    ["mempool_at_another_change", mempool(9)],
    // A change the view lacks: its block was lost.
    ["mempool_at_another_change", mempool(11)],
    ["balance_at_another_change", balance(block(11), 1, 0, block(11))],
    ["fork_beyond_window", { type: "resync_required", key, reason: "fork_beyond_window" }, "relay"],
  ])("takes a new snapshot on %s", (reason, message, by = "sdk") => {
    const { m, resyncs } = live();
    const view = m.state.boxes;
    const generation = snapshotOf(m.receive(message));
    expect(resyncs).toEqual([{ key, by, reason }]);
    // The last live view stays, marked by the phase as no longer current.
    expect(m.state).toMatchObject({ phase: "snapshot", boxes: view });
    // Messages of the old subscription are ignored until the new one starts.
    m.receive(delta("block", 11, [], []));
    expect(m.state.boxes).toBe(view);
    const sub = subscribeOf(m.snapshotLoaded(generation, block(12), [confirmed(5, 12)]));
    m.receive({ type: "subscribed", id: sub.id, key });
    m.receive(mempool(12));
    expect(m.state.phase).toBe("live");
    expect(m.state.boxes?.confirmed.map((b) => b.boxId)).toEqual([hex(5)]);
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

  test("is rejected for good when the snapshot fails with a 4xx, or the relay refuses it", () => {
    const failed = machine().m;
    const generation = snapshotOf(failed.start());
    failed.snapshotFailed(generation, { code: "unknown_token", message: "unknown", status: 404 });
    expect(failed.state).toMatchObject({ phase: "rejected", error: { code: "unknown_token" } });

    const refused = machine().m;
    refused.snapshotLoaded(snapshotOf(refused.start()), block(10), []);
    expect(
      refused.receive({ type: "rejected", id: 0, key, reason: "too_many_subscriptions" }),
    ).toEqual([]);
    expect(refused.state).toMatchObject({
      phase: "rejected",
      error: { code: "too_many_subscriptions" },
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
    m.receive(delta("block", 11, [], [1]));
    expect(m.state.boxes?.lastChange).toEqual(block(10));
  });
});

describe("the confirmed boxes", () => {
  test("are reused by an overlay that leaves them alone", () => {
    const { m } = live();
    const before = m.state.boxes;
    m.receive(mempool(10, [["a", [box(3)], [1]]]));
    const after = m.state.boxes;
    expect(after?.confirmed).toBe(before?.confirmed);
    expect(after?.spendable.map((b) => b.boxId)).toEqual([hex(2), hex(3)]);
  });

  test("a block that moves boxes keeps the keyset order without sorting them all", () => {
    const { m } = live();
    m.receive(delta("block", 11, [confirmed(9, 11), confirmed(4, 11)], [1]));
    expect(m.state.boxes?.confirmed.map((b) => b.boxId)).toEqual([hex(2), hex(4), hex(9)]);
    m.receive(delta("rollback", 11, [confirmed(1, 9)], [9, 4]));
    expect(m.state.boxes?.confirmed.map((b) => b.boxId)).toEqual([hex(1), hex(2)]);
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
    const view = m.state.boxes;
    expect(view?.spendable.map((b) => b.boxId)).toEqual([hex(2), hex(3), hex(6)]);
  });

  test("lists a box once while a block confirms a pending output before the next mempool", () => {
    const { m } = live();
    m.receive(mempool(10, [["a", [box(3)], [1]]]));
    m.receive(delta("block", 11, [confirmed(3, 11)], [1]));
    expect(m.state.boxes?.spendable.map((b) => b.boxId)).toEqual([hex(2), hex(3)]);
  });
});

describe("a balance-only subscription", () => {
  test("subscribes without a snapshot and goes live at its balance", () => {
    const { m } = machine(false);
    expect(subscribeOf(m.start())).toEqual({ op: "subscribe", key, id: 0 });
    m.receive({ type: "subscribed", id: 0, key });
    expect(m.state.phase).toBe("catching_up");
    m.receive(balance(block(10), 5, -2));
    expect(m.state).toMatchObject({ phase: "live", boxes: undefined });
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

/** The retry a failure answered: its generation and wait. */
function retryOf(actions: Action[]) {
  const [action] = actions;
  if (action?.kind !== "retry") throw new Error(`no retry in ${JSON.stringify(actions)}`);
  return action;
}

describe("across a dropped socket", () => {
  test("a live key resumes from its last change, keeping its view meanwhile", () => {
    const { m, resyncs } = live();
    const view = m.state.boxes;
    expect(m.disconnected()).toEqual([]);
    expect(m.state).toMatchObject({ phase: "disconnected", boxes: view });
    // What the old socket still delivers is not this subscription's.
    m.receive(delta("block", 11, [], [7]));
    expect(subscribeOf(m.connected())).toEqual({ op: "subscribe", key, from: block(10), id: 1 });
    expect(m.state.phase).toBe("catching_up");
    m.receive({ type: "subscribed", id: 1, key });
    m.receive(delta("block", 11, [confirmed(3, 11)], [1]));
    expect(m.state.boxes).toBe(view);
    m.receive(mempool(11));
    expect(m.state.phase).toBe("live");
    expect(m.state.boxes?.confirmed.map((b) => b.boxId)).toEqual([hex(2), hex(3)]);
    expect(resyncs).toEqual([]);
  });

  test("a resume's replay may chain from a change below its from, as after a reload", () => {
    const { m, resyncs } = live();
    m.receive(delta("block", 12, [confirmed(3, 12)], [1], block(10)));
    m.disconnected();
    const sub = subscribeOf(m.connected());
    expect(sub.from).toEqual(block(12));
    m.receive({ type: "subscribed", id: sub.id, key });
    m.receive(delta("block", 14, [confirmed(4, 14)], [3], block(11)));
    m.receive(mempool(14));
    expect(m.state.phase).toBe("live");
    expect(m.state.boxes?.lastChange).toEqual(block(14));
    expect(resyncs).toEqual([]);
  });

  test("a key cut off during its catch-up resumes from the block it reached", () => {
    const { m } = machine();
    m.snapshotLoaded(snapshotOf(m.start()), block(10), [confirmed(1, 9)]);
    m.receive({ type: "subscribed", id: 0, key });
    m.receive(delta("block", 11, [confirmed(2, 11)], [1]));
    m.disconnected();
    expect(m.state.phase).toBe("disconnected");
    expect(subscribeOf(m.connected()).from).toEqual(block(11));
  });

  test("a live key cut off again during its resume's replay resumes from the block it reached", () => {
    const { m } = live();
    m.disconnected();
    const first = subscribeOf(m.connected());
    m.receive({ type: "subscribed", id: first.id, key });
    m.receive(delta("block", 11, [confirmed(3, 11)], [1]));
    // Not published yet: the replay is not over.
    expect(m.state.boxes?.lastChange).toEqual(block(10));
    m.disconnected();
    expect(subscribeOf(m.connected()).from).toEqual(block(11));
  });

  test("a snapshot read carries on, and its subscribe waits for the socket", () => {
    const { m } = machine();
    const generation = snapshotOf(m.start());
    m.disconnected();
    expect(m.state.phase).toBe("snapshot");
    expect(m.snapshotLoaded(generation, block(10), [])).toEqual([]);
    expect(m.state.phase).toBe("disconnected");
    expect(subscribeOf(m.connected())).toEqual({ op: "subscribe", key, from: block(10), id: 0 });
  });

  test("a balance-only key subscribes again, and one started offline waits", () => {
    const { m } = machine(false);
    m.start();
    m.receive({ type: "subscribed", id: 0, key });
    m.receive(balance(block(10), 5));
    m.disconnected();
    expect(m.state).toMatchObject({ phase: "disconnected", balance: { asOf: block(10) } });
    expect(subscribeOf(m.connected())).toEqual({ op: "subscribe", key, id: 1 });

    const offline = machine(false, false).m;
    expect(offline.start()).toEqual([]);
    expect(offline.state.phase).toBe("disconnected");
    expect(subscribeOf(offline.connected())).toEqual({ op: "subscribe", key, id: 0 });
  });

  test("a socket opening changes nothing for a key it did not cut off", () => {
    const { m } = live();
    expect(m.connected()).toEqual([]);
    const reading = machine().m;
    reading.start();
    expect(reading.connected()).toEqual([]);
    expect(reading.state.phase).toBe("snapshot");
  });

  test("ending it ignores a snapshot read and a retry still in flight", () => {
    const { m } = machine();
    const generation = snapshotOf(m.start());
    m.end();
    expect(m.snapshotLoaded(generation, block(10), [])).toEqual([]);
    expect(m.connected()).toEqual([]);

    const waiting = machine().m;
    const retry = retryOf(
      waiting.snapshotFailed(snapshotOf(waiting.start()), { code: "network", message: "down" }),
    );
    waiting.end();
    expect(waiting.retry(retry.generation)).toEqual([]);
    expect(waiting.state.phase).toBe("disconnected");
  });
});

describe("a failure for now", () => {
  test("waits the relay's retryAfter, then starts again", () => {
    const { m } = machine(false);
    m.start();
    const retry = retryOf(
      m.receive({ type: "rejected", id: 0, key, reason: "capacity_exceeded", retryAfter: 120 }),
    );
    expect(retry.ms).toBe(120_000);
    expect(m.state).toMatchObject({
      phase: "waiting",
      error: { code: "capacity_exceeded", retryAfter: 120 },
    });
    expect(m.retry(retry.generation - 1)).toEqual([]);
    expect(subscribeOf(m.retry(retry.generation))).toEqual({ op: "subscribe", key, id: 1 });
    // Fired twice: the second finds nothing waiting.
    expect(m.retry(retry.generation)).toEqual([]);
  });

  test("backs off on a network error or a 5xx, from the first wait once the key was live", () => {
    const { m } = machine();
    /** Fails the snapshot the last action asked for; answers the retry. */
    const fail = (actions: Action[], code: string, status?: number) =>
      retryOf(
        m.snapshotFailed(snapshotOf(actions), {
          code,
          message: code,
          ...(status === undefined ? {} : { status }),
        }),
      );
    const first = fail(m.start(), "network");
    expect(first.ms).toBe(1000);
    expect(m.state).toMatchObject({ phase: "waiting", error: { code: "network", retryAfter: 1 } });
    const second = fail(m.retry(first.generation), "node_error", 502);
    expect(second.ms).toBe(2000);
    const third = fail(m.retry(second.generation), "http_500", 500);
    expect(third.ms).toBe(3000);
    // A proxy's timeout and rate limit are for now too.
    const fourth = fail(m.retry(third.generation), "http_408", 408);
    const fifth = fail(m.retry(fourth.generation), "http_429", 429);
    expect(fifth.ms).toBe(5000);

    const actions = m.snapshotLoaded(snapshotOf(m.retry(fifth.generation)), block(10), []);
    m.receive({ type: "subscribed", id: subscribeOf(actions).id, key });
    m.receive(mempool(10));
    expect(m.state.phase).toBe("live");
    // Over the socket a 5xx has no status: its code says it is for now.
    m.disconnected();
    const sub = subscribeOf(m.connected());
    const again = retryOf(m.receive({ type: "rejected", id: sub.id, key, reason: "node_error" }));
    expect(again.ms).toBe(1000);
  });
});
