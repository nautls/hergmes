import { isDeepStrictEqual } from "node:util";
import { ensureUTxOBigInt, type Box } from "@fleet-sdk/common";
import { afterAll, beforeAll, expect, test } from "vitest";
import { totals } from "../src/machine.ts";
import {
  Relay,
  type Assets,
  type BlockRef,
  type Key,
  type Resync,
  type Subscription,
  type WireAssets,
} from "../src/index.ts";
import { Faults } from "./faults.ts";
import { SimRelay, seeds, type Expected } from "./sim.ts";

let sim: SimRelay;

beforeAll(async () => {
  sim = await SimRelay.start();
});

afterAll(() => {
  sim.stop();
});

/**
 * How long the SDK has to reach the oracle's state after a settle. The longest wait measured
 * was 61 ms with every core busy; a run that converges stops waiting at once.
 */
const DEADLINE_MS = 10_000;
const SEED = BigInt(process.env.SEED ?? Math.floor(Math.random() * 2 ** 48));

function assets(wire: WireAssets): Assets {
  return {
    nanoErgs: BigInt(wire.nanoErgs),
    tokens: new Map(wire.tokens.map((t) => [t.tokenId, BigInt(t.amount)])),
  };
}

/** What a subscription should hold, from the oracle's state for its key. */
function wanted(expected: Expected) {
  const pending = expected.pending.map((tx) => ({
    txId: tx.txId,
    created: tx.created.map((b) => ensureUTxOBigInt(b)),
    spent: tx.spent,
  }));
  const spent = new Set(expected.pending.flatMap((tx) => tx.spent));
  const confirmed = expected.boxes.map((b) => ensureUTxOBigInt(b));
  const spendable: Box<bigint>[] = [
    ...confirmed.filter((b) => !spent.has(b.boxId)),
    ...pending.flatMap((tx) => tx.created).filter((b) => !spent.has(b.boxId)),
  ];
  const balance = expected.balance && {
    confirmed: assets(expected.balance.confirmed),
    pending: assets(expected.balance.pending),
  };
  return { confirmed, pending, spendable, spendableTotals: totals(spendable), balance };
}

/** What a subscription holds, in the shape of {@link wanted}. */
function held(sub: Subscription) {
  const { view, balance } = sub.state;
  return {
    confirmed: view?.confirmed,
    pending: view?.pending,
    spendable: view?.spendable,
    spendableTotals: view?.spendableTotals,
    balance: balance && { confirmed: balance.confirmed, pending: balance.pending },
  };
}

function sum(a: Assets, b: Assets): Assets {
  const tokens = new Map(a.tokens);
  for (const [id, amount] of b.tokens) tokens.set(id, (tokens.get(id) ?? 0n) + amount);
  for (const [id, amount] of tokens) if (amount === 0n) tokens.delete(id);
  return { nanoErgs: a.nanoErgs + b.nanoErgs, tokens };
}

function sorted(a: Assets): Assets {
  return {
    nanoErgs: a.nanoErgs,
    tokens: new Map([...a.tokens].sort(([x], [y]) => (x < y ? -1 : 1))),
  };
}

/** Backoff waits short enough for a settle to converge well inside the deadline. */
const BACKOFF = { firstMs: 10, maxMs: 50 };

test(`subscriptions converge to the oracle at every checkpoint (SEED=${String(SEED)})`, async () => {
  const frames: string[] = [];
  const resyncs: Resync[] = [];
  const debug = (event: { frame: string } | { resync: Resync }) => {
    if ("frame" in event) frames.push(event.frame);
    else resyncs.push(event.resync);
  };
  const next = seeds(SEED);
  // The network's faults: cuts, between ops and after a drawn number of frames (so some land
  // inside a catch-up), and delays. Each relay draws from its own stream of the run's seed.
  const faults = {
    boxes: new Faults({ draw: seeds(next()), maxDelayMs: 20, cutWithin: 60 }),
    balances: new Faults({ draw: seeds(next()), maxDelayMs: 20, cutWithin: 60 }),
  };
  // One box a page, so every snapshot pages; balance-only on a second connection, since a key has
  // one subscription per connection.
  const options = { debug, backoff: BACKOFF };
  const boxes = new Relay(sim.url, { ...options, pageSize: 1, WebSocket: faults.boxes.WebSocket });
  const balances = new Relay(sim.url, { ...options, WebSocket: faults.balances.WebSocket });
  const subs = new Map<string, { key: Key; box: Subscription; balance?: Subscription }>();
  const ops: unknown[] = [];

  /**
   * Subscribes every key not yet subscribed: the addresses, and the tokens under the supply
   * limit. A token the indexer does not know yet (minted in the pool only) is refused, and
   * tried again at the next call. A token no longer listed under the limit is let go: two
   * conflicting pool txs with the same first input both mint its id, and the one built last
   * gives its emission, which may be over the limit.
   */
  const subscribeAll = async () => {
    const { addresses, tokens } = await sim.keys();
    const keys: Key[] = [
      ...addresses.map((address) => ({ address })),
      ...tokens.filter((t) => BigInt(t.emission) <= 100n).map((t) => ({ tokenId: t.tokenId })),
    ];
    const listed = new Set(keys.map((key) => JSON.stringify(key)));
    for (const [name, sub] of subs) {
      if (listed.has(name)) continue;
      sub.box.close();
      sub.balance?.close();
      subs.delete(name);
    }
    for (const key of keys) {
      const name = JSON.stringify(key);
      const sub = subs.get(name);
      if (sub?.box.state.phase === "rejected") {
        expect(sub.box.state.error?.code, name).toBe("unknown_token");
        sub.box.close();
        subs.delete(name);
      }
      if (!subs.has(name)) {
        subs.set(name, {
          key,
          box: boxes.subscribe(key),
          ...("address" in key ? { balance: balances.subscribe(key, { boxes: false }) } : {}),
        });
      }
    }
  };

  try {
    for (let round = 0; round < 4; round++) {
      // Before the ops too, so the SDK follows them live, catching up from a snapshot taken
      // while they apply.
      await subscribeAll();
      faults.boxes.paused = faults.balances.paused = false;
      for (let step = 0; step < 12; step++) {
        ops.push(await sim.random(next()));
        if (next() % 3n === 0n) await sim.tick();
        if (next() % 6n === 0n) {
          const name = next() % 2n === 0n ? "boxes" : "balances";
          ops.push(`cut ${name}`);
          faults[name].cut(`at op ${String(ops.length)}`);
        }
      }
      // No cut while converging: the network has calmed down.
      faults.boxes.paused = faults.balances.paused = true;
      const tip = await sim.settle();
      const context = () =>
        `round ${String(round)}, ops ${JSON.stringify(ops)}, ` +
        `boxes' faults ${faults.boxes.log.join("; ")}, ` +
        `balances' faults ${faults.balances.log.join("; ")}, ` +
        `last frames ${frames.slice(-40).join("\n")}`;

      await subscribeAll();

      const expected = new Map<string, Expected>();
      for (const [name, { key }] of subs) expected.set(name, await sim.expected(key));
      const converged = (name: string, box: Subscription, balance?: Subscription) => {
        const exp = expected.get(name);
        if (!exp) return false;
        if (box.state.phase === "rejected") return exp.boxes.length === 0;
        const at: BlockRef | undefined = box.state.view?.at;
        const want = wanted(exp);
        return (
          box.state.phase === "live" &&
          isDeepStrictEqual(at, tip) &&
          isDeepStrictEqual(held(box), want) &&
          (balance === undefined ||
            (balance.state.phase === "live" &&
              isDeepStrictEqual(
                balance.state.balance && {
                  confirmed: balance.state.balance.confirmed,
                  pending: balance.state.balance.pending,
                },
                want.balance,
              )))
        );
      };
      const deadline = Date.now() + DEADLINE_MS;
      const all = () => [...subs].every(([name, s]) => converged(name, s.box, s.balance));
      while (!all() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));

      for (const [name, { box, balance }] of subs) {
        const exp = expected.get(name);
        if (!exp) throw new Error(`no expected state for ${name}`);
        const keyContext = `${name}: ${context()}`;
        if (box.state.phase === "rejected") {
          expect(box.state.error?.code, keyContext).toBe("unknown_token");
          expect(exp.boxes, keyContext).toEqual([]);
          continue;
        }
        expect(box.state.phase, keyContext).toBe("live");
        expect(box.state.view?.at, keyContext).toEqual(tip);
        const want = wanted(exp);
        expect(held(box), keyContext).toEqual(want);
        if (want.balance && box.state.view) {
          // Spendable now totals the balance's two halves.
          expect(sorted(box.state.view.spendableTotals), keyContext).toEqual(
            sorted(sum(want.balance.confirmed, want.balance.pending)),
          );
        }
        if (balance) {
          expect(balance.state.phase, keyContext).toBe("live");
          expect(balance.state.balance, keyContext).toMatchObject(want.balance ?? {});
        }
      }
      // Cuts and delays lose or reorder nothing a subscription receives, so the SDK never finds a
      // message its view can't take. The relay may still ask for a new snapshot: a reorg can
      // orphan the block a key was loaded at, and a key resumed after a cut may have been
      // untracked meanwhile.
      expect(
        resyncs.filter((r) => r.by === "sdk"),
        context(),
      ).toEqual([]);
    }
  } finally {
    boxes.close();
    balances.close();
  }
});
