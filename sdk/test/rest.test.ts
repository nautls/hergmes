import { afterAll, beforeAll, describe, expect, test } from "vitest";
import {
  RelayError,
  RestClient,
  type ConfirmedBox,
  type HistoryItem,
  type Key,
  type PoolBox,
} from "../src/index.ts";
import { SimRelay, seeds, type Expected } from "./sim.ts";

let sim: SimRelay;
let client: RestClient;

beforeAll(async () => {
  sim = await SimRelay.start();
  client = new RestClient(sim.url);
});

afterAll(() => {
  sim.stop();
});

/** The boxes a tx can spend now, as `with-pool` lists them: confirmed first, then pending. */
function spendable(expected: Expected): PoolBox[] {
  const spent = new Set(expected.pending.flatMap((tx) => tx.spent));
  const created = expected.pending.flatMap((tx) => tx.created);
  return [...expected.boxes, ...created].filter((box) => !spent.has(box.boxId));
}

/** Every page of a key's boxes, by cursor, each checked to be pinned to `tip`. */
async function allPages(key: Key, limit: number, tip: unknown): Promise<ConfirmedBox[]> {
  const items: ConfirmedBox[] = [];
  let cursor: string | undefined;
  do {
    const page = await client.boxes(key, cursor === undefined ? { limit } : { limit, cursor });
    expect(page.asOf).toEqual(tip);
    expect(page.items.length).toBeLessThanOrEqual(limit);
    items.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor !== undefined);
  return items;
}

/** Every page of an address's history, by cursor. */
async function allHistory(address: string): Promise<HistoryItem[]> {
  const items: HistoryItem[] = [];
  let cursor: string | undefined;
  do {
    const page = await client.history(
      address,
      cursor === undefined ? { limit: 5 } : { limit: 5, cursor },
    );
    items.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor !== undefined);
  return items;
}

function sum(txs: HistoryItem[]): bigint {
  return txs.reduce((total, tx) => total + BigInt(tx.delta.nanoErgs), 0n);
}

async function rejection(promise: Promise<unknown>): Promise<RelayError> {
  const error: unknown = await promise.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(error).toBeInstanceOf(RelayError);
  return error as RelayError;
}

const SEED = BigInt(process.env.SEED ?? Math.floor(Math.random() * 2 ** 48));

describe(`the REST routes against the oracle (SEED=${String(SEED)})`, () => {
  test("every key's pages, view with the pool and balance equal the oracle's", async () => {
    const next = seeds(SEED);
    const ops = [];
    for (let round = 0; round < 4; round++) {
      for (let step = 0; step < 12; step++) {
        ops.push(await sim.random(next()));
        if (next() % 3n === 0n) await sim.tick();
      }
      const tip = await sim.settle();
      const context = `round ${String(round)}, ops ${JSON.stringify(ops)}`;

      const status = await client.status();
      expect(status, context).toMatchObject({ network: "mainnet", tip, ready: true });

      const headers = await client.headers();
      expect(headers.asOf, context).toEqual(tip);
      expect(headers.items[0]?.id, context).toBe(tip.headerId);
      headers.items.slice(1).forEach((header, i) => {
        expect(header.id, context).toBe(headers.items[i]?.parentId);
        expect(header.height, context).toBe(tip.height - i - 1);
      });

      const { addresses, tokens } = await sim.keys();
      const keys: Key[] = [
        ...addresses.map((address) => ({ address })),
        ...tokens.filter((t) => BigInt(t.emission) <= 100n).map((t) => ({ tokenId: t.tokenId })),
      ];
      for (const key of keys) {
        const keyContext = `${context}, key ${JSON.stringify(key)}`;
        // The first read loads the key; the oracle is read after it, at the same checkpoint.
        const read = allPages(key, 3, tip);
        const pages = await read.catch((error: unknown) => {
          // A token minted in the pool only, or on an orphaned branch: the indexer has none.
          const unknown = error instanceof RelayError && error.code === "unknown_token";
          if (unknown && "tokenId" in key) return undefined;
          throw error;
        });
        const expected = await sim.expected(key);
        expect(expected.tip, keyContext).toEqual(tip);
        if (pages === undefined) {
          expect(expected.boxes, keyContext).toEqual([]);
          continue;
        }
        expect(pages, keyContext).toEqual(expected.boxes);

        const view = await client.withPool(key);
        const now = spendable(expected);
        expect(view, keyContext).toEqual({
          asOf: { height: tip.height },
          items: now,
          truncated: false,
          total: now.length,
        });

        if ("address" in key) {
          const balance = await client.balance(key.address);
          expect(balance, keyContext).toMatchObject({
            asOf: { height: tip.height },
            ...expected.balance,
          });

          const history = await allHistory(key.address);
          const ids = history.map((tx) => tx.txId);
          expect(new Set(ids).size, keyContext).toBe(ids.length);
          const firstConfirmed = history.findIndex((tx) => tx.height !== undefined);
          const pending = firstConfirmed === -1 ? history : history.slice(0, firstConfirmed);
          const confirmed = firstConfirmed === -1 ? [] : history.slice(firstConfirmed);
          expect(
            confirmed.every((tx) => tx.height !== undefined),
            keyContext,
          ).toBe(true);
          const heights = confirmed.map((tx) => tx.height ?? 0);
          expect(heights, keyContext).toEqual([...heights].sort((a, b) => b - a));
          expect(heights[0] ?? 0, keyContext).toBeLessThanOrEqual(tip.height);
          expect(new Set(pending.map((tx) => tx.txId)), keyContext).toEqual(
            new Set(expected.pending.map((tx) => tx.txId)),
          );
          expect(sum(confirmed), keyContext).toBe(BigInt(balance.confirmed.nanoErgs));
          // With both sides of a double spend in the pool, each side's delta takes the common
          // input, while the balance takes it once.
          const spends = expected.pending.flatMap((tx) => tx.spent);
          if (new Set(spends).size === spends.length) {
            expect(sum(pending), keyContext).toBe(BigInt(balance.pending.nanoErgs));
          }
        }

        // A box by id is one item of the view: a confirmed one and a pending output.
        const confirmedBox = now.find((box) => box.inclusionHeight !== undefined);
        const pendingBox = now.find((box) => box.inclusionHeight === undefined);
        for (const box of [confirmedBox, pendingBox]) {
          if (box) expect(await client.box(box.boxId), keyContext).toEqual(box);
        }
        const spentByPool = expected.pending.flatMap((tx) => tx.spent)[0];
        if (spentByPool) {
          const error = await rejection(client.box(spentByPool));
          expect(error, keyContext).toMatchObject({ status: 404, code: "box_not_found" });
        }
      }
    }
  });
});

describe("errors", () => {
  test("refusals of the request itself", async () => {
    await sim.settle();
    const { addresses, tokens } = await sim.keys();
    const address = addresses[0] ?? "";
    const id = "ab".repeat(32);

    const cases: [() => Promise<unknown>, number, string][] = [
      [() => client.boxes({ address: "nope" }), 400, "invalid_address"],
      [() => client.balance("nope"), 400, "invalid_address"],
      [() => client.boxes({ tokenId: "nope" }), 400, "invalid_token_id"],
      [() => client.box("nope"), 400, "invalid_box_id"],
      [() => client.boxes({ address }, { limit: 0 }), 400, "invalid_limit"],
      [() => client.boxes({ address }, { limit: 1001 }), 400, "invalid_limit"],
      [() => client.history(address, { limit: 101 }), 400, "invalid_limit"],
      [() => client.headers({ limit: 11 }), 400, "invalid_limit"],
      [() => client.boxes({ address }, { cursor: "garbage" }), 400, "invalid_cursor"],
      [() => client.boxes({ tokenId: id }), 404, "unknown_token"],
      [() => client.box(id), 404, "box_not_found"],
    ];
    // An over-supply token the indexer knows: one minted in the pool only, or on an orphaned
    // branch, is `unknown_token` instead.
    let large: string | undefined;
    for (const token of tokens.filter((t) => BigInt(t.emission) > 100n)) {
      const { boxes } = await sim.expected({ tokenId: token.tokenId });
      if (boxes.length > 0) {
        large = token.tokenId;
        break;
      }
    }
    expect(large, "no over-supply token on chain").toBeDefined();
    cases.push([() => client.boxes({ tokenId: large ?? "" }), 422, "supply_too_large"]);
    for (const [request, status, code] of cases) {
      const error = await rejection(request());
      expect(error).toMatchObject({ status, code, retryAfter: undefined });
      expect(error.message).not.toBe("");
    }
  });

  test("a tx the relay refuses, and one the node fails", async () => {
    const refused = await rejection(client.submitTransaction("zz"));
    expect(refused).toMatchObject({
      status: 400,
      code: "transaction_rejected",
      reason: "unparseable",
    });
    // The simulator takes no tx: its node answers 501, a node failure.
    const failed = await rejection(client.submitTransaction("00", { broadcast: false }));
    expect(failed).toMatchObject({ status: 502, code: "node_error", reason: undefined });
  });

  test("a cursor whose block left the window expires", async () => {
    await sim.settle();
    const { addresses } = await sim.keys();
    let cursor: string | undefined;
    for (const address of addresses) {
      const page = await client.boxes({ address }, { limit: 1 });
      if (page.nextCursor !== null) {
        cursor = page.nextCursor;
        await sim.ops(
          Array.from({ length: 9 }, () => ({ op: "mine", fromPool: 0, fresh: 0, seed: "1" })),
        );
        await sim.settle();
        const error = await rejection(client.boxes({ address }, { limit: 1, cursor }));
        expect(error).toMatchObject({ status: 410, code: "cursor_expired" });
        break;
      }
    }
    expect(cursor, "no address has two boxes").toBeDefined();
  });
});
