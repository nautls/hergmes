import { afterAll, beforeAll, expect, test } from "vitest";
import { RelayError, RestClient } from "../src/index.ts";
import { SimRelay } from "./sim.ts";

// Its own process: the keys must be untracked, so that a read loads them.
let sim: SimRelay;

beforeAll(async () => {
  sim = await SimRelay.start();
});

afterAll(() => {
  sim.stop();
});

/** Leaves the engine below the node's tip: the indexer lags, so a tick can't sync. */
async function lag(): Promise<void> {
  const mine = { op: "mine", fromPool: 0, fresh: 1, seed: "1" };
  await sim.ops([{ op: "indexerLag", lag: 2 }, mine, mine, mine]);
  const { synced } = await sim.tick();
  expect(synced).toBe(false);
}

test("a load while the engine lags is a 503 not_ready, with Retry-After", async () => {
  await lag();
  const [address] = (await sim.keys()).addresses;
  const client = new RestClient(sim.url, { maxAttempts: 1 });
  const error: unknown = await client.boxes({ address: address ?? "" }).catch((e: unknown) => e);
  expect(error).toBeInstanceOf(RelayError);
  expect(error).toMatchObject({ status: 503, code: "not_ready", retryAfter: 1 });
});

test("a retry after Retry-After reads the key once the engine caught up", async () => {
  await lag();
  const [, address] = (await sim.keys()).addresses;
  const statuses: number[] = [];
  let refused: () => void = () => undefined;
  const firstRefusal = new Promise<void>((resolve) => (refused = resolve));
  const fetch: typeof globalThis.fetch = async (input, init) => {
    const response = await globalThis.fetch(input, init);
    statuses.push(response.status);
    if (response.status === 503) refused();
    return response;
  };
  const client = new RestClient(sim.url, { fetch });
  const read = client.boxes({ address: address ?? "" });
  await firstRefusal;
  const tip = await sim.settle();
  const page = await read;
  expect(statuses).toEqual([503, 200]);
  expect(page.asOf).toEqual(tip);
  expect(page.items).toEqual((await sim.expected({ address: address ?? "" })).boxes);
});
