import { expect, test } from "vitest";
import { RestClient } from "../src/index.ts";

/**
 * A `fetch` answering each request with a token per id asked, a tick later, recording the ids
 * and the most requests in flight at once.
 */
function relay() {
  const asked: string[][] = [];
  let inFlight = 0;
  let most = 0;
  const fetch = async (input: string | URL | Request): Promise<Response> => {
    const url = new URL(input instanceof Request ? input.url : input.toString());
    const ids = url.searchParams.get("ids")?.split(",") ?? [];
    asked.push(ids);
    most = Math.max(most, ++inFlight);
    await new Promise((resolve) => setTimeout(resolve, 1));
    inFlight--;
    const items = ids.map((tokenId) => ({ tokenId, name: "", emission: "1" }));
    return Response.json({ items });
  };
  return { client: new RestClient("http://relay", { fetch }), asked, most: () => most };
}

const id = (n: number) => n.toString(16).padStart(64, "0");

test("over 100 ids are asked 100 per request, one after another, and answered as one list", async () => {
  const { client, asked, most } = relay();
  const ids = Array.from({ length: 250 }, (_, n) => id(n));
  const { items } = await client.tokens([...ids, ids[0] ?? ""]);
  expect(asked.map((chunk) => chunk.length)).toEqual([100, 100, 50]);
  expect(asked.flat()).toEqual(ids);
  expect(items.map((t) => t.tokenId)).toEqual(ids);
  expect(most()).toBe(1);
});

test("an id in two cases is asked once, across requests too", async () => {
  const { client, asked } = relay();
  // Each id holds a letter, so its upper case differs.
  const ids = Array.from({ length: 150 }, (_, n) => id(0xa00 + n));
  const { items } = await client.tokens([...ids, (ids[0] ?? "").toUpperCase()]);
  expect(asked.flat()).toEqual(ids);
  expect(items).toHaveLength(150);
});

test("no ids make no request", async () => {
  const { client, asked } = relay();
  expect(await client.tokens([])).toEqual({ items: [] });
  expect(asked).toEqual([]);
});
