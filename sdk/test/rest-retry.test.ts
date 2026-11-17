import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { RelayError, RestClient } from "../src/index.ts";

/** A `fetch` answering `responses` in turn, recording what it was asked. */
function stub(...responses: (() => Response)[]) {
  const calls: { url: string; init: RequestInit | undefined }[] = [];
  const fetch = (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    calls.push({ url: input instanceof Request ? input.url : input.toString(), init });
    const next = responses[calls.length - 1];
    if (!next) throw new Error(`unexpected call ${String(calls.length)}`);
    return Promise.resolve(next());
  };
  return { fetch, calls };
}

const status = { version: "0.1.0", network: "mainnet", tip: null, ready: false };
const ok = () => Response.json(status);
const busy = (retryAfter?: string) => () =>
  Response.json(
    { error: "not_ready", message: "not ready" },
    { status: 503, headers: retryAfter === undefined ? {} : { "retry-after": retryAfter } },
  );

/** Settles `promise` while advancing the fake clock; the outcome and the time it took. */
async function run<T>(promise: Promise<T>): Promise<{ outcome: T | Error; ms: number }> {
  const start = Date.now();
  const settled = promise.then(
    (value) => value,
    (error: unknown) => error as Error,
  );
  await vi.runAllTimersAsync();
  return { outcome: await settled, ms: Date.now() - start };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("a 503 is retried", () => {
  test("after each Retry-After, however long", async () => {
    const { fetch, calls } = stub(busy("120"), busy("2"), ok);
    const { outcome, ms } = await run(new RestClient("http://relay", { fetch }).status());
    expect(outcome).toEqual(status);
    expect(calls).toHaveLength(3);
    expect(ms).toBe(122_000);
  });

  test("up to 3 attempts in all, then the last error is thrown", async () => {
    const { fetch, calls } = stub(busy("1"), busy("1"), busy("7"));
    const { outcome, ms } = await run(new RestClient("http://relay", { fetch }).status());
    expect(outcome).toBeInstanceOf(RelayError);
    expect(outcome).toMatchObject({ status: 503, code: "not_ready", retryAfter: 7 });
    expect(calls).toHaveLength(3);
    expect(ms).toBe(2_000);
  });

  test("as many times as maxAttempts allows", async () => {
    const { fetch, calls } = stub(busy("1"));
    const client = new RestClient("http://relay", { fetch, maxAttempts: 1 });
    expect((await run(client.status())).outcome).toMatchObject({ status: 503 });
    expect(calls).toHaveLength(1);
  });

  test("after 1 s when it has no Retry-After", async () => {
    const { fetch } = stub(busy(), ok);
    const { outcome, ms } = await run(new RestClient("http://relay", { fetch }).status());
    expect(outcome).toEqual(status);
    expect(ms).toBe(1_000);
  });

  test("a submit too", async () => {
    const done = () => Response.json({ transactionId: "ab" });
    const { fetch, calls } = stub(busy("1"), done);
    const client = new RestClient("http://relay", { fetch });
    const { outcome } = await run(client.submitTransaction("00ff", { broadcast: false }));
    expect(outcome).toEqual({ transactionId: "ab" });
    expect(calls.map((c) => c.init?.body)).toEqual(["00ff", "00ff"]);
  });

  test("until the signal aborts the wait", async () => {
    const { fetch, calls } = stub(busy("60"), ok);
    const controller = new AbortController();
    const request = new RestClient("http://relay", { fetch }).status({
      signal: controller.signal,
    });
    const settled = request.then(
      () => undefined,
      (error: unknown) => error,
    );
    await vi.advanceTimersByTimeAsync(10_000);
    controller.abort(new Error("gave up"));
    expect(await settled).toEqual(new Error("gave up"));
    await vi.runAllTimersAsync();
    expect(calls).toHaveLength(1);
  });
});

describe("other failures are thrown at once", () => {
  test("capacity_exceeded, a 503 that frees up only in about a block", async () => {
    const full = () =>
      Response.json(
        { error: "capacity_exceeded", message: "full" },
        { status: 503, headers: { "retry-after": "120" } },
      );
    const { fetch, calls } = stub(full, ok);
    const { outcome, ms } = await run(new RestClient("http://relay", { fetch }).status());
    expect(outcome).toMatchObject({ status: 503, code: "capacity_exceeded", retryAfter: 120 });
    expect(calls).toHaveLength(1);
    expect(ms).toBe(0);
  });

  test.each([
    [400, "invalid_limit"],
    [404, "box_not_found"],
    [410, "cursor_expired"],
    [502, "node_error"],
  ])("%i %s", async (code, error) => {
    const { fetch, calls } = stub(() => Response.json({ error, message: "no" }, { status: code }));
    const { outcome } = await run(new RestClient("http://relay", { fetch }).status());
    expect(outcome).toMatchObject({ status: code, code: error, message: "no" });
    expect(calls).toHaveLength(1);
  });

  test("a body that is not the relay's error has no code", async () => {
    const bare = () => new Response(null, { status: 404 });
    const html = () => new Response("<html>Bad gateway</html>", { status: 502 });
    const json = () => Response.json({ detail: "nope" }, { status: 500 });
    const { fetch } = stub(bare, html, json);
    const client = new RestClient("http://relay", { fetch });
    expect((await run(client.status())).outcome).toMatchObject({
      status: 404,
      code: undefined,
      message: "HTTP 404",
    });
    expect((await run(client.status())).outcome).toMatchObject({
      status: 502,
      code: undefined,
      message: "<html>Bad gateway</html>",
    });
    expect((await run(client.status())).outcome).toMatchObject({ status: 500, code: undefined });
  });
});

test("routes are built under the root's path, with keys encoded", async () => {
  const page = () => Response.json({ asOf: { height: 1, headerId: "a" }, items: [] });
  const { fetch, calls } = stub(page, page, page, page);
  const client = new RestClient("http://relay/hergmes", { fetch });
  await client.boxes({ address: "9f/x" }, { limit: 3, cursor: "c+/" });
  await client.withPool({ tokenId: "ab" });
  await client.headers();
  await client.submitTransaction("00", { broadcast: true });
  expect(calls.map((c) => c.url)).toEqual([
    "http://relay/hergmes/v1/addresses/9f%2Fx/boxes?cursor=c%2B%2F&limit=3",
    "http://relay/hergmes/v1/tokens/ab/boxes/with-pool",
    "http://relay/hergmes/v1/headers",
    "http://relay/hergmes/v1/transactions",
  ]);
});
