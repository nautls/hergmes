import type { SignedTransaction } from "@fleet-sdk/common";
import { serializeTransaction } from "@fleet-sdk/serializer";
import { RelayError } from "./error.ts";
import type {
  Balance,
  BoxPage,
  HeadersPage,
  HistoryPage,
  Key,
  PoolBox,
  Status,
  Submitted,
  WithPool,
} from "./wire.ts";

export interface RestOptions {
  /**
   * Attempts in all for a request answered `503`, waiting `Retry-After` between them; 3 unless
   * set. `capacity_exceeded` is never retried.
   */
  maxAttempts?: number;
  /** The `fetch` to use; the global one by default. */
  fetch?: typeof fetch;
}

export interface RequestOptions {
  /** Cancels the request, and any wait before a retry. */
  signal?: AbortSignal;
}

export interface PageOptions extends RequestOptions {
  /** The previous page's `nextCursor`; the first page without it. */
  cursor?: string;
  /** Items per page; the relay's default without it. */
  limit?: number;
}

export interface HeadersOptions extends RequestOptions {
  /** How many headers, 1 to 10; 10 without it. */
  limit?: number;
}

export interface SubmitOptions extends RequestOptions {
  /** `false` only checks the tx, with no broadcast. */
  broadcast?: boolean;
}

/** Attempts in all for a `503` unless set: the first, and two retries. */
const MAX_ATTEMPTS = 3;

/** The wait before retrying a `503` with no `Retry-After`, about one relay tick. */
const DEFAULT_RETRY_SECONDS = 1;

/**
 * The relay's REST API. Each method answers the route's JSON as it came, and throws a
 * {@link RelayError} for any other status. A `503` is retried after its `Retry-After`, however
 * long, up to `maxAttempts` in all; the last error is thrown. `capacity_exceeded` is not: it
 * frees up only as idle keys leave, about a block, too long to hold a call.
 */
export class RestClient {
  readonly #base: URL;
  readonly #fetch: typeof fetch;
  readonly #maxAttempts: number;

  /** `url` is the relay's root, under which `/v1` is served; it may have a path. */
  constructor(url: string | URL, options: RestOptions = {}) {
    const base = new URL(url);
    if (!base.pathname.endsWith("/")) base.pathname += "/";
    this.#base = base;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#maxAttempts = Math.max(1, options.maxAttempts ?? MAX_ATTEMPTS);
  }

  /** The root the routes are resolved against, its path ending in `/`. */
  get root(): URL {
    return new URL(this.#base);
  }

  /** The relay's version, network, tip, and whether it is ready. */
  status(options: RequestOptions = {}): Promise<Status> {
    return this.#get("v1/status", {}, options);
  }

  /** A page of a key's unspent boxes, in `(inclusionHeight, boxId)` order. */
  boxes(key: Key, options: PageOptions = {}): Promise<BoxPage> {
    const query = { cursor: options.cursor, limit: options.limit };
    return this.#get(`${keyPath(key)}/boxes`, query, options);
  }

  /** A key's boxes with the mempool applied, in one response. */
  withPool(key: Key, options: RequestOptions = {}): Promise<WithPool> {
    return this.#get(`${keyPath(key)}/boxes/with-pool`, {}, options);
  }

  /** An address's balance, confirmed and pending. */
  balance(address: string, options: RequestOptions = {}): Promise<Balance> {
    return this.#get(`${keyPath({ address })}/balance`, {}, options);
  }

  /** A page of an address's txs, newest first, pending on top. */
  history(address: string, options: PageOptions = {}): Promise<HistoryPage> {
    const query = { cursor: options.cursor, limit: options.limit };
    return this.#get(`${keyPath({ address })}/transactions`, query, options);
  }

  /** The last headers up to the relay's tip, newest first. */
  headers(options: HeadersOptions = {}): Promise<HeadersPage> {
    return this.#get("v1/headers", { limit: options.limit }, options);
  }

  /** A box a tx can spend now: confirmed, or a mempool output. */
  box(boxId: string, options: RequestOptions = {}): Promise<PoolBox> {
    return this.#get(`v1/boxes/${encodeURIComponent(boxId)}`, {}, options);
  }

  /**
   * Sends a signed tx, serialized in hex or as Fleet's JSON, or with `broadcast: false` only
   * checks it. The JSON is serialized here, since the relay takes the bytes only.
   */
  submitTransaction(
    tx: string | SignedTransaction,
    options: SubmitOptions = {},
  ): Promise<Submitted> {
    const body = typeof tx === "string" ? tx : toHex(serializeTransaction(tx).toBytes());
    const query = { broadcast: options.broadcast === false ? "false" : undefined };
    const init = { method: "POST", body, headers: { "content-type": "text/plain" } };
    return this.#request("v1/transactions", query, init, options);
  }

  #get<T>(path: string, query: Query, options: RequestOptions): Promise<T> {
    return this.#request(path, query, { method: "GET" }, options);
  }

  async #request<T>(
    path: string,
    query: Query,
    init: RequestInit,
    { signal }: RequestOptions,
  ): Promise<T> {
    const url = new URL(path, this.#base);
    for (const [name, value] of Object.entries(query)) {
      if (value !== undefined) url.searchParams.set(name, String(value));
    }
    for (let attempt = 1; ; attempt++) {
      const response = await this.#fetch(url, signal ? { ...init, signal } : init);
      if (response.ok) return (await response.json()) as T;
      const error = await RelayError.from(response);
      const retry = response.status === 503 && error.code !== "capacity_exceeded";
      if (!retry || attempt >= this.#maxAttempts) throw error;
      await sleep((error.retryAfter ?? DEFAULT_RETRY_SECONDS) * 1000, signal);
    }
  }
}

type Query = Record<string, string | number | undefined>;

function keyPath(key: Key): string {
  return "address" in key
    ? `v1/addresses/${encodeURIComponent(key.address)}`
    : `v1/tokens/${encodeURIComponent(key.tokenId)}`;
}

function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve, reject) => {
    signal?.throwIfAborted();
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason as Error);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
