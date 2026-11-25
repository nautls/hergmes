/**
 * An address's tx history, read page by page from the relay, newest first with its pending txs
 * on top. History is not streamed: with `refresh`, the first page is read again whenever the
 * address's balance changes, and joined onto the older pages already read.
 */

import {
  assets,
  failure,
  transient,
  type Assets,
  type Balance,
  type SubscriptionError,
  type SubscriptionState,
} from "./machine.ts";
import { call, store } from "./call.ts";
import type { RestClient } from "./rest.ts";
import type { HistoryItem as WireHistoryItem, HistoryPage } from "./wire.ts";

/**
 * A tx in an address's history: its signed net change for the address, its fee, and when it
 * was included. A pending tx has no `height` and no `timestamp`.
 */
export interface HistoryItem {
  txId: string;
  height?: number;
  /** The including block's time, in milliseconds. */
  timestamp?: number;
  delta: Assets;
  /** What the tx pays the miners' fee contract, in nanoERGs; absent if it has no output to it. */
  fee?: bigint;
}

/** A history's state, replaced whole on every change. */
export interface HistoryState {
  address: string;
  /** Newest first, pending txs on top: the first page and the older pages read since. */
  items: readonly HistoryItem[];
  /** `items` reach the address's first tx: {@link TxHistory.more} has nothing left to read. */
  end: boolean;
  /** A read is in flight. */
  loading: boolean;
  /**
   * Why the last read failed, until one succeeds. A first page that failed for now has a
   * `retryAfter`, when it is read again.
   */
  error: SubscriptionError | undefined;
  /**
   * How many times the list was read again from the first page with its older pages
   * dropped: an older page's cursor expired (a reorg reached it), or a refreshed first page
   * no longer joined the older pages (a page or more of txs arrived since it was last read,
   * or a reorg). An app that keeps a scroll position compares it with the last it saw. A
   * count, not a flag, so it is not lost when changes come together.
   */
  resets: number;
}

export interface HistoryOptions {
  /**
   * Reads the first page again whenever the address's balance changes, which takes a
   * balance-only hold on the address as {@link Relay.subscribe} does, sharing the key's
   * subscription if the app has one. Older pages are not checked again. A tx that changes
   * neither the confirmed nor the pending balance (one that gives back exactly what it took,
   * its fee paid by another address) shows only at the next change. Defaults to `false`.
   */
  refresh?: boolean;
  /** Txs per page, at most 100; the relay's default, 25, without it. */
  limit?: number;
}

/** One app's view of an address's history. */
export interface TxHistory {
  readonly address: string;
  /** The current state, replaced whole on every change. */
  readonly state: HistoryState;
  /**
   * Reads the next older page and appends it, or with no first page yet, reads that. Does
   * nothing at the end, or while a read is in flight: it then answers that read. Answers when
   * the read is done; a failure is in `state.error`, and the next call reads on from the last
   * cursor the failed one reached.
   */
  more(): Promise<void>;
  /**
   * Calls `listener` with the current state at once, then with the latest state after changes,
   * once for all the changes made together (on a `setTimeout(0)`), never twice with the same
   * state. Answers a function that removes the listener. The contract of a Svelte store, so
   * the handle is one (`$history`).
   */
  subscribe(listener: (state: HistoryState) => void): () => void;
  /** Stops reading and releases the address's hold, if `refresh` took one. */
  close(): void;
}

/** Empty pages with a cursor in a row before a read gives up, rather than paging forever. */
export const MAX_EMPTY_PAGES = 3;

/** What a history needs of its relay. */
export interface HistoryContext {
  rest: RestClient;
  /** How long to wait before the `attempt`-th retry (from 0) of a failure with no `retryAfter`. */
  backoff(attempt: number): number;
  /** With `refresh`: a balance-only hold on the address, released on close. */
  hold: SubscriptionLike | undefined;
  /** Told once the history is closed. */
  closed(history: HistoryFeed): void;
}

/** The part of a subscription a history uses. */
export interface SubscriptionLike {
  readonly state: SubscriptionState;
  subscribe(listener: (state: SubscriptionState) => void): () => void;
  close(): void;
}

/**
 * Reads one at a time: a balance during a read makes one more read of the first page after
 * it, however many arrived. A first page that fails for now is read again after its
 * `retryAfter` or the relay's backoff, since nothing else may ask for it soon; an older page is
 * read again only when the app asks.
 */
export class HistoryFeed implements TxHistory {
  readonly address: string;
  readonly #context: HistoryContext;
  readonly #limit: number | undefined;
  readonly #listeners = new Set<(state: HistoryState) => void>();
  #state: HistoryState;
  /** The state the listeners were last called with. */
  #notified: HistoryState;
  #notifying: ReturnType<typeof setTimeout> | undefined;
  /** The oldest page's `nextCursor`: `null` at the end, `undefined` before a first page. */
  #cursor: string | null | undefined;
  /** `items` hold older pages below the first. */
  #scrolled = false;
  /** The read in flight, and what cancels it. */
  #read: Promise<void> | undefined;
  #abort: AbortController | undefined;
  /** A first page is to be read once the read in flight is done. */
  #due = false;
  /** The next first page replaces the list whatever it holds: an older page's cursor expired. */
  #resetting = false;
  #retry: ReturnType<typeof setTimeout> | undefined;
  /** First-page failures since one last succeeded, for the backoff. */
  #attempt = 0;
  /** The address's balance the last read was asked for. */
  #seen: Balance | undefined;
  /** The first read waits for the hold's subscription to settle. */
  #deferred = false;
  /** Why the last first-page read failed, until one succeeds: an older page's success keeps it. */
  #topError: SubscriptionError | undefined;
  #unlisten: (() => void) | undefined;
  #closed = false;

  constructor(address: string, limit: number | undefined, context: HistoryContext) {
    this.address = address;
    this.#limit = limit;
    this.#context = context;
    this.#state = Object.freeze({
      address,
      items: [],
      end: false,
      loading: false,
      error: undefined,
      resets: 0,
    });
    this.#notified = this.#state;
    const hold = context.hold;
    if (hold) {
      // A subscription still loading would send a balance right after this read, and with it
      // a second read of the same page: the first read waits for it to settle instead.
      this.#deferred = settling(hold.state);
      this.#seen = hold.state.balance;
      // Called at once with the state just read, which the checks below pass over.
      this.#unlisten = hold.subscribe((state) => {
        if (this.#deferred) {
          if (settling(state)) return;
          this.#deferred = false;
        } else if (state.balance === undefined || state.balance === this.#seen) {
          return;
        }
        this.#seen = state.balance;
        this.#refresh();
      });
    }
    if (!this.#deferred) void this.#start("first");
  }

  get state(): HistoryState {
    return this.#state;
  }

  more(): Promise<void> {
    if (this.#closed) return Promise.resolve();
    if (this.#read) return this.#read;
    if (this.#cursor === null) return Promise.resolve();
    if (this.#cursor === undefined) {
      clearTimeout(this.#retry);
      this.#retry = undefined;
      return this.#start("first");
    }
    return this.#start("next");
  }

  subscribe(listener: (state: HistoryState) => void): () => void {
    return store(this.#state, this.#listeners, listener);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#abort?.abort();
    this.#read = undefined;
    clearTimeout(this.#retry);
    clearTimeout(this.#notifying);
    this.#unlisten?.();
    this.#context.hold?.close();
    this.#context.closed(this);
  }

  /** The address changed: reads the first page now, or after the read in flight. */
  #refresh(): void {
    if (this.#closed) return;
    clearTimeout(this.#retry);
    this.#retry = undefined;
    if (this.#read) this.#due = true;
    else void this.#start("first");
  }

  #start(kind: "first" | "next"): Promise<void> {
    const abort = new AbortController();
    this.#abort = abort;
    if (!this.#state.loading) this.#set({ loading: true });
    const read = this.#run(kind, abort.signal).then(() => {
      if (this.#read !== read) return;
      this.#read = undefined;
      this.#abort = undefined;
      if (this.#due) {
        // The read the balance asked for: a retry of a failed one waits no more.
        this.#due = false;
        clearTimeout(this.#retry);
        this.#retry = undefined;
        void this.#start("first");
      } else {
        this.#set({ loading: false });
      }
    });
    this.#read = read;
    return read;
  }

  async #run(kind: "first" | "next", signal: AbortSignal): Promise<void> {
    const rest = this.#context.rest;
    const limit = this.#limit === undefined ? {} : { limit: this.#limit };
    try {
      if (kind === "first") {
        const page = await rest.history(this.address, { ...limit, signal });
        if (signal.aborted) return;
        this.#first(page);
        return;
      }
      for (let empty = 1; ; empty++) {
        const cursor = this.#cursor ?? undefined;
        const at = cursor === undefined ? {} : { cursor };
        const page = await rest.history(this.address, { ...at, ...limit, signal });
        if (signal.aborted) return;
        if (page.items.length > 0 || page.nextCursor === null || empty === MAX_EMPTY_PAGES) {
          // Given up on only while there is more: an empty last page is the end.
          this.#next(page, page.items.length === 0 && page.nextCursor !== null);
          return;
        }
        // An empty page with a cursor: txs arrived above the anchor faster than the reads.
        this.#cursor = page.nextCursor;
      }
    } catch (e) {
      if (signal.aborted) return;
      const error = failure(e);
      if (kind === "next" && error.code === "cursor_expired") {
        // A reorg reached the older pages: the list starts again from the first.
        this.#resetting = true;
        await this.#run("first", signal);
        return;
      }
      if (kind === "next") {
        this.#set({ error });
        return;
      }
      if (!transient(error)) {
        this.#topError = error;
        this.#set({ error });
        return;
      }
      const ms =
        error.retryAfter === undefined
          ? this.#context.backoff(this.#attempt++)
          : error.retryAfter * 1000;
      this.#topError = { ...error, retryAfter: ms / 1000 };
      this.#set({ error: this.#topError });
      this.#retry = setTimeout(() => {
        this.#retry = undefined;
        this.#refresh();
      }, ms);
    }
  }

  /**
   * A first page: on top of the older pages if it joins them, in place of the list if not.
   * It joins at its oldest tx, found below with the same height: the txs below it follow it on
   * the address's list unless a reorg rewrote them, and a reorg that did would have dropped
   * it too, or put back some of the page's txs below it.
   */
  #first(page: HistoryPage): void {
    if (this.#closed) return;
    this.#attempt = 0;
    this.#topError = undefined;
    // A retry still waiting has nothing left to read.
    clearTimeout(this.#retry);
    this.#retry = undefined;
    const fresh = page.items.map(item);
    const resetting = this.#resetting;
    this.#resetting = false;
    let joined: HistoryItem[] | undefined;
    if (!resetting && this.#scrolled && page.nextCursor !== null) {
      const oldest = fresh.at(-1);
      const at = this.#state.items.findIndex(
        (i) => oldest?.height !== undefined && i.txId === oldest.txId && i.height === oldest.height,
      );
      const tail = at < 0 ? [] : this.#state.items.slice(at + 1);
      const ids = new Set(fresh.map((i) => i.txId));
      if (at >= 0 && !tail.some((i) => ids.has(i.txId))) joined = [...fresh, ...tail];
    }
    if (joined) {
      this.#set({ items: joined, error: undefined });
      return;
    }
    // Older pages dropped though the list goes on below this one: a reset.
    const reset = this.#scrolled && (resetting || page.nextCursor !== null);
    this.#cursor = page.nextCursor;
    this.#scrolled = false;
    this.#set({
      items: fresh,
      end: page.nextCursor === null,
      error: undefined,
      resets: this.#state.resets + (reset ? 1 : 0),
    });
  }

  /** An older page, appended; `empty` when it ends a run of empty pages, an error. */
  #next(page: HistoryPage, empty: boolean): void {
    if (this.#closed) return;
    this.#cursor = page.nextCursor;
    if (empty) {
      const message = `${String(MAX_EMPTY_PAGES)} empty pages in a row`;
      this.#set({ error: { code: "empty_pages", message } });
      return;
    }
    this.#scrolled = true;
    this.#set({
      items: [...this.#state.items, ...page.items.map(item)],
      end: page.nextCursor === null,
      // A first page still failing is still stale.
      error: this.#topError,
    });
  }

  #set(changes: Partial<HistoryState>): void {
    if (this.#closed) return;
    this.#state = Object.freeze({ ...this.#state, ...changes });
    this.#notifying ??= setTimeout(() => {
      this.#notify();
    }, 0);
  }

  #notify(): void {
    this.#notifying = undefined;
    const state = this.#state;
    if (state === this.#notified) return;
    this.#notified = state;
    for (const listener of this.#listeners) call(listener, state);
  }
}

/** A subscription still loading, which settles with a balance or a failure. */
function settling(state: SubscriptionState): boolean {
  return state.phase === "snapshot" || state.phase === "catching_up";
}

function item(wire: WireHistoryItem): HistoryItem {
  const { txId, height, timestamp, fee } = wire;
  return Object.freeze({
    txId,
    ...(height === undefined ? {} : { height }),
    ...(timestamp === undefined ? {} : { timestamp }),
    delta: assets(wire.delta),
    ...(fee === undefined ? {} : { fee: BigInt(fee) }),
  });
}
