import { call, store } from "./call.ts";
import { HistoryFeed, type HistoryOptions, type TxHistory } from "./history.ts";
import {
  KeyMachine,
  failure,
  type Action,
  type MachineContext,
  type Resync,
  type SubscriptionState,
} from "./machine.ts";
import { keyName, type ClientMessage, type ServerMessage } from "./protocol.ts";
import { RestClient, type RestOptions } from "./rest.ts";
import type { BlockRef, ConfirmedBox, Key } from "./wire.ts";

export interface RelayOptions extends RestOptions {
  /** Boxes per snapshot page, at most 1000, the relay's limit and one node call. */
  pageSize?: number;
  /** The `WebSocket` to use; the global one by default. */
  WebSocket?: typeof WebSocket;
  /** Sees every message the relay sends, as text, and every new snapshot taken. For logging. */
  debug?: (event: { frame: string } | { resync: Resync }) => void;
  /**
   * The waits before reconnecting the socket and before retrying a key that failed for now
   * with no `retryAfter`: each drawn from 0 to a bound that starts at `firstMs` and doubles,
   * up to `maxMs`, so clients a relay restart cut off together don't come back together, at
   * the first try or any later one. Both start over once a key is live again. Defaults to 1 s
   * and 30 s.
   */
  backoff?: { firstMs?: number; maxMs?: number };
}

/** How to follow an address. Token and template keys have no balance, so they always follow boxes. */
export interface SubscribeOptions {
  /**
   * Follow the key's boxes: its confirmed set, the mempool's overlay and what is spendable
   * now. `false` follows only the balance, which needs no snapshot. Defaults to `true`.
   */
  boxes?: boolean;
}

/** One app's hold on a key. Several on one key share its state. */
export interface Subscription {
  readonly key: Key;
  /**
   * The key's current state, replaced whole on every change, as each message arrives.
   *
   * The relay sends a block's effect on a key as separate messages: the `block`, then the
   * `mempool` and the `balance` if they changed, and nothing marks the last. So for a moment a
   * live state can pair a new block with the previous overlay and balance: a pending tx the
   * block replaced with another spender still shows its outputs as spendable, and `spendable`
   * disagrees with `balance`. The messages that follow correct it.
   */
  readonly state: SubscriptionState;
  /**
   * Calls `listener` with the current state at once, then with the latest state after changes,
   * once for all the messages that arrived together (on a `setTimeout(0)`), never twice with
   * the same state. Batching hides most of those moments and spares a render per message in a
   * burst. A browser throttles timers in a hidden tab, so a call can come up to about a second
   * late there; `state` is always current. Answers a function that removes the listener. The
   * contract of a Svelte store, so the handle is one (`$sub`).
   */
  subscribe(listener: (state: SubscriptionState) => void): () => void;
  /** Ends this hold; the key is unsubscribed once no hold is left. */
  close(): void;
}

const PAGE_SIZE = 1000;

interface Entry {
  machine: KeyMachine;
  holds: number;
  listeners: Set<(state: SubscriptionState) => void>;
  /** The state the listeners were last called with. */
  notified: SubscriptionState;
  /** The machine's pending retry. */
  retry: ReturnType<typeof setTimeout> | undefined;
  /** Cancels the machine's snapshot read in flight. */
  reading: AbortController | undefined;
}

/**
 * A relay: its REST API, its tip, and subscriptions to keys over one WebSocket, opened on the
 * first subscribe. A key is subscribed once however many holds it has. When the socket closes,
 * for whatever reason, it opens again after a backoff while any key is held, and each key
 * resumes from the block it had reached.
 */
export class Relay {
  readonly rest: RestClient;
  readonly #wsUrl: string;
  readonly #WebSocket: typeof WebSocket;
  readonly #pageSize: number;
  readonly #debug: RelayOptions["debug"];
  readonly #entries = new Map<string, Entry>();
  readonly #histories = new Set<HistoryFeed>();
  readonly #tipListeners = new Set<(tip: BlockRef) => void>();
  readonly #backoff: (attempt: number) => number;
  readonly #context: MachineContext;
  #socket: WebSocket | undefined;
  #queue: string[] = [];
  /** The wait before the socket opens again; no socket is opened meanwhile. */
  #reconnect: ReturnType<typeof setTimeout> | undefined;
  /** Reconnects since a key last went live. */
  #attempt = 0;
  #tip: BlockRef | undefined;
  #ids = 0;
  /** Keys whose state changed since their listeners were last called. */
  readonly #changed = new Set<Entry>();
  #notifying: ReturnType<typeof setTimeout> | undefined;

  /** `url` is the relay's root, under which `/v1` is served; it may have a path. */
  constructor(url: string | URL, options: RelayOptions = {}) {
    this.rest = new RestClient(url, options);
    const ws = new URL("v1/ws", this.rest.root);
    ws.protocol = ws.protocol === "https:" ? "wss:" : "ws:";
    this.#wsUrl = ws.href;
    this.#WebSocket = options.WebSocket ?? globalThis.WebSocket;
    this.#pageSize = Math.min(PAGE_SIZE, Math.max(1, options.pageSize ?? PAGE_SIZE));
    this.#debug = options.debug;
    this.#backoff = backoff(options.backoff?.firstMs ?? 1000, options.backoff?.maxMs ?? 30_000);
    this.#context = {
      nextId: () => this.#ids++ % 2 ** 32,
      onResync: (resync) => {
        call(this.#debug, { resync });
      },
      backoff: this.#backoff,
    };
  }

  /** The relay's tip, the block every box and balance it serves is at, once known. */
  get tip(): BlockRef | undefined {
    return this.#tip;
  }

  /** Calls `listener` on every new tip; answers a function that removes it. */
  onTip(listener: (tip: BlockRef) => void): () => void {
    this.#tipListeners.add(listener);
    return () => this.#tipListeners.delete(listener);
  }

  /** Holds `key`. Only an address has a choice in what to follow, so only it takes options. */
  subscribe(key: { address: string }, options?: SubscribeOptions): Subscription;
  subscribe(key: Key): Subscription;
  subscribe(key: Key, options: SubscribeOptions = {}): Subscription {
    const boxes = !("address" in key) || options.boxes !== false;
    const name = keyName(key);
    let entry = this.#entries.get(name);

    if (!entry) {
      const machine = this.#machine(key, boxes);
      entry = {
        machine,
        holds: 0,
        listeners: new Set(),
        notified: machine.state,
        retry: undefined,
        reading: undefined,
      };
      this.#entries.set(name, entry);
      const created = entry;
      this.#change(created, () => created.machine.start());
    } else if (entry.machine.state.phase === "rejected" || (boxes && !entry.machine.boxes)) {
      // A subscription the relay refused for good starts again; a box subscription replaces a
      // balance-only one, since it carries the balance too. A dropped socket needs nothing: the
      // reconnect resumes the key.
      const replaced = entry;
      const following = boxes || replaced.machine.boxes;
      forget(replaced);
      this.#change(replaced, () => {
        replaced.machine = this.#machine(key, following, replaced.machine.state);
        return replaced.machine.start();
      });
    }

    entry.holds++;
    const held = entry;
    let open = true;

    return {
      key,
      get state() {
        return held.machine.state;
      },
      subscribe(listener) {
        return store(held.machine.state, held.listeners, listener);
      },
      close: () => {
        if (!open) return;
        open = false;
        // The key's subscription may have ended already (the relay closed), and the key
        // been subscribed again since: this hold is not part of that one.
        if (this.#entries.get(name) !== held) return;
        if (--held.holds > 0) return;
        this.#entries.delete(name);
        forget(held);
        this.#change(held, () => held.machine.stop());
        if (this.#entries.size === 0) this.#stopReconnecting();
      },
    };
  }

  /**
   * An address's tx history, newest first, pending txs on top: its first page is read now,
   * and older pages as the app asks. See {@link HistoryOptions.refresh} to keep the first
   * page current.
   */
  history(address: string, options: HistoryOptions = {}): TxHistory {
    const history = new HistoryFeed(address, options.limit, {
      rest: this.rest,
      backoff: this.#backoff,
      hold: options.refresh ? this.subscribe({ address }, { boxes: false }) : undefined,
      closed: (h) => this.#histories.delete(h),
    });
    this.#histories.add(history);
    return history;
  }

  /**
   * Closes the socket, ends every subscription and stops every history. A later subscribe
   * opens a new socket.
   */
  close(): void {
    const socket = this.#socket;
    this.#socket = undefined;
    this.#queue = [];
    this.#tip = undefined;
    this.#stopReconnecting();
    socket?.close(1000);
    for (const history of this.#histories) history.close();
    for (const entry of this.#entries.values()) {
      forget(entry);
      this.#change(entry, () => entry.machine.end());
    }
    this.#entries.clear();
  }

  #machine(key: Key, boxes: boolean, previous?: SubscriptionState): KeyMachine {
    return new KeyMachine(key, boxes, this.#context, this.#online(), previous);
  }

  /** Whether a subscribe can go out now: the socket is open or opening, or may be opened. */
  #online(): boolean {
    return this.#socket !== undefined || this.#reconnect === undefined;
  }

  #stopReconnecting(): void {
    clearTimeout(this.#reconnect);
    this.#reconnect = undefined;
    this.#attempt = 0;
  }

  /** Does what a machine asked, then has the key's listeners called if its state changed. */
  #run(entry: Entry, actions: Action[], before: SubscriptionState): void {
    for (const action of actions) {
      if (action.kind === "send") this.#send(action.message);
      else if (action.kind === "snapshot")
        void this.#snapshot(entry, entry.machine, action.generation);
      else this.#retry(entry, entry.machine, action.generation, action.ms);
    }
    if (entry.machine.state === before) return;
    // A key live again: the socket carried a whole subscribe, so the relay is back for good.
    if (entry.machine.state.phase === "live" && before.phase !== "live") this.#attempt = 0;
    this.#changed.add(entry);
    if (this.#notifying === undefined) {
      this.#notifying = setTimeout(() => {
        this.#notify();
      }, 0);
    }
  }

  /** Calls the listeners of every key that changed, once each, with its latest state. */
  #notify(): void {
    this.#notifying = undefined;
    const changed = [...this.#changed];
    this.#changed.clear();
    for (const entry of changed) {
      const state = entry.machine.state;
      if (state === entry.notified) continue;
      entry.notified = state;
      for (const listener of entry.listeners) call(listener, state);
    }
  }

  #change(entry: Entry, step: () => Action[]): void {
    const before = entry.machine.state;
    this.#run(entry, step(), before);
  }

  #retry(entry: Entry, machine: KeyMachine, generation: number, ms: number): void {
    clearTimeout(entry.retry);
    entry.retry = setTimeout(() => {
      entry.retry = undefined;
      if (entry.machine !== machine) return;
      this.#change(entry, () => machine.retry(generation));
    }, ms);
  }

  async #snapshot(entry: Entry, machine: KeyMachine, generation: number): Promise<void> {
    // A newer read replaces this machine's last one.
    entry.reading?.abort();
    const reading = new AbortController();
    entry.reading = reading;
    let read: { asOf: BlockRef; boxes: ConfirmedBox[] };
    try {
      read = await this.#read(machine.key, reading.signal);
    } catch (e) {
      if (entry.machine !== machine || reading.signal.aborted) return;
      this.#change(entry, () => machine.snapshotFailed(generation, failure(e)));
      return;
    }
    if (entry.machine !== machine) return;
    this.#change(entry, () => machine.snapshotLoaded(generation, read.asOf, read.boxes));
  }

  /** Every page of a key's boxes, pinned to the first page's block. */
  async #read(key: Key, signal: AbortSignal): Promise<{ asOf: BlockRef; boxes: ConfirmedBox[] }> {
    const first = await this.rest.boxes(key, { limit: this.#pageSize, signal });
    const boxes = [...first.items];
    let cursor = first.nextCursor;
    while (cursor !== null) {
      const page = await this.rest.boxes(key, { limit: this.#pageSize, cursor, signal });
      boxes.push(...page.items);
      cursor = page.nextCursor;
    }
    return { asOf: first.asOf, boxes };
  }

  #send(message: ClientMessage): void {
    // With no socket there is no subscription to end: the relay ended it with the connection.
    if (message.op === "unsubscribe" && !this.#socket) return;
    // Waiting to reconnect: machines send nothing then, and subscribe again once it opens.
    if (!this.#online()) return;
    const text = JSON.stringify(message);
    const socket = this.#connect();
    if (socket.readyState === this.#WebSocket.OPEN) socket.send(text);
    else this.#queue.push(text);
  }

  #connect(): WebSocket {
    if (this.#socket) return this.#socket;
    const socket = new this.#WebSocket(this.#wsUrl);
    this.#socket = socket;
    socket.addEventListener("open", () => {
      for (const text of this.#queue) socket.send(text);
      this.#queue = [];
    });
    socket.addEventListener("message", (event: MessageEvent) => {
      // A frame of a socket already given up carries nothing the keys still wait for.
      if (this.#socket !== socket) return;
      if (typeof event.data === "string") this.#receive(event.data);
    });
    socket.addEventListener("close", () => {
      if (this.#socket !== socket) return;
      this.#socket = undefined;
      this.#queue = [];
      if (this.#entries.size > 0) {
        this.#reconnect = setTimeout(() => {
          this.#reconnected();
        }, this.#backoff(this.#attempt++));
      }
      for (const entry of this.#entries.values()) {
        this.#change(entry, () => entry.machine.disconnected());
      }
    });
    return socket;
  }

  /**
   * The backoff ended: opens a socket, and once it is open every key the cut ended subscribes
   * again on it. Until then they stay `disconnected`, as they do if it fails to open.
   */
  #reconnected(): void {
    this.#reconnect = undefined;
    if (this.#entries.size === 0) return;
    const socket = this.#connect();
    socket.addEventListener("open", () => {
      if (this.#socket !== socket) return;
      for (const entry of this.#entries.values()) {
        this.#change(entry, () => entry.machine.connected());
      }
    });
  }

  #receive(text: string): void {
    call(this.#debug, { frame: text });
    let message: ServerMessage | null;
    try {
      message = JSON.parse(text) as ServerMessage | null;
    } catch {
      return;
    }
    // A message this SDK does not know, of a newer relay's or a proxy's, is ignored.
    if (typeof message !== "object" || message === null) return;
    if (message.type === "tip") {
      const tip = { height: message.height, headerId: message.headerId };
      this.#tip = tip;
      for (const listener of this.#tipListeners) call(listener, tip);
      return;
    }
    if (typeof message.key !== "object") return;
    const entry = this.#entries.get(keyName(message.key));
    if (entry) this.#change(entry, () => entry.machine.receive(message));
  }
}

/** The wait before the `attempt`-th retry, from 0: see {@link RelayOptions.backoff}. */
function backoff(firstMs: number, maxMs: number): (attempt: number) => number {
  return (attempt) => Math.random() * Math.min(maxMs, firstMs * 2 ** attempt);
}

/** Cancels what a key's machine still has under way: its retry's wait and its snapshot read. */
function forget(entry: Entry): void {
  clearTimeout(entry.retry);
  entry.reading?.abort();
}
