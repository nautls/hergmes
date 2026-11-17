import { RelayError } from "./error.ts";
import { KeyMachine, type Action, type Resync, type SubscriptionState } from "./machine.ts";
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
}

export interface SubscribeOptions {
  /**
   * Follow the key's boxes: its confirmed set, the mempool's overlay and what is spendable
   * now. `false` follows only an address's balance, which needs no snapshot. Token keys have
   * no balance, so they always follow boxes. Defaults to `true`.
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
   * block replaced with another spender still shows its outputs as spendable, and
   * `spendableTotals` differs from `balance`. The messages that follow correct it.
   */
  readonly state: SubscriptionState;
  /**
   * Calls `listener` with the latest state after changes, once for all the messages that
   * arrived together (on a `setTimeout(0)`), which hides most of those moments and spares a
   * render per message in a burst. A browser throttles timers in a hidden tab, so a call can
   * come up to about a second late there; `state` is always current. Answers a function that
   * removes the listener.
   */
  onChange(listener: (state: SubscriptionState) => void): () => void;
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
}

/**
 * A relay: its REST API, its tip, and subscriptions to keys over one WebSocket, opened on the
 * first subscribe. A key is subscribed once however many holds it has.
 */
export class Relay {
  readonly rest: RestClient;
  readonly #wsUrl: string;
  readonly #WebSocket: typeof WebSocket;
  readonly #pageSize: number;
  readonly #debug: RelayOptions["debug"];
  readonly #entries = new Map<string, Entry>();
  readonly #tipListeners = new Set<(tip: BlockRef) => void>();
  #socket: WebSocket | undefined;
  #queue: string[] = [];
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

  subscribe(key: Key, options: SubscribeOptions = {}): Subscription {
    const boxes = "tokenId" in key || options.boxes !== false;
    const name = keyName(key);
    let entry = this.#entries.get(name);
    if (!entry) {
      const machine = this.#machine(key, boxes);
      entry = { machine, holds: 0, listeners: new Set(), notified: machine.state };
      this.#entries.set(name, entry);
      const created = entry;
      this.#change(created, () => created.machine.start());
    } else if (restarts(entry.machine.state) || (boxes && !entry.machine.boxes)) {
      // A subscription that ended (its socket dropped, or the relay refused it) starts again;
      // a box subscription replaces a balance-only one, since it carries the balance too.
      const replaced = entry;
      const following = boxes || replaced.machine.boxes;
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
      onChange(listener) {
        held.listeners.add(listener);
        return () => held.listeners.delete(listener);
      },
      close: () => {
        if (!open) return;
        open = false;
        // The key's subscription may have ended already (the relay closed), and the key
        // been subscribed again since: this hold is not part of that one.
        if (this.#entries.get(name) !== held) return;
        if (--held.holds > 0) return;
        this.#entries.delete(name);
        this.#change(held, () => held.machine.stop());
      },
    };
  }

  /** Closes the socket and ends every subscription. A later subscribe opens a new socket. */
  close(): void {
    const socket = this.#socket;
    this.#socket = undefined;
    this.#queue = [];
    this.#tip = undefined;
    socket?.close(1000);
    for (const entry of this.#entries.values()) {
      this.#change(entry, () => entry.machine.disconnected());
    }
    this.#entries.clear();
  }

  #machine(key: Key, boxes: boolean, previous?: SubscriptionState): KeyMachine {
    return new KeyMachine(
      key,
      boxes,
      () => this.#ids++ % 2 ** 32,
      (resync) => {
        call(this.#debug, { resync });
      },
      previous,
    );
  }

  /** Does what a machine asked, then has the key's listeners called if its state changed. */
  #run(entry: Entry, actions: Action[], before: SubscriptionState): void {
    for (const action of actions) {
      if (action.kind === "send") this.#send(action.message);
      else void this.#snapshot(entry, entry.machine, action.generation);
    }
    if (entry.machine.state === before) return;
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

  async #snapshot(entry: Entry, machine: KeyMachine, generation: number): Promise<void> {
    let read: { asOf: BlockRef; boxes: ConfirmedBox[] };
    try {
      read = await this.#read(machine.key);
    } catch (e) {
      if (entry.machine !== machine) return;
      const error =
        e instanceof RelayError
          ? {
              code: e.code ?? `http_${String(e.status)}`,
              message: e.message,
              ...(e.retryAfter === undefined ? {} : { retryAfter: e.retryAfter }),
            }
          : { code: "network", message: String(e) };
      this.#change(entry, () => machine.snapshotFailed(generation, error));
      return;
    }
    if (entry.machine !== machine) return;
    this.#change(entry, () => machine.snapshotLoaded(generation, read.asOf, read.boxes));
  }

  /** Every page of a key's boxes, pinned to the first page's block. */
  async #read(key: Key): Promise<{ asOf: BlockRef; boxes: ConfirmedBox[] }> {
    const first = await this.rest.boxes(key, { limit: this.#pageSize });
    const boxes = [...first.items];
    let cursor = first.nextCursor;
    while (cursor !== null) {
      const page = await this.rest.boxes(key, { limit: this.#pageSize, cursor });
      boxes.push(...page.items);
      cursor = page.nextCursor;
    }
    return { asOf: first.asOf, boxes };
  }

  #send(message: ClientMessage): void {
    // With no socket there is no subscription to end: the relay ended it with the connection.
    if (message.op === "unsubscribe" && !this.#socket) return;
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
      if (typeof event.data === "string") this.#receive(event.data);
    });
    socket.addEventListener("close", () => {
      if (this.#socket !== socket) return;
      this.#socket = undefined;
      this.#queue = [];
      for (const entry of this.#entries.values()) {
        this.#change(entry, () => entry.machine.disconnected());
      }
    });
    return socket;
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

/** Whether a subscribe for the key starts its subscription again: it ended. */
function restarts(state: SubscriptionState): boolean {
  return state.phase === "disconnected" || state.phase === "rejected";
}

/**
 * Calls an app's function. What it throws is the app's: rethrown on a task of its own, where
 * it surfaces as any uncaught error does, so the SDK's own work and the other listeners carry
 * on.
 */
function call<T>(listener: ((value: T) => void) | undefined, value: T): void {
  try {
    listener?.(value);
  } catch (error) {
    setTimeout(() => {
      throw error;
    }, 0);
  }
}
