/**
 * One key's subscription as a state machine, with no socket and no timer: it takes the
 * relay's messages and the snapshot's result, and answers what to send or fetch. A box
 * subscription goes snapshot → catching up → live, and back to a snapshot whenever its view
 * can't be trusted: the relay asks (`resync_required`), a block does not chain, or a message
 * names boxes the view does not hold as it says. It survives the socket: after a reconnect it
 * subscribes from the block its view is at, and a failure that is for now waits and starts
 * again.
 */

import { ensureUTxOBigInt, type Box } from "@fleet-sdk/common";
import { RelayError } from "./error.ts";
import type { ClientMessage, ServerMessage, WirePendingTx } from "./protocol.ts";
import type {
  Assets as WireAssets,
  BlockRef,
  ConfirmedBox as WireConfirmedBox,
  Key,
} from "./wire.ts";

/** A box in a block, amounts as `bigint`. */
export type ConfirmedBox = Box<bigint> & { inclusionHeight: number };

/** ERG and tokens. Signed when it is a change. */
export interface Assets {
  nanoErgs: bigint;
  tokens: ReadonlyMap<string, bigint>;
}

/** A pending tx as it affects the key: its outputs to the key, and the key's boxes it spends. */
export interface PendingTx {
  txId: string;
  created: readonly Box<bigint>[];
  spent: readonly string[];
}

/** An address's balance at `asOf`: confirmed, and the signed change the mempool makes to it. */
export interface Balance {
  asOf: BlockRef;
  confirmed: Assets;
  pending: Assets;
}

/** A key's boxes at block `at`, and the mempool's effect on them. */
export interface BoxView {
  at: BlockRef;
  /** In `(inclusionHeight, boxId)` order. */
  confirmed: readonly ConfirmedBox[];
  /** The txs the overlay holds, parents before children. */
  pending: readonly PendingTx[];
  /**
   * What a tx can spend now: the confirmed boxes and pending outputs no pending tx spends,
   * confirmed first. For an address its total is the balance's `confirmed + pending`.
   */
  spendable: readonly Box<bigint>[];
  spendableTotals: Assets;
}

/**
 * - `snapshot`: reading the key's boxes; `catching_up`: subscribed, replaying to the tip;
 *   `live`: following the chain.
 * - `waiting`: the relay refused the key for now, or the snapshot failed for now (a network
 *   error, a 5xx); `error` says why, and its `retryAfter` when it starts again.
 * - `rejected`: the relay refused the key, or the snapshot failed, for good; `error` says why.
 *   A later subscribe to the key tries again.
 * - `disconnected`: the socket closed; the subscription resumes once it opens again, unless
 *   the hold or the relay was closed.
 */
export type Phase = "snapshot" | "catching_up" | "live" | "waiting" | "rejected" | "disconnected";

export interface SubscriptionError {
  code: string;
  message: string;
  /** The HTTP status, when a snapshot read failed with one. */
  status?: number;
  /** Seconds until the key starts again, when the failure is for now. */
  retryAfter?: number;
}

/**
 * A subscription's state, replaced whole on every change. `view` and `balance` keep their
 * last live values while the subscription recovers; `phase` says whether they are current.
 */
export interface SubscriptionState {
  key: Key;
  phase: Phase;
  /** A box subscription's view, once it was live. */
  view: BoxView | undefined;
  /** An address's latest balance. */
  balance: Balance | undefined;
  error: SubscriptionError | undefined;
}

/** What the machine asks of its driver. */
export type Action =
  | { kind: "send"; message: ClientMessage }
  /** Read a snapshot and hand it to {@link KeyMachine.snapshotLoaded} with `generation`. */
  | { kind: "snapshot"; generation: number }
  /** Call {@link KeyMachine.retry} with `generation` after `ms`. */
  | { kind: "retry"; generation: number; ms: number };

/** What a machine needs of its driver. */
export interface MachineContext {
  /** A subscribe's id, unique on the socket. */
  nextId(): number;
  /** Told of every new snapshot taken, and why. */
  onResync(resync: Resync): void;
  /** How long to wait before the `attempt`-th retry (from 0) of a failure with no `retryAfter`. */
  backoff(attempt: number): number;
}

/**
 * Codes a refusal or a failed read carries when it is for now: no answer, an answer that is not
 * the relay's (a proxy's page), and over the socket, which has no status, the 5xx codes. A 5xx
 * status with any code is for now too.
 */
const TRANSIENT = new Set([
  "network",
  "bad_response",
  "node_error",
  "internal_error",
  "node_unavailable",
  "not_ready",
]);

/** Statuses below 500 that are for now: a timeout and a rate limit, as a proxy may answer. */
const TRANSIENT_STATUS = new Set([408, 429]);

/** Whether a failure is for now: it has a `retryAfter`, or it is a network error or a 5xx. */
export function transient(error: SubscriptionError): boolean {
  return (
    error.retryAfter !== undefined ||
    (error.status ?? 0) >= 500 ||
    (error.status !== undefined && TRANSIENT_STATUS.has(error.status)) ||
    (error.status === undefined && TRANSIENT.has(error.code))
  );
}

/** A failed REST read as a {@link SubscriptionError}. */
export function failure(e: unknown): SubscriptionError {
  if (e instanceof RelayError) {
    return {
      code: e.code ?? `http_${String(e.status)}`,
      message: e.message,
      status: e.status,
      ...(e.retryAfter === undefined ? {} : { retryAfter: e.retryAfter }),
    };
  }
  // `fetch` fails with a `TypeError` when the request never got an answer.
  return { code: e instanceof TypeError ? "network" : "bad_response", message: String(e) };
}

/**
 * Why the machine took a new snapshot, for diagnostics: the relay asked (`resync_required`, or
 * an expired cursor), or the SDK found a message its view can't take, which only a lost or
 * corrupted message, or a bug, causes.
 */
export interface Resync {
  key: Key;
  by: "relay" | "sdk";
  reason: string;
}

interface Working {
  at: BlockRef;
  boxes: Map<string, ConfirmedBox>;
  pending: PendingTx[];
  /**
   * `boxes` in keyset order, and what is spendable now, as last published; `undefined` once
   * stale. The relay sends a block to every key on every block, most of them touching
   * nothing, and those reuse both rather than sorting and copying every box again.
   */
  confirmed: ConfirmedBox[] | undefined;
  spendable: { boxes: Box<bigint>[]; totals: Assets } | undefined;
}

export class KeyMachine {
  readonly key: Key;
  /** Whether the subscription follows boxes; otherwise only an address's balance. */
  readonly boxes: boolean;
  #state: SubscriptionState;
  readonly #context: MachineContext;
  /** Whether a subscribe can be sent now; otherwise it waits for {@link connected}. */
  #online: boolean;
  /** The id of the subscribe whose answer is awaited; the key's other messages are dropped. */
  #awaiting: number | undefined;
  /** Bumped whenever a snapshot read or a retry in flight stops being the current one. */
  #generation = 0;
  #working: Working | undefined;
  /** Retries since the key was last live. */
  #attempt = 0;
  /** Ended for good: nothing resumes it. */
  #ended = false;

  /**
   * `online` says whether the socket is open or opening. `previous` is the state of the
   * subscription this one replaces, if any: its last view and balance stay visible until this
   * one has its own, as while recovering.
   */
  constructor(
    key: Key,
    boxes: boolean,
    context: MachineContext,
    online = true,
    previous?: SubscriptionState,
  ) {
    this.key = key;
    this.boxes = boxes;
    this.#context = context;
    this.#online = online;
    const phase = boxes ? "snapshot" : "catching_up";
    const view = boxes ? previous?.view : undefined;
    const balance = previous?.balance;
    this.#state = Object.freeze({ key, phase, view, balance, error: undefined });
  }

  get state(): SubscriptionState {
    return this.#state;
  }

  /** The first actions: a snapshot, or a balance-only subscribe. */
  start(): Action[] {
    return this.boxes ? this.#snapshot() : this.#subscribe(undefined);
  }

  /** A retry's wait ended. Stale generations are ignored. */
  retry(generation: number): Action[] {
    if (generation !== this.#generation || this.#state.phase !== "waiting") return [];
    return this.start();
  }

  /** The snapshot read: its block and every box at it. Stale generations are ignored. */
  snapshotLoaded(generation: number, asOf: BlockRef, boxes: readonly WireConfirmedBox[]): Action[] {
    if (generation !== this.#generation || this.#state.phase !== "snapshot") return [];
    this.#working = {
      at: asOf,
      boxes: new Map(boxes.map((box) => [box.boxId, ensureUTxOBigInt(box) as ConfirmedBox])),
      pending: [],
      confirmed: undefined,
      spendable: undefined,
    };
    return this.#subscribe(asOf);
  }

  /**
   * The snapshot failed. An expired cursor starts it again; a failure for now waits and starts
   * again; anything else is a refusal.
   */
  snapshotFailed(generation: number, error: SubscriptionError): Action[] {
    if (generation !== this.#generation || this.#state.phase !== "snapshot") return [];
    if (error.code === "cursor_expired") {
      this.#context.onResync({ key: this.key, by: "relay", reason: "cursor_expired" });
      return this.#snapshot();
    }
    return this.#fail(error);
  }

  /** A message for this key. */
  receive(message: ServerMessage): Action[] {
    const phase = this.#state.phase;
    if (phase !== "catching_up" && phase !== "live") return [];
    if (this.#awaiting !== undefined) {
      const answer =
        message.type === "subscribed" ||
        message.type === "rejected" ||
        message.type === "resync_required";
      if (!answer || message.id !== this.#awaiting) return [];
      this.#awaiting = undefined;
    }
    switch (message.type) {
      case "subscribed":
        return [];
      case "rejected": {
        const error: SubscriptionError = { code: message.reason, message: message.reason };
        if (message.retryAfter !== undefined) error.retryAfter = message.retryAfter;
        return this.#fail(error);
      }
      case "resync_required":
        return this.#resync(message.reason, "relay");
      case "block":
        return this.#block(message);
      case "rollback":
        return this.#rollback(message);
      case "mempool":
        return this.#mempool(message.tipHeight, message.pending);
      case "balance":
        return this.#balance(message);
      case "tip":
        return [];
    }
    // A message type of a newer relay's.
    return [];
  }

  /**
   * The socket closed: nothing more comes for the subscription it carried. The working view
   * stays, to resume from; a snapshot read carries on, and a retry still waits.
   */
  disconnected(): Action[] {
    this.#online = false;
    this.#awaiting = undefined;
    const phase = this.#state.phase;
    if (phase === "catching_up" || phase === "live") this.#set({ phase: "disconnected" });
    return [];
  }

  /**
   * A socket is open or opening again: a subscription the cut ended subscribes from the block
   * its view is at, or, balance-only, subscribes again.
   */
  connected(): Action[] {
    this.#online = true;
    if (this.#ended || this.#state.phase !== "disconnected") return [];
    if (!this.boxes) return this.#subscribe(undefined);
    const w = this.#working;
    return w ? this.#subscribe(w.at) : this.#snapshot();
  }

  /** The subscription is over (the relay closed): nothing more is read, sent or retried. */
  end(): Action[] {
    this.#ended = true;
    this.#online = false;
    this.#awaiting = undefined;
    this.#working = undefined;
    this.#generation++;
    this.#set({ phase: "disconnected" });
    return [];
  }

  /** The app is done with the key. */
  stop(): Action[] {
    return [...this.end(), { kind: "send", message: { op: "unsubscribe", key: this.key } }];
  }

  #set(changes: Partial<SubscriptionState>): void {
    this.#state = Object.freeze({ ...this.#state, ...changes });
  }

  #snapshot(): Action[] {
    this.#awaiting = undefined;
    this.#working = undefined;
    this.#generation++;
    this.#set({ phase: "snapshot", error: undefined });
    return [{ kind: "snapshot", generation: this.#generation }];
  }

  /** Subscribes, or waits for the socket as `disconnected` when it is not open. */
  #subscribe(from: BlockRef | undefined): Action[] {
    this.#awaiting = undefined;
    if (!this.#online) {
      this.#set({ phase: "disconnected", error: undefined });
      return [];
    }
    const id = this.#context.nextId();
    this.#awaiting = id;
    this.#set({ phase: "catching_up", error: undefined });
    const message: ClientMessage = from
      ? { op: "subscribe", key: this.key, from, id }
      : { op: "subscribe", key: this.key, id };
    return [{ kind: "send", message }];
  }

  /**
   * A refusal or a failed snapshot read. With a `retryAfter`, or when it is for now (a network
   * error, a 5xx), the key waits and starts again; otherwise it is rejected for good.
   */
  #fail(error: SubscriptionError): Action[] {
    this.#awaiting = undefined;
    this.#working = undefined;
    this.#generation++;
    if (!transient(error)) {
      this.#set({ phase: "rejected", error });
      return [];
    }
    const ms =
      error.retryAfter === undefined
        ? this.#context.backoff(this.#attempt++)
        : error.retryAfter * 1000;
    this.#set({ phase: "waiting", error: { ...error, retryAfter: ms / 1000 } });
    return [{ kind: "retry", generation: this.#generation, ms }];
  }

  /** A new snapshot for a box subscription; a balance-only one just subscribes again. */
  #resync(reason: string, by: Resync["by"] = "sdk"): Action[] {
    this.#context.onResync({ key: this.key, by, reason });
    return this.boxes ? this.#snapshot() : this.#subscribe(undefined);
  }

  #block(m: Extract<ServerMessage, { type: "block" }>): Action[] {
    const w = this.#working;
    if (!w) return this.#resync("block_without_view");
    if (m.parentId !== w.at.headerId || m.height !== w.at.height + 1) {
      return this.#resync("block_does_not_chain");
    }
    if (!m.spent.every((s) => w.boxes.has(s.boxId))) return this.#resync("spends_unknown_box");
    const created = m.created.every((b) => !w.boxes.has(b.boxId) && b.inclusionHeight === m.height);
    if (!created) return this.#resync("creates_held_box");
    if (m.spent.length > 0 || m.created.length > 0) {
      const spent = new Set(m.spent.map((s) => s.boxId));
      for (const id of spent) w.boxes.delete(id);
      const created = m.created.map((b) => ensureUTxOBigInt(b) as ConfirmedBox);
      for (const b of created) w.boxes.set(b.boxId, b);
      // A block's boxes are the highest, so they go after the rest, in id order.
      w.confirmed = w.confirmed && [
        ...w.confirmed.filter((b) => !spent.has(b.boxId)),
        ...created.sort(keysetOrder),
      ];
      w.spendable = undefined;
    }
    w.at = { height: m.height, headerId: m.headerId };
    this.#publish();
    return [];
  }

  #rollback(m: Extract<ServerMessage, { type: "rollback" }>): Action[] {
    const w = this.#working;
    if (!w) return this.#resync("rollback_without_view");
    if (m.headerId !== w.at.headerId || m.height !== w.at.height) {
      return this.#resync("rollback_of_another_block");
    }
    if (!m.spent.every((s) => w.boxes.has(s.boxId))) return this.#resync("undoes_unknown_box");
    const restored = m.created.every((b) => !w.boxes.has(b.boxId) && b.inclusionHeight < m.height);
    if (!restored) return this.#resync("restores_held_box");
    if (m.spent.length > 0 || m.created.length > 0) {
      for (const s of m.spent) w.boxes.delete(s.boxId);
      for (const b of m.created) w.boxes.set(b.boxId, ensureUTxOBigInt(b) as ConfirmedBox);
      // Restored boxes are older ones, anywhere in the order.
      w.confirmed = undefined;
      w.spendable = undefined;
    }
    w.at = { height: m.height - 1, headerId: m.parentId };
    this.#publish();
    return [];
  }

  #mempool(tipHeight: number, pending: readonly WirePendingTx[]): Action[] {
    const w = this.#working;
    if (!w) return this.#resync("mempool_without_view");
    if (tipHeight !== w.at.height) return this.#resync("mempool_at_another_height");
    w.pending = pending.map((tx) => ({
      txId: tx.txId,
      created: tx.created.map((b) => ensureUTxOBigInt(b)),
      spent: tx.spent,
    }));
    w.spendable = undefined;
    // The catch-up ends with the key's mempool, even an empty one.
    this.#publish("live");
    return [];
  }

  #balance(m: Extract<ServerMessage, { type: "balance" }>): Action[] {
    const balance = { asOf: m.asOf, confirmed: assets(m.confirmed), pending: assets(m.pending) };
    if (this.boxes) {
      // Sent after the block and the overlay it is computed with.
      const at = this.#working?.at;
      if (at?.headerId !== m.asOf.headerId) return this.#resync("balance_at_another_block");
      this.#set({ balance });
      return [];
    }
    // A balance-only subscription's catch-up is its one balance.
    this.#attempt = 0;
    this.#set({ balance, phase: "live" });
    return [];
  }

  /** Publishes the working view, once the catch-up is done or as `phase` makes it. */
  #publish(phase?: Phase): void {
    const w = this.#working;
    if (!w) return;
    const next = phase ?? this.#state.phase;
    if (next !== "live") return;
    this.#attempt = 0;
    const confirmed = (w.confirmed ??= [...w.boxes.values()].sort(keysetOrder));
    w.spendable ??= spendableNow(confirmed, w.boxes, w.pending);
    const view: BoxView = {
      at: w.at,
      confirmed,
      pending: w.pending,
      spendable: w.spendable.boxes,
      spendableTotals: w.spendable.totals,
    };
    this.#set({ phase: next, view: Object.freeze(view) });
  }
}

/**
 * The confirmed boxes and pending outputs no pending tx spends, confirmed first. A pending
 * output already confirmed (a block mined its tx ahead of the next overlay) is listed once.
 */
function spendableNow(
  confirmed: readonly ConfirmedBox[],
  held: ReadonlyMap<string, ConfirmedBox>,
  pending: readonly PendingTx[],
): { boxes: Box<bigint>[]; totals: Assets } {
  const spent = new Set(pending.flatMap((tx) => tx.spent));
  const boxes: Box<bigint>[] = confirmed.filter((b) => !spent.has(b.boxId));
  for (const tx of pending) {
    for (const b of tx.created) {
      if (!spent.has(b.boxId) && !held.has(b.boxId)) boxes.push(b);
    }
  }
  return { boxes, totals: totals(boxes) };
}

function keysetOrder(a: ConfirmedBox, b: ConfirmedBox): number {
  if (a.inclusionHeight !== b.inclusionHeight) return a.inclusionHeight - b.inclusionHeight;
  return a.boxId < b.boxId ? -1 : a.boxId > b.boxId ? 1 : 0;
}

/** Wire amounts, as `bigint`. */
export function assets(wire: WireAssets): Assets {
  return {
    nanoErgs: BigInt(wire.nanoErgs),
    tokens: new Map(wire.tokens.map((t) => [t.tokenId, BigInt(t.amount)])),
  };
}

/** The ERG and tokens in `boxes`, tokens in id order. */
export function totals(boxes: readonly Box<bigint>[]): Assets {
  let nanoErgs = 0n;
  const tokens = new Map<string, bigint>();
  for (const box of boxes) {
    nanoErgs += box.value;
    for (const t of box.assets) tokens.set(t.tokenId, (tokens.get(t.tokenId) ?? 0n) + t.amount);
  }
  const sorted = new Map([...tokens].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  return { nanoErgs, tokens: sorted };
}
