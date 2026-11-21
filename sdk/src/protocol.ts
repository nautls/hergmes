/**
 * The WebSocket's messages at `/v1/ws`, as JSON. Amounts are strings, as over REST.
 */

import type { Assets, BlockRef, ConfirmedBox, Key, PoolBox } from "./wire.ts";

/** What a client sends. A subscribe with `from` follows the key's boxes; without, its balance. */
export type ClientMessage =
  { op: "subscribe"; key: Key; from?: BlockRef; id?: number } | { op: "unsubscribe"; key: Key };

/** A box leaving a key's set, and the tx that spent it. */
export interface Spend {
  boxId: string;
  txId: string;
}

/** A pending tx as it affects one key: its outputs to the key, and the key's boxes it spends. */
export interface WirePendingTx {
  txId: string;
  created: PoolBox[];
  spent: string[];
}

/** Why the relay asks a client to take a new snapshot. A relay newer than this SDK may send others. */
export type ResyncReason =
  | "outside_window"
  | "fork_beyond_window"
  | "not_tracked"
  | "slow_consumer"
  | "balance_mismatch"
  | (string & {});

/**
 * A block applied, or an orphaned block undone: `created` and `spent` are the key's boxes it
 * adds and removes (for a rollback, the block's own sides swapped).
 */
export interface Delta<T extends "block" | "rollback"> {
  type: T;
  key: Key;
  height: number;
  headerId: string;
  parentId: string;
  created: ConfirmedBox[];
  spent: Spend[];
}

/** What the relay sends. */
export type ServerMessage =
  | { type: "tip"; height: number; headerId: string }
  | { type: "subscribed"; id?: number; key: Key }
  | Delta<"block">
  | Delta<"rollback">
  | { type: "mempool"; key: Key; tipHeight: number; pending: WirePendingTx[] }
  | { type: "balance"; key: Key; asOf: BlockRef; confirmed: Assets; pending: Assets }
  | { type: "resync_required"; id?: number; key: Key; reason: ResyncReason }
  | { type: "rejected"; id?: number; key: Key; reason: string; retryAfter?: number };

/** A key's name for routing: its kind and the spelling sent, which the relay echoes. */
export function keyName(key: Key): string {
  return "address" in key ? `a:${key.address}` : `t:${key.tokenId}`;
}
