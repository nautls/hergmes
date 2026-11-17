/**
 * What the relay sends over REST, as JSON.
 *
 * Amounts are base-10 integer strings: ERG supply in nanoErgs and token amounts exceed 2^53,
 * which a JSON number does not carry exactly. Boxes are Fleet's `Box<string>`, so they go to
 * Fleet as they are.
 */

import type { BlockHeader, Box, TokenAmount } from "@fleet-sdk/common";

/** A key the relay tracks: an address (any encoding of its ErgoTree) or a token id. */
export type Key = { address: string } | { tokenId: string };

/** A block: what a page is pinned to, and what a subscription resumes from. */
export interface BlockRef {
  height: number;
  headerId: string;
}

/**
 * The height a one-off read is as of. It has no `headerId`, so it can't be mistaken for a
 * point to stream from: a balance or a view with the pool has the mempool applied.
 */
export interface AsOfHeight {
  height: number;
}

/** A box in a block, with the height of the block that included it. */
export type ConfirmedBox = Box<string> & { inclusionHeight: number };

/** A box with the pool applied: a mempool output has no `inclusionHeight`. */
export type PoolBox = Box<string> & { inclusionHeight?: number };

/** A page of a key's unspent boxes, pinned to `asOf`; `nextCursor` is `null` on the last. */
export interface BoxPage {
  asOf: BlockRef;
  items: ConfirmedBox[];
  nextCursor: string | null;
}

/**
 * A key's boxes with the mempool applied, confirmed ones first. `truncated` says the confirmed
 * part was cut to the route's limit; `total` is the whole view's size either way.
 */
export interface WithPool {
  asOf: AsOfHeight;
  items: PoolBox[];
  truncated: boolean;
  total: number;
}

/** ERG and tokens, tokens in id order. Signed when it is a change. */
export interface Assets {
  nanoErgs: string;
  tokens: TokenAmount<string>[];
}

/**
 * An address's balance: `confirmed` at the tip, and `pending`, the signed change the mempool
 * makes to it. `used` is whether the address ever held a box.
 */
export interface Balance {
  asOf: AsOfHeight;
  confirmed: Assets;
  pending: Assets;
  used: boolean;
}

/**
 * A tx in an address's history, with its signed net change to the address. A pending tx has
 * no `height` and no `timestamp`.
 */
export interface HistoryItem {
  txId: string;
  height?: number;
  timestamp?: number;
  delta: Assets;
}

/** A page of an address's txs, newest first, pending on top; `nextCursor` `null` on the last. */
export interface HistoryPage {
  items: HistoryItem[];
  nextCursor: string | null;
}

/** The last headers up to the tip, newest first, each the node's JSON; `asOf` is the tip. */
export interface HeadersPage {
  asOf: BlockRef;
  items: BlockHeader[];
}

/** The id of a tx the node took, sent or only checked. */
export interface Submitted {
  transactionId: string;
}

/**
 * The relay's state. `tip` is `null` until it has applied a block; `ready` is whether it is
 * at the node's tip and can load a key it does not track yet.
 */
export interface Status {
  version: string;
  network: "mainnet" | "testnet";
  tip: BlockRef | null;
  ready: boolean;
}
