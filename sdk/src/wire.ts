/**
 * What the relay sends over REST, as JSON.
 *
 * Amounts are base-10 integer strings: ERG supply in nanoErgs and token amounts exceed 2^53,
 * which a JSON number does not carry exactly. Boxes are Fleet's `Box<string>`, so they go to
 * Fleet as they are.
 */

import type { BlockHeader, Box, DataInput, ProverResult, TokenAmount } from "@fleet-sdk/common";

/**
 * A key the relay tracks: an address (any encoding of its ErgoTree), a token id, or a template
 * in hex: a constant-segregated ErgoTree's bytes after its constants, naming every instance of
 * a contract whatever its constants.
 */
export type Key = { address: string } | { tokenId: string } | { template: string };

/** A block: what a page is pinned to, and what a subscription resumes from. */
export interface BlockRef {
  height: number;
  headerId: string;
}

/**
 * The height a one-off read is as of. It has no `headerId`, so it can't be mistaken for a
 * point to stream from: a balance or a spendable view has the mempool applied.
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
export interface Spendable {
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
 * no `height` and no `timestamp`. `fee` is what the tx pays the miners' fee contract, absent
 * if it has no output to it.
 */
export interface HistoryItem {
  txId: string;
  height?: number;
  timestamp?: number;
  delta: Assets;
  fee?: string;
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

/**
 * A token's metadata, by the standard its mint follows. A standard's fields are under
 * `metadata`, so another standard adds a variant and changes none; narrow on `standard`.
 */
export type TokenMetadata = TokenBase &
  ({ standard: "none" } | { standard: "eip-4"; metadata: Eip4Metadata });

/** The standard a token's mint follows. */
export type TokenStandard = TokenMetadata["standard"];

/** What every token has, whatever its standard. */
export interface TokenBase {
  tokenId: string;
  /** The tx that minted the token. */
  mintingTxId: string;
  /** Every unit minted. */
  emission: string;
}

/**
 * What an EIP-4 mint says of its token: a token is `eip-4` if its minting box names it,
 * describes it, gives its decimals (R4, R5, R6), even decimals of `0`, or its asset type
 * (R7). These are what its minter wrote, so they say nothing about who issued it. A token of
 * no standard has none, and its amounts have no decimals.
 */
export interface Eip4Metadata {
  /** Empty if R4 is absent, empty or not text. */
  name: string;
  /** Empty if R5 is absent, empty or not text. */
  description: string;
  /** `0` if R6 is absent or neither an `Int` nor one written in decimal. */
  decimals: number;
  /**
   * The minting box's EIP-4 asset type (R7). `nft` is an NFT of no registered subcategory.
   * Absent if R7 names no registered type.
   */
  type?: AssetType;
  /** The SHA-256 of its artwork (R8), in hex: only for a picture, audio, video or plain `nft`. */
  artworkHash?: string;
  /** A link to its artwork (R9), for the same types. */
  artworkUrl?: string;
  /** A link to a cover image, the second link of R9, as for audio. */
  artworkCover?: string;
}

/** The EIP-4 asset types, by R7: `0101`, `0102`, `0103`, `0104`, `010f`, other `01`, `0201`. */
export type AssetType =
  | "nft-picture"
  | "nft-audio"
  | "nft-video"
  | "nft-artwork-collection"
  | "nft-file-attachments"
  | "nft"
  | "membership-threshold-signature";

/** The known tokens among those asked for, in their order; unknown ids are left out. */
export interface TokenList {
  items: TokenMetadata[];
}

/**
 * A pending tx, as the relay's mempool view has it: Fleet's `SignedTransaction`, so it goes to
 * Fleet as it is, each input also the box it spends.
 */
export interface MempoolTransaction {
  id: string;
  inputs: MempoolInput[];
  dataInputs: DataInput[];
  outputs: Box<string>[];
}

/** A pending tx's input: the box it spends, with Fleet's `SignedInput` proof. */
export type MempoolInput = Box<string> & { spendingProof: ProverResult };

/** The id of a tx the node took, sent or only checked. */
export interface Submitted {
  transactionId: string;
}

/**
 * The chain's parameters as the relay's node reports them in `/info`, under the node's names:
 * what a tx builder needs to size fees, min box values and costs. Miners vote them; they change
 * only at the start of a voting epoch, at `height`. `subblocksPerBlock` is `null` until it is
 * voted in, where the node leaves it out.
 */
export interface ChainParameters {
  height: number;
  blockVersion: number;
  storageFeeFactor: number;
  minValuePerByte: number;
  maxBlockSize: number;
  maxBlockCost: number;
  tokenAccessCost: number;
  inputCost: number;
  dataInputCost: number;
  outputCost: number;
  subblocksPerBlock: number | null;
}

/**
 * The relay's state. `tip` is `null` until it has applied a block; `ready` is whether it has
 * started: it has reached the node's tip once and its configured keys have loaded. It stays
 * true while the relay is behind, as it is for a moment after each block, so a load can
 * still answer `not_ready` while the relay catches up. Comparing `tip` with the node's tells
 * how far behind it is.
 */
export interface Status {
  version: string;
  network: "mainnet" | "testnet";
  tip: BlockRef | null;
  ready: boolean;
}
