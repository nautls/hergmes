import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import { inject } from "vitest";
import type {
  BlockRef,
  Key,
  PoolBox,
  WireAssets as Assets,
  WireConfirmedBox as ConfirmedBox,
} from "../src/index.ts";

/** A chain op, as the sim relay draws and applies them; seeds are decimal strings. */
export type SimOp = { op: string } & Record<string, unknown>;

/** A pending tx as it affects one key. */
export interface PendingTx {
  txId: string;
  created: PoolBox[];
  spent: string[];
}

/** The oracle's state for a key at the tip. */
export interface Expected {
  tip: BlockRef;
  boxes: ConfirmedBox[];
  pending: PendingTx[];
  balance?: { confirmed: Assets; pending: Assets };
}

export interface SimKeys {
  addresses: string[];
  tokens: { tokenId: string; emission: string }[];
}

/** A sim relay process: its URL, and its control routes. */
export class SimRelay {
  readonly url: string;
  readonly #child: ChildProcess;

  private constructor(url: string, child: ChildProcess) {
    this.url = url;
    this.#child = child;
  }

  /** Starts the binary the global setup built, on a free port. */
  static async start(): Promise<SimRelay> {
    const child = spawn(inject("simRelay"), [], { stdio: ["ignore", "pipe", "inherit"] });
    const lines = createInterface({ input: child.stdout });
    const exited = new Promise<never>((_, reject) =>
      child.once("exit", (code) => {
        reject(new Error(`sim-relay exited with ${String(code)}`));
      }),
    );
    const first = new Promise<string>((resolve) => lines.once("line", resolve));
    const line = await Promise.race([first, exited]);
    lines.close();
    const { port } = JSON.parse(line) as { port: number };
    return new SimRelay(`http://127.0.0.1:${String(port)}`, child);
  }

  stop(): void {
    this.#child.kill();
  }

  /** Draws one op seeded by `seed`, applies it and answers it. */
  random(seed: bigint): Promise<SimOp> {
    return this.#call("POST", "/sim/random", { seed: seed.toString() });
  }

  async ops(ops: SimOp[]): Promise<void> {
    await this.#call("POST", "/sim/ops", ops, false);
  }

  tick(): Promise<{ tip: BlockRef | null; synced: boolean }> {
    return this.#call("POST", "/sim/tick");
  }

  /** The indexer brought to the tip, and the engine synced to it. */
  async settle(): Promise<BlockRef> {
    const { tip } = await this.#call<{ tip: BlockRef }>("POST", "/sim/settle");
    return tip;
  }

  expected(key: Key): Promise<Expected> {
    const path = "address" in key ? `addresses/${key.address}` : `tokens/${key.tokenId}`;
    return this.#call("GET", `/sim/expected/${path}`);
  }

  keys(): Promise<SimKeys> {
    return this.#call("GET", "/sim/keys");
  }

  async #call<T>(method: string, path: string, body?: unknown, json = true): Promise<T> {
    const response = await fetch(this.url + path, {
      method,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), headers: { "content-type": "application/json" } }),
    });
    if (!response.ok) {
      throw new Error(`${method} ${path}: ${String(response.status)} ${await response.text()}`);
    }
    return (json ? await response.json() : undefined) as T;
  }
}

/** splitmix64: the run's seeds, from one seed printed on failure. */
export function seeds(seed: bigint): () => bigint {
  const mask = (1n << 64n) - 1n;
  let state = seed & mask;
  return () => {
    state = (state + 0x9e3779b97f4a7c15n) & mask;
    let z = state;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & mask;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & mask;
    return z ^ (z >> 31n);
  };
}

/** Waits until `done` holds, or fails after 10 s: far past the longest wait measured. */
export async function until(done: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!done()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}
