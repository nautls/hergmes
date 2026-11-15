/**
 * Record live Ergo node responses into tests/fixtures/.
 *
 * Usage:
 *   node scripts/record-fixtures.ts [--node URL] [--out DIR]
 *       [--address ADDR] [--small-token TOKEN_ID]
 *   node scripts/record-fixtures.ts --history-only [--node URL] [--out DIR]
 *
 * --history-only records only the address tx history fixtures, for the sample address of
 * the existing manifest.json, and adds them to it, leaving every other fixture as it is.
 *
 * Runs on Node 23.6+ (native type stripping) with no dependencies.
 * The node URL defaults to $ERGO_NODE_URL. The node must run the extra indexer.
 * Samples (an address with several boxes, a token with emission <= 100 spread
 * over several boxes) are discovered from recent blocks unless given explicitly.
 *
 * Every fixture is the response body, pretty-printed with numbers kept exactly as
 * the node sent them (some exceed 2^53). manifest.json records for each one the
 * request, the HTTP status, and notes. Fixtures marked "synthetic" are derived from
 * a real response and say how in their notes.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const UNKNOWN_ID = "ab".repeat(32);
// Valid mainnet P2PK address (Fleet SDK test vectors) that holds no boxes.
const EMPTY_ADDRESS = "9gNvAv97W71Wm33GoXgSQBFJxinFubKvE6wh2dEhFTSgYEe783j";
// Valid mainnet P2PK address of a random curve point, so no one holds its key: no box and no
// tx has ever touched it. EMPTY_ADDRESS holds no boxes but has a history.
const UNUSED_ADDRESS = "9iKCdiitfAVb1Wux5nFXiMcdYXGkhdNksAxJHQ28qiAMkxXupbt";
const SMALL_SUPPLY_LIMIT = 100;
const BLOCKS_TO_SCAN = 50;
const MEMPOOL_LIMIT = 20;

type Json = any;
type Method = "GET" | "POST";

interface Response {
  status: number;
  /** Parsed with plain JSON.parse: fine for logic, lossy above 2^53. */
  value: Json;
  /** Parsed keeping every number's source text, for writing fixtures. */
  exact: Json;
  /** The raw body, for taking an independent exact copy with parseExact(). */
  text: string;
}

interface ManifestEntry {
  method: Method;
  path: string;
  status: number;
  body?: Json;
  synthetic?: true;
  notes?: string;
}

/** JSON.parse that keeps each number's exact source text (JSON.rawJSON, Node 21+). */
function parseExact(text: string): Json {
  return JSON.parse(text, (_key, value, context) =>
    typeof value === "number" ? (JSON as any).rawJSON(context.source) : value,
  );
}

function fail(message: string): never {
  console.error(message);
  process.exit(1);
}

class Node {
  readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
  }

  async request(method: Method, path: string, body?: Json): Promise<Response> {
    const res = await fetch(this.baseUrl + path, {
      method,
      headers: body === undefined ? undefined : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(60_000),
    });
    const text = await res.text();
    return {
      status: res.status,
      value: text ? JSON.parse(text) : null,
      exact: text ? parseExact(text) : null,
      text,
    };
  }

  async ok(method: Method, path: string, body?: Json): Promise<Json> {
    const res = await this.request(method, path, body);
    if (res.status !== 200) {
      fail(`${method} ${path} returned ${res.status}: ${JSON.stringify(res.value)}`);
    }
    return res.value;
  }
}

class Recorder {
  readonly node: Node;
  readonly outDir: string;
  readonly manifest: Record<string, ManifestEntry> = {};

  constructor(node: Node, outDir: string) {
    this.node = node;
    this.outDir = outDir;
  }

  async record(name: string, method: Method, path: string, body?: Json, notes = "") {
    const res = await this.node.request(method, path, body);
    this.save(name, { method, path, status: res.status, body, notes }, res.exact);
    return res;
  }

  save(name: string, entry: ManifestEntry, payload: Json) {
    writeFileSync(join(this.outDir, `${name}.json`), JSON.stringify(payload, null, 2) + "\n");
    const clean: ManifestEntry = { method: entry.method, path: entry.path, status: entry.status };
    if (entry.body !== undefined) clean.body = entry.body;
    if (entry.synthetic) clean.synthetic = true;
    if (entry.notes) clean.notes = entry.notes;
    this.manifest[name] = clean;
    console.log(`  ${name}.json  [${entry.status}]`);
  }
}

interface Samples {
  address: string;
  nft: string;
  small: string;
  large: string;
}

/** Pick sample ids from recent indexed blocks (newest first). */
async function discover(
  node: Node,
  blocks: AsyncIterable<Json>,
  given: { address?: string; smallToken?: string },
): Promise<Samples> {
  const emissions = new Map<string, number | null>();
  const emission = async (tokenId: string) => {
    if (!emissions.has(tokenId)) {
      const res = await node.request("GET", `/blockchain/token/byId/${tokenId}`);
      emissions.set(tokenId, res.status === 200 ? res.value.emissionAmount : null);
    }
    return emissions.get(tokenId)!;
  };

  let address = given.address;
  let small = given.smallToken;
  let nft: string | undefined;
  let large: string | undefined;

  for await (const block of blocks) {
    for (const tx of block.transactions) {
      const outputs: Json[] = tx.outputs;

      if (address === undefined) {
        for (const box of outputs) {
          if (!box.ergoTree.startsWith("0008cd")) continue;
          const found = await node.ok(
            "POST",
            "/blockchain/box/unspent/byAddress?limit=5",
            box.address,
          );
          if (found.length >= 2) {
            address = box.address;
            break;
          }
        }
      }

      const tokenIds = new Set<string>(outputs.flatMap((b) => b.assets.map((a: Json) => a.tokenId)));
      for (const tokenId of tokenIds) {
        const amount = await emission(tokenId);
        if (amount === null) continue;
        if (amount === 1) {
          nft ??= tokenId;
        } else if (amount > SMALL_SUPPLY_LIMIT) {
          large ??= tokenId;
        } else if (amount >= 2 && small === undefined) {
          const boxes = await node.ok(
            "GET",
            `/blockchain/box/unspent/byTokenId/${tokenId}?limit=100`,
          );
          if (boxes.length >= 2) small = tokenId;
        }
      }
    }
    if (address && nft && large && small) break;
  }

  const missing = Object.entries({
    address,
    nft,
    "large token": large,
    "small-supply token": small,
  })
    .filter(([, v]) => v === undefined)
    .map(([k]) => k);
  if (missing.length) fail(`Could not discover: ${missing.join(", ")}. Pass them explicitly.`);

  return { address: address!, nft: nft!, small: small!, large: large! };
}

/**
 * An address's tx history, newest first, offset from the newest end. Two overlapping reads
 * are recorded together, and kept only if the second is the first's tail with the same total,
 * which is what the relay's anchored cursor relies on. A tx that lands between the two reads
 * breaks that, so they are read again.
 */
async function recordHistory(rec: Recorder, address: string, height: number) {
  const path = (offset: number, limit: number) =>
    `/blockchain/transaction/byAddress?offset=${offset}&limit=${limit}`;
  const notes = `Newest first; offset counts from the newest end. Recorded at height ${height}.`;
  for (let attempt = 1; attempt <= 3; attempt++) {
    const first = await rec.node.request("POST", path(0, 3), address);
    const later = await rec.node.request("POST", path(1, 2), address);
    if (first.status !== 200 || later.status !== 200) {
      fail(`transaction/byAddress returned ${first.status} and ${later.status}`);
    }
    const ids = (r: Response) => r.value.items.map((t: Json) => t.id);
    const tail = ids(first).slice(1);
    const same =
      first.value.total === later.value.total &&
      // More than the first read holds, so the offset read pages into a longer list.
      first.value.total > 3 &&
      JSON.stringify(tail) === JSON.stringify(ids(later));
    if (!same) {
      console.log(`  (history moved between reads, attempt ${attempt})`);
      continue;
    }
    const entry = (offset: number, limit: number) => ({
      method: "POST" as Method,
      path: path(offset, limit),
      status: 200,
      body: address,
    });
    rec.save("transactions_by_address", { ...entry(0, 3), notes }, first.exact);
    rec.save(
      "transactions_by_address_offset",
      { ...entry(1, 2), notes: `${notes} The tail of transactions_by_address, same total.` },
      later.exact,
    );
    await rec.record(
      "transactions_by_address_empty",
      "POST",
      path(0, 3),
      UNUSED_ADDRESS,
      "An address with no txs is an empty page with total 0, not a 404.",
    );
    return;
  }
  fail("The sample address's history kept moving; try again.");
}

async function historyOnly(node: Node, outDir: string) {
  const manifestPath = join(outDir, "manifest.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  const info = await node.ok("GET", "/info");
  if (info.appVersion !== manifest.nodeVersion) {
    fail(`The node runs ${info.appVersion}, the fixtures were recorded from ${manifest.nodeVersion}`);
  }
  const rec = new Recorder(node, outDir);
  console.log(`Recording history fixtures for ${manifest.samples.address} into ${outDir}`);
  await recordHistory(rec, manifest.samples.address, info.fullHeight);
  manifest.fixtures = { ...manifest.fixtures, ...rec.manifest };
  writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
  console.log(`Added ${Object.keys(rec.manifest).length} fixtures to manifest.json`);
}

async function main() {
  const { values: args } = parseArgs({
    options: {
      node: { type: "string", default: process.env.ERGO_NODE_URL },
      out: { type: "string", default: fileURLToPath(new URL("../tests/fixtures", import.meta.url)) },
      address: { type: "string" },
      "small-token": { type: "string" },
      "history-only": { type: "boolean", default: false },
    },
  });
  if (!args.node) fail("Pass --node or set ERGO_NODE_URL");

  const node = new Node(args.node);
  const outDir = resolve(args.out!);
  mkdirSync(outDir, { recursive: true });
  if (args["history-only"]) return historyOnly(node, outDir);
  const rec = new Recorder(node, outDir);

  const info = await node.ok("GET", "/info");
  const indexed = await node.ok("GET", "/blockchain/indexedHeight");
  if (indexed.indexedHeight !== indexed.fullHeight) {
    fail(`Indexer is behind: ${JSON.stringify(indexed)}`);
  }

  console.log("Scanning recent blocks for samples...");
  const headers: Json[] = await node.ok("GET", `/blocks/lastHeaders/${BLOCKS_TO_SCAN}`);
  // Newest first, skipping the tip so the sample block is less likely to be reorged.
  headers.sort((a, b) => b.height - a.height);
  headers.shift();

  const fetched = new Map<string, Json>();
  const fetchBlock = async (id: string) => {
    if (!fetched.has(id)) fetched.set(id, await node.ok("GET", `/blockchain/block/byHeaderId/${id}`));
    return fetched.get(id);
  };
  async function* blocks() {
    for (const header of headers) yield await fetchBlock(header.id);
  }

  const { address, nft, small, large } = await discover(node, blocks(), {
    address: args.address,
    smallToken: args["small-token"],
  });

  // Prefer a block where a tx spends a box created earlier in the same block.
  const spendsOwnOutput = (block: Json) =>
    block.transactions.some((tx: Json) =>
      tx.inputs.some((i: Json) => i.inclusionHeight === block.header.height),
    );
  let sampleBlock: Json = await fetchBlock(headers[0].id);
  for (const header of headers) {
    const block = await fetchBlock(header.id);
    if (spendsOwnOutput(block)) {
      sampleBlock = block;
      break;
    }
  }
  console.log(
    `  address: ${address}\n  nft: ${nft}\n  small-supply token: ${small}\n  large token: ${large}`,
  );
  console.log(`Recording into ${outDir}`);

  // Chain and node state
  await rec.record("info", "GET", "/info");
  await rec.record("indexed_height", "GET", "/blockchain/indexedHeight");

  // Blocks
  const headerId: string = sampleBlock.header.id;
  await rec.record(
    "indexed_block",
    "GET",
    `/blockchain/block/byHeaderId/${headerId}`,
    undefined,
    "Inputs are full indexed boxes. The live shape differs from the spec " +
      "(top-level header/transactions/height/size).",
  );
  await rec.record("indexed_block_unknown", "GET", `/blockchain/block/byHeaderId/${UNKNOWN_ID}`);

  // Mempool
  const mempool = await rec.record("mempool", "GET", `/transactions/unconfirmed?limit=${MEMPOOL_LIMIT}`);
  const unresolved = mempool.value.some((tx: Json) => tx.inputs.some((i: Json) => !("value" in i)));
  if (mempool.exact.length && !unresolved) {
    const txs = parseExact(mempool.text);
    const first = txs[0].inputs[0];
    txs[0].inputs[0] = { boxId: first.boxId, spendingProof: first.spendingProof };
    rec.save(
      "mempool_unresolved_input",
      {
        method: "GET",
        path: "/transactions/unconfirmed",
        status: 200,
        synthetic: true,
        notes:
          "Copy of mempool.json with the first tx's first input reduced to " +
          "boxId + spendingProof, as the node returns for ergo#2248. " +
          "No such input was live when recording.",
      },
      txs,
    );
  } else if (unresolved) {
    console.log("  (live mempool already contains an unresolved input)");
  }

  // Address, queried by its ErgoTree
  const tree = (addr: string) => node.ok("GET", `/script/addressToTree/${addr}`).then((r) => r.tree);
  const ergoTree: string = await tree(address);
  const unspentPath = "/blockchain/box/unspent/byErgoTree?limit=5&sortDirection=asc";
  await rec.record(
    "unspent_by_ergo_tree",
    "POST",
    unspentPath,
    ergoTree,
    "A plain array (the spec says {items, total}).",
  );
  await rec.record(
    "unspent_by_ergo_tree_empty",
    "POST",
    unspentPath,
    await tree(EMPTY_ADDRESS),
    "Empty set is 200 [] (the spec says 404).",
  );
  await rec.record("balance", "POST", "/blockchain/balance", address);
  await recordHistory(rec, address, info.fullHeight);
  await rec.record("balance_empty", "POST", "/blockchain/balance", EMPTY_ADDRESS);

  // Tokens
  const unspentByToken = (id: string) =>
    `/blockchain/box/unspent/byTokenId/${id}?limit=100&sortDirection=asc`;
  await rec.record("token_nft", "GET", `/blockchain/token/byId/${nft}`);
  await rec.record("token_small_supply", "GET", `/blockchain/token/byId/${small}`);
  await rec.record("token_large_supply", "GET", `/blockchain/token/byId/${large}`);
  await rec.record("token_unknown", "GET", `/blockchain/token/byId/${UNKNOWN_ID}`);
  await rec.record("unspent_by_token_nft", "GET", unspentByToken(nft));
  await rec.record("unspent_by_token_small_supply", "GET", unspentByToken(small));
  await rec.record(
    "unspent_by_token_unknown",
    "GET",
    unspentByToken(UNKNOWN_ID),
    undefined,
    "Unknown token is 200 [] (the spec says 404).",
  );

  // Boxes
  const spentId: string = sampleBlock.transactions[0].inputs[0].boxId;
  await rec.record(
    "box_by_id_spent",
    "GET",
    `/blockchain/box/byId/${spentId}`,
    undefined,
    "Spent box: spentTransactionId is set, spendingHeight is absent.",
  );
  await rec.record("box_by_id_unknown", "GET", `/blockchain/box/byId/${UNKNOWN_ID}`);
  const [unspent] = await node.ok("POST", "/blockchain/box/unspent/byErgoTree?limit=1", ergoTree);
  await rec.record(
    "with_pool_partial",
    "POST",
    "/utxo/withPool/byIds",
    [UNKNOWN_ID, unspent.boxId],
    "Missing ids are silently omitted.",
  );
  await rec.record(
    "with_pool_none",
    "POST",
    "/utxo/withPool/byIds",
    [UNKNOWN_ID],
    "No id exists: 200 [] (the spec says 404).",
  );

  const manifest = {
    recordedAt: new Date().toISOString().replace(/\.\d+Z$/, "Z"),
    nodeVersion: info.appVersion,
    network: info.network,
    fullHeight: info.fullHeight,
    samples: { address, ergoTree, nft, smallSupplyToken: small, largeSupplyToken: large, headerId },
    fixtures: rec.manifest,
  };
  writeFileSync(join(outDir, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
  console.log(`Wrote ${Object.keys(rec.manifest).length} fixtures and manifest.json`);
}

await main();
