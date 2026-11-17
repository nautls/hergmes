import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import type { TestProject } from "vitest/node";

declare module "vitest" {
  export interface ProvidedContext {
    simRelay: string;
  }
}

/**
 * Builds the sim relay once for the whole run (a no-op when it is fresh) and hands its path to
 * the test files, which start the executable directly: `cargo run` per file would contend on
 * cargo's lock, and killing cargo leaves its child running.
 */
export default function setup(project: TestProject): void {
  const root = fileURLToPath(new URL("../..", import.meta.url));
  const out = execFileSync(
    "cargo",
    [
      "build",
      "--features",
      "sim",
      "--bin",
      "sim-relay",
      "--message-format",
      "json-render-diagnostics",
    ],
    // A line per crate, about 200 KB, against a default cap of 1 MiB.
    { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], maxBuffer: 64 << 20 },
  );
  for (const line of out.split("\n")) {
    if (!line.startsWith("{")) continue;
    const message = JSON.parse(line) as { reason?: string; executable?: string | null };
    if (message.reason === "compiler-artifact" && message.executable) {
      project.provide("simRelay", message.executable);
      return;
    }
  }
  throw new Error("cargo built no sim-relay executable");
}
