# AGENTS.md

hergmes is a realtime, mempool-aware relay for the Ergo blockchain. It sits in front of an indexed
Ergo node and streams box, balance and mempool changes for tracked addresses, small-supply tokens
and contract templates. See `README.md` for the overview.

## Commands

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- --deny warnings
cargo test                    # long property runs: cargo test --release
RUST_LOG=hergmes=debug cargo run
node scripts/record-fixtures.ts --node http://127.0.0.1:9053   # re-record node fixtures
```

The TypeScript client SDK lives in `sdk/` (pnpm; it runs on Node 22, its tooling needs 22.18).
Its tests start the `sim-relay` binary (`src/bin/sim-relay.rs`, the relay over `SimChain`), which
the test setup builds. `sdk/examples/vue`, a Vue page on the SDK, is a workspace package built
against the SDK's source; `pnpm check` and `pnpm build` cover it:

```sh
cd sdk
pnpm install
pnpm format:check             # pnpm format to fix
pnpm lint
pnpm check                    # type check
pnpm build
pnpm test                     # replay a run: SEED=<seed> pnpm test
```

CI runs the first three cargo commands and every `sdk/` command. All of them must pass before a
change is done.

## Workflow

- Never commit, and never offer to. Leave changes in the working tree; the user commits.
- Every change leaves `master` runnable.
- A change is done when it does what it was for and the commands above pass, not when the code
  compiles.

## Node behaviour

- The node's source code is the source of truth, not the OpenAPI spec, SDK conventions or memory.
  Check behaviour in `ergoplatform/ergo` at the commit of the node the fixtures were recorded
  from (`appVersion` in `tests/fixtures/info.json`; 6.0.8 is `c1a69f65`). Clone it into a
  scratch directory, since there is no local checkout.
- Cite the file and function you relied on in code comments, so the check can be repeated after
  a node upgrade.
- Build serde types from the recorded fixtures, not from the spec.

## Architecture

```
NodeSource (trait)  ──►  Engine (pure state machine)  ──►  Transport (REST + WS)
 real: NodeClient         Engine::apply(Event) -> Result<Vec<Output>, EngineError>
 test: SimChain           Engine::query(Query) -> Reply
```

- `src/engine/` has no I/O and never reads a clock: no async, no randomness. Time comes in as
  data: the engine task sets a monotonic time on the engine before each message
  (`Engine::set_now`), and tests and the simulator drive a `ManualClock`, so the engine stays
  deterministic and replayable. Idle times (the grace period, the wait before eviction) are on
  that clock; what the chain bounds (the ring window) is counted in blocks.
- Adapters (`src/adapters/`) do all I/O. They stay thin: fetch, stamp with the observed tip, and
  send events to the engine.
- The engine refuses events that are not about its tip (`EngineError`) and changes nothing; the
  caller resynchronizes.
- Within one event, outputs go `block` / `rollback` / `resync_required` / `LoadRejected` /
  `CapacityExceeded`, then `mempool`, then `balance`, each in key order (`Engine::apply`). Keep
  that order.
- The engine task publishes the tip before routing an event's outputs, and a connection sends a
  changed tip before anything else, so a client's tip is never older than its boxes. The tip is
  not an engine output.

## Be frugal with resources

Memory, CPU and node calls are budgets, not afterthoughts.

- Wire types deserialize only the fields the relay reads. A field comes back only when a concrete
  use needs it.
- Per-key state is ids only. Full boxes live in bounded structures (ring buffer, LRU cache).
- Prefer fixed-size byte arrays to hex strings, integers to strings, and shared data stored once.
- Batch node calls where the node allows it, cache what never changes, and reuse data already
  fetched.

## Logging

- Use `tracing`. The default filter is `info`; `RUST_LOG` overrides it.
- The engine logs at `debug`, and only the decisions that leave no trace in what it returns (a
  mempool snapshot held, ignored or pruned, an idle key untracked, a duplicate load).
- Whatever the engine returns (`EngineError`, `resync_required`, `LoadRejected`, block deltas) is
  logged by the engine task that receives it, never inside the engine, so nothing is logged
  twice.
- Adapters log their own I/O decisions: reorg depth, back-off, retries, dropped txs.

## Testing

- Unit tests live in a sibling `<module>_tests.rs`, included from the module with
  `#[cfg(test)] #[path = "<module>_tests.rs"] mod tests;`.
- The engine is tested against `SimChain` and the oracle in `src/sim/` (compiled for tests, or
  with the `sim` feature for integration tests). The oracle recomputes state from scratch and is
  meant to be slow and obviously correct.
- Write the properties first, then the code.
- Check that a property can fail: plant bugs one at a time and confirm a test catches each.
  Cases the simulator can't reach get a hand-driven test.
- Node client tests run against `tests/fixtures/` served with `wiremock`. A new endpoint needs a
  recorded fixture.
- Keep the `proptest-regressions/` files: they are tracked, and replay past failures.

## Style

- `rustfmt.toml` sets the format (edition 2024, width 100). Run `cargo fmt`.
- Doc comments say what a thing is and why it is that way, not how the code does it.
