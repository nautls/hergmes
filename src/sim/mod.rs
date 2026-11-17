//! Test-only simulation: `SimChain` (a controllable fake node and indexer) and
//! the oracle that recomputes unspent sets from scratch.
//!
//! Compiled for unit tests, or with the `sim` feature for integration tests and the
//! `sim-relay` binary.
//!
//! - [`Chain`]: the model. A block tree with a selected chain, a mempool and a lagging
//!   indexer, driven with `&mut self` (mine, reorg, submit, drop, lag) or by [`SimOp`]s.
//! - [`SimChain`]: a shared `Chain` served through [`NodeSource`](crate::clients::NodeSource),
//!   with a call log and a hook that runs before each call.
//! - [`Oracle`]: unspent sets, totals and the reference overlay, recomputed by replaying a
//!   branch from genesis.
//! - [`strategy`]: proptest strategies for random histories.
//! - [`relay`]: the real API over a `SimChain`, with control routes, for the client SDK's
//!   tests.

mod chain;
mod ledger;
mod oracle;
pub mod relay;
mod rng;
mod source;
pub mod strategy;
#[cfg(test)]
pub(crate) mod testing;

pub use chain::{
    Chain, GENESIS_TOKENS, OutputSpec, RETRY_AFTER, SimBlock, SimConfig, SimOp, SimTx, timestamp,
    token_id,
};
pub use oracle::{Oracle, filter_overlay, overlay, pending_order, reference_delta, totals};
pub use source::{Endpoint, Hook, SimChain};
