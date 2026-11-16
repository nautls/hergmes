//! The transaction service: a signed tx, serialized, handed to the node to validate and
//! broadcast, or only to validate.
//!
//! The relay holds no tx type. It checks only that the body is hex within the node's size
//! limit and forwards it unparsed; the node validates everything. No key policy and no
//! engine: a broadcast tx reaches the overlay at the next mempool poll, like any other.

use reqwest::StatusCode;

use crate::{
    api::ApiError,
    clients::{NodeError, NodeSource},
    types::ergo::TxId,
};

/// The node's default largest tx, in bytes (node 6.0.8, `maxTransactionSize` in
/// `application.conf`, checked in `TransactionsApiRoute.validateTransactionAndProcess`). A
/// constant rather than a setting: an operator who raises the node's limit is rare.
pub const MAX_TX_BYTES: usize = 98_304;

/// The longest body: two hex characters a byte. The node refuses only a tx above its limit,
/// so a tx of exactly [`MAX_TX_BYTES`] is let through.
pub const MAX_HEX_CHARS: usize = 2 * MAX_TX_BYTES;

/// Why the node refused a tx. The node has no codes, only a message per refusal, so the
/// reason is read from the fixed text the node writes ([`Refusal::of`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Not a serialized tx.
    Unparseable,
    /// Above the node's size limit.
    TooLarge,
    /// Fails validation: a script false, the cost too high, an input in neither the UTXO set
    /// nor the pool (also an input already spent in a block). Also any message not known,
    /// so a new or reworded refusal still has a reason.
    Invalid,
    /// Lost to a pool tx spending the same input with a better fee weight.
    DoubleSpend,
    FeeTooLow,
    AlreadyInPool,
    /// The pool is full and the tx's fee doesn't win a place in it. Not a wait: the node
    /// gives none, and the same tx gets in only if its fee wins.
    PoolFull,
    /// The node's mempool policy: the re-emission token in outputs, a storage-rent claim, a
    /// blacklisted tx.
    Policy,
}

impl Refusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::Unparseable => "unparseable",
            Self::TooLarge => "too_large",
            Self::Invalid => "invalid",
            Self::DoubleSpend => "double_spend",
            Self::FeeTooLow => "fee_too_low",
            Self::AlreadyInPool => "already_in_pool",
            Self::PoolFull => "pool_full",
            Self::Policy => "policy",
        }
    }

    /// The reason in a refusal's message, matched against the fixed text the node writes
    /// (node 6.0.8, `c1a69f65`). Read again on a node upgrade. Matched by the start or the end
    /// of the message, never inside it, since the validation error inside `Malformed
    /// transaction` is free text.
    pub fn of(message: &str) -> Self {
        // `TransactionsApiRoute.sendTransactionAsBytesR` and `checkTransactionAsBytesR`.
        if message.starts_with("Can not parse transaction bytes: ") {
            return Self::Unparseable;
        }
        // `TransactionsApiRoute.validateTransactionAndProcess`: "Transaction <tx> has too
        // large size <n>", the tx written out in the middle.
        if message.starts_with("Transaction ")
            && let Some((_, size)) = message.rsplit_once(" has too large size ")
            && !size.is_empty()
            && size.bytes().all(|b| b.is_ascii_digit())
        {
            return Self::TooLarge;
        }
        // `TransactionsApiRoute.validateTransactionAndProcess`, from
        // `ErgoBaseApiRoute.verifyTransaction` (`UtxoStateReader.validateWithCost`).
        if message.starts_with("Malformed transaction: ") {
            return Self::Invalid;
        }
        // `ErgoBaseApiRoute.sendLocalTransactionRoute`, on `ErgoMemPool.acceptIfNoDoubleSpend`
        // finding the tx a double-spending loser.
        if message == "Double spending attempt" {
            return Self::DoubleSpend;
        }
        // The rest are `ErgoMemPool.process`, unless named.
        if message.starts_with("Min fee not met: ") {
            return Self::FeeTooLow;
        }
        if message.starts_with("Pool can not accept transaction ") {
            if message.ends_with(", it is already in the mempool") {
                return Self::AlreadyInPool;
            }
            if message.ends_with(", the mempool is full") {
                return Self::PoolFull;
            }
        }
        // `ErgoMemPool.acceptIfNoDoubleSpend`.
        if message == "Transaction pays less than any other in the pool being full" {
            return Self::PoolFull;
        }
        if message
            == "Mempool policy declines a transaction carrying the re-emission token in outputs"
            || message == "Mempool policy declines a storage rent collection transaction"
            || message == "blacklisted tx"
        {
            return Self::Policy;
        }
        Self::Invalid
    }
}

pub struct Transactions<S> {
    source: S,
}

impl<S: NodeSource> Transactions<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }

    /// Hands the tx in `hex` to the node: validated and broadcast, or with `broadcast` false
    /// only validated. Its id if the node took it.
    ///
    /// A body that is not hex, or longer than [`MAX_HEX_CHARS`], is refused here with the
    /// reason the node would give, and costs no node call. Whitespace around the hex is
    /// left out, so a file with a trailing newline can be sent as it is, and so is one pair
    /// of quotes, since the node also takes the hex as a JSON string
    /// (`ErgoBaseApiRoute.fromJsonOrPlain`).
    ///
    /// A check covers less than a broadcast: `checkBytes` runs `verifyTransaction` alone and
    /// never `ErgoMemPool.process`, so it never answers `fee_too_low`, `pool_full`,
    /// `already_in_pool`, `double_spend` or `policy`, and its view of the pool does not leave
    /// out inputs a pool tx spends. A tx can pass the check and be refused when sent.
    pub async fn submit(&self, hex: &str, broadcast: bool) -> Result<TxId, ApiError> {
        let hex = hex.trim_ascii();
        let hex = (hex.strip_prefix('"').and_then(|h| h.strip_suffix('"'))).unwrap_or(hex);
        if hex.len() > MAX_HEX_CHARS {
            return Err(too_large());
        }
        if hex.is_empty()
            || !hex.len().is_multiple_of(2)
            || !hex.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(unparseable());
        }
        match self.source.submit_transaction(hex, broadcast).await {
            Ok(id) => Ok(id),
            // Every refusal is a `400` with the node's message (`sendLocalTransactionRoute`,
            // `validateTransactionAndProcess`), the client's own tx, so its text is passed on.
            Err(NodeError::Api { status: StatusCode::BAD_REQUEST, detail, .. }) => {
                let message = detail
                    .filter(|d| !d.is_empty())
                    .unwrap_or_else(|| "the node refused the tx".to_owned());
                Err(rejected(Refusal::of(&message), message))
            }
            Err(error) => Err((&error).into()),
        }
    }
}

fn rejected(reason: Refusal, message: String) -> ApiError {
    ApiError::TransactionRejected { reason, message }
}

/// The relay's own refusal of a body longer than a tx can be, before the node sees it.
pub(crate) fn too_large() -> ApiError {
    let message = format!("a tx is at most {MAX_TX_BYTES} bytes, {MAX_HEX_CHARS} hex characters");
    rejected(Refusal::TooLarge, message)
}

/// The relay's own refusal of a body that is not a tx in hex, before the node sees it.
pub(crate) fn unparseable() -> ApiError {
    rejected(Refusal::Unparseable, "a tx is sent as its bytes in hex".to_owned())
}

#[cfg(test)]
#[path = "transactions_tests.rs"]
mod tests;
