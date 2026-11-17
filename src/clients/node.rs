use std::{collections::HashSet, time::Duration};

use reqwest::{RequestBuilder, Response, StatusCode, header::RETRY_AFTER};
use serde::{Deserialize, de::DeserializeOwned};
use tracing::debug;

use crate::{
    clients::source::{NodeSource, Page},
    types::{
        address::{self, Network, UnknownNetwork},
        ergo::{
            AddressTransactions, Balance, BalanceResponse, BlockHeader, BoxById, BoxId, ErgoTree,
            HeaderId, IndexedBlock, IndexedBox, NodeBox, TokenId, TokenInfo, TokenMetadata, TxId,
            UTxO, UnconfirmedTransaction,
        },
    },
};

/// The wait for one page of an address's history, above the client's 30 s. The node sends
/// every box of every tx in full, and one airdrop tx to thousands of addresses is in each
/// recipient's history: 25 txs of a P2PK wallet were 22,574 boxes and 23 MB, which the node
/// took 10 to 16 s to send. Under nginx's default `proxy_read_timeout` (60 s) and Cloudflare's
/// 100 s, so a slow page still reaches the client as the relay's own error rather than the
/// proxy's.
pub const HISTORY_TIMEOUT: Duration = Duration::from_secs(55);

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error(transparent)]
    HttpError(#[from] reqwest::Error),

    #[error("The node is unavailable (503), retry after {retry_after:?}.")]
    Unavailable { retry_after: Option<Duration> },

    #[error("The node returned {status}: {reason} ({detail:?}).")]
    Api { status: StatusCode, reason: String, detail: Option<String> },

    #[error("The node is not fully indexed.")]
    NotIndexed(IndexedHeightResponse),

    #[error(transparent)]
    UnknownNetwork(#[from] UnknownNetwork),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexedHeightResponse {
    pub indexed_height: u32,
    pub full_height: u32,
}

/// Error body returned by the node, e.g. `{"error": 404, "reason": "not-found", "detail": null}`.
#[derive(Debug, Deserialize)]
pub struct ApiError {
    pub reason: String,
    pub detail: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct InfoResponse {
    /// Wall-clock time (ms) of the last `ChangedMempool` event: set at launch, and on every
    /// applied block, accepted or invalidated tx, and cleanup recheck, changed or not. It is a
    /// hint, not a version: it is set by a different actor than the one the mempool endpoints
    /// read, so a read right after it moves can still return the previous pool, and it can
    /// move backwards if the node's clock does.
    #[serde(rename = "lastMemPoolUpdateTime")]
    pub last_mempool_update: u64,
    pub network: String,
    /// The best full block: the tip of the chain the node's UTXO set, mempool and indexer
    /// follow. Absent before the first full block. Not the best header, which can run ahead of
    /// it (`/blocks/lastHeaders` follows headers).
    #[serde(rename = "bestFullHeaderId")]
    pub best_full_header_id: Option<HeaderId>,
    #[serde(rename = "fullHeight")]
    pub full_height: Option<u32>,
}

/// A page of `/blockchain/box/byAddress`, read for its `total` alone: the relay asks for no
/// box.
#[derive(Debug, Deserialize)]
pub(crate) struct BoxCountResponse {
    pub total: u64,
}

#[derive(Debug, Clone)]
pub struct NodeClient {
    http_client: reqwest::Client,
    base_url: String,
    network: Network,
}

/// A pool tx as `/transactions/unconfirmed` lists it: inputs the node could not resolve are
/// `None` (only `boxId` and `spendingProof` are sent).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct MempoolTransactionResponse {
    pub id: TxId,
    pub inputs: Vec<MempoolTransactionInput>,
    pub outputs: Vec<UTxO>,
}

/// `None` unless every field of a box is there: the node sends only `boxId` and
/// `spendingProof` for an input it could not resolve. A field that is there but malformed
/// fails the whole list, as a malformed output does: it is the node's fault, not an
/// unresolved input.
#[derive(Debug, Clone)]
pub(crate) struct MempoolTransactionInput {
    pub utxo: Option<UTxO>,
}

impl<'de> Deserialize<'de> for MempoolTransactionInput {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self { utxo: UTxO::try_from(NodeBox::deserialize(deserializer)?).ok() })
    }
}

impl MempoolTransactionResponse {
    /// The tx with its inputs, or its outputs if the node could not resolve any input.
    fn resolve(self) -> Result<UnconfirmedTransaction, Vec<UTxO>> {
        match self.inputs.into_iter().map(|i| i.utxo).collect() {
            Some(inputs) => {
                Ok(UnconfirmedTransaction { id: self.id, inputs, outputs: self.outputs })
            }
            None => Err(self.outputs),
        }
    }
}

/// Drops the txs with an input the node could not resolve, then every tx spending an output
/// of a dropped tx, recursively.
///
/// The node resolves inputs from its UTXO set and from the outputs of every pool tx
/// (`TransactionsApiRoute.resolveTransactionInputs`), hidden ones included. Without the
/// cascade, a tx whose parent is hidden (its input was spent by a block, or its own parent
/// left the pool; ergo#2248) would be kept with an input that is neither confirmed nor
/// pending. With it, every input of a kept tx is either created by another kept tx or was in
/// the node's UTXO set when it answered, which is what the engine's overlay relies on.
///
/// The cascade can also drop valid txs for a moment. The node's state reader reads the live
/// UTXO store, while its pool reader is only replaced when `ChangedMempool` reaches
/// `ErgoReadersHolder`, after the block is written (`ErgoNodeViewHolder.updateNodeView`). A
/// tx just mined can still be listed with its inputs spent, and its children, whose inputs
/// are now confirmed, are dropped with it until the pool reader catches up. Leaving out a
/// valid tx for one poll is harmless; keeping an invalid one is not.
///
/// See <https://github.com/ergoplatform/ergo/issues/2248#issuecomment-3463844934>.
pub(crate) fn filter_out_unresolved(
    txs: Vec<MempoolTransactionResponse>,
) -> Vec<UnconfirmedTransaction> {
    let mut dropped: HashSet<BoxId> = HashSet::new();
    let mut kept = Vec::with_capacity(txs.len());
    for tx in txs {
        match tx.resolve() {
            Ok(tx) => kept.push(tx),
            Err(outputs) => dropped.extend(outputs.iter().map(|o| o.id)),
        }
    }
    loop {
        let before = kept.len();
        kept.retain(|tx: &UnconfirmedTransaction| {
            let orphan = tx.inputs.iter().any(|i| dropped.contains(&i.id));
            if orphan {
                dropped.extend(tx.outputs.iter().map(|o| o.id));
            }
            !orphan
        });
        if kept.len() == before {
            return kept;
        }
    }
}

impl NodeClient {
    pub fn new(http_client: reqwest::Client, base_url: &str, network: Network) -> Self {
        Self { http_client, base_url: base_url.trim_end_matches('/').to_string(), network }
    }

    /// Builds a client for the network the node reports.
    pub async fn connect(http_client: reqwest::Client, base_url: &str) -> Result<Self, NodeError> {
        let mut client = Self::new(http_client, base_url, Network::Mainnet);
        client.network = client.info().await?.network.parse()?;
        Ok(client)
    }

    pub fn network(&self) -> Network {
        self.network
    }

    fn get(&self, path: &str) -> RequestBuilder {
        self.http_client.get(format!("{}/{}", self.base_url, path))
    }

    fn post(&self, path: &str) -> RequestBuilder {
        self.http_client.post(format!("{}/{}", self.base_url, path))
    }

    fn unspent_page(request: RequestBuilder, page: Page) -> RequestBuilder {
        request.query(&[
            ("offset", page.offset.to_string()),
            ("limit", page.limit.to_string()),
            ("sortDirection", "asc".to_string()),
        ])
    }
}

/// Sends a request and parses a 2xx body.
async fn fetch<T: DeserializeOwned>(request: RequestBuilder) -> Result<T, NodeError> {
    let response = check(request.send().await?).await?;
    Ok(response.json().await?)
}

/// Like [`fetch`], but a 404 is `None`.
async fn fetch_optional<T: DeserializeOwned>(
    request: RequestBuilder,
) -> Result<Option<T>, NodeError> {
    let response = request.send().await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(check(response).await?.json().await?))
}

async fn check(response: Response) -> Result<Response, NodeError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if status == StatusCode::SERVICE_UNAVAILABLE {
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse().ok())
            .map(Duration::from_secs);
        return Err(NodeError::Unavailable { retry_after });
    }

    let body = response.text().await.unwrap_or_default();
    let (reason, detail) = match serde_json::from_str::<ApiError>(&body) {
        Ok(e) => (e.reason, e.detail),
        Err(_) => (body, None),
    };
    Err(NodeError::Api { status, reason, detail })
}

impl NodeSource for NodeClient {
    #[tracing::instrument(skip(self))]
    async fn info(&self) -> Result<InfoResponse, NodeError> {
        fetch(self.get("info")).await
    }

    #[tracing::instrument(skip(self))]
    async fn indexed_height(&self) -> Result<IndexedHeightResponse, NodeError> {
        fetch(self.get("blockchain/indexedHeight")).await
    }

    #[tracing::instrument(skip(self))]
    async fn indexed_block(&self, header_id: &HeaderId) -> Result<Option<IndexedBlock>, NodeError> {
        fetch_optional(self.get(&format!("blockchain/block/byHeaderId/{header_id}"))).await
    }

    /// Not `/blocks/{id}/header`, which reads the block's transactions, extension and AD
    /// proofs to return its header (`BlocksApiRoute.getFullBlockByHeaderId`).
    #[tracing::instrument(skip(self))]
    async fn header(&self, header_id: &HeaderId) -> Result<Option<BlockHeader>, NodeError> {
        fetch_optional(self.get(&format!("blocks/modifier/{header_id}"))).await
    }

    #[tracing::instrument(skip(self))]
    async fn mempool(&self) -> Result<Vec<UnconfirmedTransaction>, NodeError> {
        let request = self
            .get("transactions/unconfirmed")
            .query(&[("limit", i32::MAX)]);
        let listed: Vec<MempoolTransactionResponse> = fetch(request).await?;
        let count = listed.len();
        let kept = filter_out_unresolved(listed);
        if kept.len() < count {
            debug!(
                dropped = count - kept.len(),
                kept = kept.len(),
                "Mempool txs dropped for an unresolved input, with their descendants"
            );
        }
        Ok(kept)
    }

    #[tracing::instrument(skip(self))]
    async fn unspent_by_ergo_tree(
        &self,
        ergo_tree: &ErgoTree,
        page: Page,
    ) -> Result<Vec<IndexedBox>, NodeError> {
        let request = self
            .post("blockchain/box/unspent/byErgoTree")
            .json(ergo_tree);
        Ok(fetch_optional(Self::unspent_page(request, page))
            .await?
            .unwrap_or_default())
    }

    #[tracing::instrument(skip(self))]
    async fn unspent_by_token(
        &self,
        token_id: &TokenId,
        page: Page,
    ) -> Result<Vec<IndexedBox>, NodeError> {
        let request = self.get(&format!("blockchain/box/unspent/byTokenId/{token_id}"));
        Ok(fetch_optional(Self::unspent_page(request, page))
            .await?
            .unwrap_or_default())
    }

    #[tracing::instrument(skip(self))]
    async fn token_info(&self, token_id: &TokenId) -> Result<Option<TokenInfo>, NodeError> {
        fetch_optional(self.get(&format!("blockchain/token/byId/{token_id}"))).await
    }

    #[tracing::instrument(skip(self, ids), fields(count = ids.len()))]
    async fn tokens(&self, ids: &[TokenId]) -> Result<Vec<TokenMetadata>, NodeError> {
        fetch(self.post("blockchain/tokens").json(ids)).await
    }

    #[tracing::instrument(skip(self, ids), fields(count = ids.len()))]
    async fn boxes_with_pool(&self, ids: &[BoxId]) -> Result<Vec<UTxO>, NodeError> {
        fetch(self.post("utxo/withPool/byIds").json(ids)).await
    }

    #[tracing::instrument(skip(self))]
    async fn indexed_box(&self, id: &BoxId) -> Result<Option<BoxById>, NodeError> {
        fetch_optional(self.get(&format!("blockchain/box/byId/{id}"))).await
    }

    /// The address goes in the body, as for `/blockchain/balance`: a P2S address can be
    /// thousands of characters. An address the indexer has never seen is an empty page. Waits
    /// up to [`HISTORY_TIMEOUT`], not the client's timeout.
    #[tracing::instrument(skip(self))]
    async fn address_transactions(
        &self,
        ergo_tree: &ErgoTree,
        offset: u32,
        limit: u32,
    ) -> Result<AddressTransactions, NodeError> {
        let address = address::encode(&ergo_tree.0, self.network);
        let request = self
            .post("blockchain/transaction/byAddress")
            .query(&[("offset", offset), ("limit", limit)])
            .json(&address)
            .timeout(HISTORY_TIMEOUT);
        fetch(request).await
    }

    #[tracing::instrument(skip(self))]
    async fn confirmed_balance(&self, ergo_tree: &ErgoTree) -> Result<Balance, NodeError> {
        let address = address::encode(&ergo_tree.0, self.network);
        let response: BalanceResponse =
            fetch(self.post("blockchain/balance").json(&address)).await?;
        Ok(response.confirmed)
    }

    /// The hex goes as it is, as `text/plain`: both routes take plain hex or a JSON string
    /// (node 6.0.8, `TransactionsApiRoute.sendTransactionAsBytesR` and
    /// `checkTransactionAsBytesR`, through `ErgoBaseApiRoute.fromJsonOrPlain`).
    #[tracing::instrument(skip(self, hex), fields(chars = hex.len()))]
    async fn submit_transaction(&self, hex: &str, broadcast: bool) -> Result<TxId, NodeError> {
        let route = if broadcast { "transactions/bytes" } else { "transactions/checkBytes" };
        let request = self
            .post(route)
            .header(reqwest::header::CONTENT_TYPE, "text/plain")
            .body(hex.to_owned());
        fetch(request).await
    }

    /// The address goes in the body, as for `/blockchain/balance`.
    #[tracing::instrument(skip(self))]
    async fn box_count(&self, ergo_tree: &ErgoTree) -> Result<u64, NodeError> {
        let address = address::encode(&ergo_tree.0, self.network);
        let request = self
            .post("blockchain/box/byAddress")
            .query(&[("limit", 0)])
            .json(&address);
        let response: BoxCountResponse = fetch(request).await?;
        Ok(response.total)
    }
}

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
