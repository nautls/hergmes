//! The metrics an operator reads, as OpenMetrics text on `GET /metrics` (DESIGN.md,
//! "Metrics").
//!
//! Aggregates only. No series names a key: which addresses are watched is private to the
//! clients watching them, so every label value comes from a fixed set (an error code, a
//! resync reason, a key's kind). The route is served on its own address, never the public one.
//!
//! Counters are counted where a refusal or a resync goes out to a client. Sizes are read
//! when the route is scraped, so nothing is kept up to date between scrapes.

use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeLabelSet, EncodeMetric, text::encode},
    metrics::{
        counter::{ConstCounter, Counter},
        family::Family,
        gauge::{ConstGauge, Gauge},
    },
    registry::Registry,
};

use crate::{
    adapters::{BoxCache, EngineGone, EngineHandle},
    engine::{Query, Reply, ResyncReason, Stats},
};

/// What the text is, as the crate's encoder writes it.
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Which side refused a request. A WebSocket subscribe loads a key only when it is
/// balance-only, so `too_many_boxes` under `ws` counts the balance subscriptions to
/// addresses over the per-key cap, apart from REST's (PLAN.md, step 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transport {
    Rest,
    Ws,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct Rejection {
    transport: &'static str,
    code: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct Resync {
    reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct KeyKind {
    kind: &'static str,
}

/// The relay's counters and gauges. Cloning shares them.
#[derive(Clone)]
pub struct Metrics(Arc<Inner>);

struct Inner {
    registry: Registry,
    rejections: Family<Rejection, Counter>,
    resyncs: Family<Resync, Counter>,
    tracked_keys: Family<KeyKind, Gauge>,
    tracked_boxes: Gauge,
    ring_blocks: Gauge,
    ring_boxes: Gauge,
    resident_memory: Gauge,
}

impl Metrics {
    /// Metrics reporting `cache`'s size and reads next to the relay's own.
    pub fn new(cache: BoxCache) -> Self {
        let mut registry = Registry::default();
        let rejections = Family::<Rejection, Counter>::default();
        let resyncs = Family::<Resync, Counter>::default();
        let tracked_keys = Family::<KeyKind, Gauge>::default();
        let [tracked_boxes, ring_blocks, ring_boxes, resident_memory] =
            [(); 4].map(|()| Gauge::default());
        // The encoder adds a counter's `_total`.
        registry.register(
            "hergmes_rejections",
            "Requests and subscribes refused, by transport and error code",
            rejections.clone(),
        );
        registry.register(
            "hergmes_resyncs",
            "resync_required messages sent to WebSocket clients, by reason",
            resyncs.clone(),
        );
        registry.register("hergmes_tracked_keys", "Tracked keys, by kind", tracked_keys.clone());
        registry.register(
            "hergmes_tracked_boxes",
            "Tracked boxes, one per box and key",
            tracked_boxes.clone(),
        );
        registry.register("hergmes_ring_blocks", "Blocks in the ring buffer", ring_blocks.clone());
        registry.register(
            "hergmes_ring_boxes",
            "Full boxes held by the ring buffer",
            ring_boxes.clone(),
        );
        registry.register(
            "process_resident_memory_bytes",
            "Resident memory",
            resident_memory.clone(),
        );
        registry.register_collector(Box::new(CacheCollector(cache)));
        Self(Arc::new(Inner {
            registry,
            rejections,
            resyncs,
            tracked_keys,
            tracked_boxes,
            ring_blocks,
            ring_boxes,
            resident_memory,
        }))
    }

    /// Counts a refusal sent to a client, by its error code.
    pub fn rejected(&self, transport: Transport, code: &'static str) {
        let transport = match transport {
            Transport::Rest => "rest",
            Transport::Ws => "ws",
        };
        self.0
            .rejections
            .get_or_create(&Rejection { transport, code })
            .inc();
    }

    /// Counts a `resync_required` sent to a client.
    pub fn resync(&self, reason: ResyncReason) {
        let reason = match reason {
            ResyncReason::OutsideWindow => "outside_window",
            ResyncReason::ForkBeyondWindow => "fork_beyond_window",
            ResyncReason::NotTracked => "not_tracked",
            ResyncReason::SlowConsumer => "slow_consumer",
        };
        self.0.resyncs.get_or_create(&Resync { reason }).inc();
    }

    /// The metrics as OpenMetrics text, with the engine's sizes as of this call.
    pub async fn render(&self, engine: &impl EngineHandle) -> Result<String, EngineGone> {
        let stats = match engine.query(Query::Stats).await? {
            Reply::Stats(stats) => stats,
            other => unreachable!("`Query::Stats` answered {other:?}"),
        };
        Ok(self.encode(&stats, resident_memory()))
    }

    fn encode(&self, stats: &Stats, resident: Option<u64>) -> String {
        let inner = &self.0;
        let kind = |kind| inner.tracked_keys.get_or_create(&KeyKind { kind });
        kind("address").set(gauge(stats.address_keys));
        kind("token").set(gauge(stats.token_keys));
        inner.tracked_boxes.set(gauge(stats.tracked_boxes));
        inner.ring_blocks.set(gauge(stats.ring_blocks));
        inner.ring_boxes.set(gauge(stats.ring_boxes));
        // 0 where the platform has no `/proc`: an absent series would need a second registry.
        inner
            .resident_memory
            .set(resident.map_or(0, |bytes| i64::try_from(bytes).unwrap_or(i64::MAX)));
        let mut text = String::new();
        encode(&mut text, &inner.registry).expect("writing to a string never fails");
        text
    }
}

#[cfg(test)]
impl Metrics {
    /// The value of the series written as `series` (its name and labels), 0 if absent.
    pub fn count(&self, series: &str) -> u64 {
        let text = self.encode(&Stats::default(), None);
        text.lines()
            .find_map(|line| line.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
            .unwrap_or(0)
    }
}

impl Default for Metrics {
    /// Metrics with no cache to report, for tests.
    fn default() -> Self {
        Self::new(BoxCache::disabled())
    }
}

fn gauge(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// The box cache's size and reads, read from the cache on each scrape: its counts live in
/// the cache, so they are encoded as constants rather than copied into counters.
struct CacheCollector(BoxCache);

impl std::fmt::Debug for CacheCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CacheCollector")
    }
}

impl Collector for CacheCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let stats = self.0.stats();
        let mut gauge = |name, help, value: u64| {
            let metric = ConstGauge::new(value);
            metric.encode(encoder.encode_descriptor(name, help, None, metric.metric_type())?)
        };
        gauge("hergmes_box_cache_entries", "Boxes in the box cache.", stats.entries)?;
        gauge(
            "hergmes_box_cache_weighted_bytes",
            "The box cache's estimated size, which its budget bounds.",
            stats.weighted_bytes,
        )?;
        let mut counter = |name, help, value: u64| {
            let metric = ConstCounter::new(value);
            metric.encode(encoder.encode_descriptor(name, help, None, metric.metric_type())?)
        };
        counter("hergmes_box_cache_hits", "Box cache reads that found the box.", stats.hits)?;
        counter("hergmes_box_cache_misses", "Box cache reads that did not.", stats.misses)
    }
}

/// The process's resident memory in bytes, from `VmRSS` in `/proc/self/status`: in kB there,
/// so no page size is needed. `None` where there is no `/proc`.
pub fn resident_memory() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_resident(&status)
}

fn parse_resident(status: &str) -> Option<u64> {
    let line = status.lines().find_map(|l| l.strip_prefix("VmRSS:"))?;
    let kb: u64 = line.trim().strip_suffix("kB")?.trim().parse().ok()?;
    Some(kb * 1024)
}

struct Scrape<E> {
    engine: E,
    metrics: Metrics,
}

/// The metrics route, for a listener of its own: operators read it, the public does not.
pub fn router<E: EngineHandle + 'static>(engine: E, metrics: Metrics) -> Router {
    Router::new()
        .route("/metrics", get(scrape::<E>))
        .with_state(Arc::new(Scrape { engine, metrics }))
}

async fn scrape<E: EngineHandle>(State(scrape): State<Arc<Scrape<E>>>) -> Response {
    match scrape.metrics.render(&scrape.engine).await {
        Ok(text) => ([(CONTENT_TYPE, OPENMETRICS)], text).into_response(),
        // The engine stopped; the process is exiting.
        Err(EngineGone) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(test)]
#[path = "metrics_tests.rs"]
mod tests;
