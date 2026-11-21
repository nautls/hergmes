//! The metrics text: every series by name, the counters by label, the cache's own counts,
//! and the route.

use std::sync::{Arc, Mutex};

use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::*;
use crate::{
    engine::{BlockRef, Engine},
    sim::testing::{digest, utxo},
    types::Digest,
};

fn sizes() -> Stats {
    Stats {
        tip: Some(BlockRef { height: 7, header_id: Digest([7; 32]) }),
        address_keys: 3,
        balance_keys: 4,
        token_keys: 2,
        tracked_boxes: 40,
        ring_blocks: 5,
        ring_boxes: 11,
        evictable_boxes: 6,
        evicted_keys: 9,
    }
}

/// The value lines: comments (`# HELP`, `# TYPE`, `# EOF`) left out.
fn lines(text: &str) -> Vec<&str> {
    text.lines().filter(|l| !l.starts_with('#')).collect()
}

#[test]
fn every_series_is_written_with_the_engines_sizes() {
    let cache = BoxCache::new(1 << 20);
    let b = Arc::new(utxo(1, 0, 0, &[]));
    cache.insert(b.clone());
    assert!(cache.get(&b.id).is_some());
    assert!(cache.get(&digest(9)).is_none() && cache.get(&digest(8)).is_none());
    let weight = crate::adapters::cache::weight(&b);

    let metrics = Metrics::new(cache, Timings::default());
    metrics.rejected(Transport::Rest, "invalid_limit");
    metrics.rejected(Transport::Ws, "too_many_boxes");
    metrics.rejected(Transport::Ws, "too_many_boxes");
    metrics.resync(ResyncReason::SlowConsumer);
    let text = metrics.encode(&sizes(), Some(4096));

    // The engine task's timings have a test of their own.
    let mut lines: Vec<_> = lines(&text)
        .into_iter()
        .filter(|l| !l.starts_with("hergmes_engine_"))
        .collect();
    lines.sort_unstable();
    let weighted = format!("hergmes_box_cache_weighted_bytes {weight}");
    let mut expected = vec![
        "hergmes_box_cache_entries 1",
        "hergmes_box_cache_hits_total 1",
        "hergmes_box_cache_misses_total 2",
        &weighted,
        "hergmes_rejections_total{transport=\"rest\",code=\"invalid_limit\"} 1",
        "hergmes_rejections_total{transport=\"ws\",code=\"too_many_boxes\"} 2",
        "hergmes_resyncs_total{reason=\"slow_consumer\"} 1",
        "hergmes_evictable_boxes 6",
        "hergmes_evicted_keys_total 9",
        "hergmes_ring_blocks 5",
        "hergmes_ring_boxes 11",
        "hergmes_tracked_boxes 40",
        "hergmes_tracked_keys{kind=\"address\"} 3",
        "hergmes_tracked_keys{kind=\"balance\"} 4",
        "hergmes_tracked_keys{kind=\"token\"} 2",
        "process_resident_memory_bytes 4096",
    ];
    expected.sort_unstable();
    assert_eq!(lines, expected, "{text}");
    assert!(text.ends_with("# EOF\n"), "OpenMetrics text ends with its marker");
}

/// Sizes are as of the scrape, not summed over scrapes; counters only grow.
#[test]
fn gauges_are_replaced_and_counters_kept_between_scrapes() {
    let metrics = Metrics::default();
    metrics.resync(ResyncReason::NotTracked);
    metrics.encode(&sizes(), None);
    let text = metrics.encode(&Stats::default(), None);
    for line in [
        "hergmes_tracked_keys{kind=\"address\"} 0",
        "hergmes_tracked_boxes 0",
        "hergmes_ring_boxes 0",
        "hergmes_resyncs_total{reason=\"not_tracked\"} 1",
        "process_resident_memory_bytes 0",
    ] {
        assert!(lines(&text).contains(&line), "{line} in {text}");
    }
}

/// Every message the task takes is counted once in the inbox wait, and once under its kind.
/// The runtime is single-threaded, so the task has timed a message before its caller resumes.
#[tokio::test]
async fn the_engine_tasks_messages_are_timed_by_kind() {
    let (engine, _task) = crate::runtime::spawn(Engine::new(Default::default()));
    let metrics = Metrics::new(BoxCache::disabled(), engine.timings());
    engine.query(Query::Tip).await.unwrap();
    engine.query(Query::Tip).await.unwrap();
    metrics.render(&engine).await.unwrap();
    let text = metrics.encode(&Stats::default(), None);
    for line in [
        // The scrape's own `Stats` is in the second encode.
        "hergmes_engine_inbox_wait_seconds_count 3",
        "hergmes_engine_message_seconds_count{message=\"query_tip\"} 2",
        "hergmes_engine_message_seconds_count{message=\"query_stats\"} 1",
    ] {
        assert!(lines(&text).contains(&line), "{line} in {text}");
    }
    // Times, not only counts: a message takes some time to wait for and to answer.
    for sum in [
        "hergmes_engine_inbox_wait_seconds_sum ",
        "hergmes_engine_message_seconds_sum{message=\"query_tip\"} ",
    ] {
        let seconds: f64 = lines(&text)
            .into_iter()
            .find_map(|l| l.strip_prefix(sum)?.parse().ok())
            .unwrap_or_else(|| panic!("{sum} in {text}"));
        assert!(seconds > 0.0, "{sum}{seconds}");
    }
    let buckets = lines(&text)
        .into_iter()
        .filter(|l| l.starts_with("hergmes_engine_inbox_wait_seconds_bucket"))
        .count();
    assert_eq!(buckets, 24, "23 bounds and +Inf: {text}");
}

#[test]
fn every_resync_reason_has_its_own_label() {
    let metrics = Metrics::default();
    let reasons = [
        (ResyncReason::OutsideWindow, "outside_window"),
        (ResyncReason::ForkBeyondWindow, "fork_beyond_window"),
        (ResyncReason::NotTracked, "not_tracked"),
        (ResyncReason::SlowConsumer, "slow_consumer"),
    ];
    for (reason, label) in reasons {
        metrics.resync(reason);
        // The label is the reason as the WebSocket spells it.
        assert_eq!(serde_json::to_value(reason).unwrap(), label);
        assert_eq!(metrics.count(&format!("hergmes_resyncs_total{{reason=\"{label}\"}}")), 1);
    }
}

#[test]
fn resident_memory_is_read_from_vm_rss_in_kilobytes() {
    let status = "Name:\thergmes\nVmHWM:\t   52000 kB\nVmRSS:\t   51200 kB\nThreads:\t9\n";
    assert_eq!(parse_resident(status), Some(51200 * 1024));
    assert_eq!(parse_resident("Name:\thergmes\n"), None);
    assert_eq!(parse_resident("VmRSS:\t lots kB\n"), None);
    if cfg!(target_os = "linux") {
        assert!(resident_memory().is_some_and(|bytes| bytes > 1 << 20));
    }
}

#[tokio::test]
async fn the_route_serves_openmetrics_text_with_the_engines_sizes() {
    let engine = Mutex::new(Engine::new(Default::default()));
    let router = router(engine, Metrics::default());
    let request = Request::get("/metrics").body(Body::empty()).unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], OPENMETRICS);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(lines(&text).contains(&"hergmes_tracked_boxes 0"), "{text}");
    assert!(lines(&text).contains(&"hergmes_box_cache_entries 0"), "a disabled cache: {text}");
}
