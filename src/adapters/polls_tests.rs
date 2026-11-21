use super::*;

/// A run of polls: the first `retry` of attempts that stopped before their reads, ended by one
/// that got past them, lapsed once none was behind for two `retry`s, and left as it is by an
/// error that says nothing of the checks.
#[test]
fn a_run_of_polls_starts_ends_and_lapses() {
    let retry = Duration::from_secs(1);
    let start = Instant::now();
    let at = |ms: u64| start + Duration::from_millis(ms);
    let mut polls = Polls::default();
    assert!(polls.record(at(0), Checks::Behind, retry), "a run's first attempt");
    assert!(polls.record(at(800), Checks::Behind, retry), "within its first retry");
    assert!(!polls.record(at(1_000), Checks::Behind, retry), "past it");
    // A node `503` in the middle of a long run does not start a new one.
    assert!(!polls.record(at(1_500), Checks::Unknown, retry));
    assert!(!polls.record(at(2_000), Checks::Behind, retry), "the same run");
    // Another attempt gets past the checks: the run ends, and the next behind attempt starts
    // one.
    assert!(!polls.record(at(2_100), Checks::Passed, retry));
    assert!(polls.record(at(2_200), Checks::Behind, retry), "a new run");
    assert!(!polls.record(at(3_200), Checks::Behind, retry));
    // Every attempt gave up and the next comes long after: a new run.
    assert!(!polls.record(at(5_200), Checks::Behind, retry), "two retries later, still one");
    assert!(polls.record(at(7_201), Checks::Behind, retry), "lapsed");
}
