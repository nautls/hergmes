use std::ffi::OsStr;

use super::{ARENA_MAX, cap};

fn os(s: &str) -> Option<&OsStr> {
    Some(OsStr::new(s))
}

#[test]
fn caps_when_the_environment_sets_nothing() {
    assert_eq!(cap(None, None), Some(ARENA_MAX));
}

#[test]
fn malloc_arena_max_wins() {
    assert_eq!(cap(os("8"), None), None);
    assert_eq!(cap(os("8"), os("glibc.malloc.tcache_count=0")), None);
}

#[test]
fn an_arena_max_tunable_wins() {
    assert_eq!(cap(None, os("glibc.malloc.arena_max=2")), None);
    assert_eq!(cap(None, os("glibc.malloc.tcache_count=0:glibc.malloc.arena_max=2")), None);
}

#[test]
fn other_tunables_still_cap() {
    assert_eq!(cap(None, os("glibc.malloc.tcache_count=0")), Some(ARENA_MAX));
    // A name that only starts like it is another tunable.
    assert_eq!(cap(None, os("glibc.malloc.arena_max_x=2")), Some(ARENA_MAX));
    assert_eq!(cap(None, os("")), Some(ARENA_MAX));
}

#[test]
fn values_glibc_ignores_still_cap() {
    for value in ["", "0", "-1", "four"] {
        assert_eq!(cap(os(value), None), Some(ARENA_MAX), "MALLOC_ARENA_MAX={value:?}");
        let tunable = format!("glibc.malloc.arena_max={value}");
        assert_eq!(cap(None, os(&tunable)), Some(ARENA_MAX), "{tunable:?}");
    }
    assert_eq!(cap(None, os("glibc.malloc.arena_max")), Some(ARENA_MAX));
}
