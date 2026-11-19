//! glibc malloc settings the relay applies at startup.
//!
//! glibc gives each thread that meets a locked arena a new one, up to eight per core, and
//! keeps the memory each arena frees for reuse by that arena alone. Under saturation that
//! left about a third of what the relay took from the OS free but held. Four arenas kept
//! nearly all of the memory saving of two for half the throughput cost under burst load, so
//! the relay caps them at four unless the operator chose otherwise.

use std::ffi::OsStr;

/// The arena cap when the environment sets none.
pub const ARENA_MAX: i32 = 4;

/// What became of the arena setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arenas {
    /// Capped by the relay at this many.
    Capped(i32),
    /// Left to glibc, which reads the operator's setting from the environment.
    FromEnvironment,
    /// glibc refused the setting.
    Refused,
    /// Not built against glibc: nothing to set.
    NotGlibc,
}

/// The cap to set, or `None` when the operator set one: through `MALLOC_ARENA_MAX`, or
/// `glibc.malloc.arena_max` in `GLIBC_TUNABLES` (a `:`-separated list of `name=value`). glibc
/// reads both itself, and a `mallopt` call would override them. A value that is not a
/// positive integer (empty, `0`, junk) leaves glibc on its default, so it counts as unset.
pub fn cap(malloc_arena_max: Option<&OsStr>, glibc_tunables: Option<&OsStr>) -> Option<i32> {
    if malloc_arena_max.is_some_and(positive) {
        return None;
    }
    let tuned = glibc_tunables.is_some_and(|tunables| {
        tunables.to_string_lossy().split(':').any(|tunable| {
            tunable
                .split_once('=')
                .is_some_and(|(name, value)| name == "glibc.malloc.arena_max" && positive(value))
        })
    });
    (!tuned).then_some(ARENA_MAX)
}

fn positive(value: impl AsRef<OsStr>) -> bool {
    value
        .as_ref()
        .to_str()
        .and_then(|v| v.parse::<u32>().ok())
        .is_some_and(|n| n > 0)
}

/// Applies [`cap`] to the process. Call it before any thread starts: glibc fixes its arena
/// limit the first time it needs a new arena past its eighth (glibc `malloc/arena.c`,
/// `arena_get2`, the `narenas_limit` static), so a later call may change nothing.
pub fn configure() -> Arenas {
    if !cfg!(all(target_os = "linux", target_env = "gnu")) {
        return Arenas::NotGlibc;
    }
    let cap = cap(
        std::env::var_os("MALLOC_ARENA_MAX").as_deref(),
        std::env::var_os("GLIBC_TUNABLES").as_deref(),
    );
    match cap {
        Some(arenas) => set_arena_max(arenas),
        None => Arenas::FromEnvironment,
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn set_arena_max(arenas: i32) -> Arenas {
    // SAFETY: `mallopt` only sets a malloc parameter; it returns 1 on success, 0 on error.
    match unsafe { libc::mallopt(libc::M_ARENA_MAX, arenas) } {
        1 => Arenas::Capped(arenas),
        _ => Arenas::Refused,
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn set_arena_max(_arenas: i32) -> Arenas {
    Arenas::NotGlibc
}

#[cfg(test)]
#[path = "allocator_tests.rs"]
mod tests;
