//! The process's open-file limit, raised at startup.
//!
//! Each WebSocket connection holds a file descriptor, and systemd starts services with a soft
//! limit of 1,024 under a hard limit of 524,288. At the soft limit `accept` fails with
//! `EMFILE`, so the relay can't take much more than a thousand connections. The soft limit is
//! the process's own to raise up to the hard one, so the relay does that rather than leave it
//! to each deployment.

/// Below this many open files, the relay warns: a few thousand connections is a load it
/// should take on any deployment.
pub const LOW: u64 = 4_096;

/// The open-file limit before and after raising.
#[derive(Debug)]
pub struct OpenFiles {
    pub before: u64,
    /// The soft limit in force: the raised one, or `before` if raising failed or was not
    /// needed.
    pub now: u64,
    /// Why raising failed.
    pub refused: Option<std::io::Error>,
}

/// The soft limit to set, or `None` to leave it. `hard` is `None` when unlimited: the soft
/// limit is then left as it is, since Linux refuses one above `fs.nr_open` and guessing a
/// value under it could fail.
pub fn target(soft: u64, hard: Option<u64>) -> Option<u64> {
    hard.filter(|&hard| soft < hard)
}

/// Raises the soft open-file limit to the hard one. Fails only if the limit can't be read.
// `rlim_t` is `u64` on Linux but not on every Unix.
#[allow(clippy::unnecessary_cast)]
pub fn raise_open_files() -> std::io::Result<OpenFiles> {
    use std::io::Error;

    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: `getrlimit` writes the limit into `limit`, a valid `rlimit`.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(Error::last_os_error());
    }
    let before = limit.rlim_cur as u64;
    let hard = (limit.rlim_max != libc::RLIM_INFINITY).then_some(limit.rlim_max as u64);
    let Some(now) = target(before, hard) else {
        return Ok(OpenFiles { before, now: before, refused: None });
    };
    limit.rlim_cur = now as libc::rlim_t;
    // SAFETY: `setrlimit` reads `limit`, a valid `rlimit`.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        let refused = Some(Error::last_os_error());
        return Ok(OpenFiles { before, now: before, refused });
    }
    Ok(OpenFiles { before, now, refused: None })
}

#[cfg(test)]
#[path = "limits_tests.rs"]
mod tests;
