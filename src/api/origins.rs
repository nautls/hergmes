//! The operator's origin policy: which browser pages and extensions may use the relay.
//!
//! It keeps other sites from building on a relay run for one wallet or dapp, and nothing more.
//! A browser sets `Origin` on every cross-origin request and WebSocket handshake, and a page
//! can't change or drop it, so the check holds against pages. A program outside a browser can
//! send any `Origin` or none: a request without one is let through, since `curl`, servers and
//! native apps send none, and only rate limits stand in their way. CORS does not cover the
//! WebSocket, so the check runs ahead of every route, the upgrade included.

use std::{fmt, sync::Arc};

use axum::{
    Router,
    extract::{Request, State},
    http::header::ORIGIN,
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
};

use crate::api::{ApiError, Metrics, metrics::Transport};

/// Which origins are admitted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum OriginPolicy {
    /// `["*"]`, or the setting omitted: every origin.
    #[default]
    Any,
    /// Only these, and requests without an `Origin`; empty, only those.
    Listed {
        /// Whole origins, as browsers write them: `scheme://host[:port]`, lowercase.
        exact: Vec<String>,
        /// Schemes admitted whole, from `scheme://*`: an extension's origin has a host that
        /// is its id, which Firefox and Safari draw at random for each install.
        schemes: Vec<String>,
    },
}

impl OriginPolicy {
    /// Reads the entries of `allow_origins`: `*` alone, or any of `scheme://*` and exact
    /// origins. An entry a browser would never send is refused, so a typo is seen at startup
    /// rather than as refused pages.
    pub fn parse(entries: &[String]) -> Result<Self, String> {
        if entries.iter().any(|e| e == "*") {
            return match entries.len() {
                1 => Ok(Self::Any),
                _ => Err("\"*\" allows every origin, so it is listed alone".to_owned()),
            };
        }
        let (mut exact, mut schemes) = (Vec::new(), Vec::new());
        for entry in entries {
            let fail = |reason: &str| format!("{entry:?}: {reason}");
            let lower = entry.to_ascii_lowercase();
            let Some((scheme, rest)) = lower.split_once("://") else {
                return Err(fail("an origin is scheme://host[:port], or scheme://* for a scheme"));
            };
            let scheme_ok = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
            if !scheme_ok {
                return Err(fail("not a URL scheme"));
            }
            if rest == "*" {
                schemes.push(scheme.to_owned());
                continue;
            }
            if rest.is_empty() || rest.contains(['/', '*', '?', '#', '@']) {
                return Err(fail(
                    "an origin has no path, no trailing slash and no wildcard but scheme://*",
                ));
            }
            if rest.chars().any(char::is_whitespace) {
                return Err(fail("an origin has no spaces"));
            }
            if !rest.is_ascii() {
                return Err(fail("browsers send a domain in its ASCII form: write it as xn--..."));
            }
            // The port follows the last `:`, unless that is inside an IPv6 address's brackets.
            let (host, port) = match rest.rsplit_once(':') {
                Some((host, port)) if !rest.ends_with(']') => (host, Some(port)),
                _ => (rest, None),
            };
            let bracketed = host.starts_with('[') && host.ends_with(']');
            if host.is_empty() || (host.contains(':') && !bracketed) {
                return Err(fail("not a host"));
            }
            if let Some(port) = port {
                // Browsers write a port as a plain number, and leave the scheme's default out,
                // so any other spelling never matches.
                let number = port.parse::<u16>().ok().filter(|n| *n > 0);
                let Some(number) = number.filter(|n| n.to_string() == port) else {
                    return Err(fail("a port is a number from 1 to 65535, without leading zeros"));
                };
                let default = match scheme {
                    "http" => Some(80),
                    "https" => Some(443),
                    _ => None,
                };
                if default == Some(number) {
                    return Err(fail("browsers leave the default port out of an origin"));
                }
            }
            exact.push(lower);
        }
        Ok(Self::Listed { exact, schemes })
    }

    /// Whether a request with this `Origin` header, or none, is admitted. Origins are compared
    /// without case, as their scheme and host are.
    pub fn admits(&self, origin: Option<&[u8]>) -> bool {
        let Self::Listed { exact, schemes } = self else { return true };
        let Some(origin) = origin else { return true };
        let Ok(origin) = std::str::from_utf8(origin) else { return false };
        exact.iter().any(|entry| entry.eq_ignore_ascii_case(origin))
            || schemes.iter().any(|scheme| {
                origin
                    .split_once("://")
                    .is_some_and(|(s, _)| s.eq_ignore_ascii_case(scheme))
            })
    }
}

impl fmt::Display for OriginPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => write!(f, "any"),
            Self::Listed { exact, schemes } => {
                let mut entries: Vec<String> = schemes.iter().map(|s| format!("{s}://*")).collect();
                entries.extend(exact.iter().cloned());
                entries.push("no Origin".to_owned());
                write!(f, "{}", entries.join(", "))
            }
        }
    }
}

struct Guard {
    policy: OriginPolicy,
    metrics: Metrics,
}

/// `routes` behind the policy: a request from an origin it does not admit is refused with
/// `403 origin_not_allowed` before any route or CORS sees it, preflights included. The refusal
/// carries no CORS headers, so the page can't read it; the browser's console shows the
/// status.
pub fn guard(routes: Router, policy: OriginPolicy, metrics: Metrics) -> Router {
    if policy == OriginPolicy::Any {
        return routes;
    }
    routes.layer(from_fn_with_state(Arc::new(Guard { policy, metrics }), check))
}

async fn check(State(guard): State<Arc<Guard>>, request: Request, next: Next) -> Response {
    let mut origins = request.headers().get_all(ORIGIN).iter();
    // Browsers send one `Origin`; two are not a browser's, and are refused rather than judged
    // by either.
    let first = origins.next();
    let admitted = origins.next().is_none() && guard.policy.admits(first.map(|o| o.as_bytes()));
    if admitted {
        return next.run(request).await;
    }
    let transport = match request.uri().path() {
        "/v1/ws" => Transport::Ws,
        _ => Transport::Rest,
    };
    let error = ApiError::OriginNotAllowed;
    guard.metrics.rejected(transport, error.code());
    error.into_response()
}

#[cfg(test)]
#[path = "origins_tests.rs"]
mod tests;
