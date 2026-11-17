//! The policy's parsing and matching, and the guard over a router: refused before any route
//! or CORS, counted, and absent when every origin is allowed.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header::ORIGIN},
    routing::get,
};
use tower::ServiceExt;

use super::*;

fn policy(entries: &[&str]) -> OriginPolicy {
    let entries: Vec<String> = entries.iter().map(|e| e.to_string()).collect();
    OriginPolicy::parse(&entries).unwrap()
}

fn refused(entry: &str) -> String {
    OriginPolicy::parse(&[entry.to_owned()]).unwrap_err()
}

const EXTENSIONS: [&str; 3] =
    ["chrome-extension://*", "moz-extension://*", "safari-web-extension://*"];

#[test]
fn a_star_alone_allows_any_origin() {
    assert_eq!(policy(&["*"]), OriginPolicy::Any);
    assert_eq!(OriginPolicy::default(), OriginPolicy::Any, "the setting omitted");
    for origin in ["https://a.com", "null", "chrome-extension://abc"] {
        assert!(OriginPolicy::Any.admits(Some(origin.as_bytes())), "{origin}");
    }
    let mixed = OriginPolicy::parse(&["*".to_owned(), "https://a.com".to_owned()]);
    assert!(mixed.unwrap_err().contains("alone"));
}

#[test]
fn a_request_without_an_origin_is_always_admitted() {
    for policy in [policy(&[]), policy(&["https://a.com"]), policy(&EXTENSIONS)] {
        assert!(policy.admits(None), "{policy}");
    }
}

#[test]
fn an_empty_list_admits_no_origin() {
    let policy = policy(&[]);
    for origin in ["https://a.com", "null", "chrome-extension://abc"] {
        assert!(!policy.admits(Some(origin.as_bytes())), "{origin}");
    }
    assert_eq!(policy.to_string(), "no Origin");
}

/// Scheme, host and port all count; case does not.
#[test]
fn an_exact_origin_admits_only_itself() {
    let policy = policy(&["http://Test.com"]);
    assert!(policy.admits(Some(b"http://test.com")));
    assert!(policy.admits(Some(b"HTTP://TEST.COM")));
    for origin in [
        "https://test.com",
        "http://www.test.com",
        "http://test.com:8080",
        "http://test.com.evil.com",
        "http://evil.com",
        "null",
        "chrome-extension://abc",
    ] {
        assert!(!policy.admits(Some(origin.as_bytes())), "{origin}");
    }
}

#[test]
fn a_scheme_entry_admits_every_origin_of_the_scheme() {
    let policy = policy(&EXTENSIONS);
    for origin in
        ["chrome-extension://abcdef", "moz-extension://1b2c-uuid", "safari-web-extension://x"]
    {
        assert!(policy.admits(Some(origin.as_bytes())), "{origin}");
    }
    for origin in [
        "https://a.com",
        "http://chrome-extension.com",
        "chrome-extensionx://abc",
        "chrome-extension:abc",
        "null",
    ] {
        assert!(!policy.admits(Some(origin.as_bytes())), "{origin}");
    }
    assert_eq!(
        policy.to_string(),
        "chrome-extension://*, moz-extension://*, safari-web-extension://*, no Origin"
    );
}

#[test]
fn an_origin_that_is_not_utf8_is_refused() {
    assert!(!policy(&EXTENSIONS).admits(Some(b"chrome-extension://\xff")));
}

/// Each an entry a browser never sends, so it would match nothing.
#[test]
fn an_entry_a_browser_never_sends_is_refused() {
    for entry in [
        "test.com",
        "https://test.com/",
        "https://test.com/path",
        "https://*.test.com",
        "https://",
        "https://test.com:443",
        "http://test.com:80",
        "1http://test.com",
        "null",
        "",
        "https://test.com:",
        "https://test.com:abc",
        "https://test.com:+8443",
        "https://test.com:08443",
        "https://test.com:0",
        "https://test.com:65536",
        "https://test.com ",
        " https://test.com",
        "https://te st.com",
        "https://bücher.example",
        "https://:8443",
        "https://a:b:8443",
        "https://[::1]:443",
    ] {
        let reason = refused(entry);
        assert!(reason.starts_with(&format!("{entry:?}")), "{entry}: {reason}");
    }
    assert!(refused("https://bücher.example").contains("xn--"));
}

#[test]
fn an_origin_a_browser_sends_is_accepted() {
    for entry in [
        "https://test.com:8443",
        "http://localhost:5173",
        "https://xn--bcher-kva.example",
        "http://127.0.0.1:65535",
        "http://[::1]",
        "http://[::1]:8080",
        "tauri://localhost",
    ] {
        assert!(OriginPolicy::parse(&[entry.to_owned()]).is_ok(), "{entry}");
    }
}

fn app(policy: OriginPolicy, metrics: Metrics) -> Router {
    let routes = Router::new()
        .route("/v1/status", get(|| async { "ok" }))
        .route("/v1/ws", get(|| async { "upgraded" }));
    guard(routes, policy, metrics)
}

async fn status(app: &Router, method: Method, path: &str, origins: &[&str]) -> StatusCode {
    let mut request = Request::builder().method(method).uri(path);
    for origin in origins {
        request = request.header(ORIGIN, *origin);
    }
    let request = request.body(Body::empty()).unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

#[tokio::test]
async fn the_guard_refuses_an_origin_before_any_route_and_counts_it() {
    let metrics = Metrics::default();
    let app = app(policy(&EXTENSIONS), metrics.clone());
    let ok = StatusCode::OK;
    assert_eq!(status(&app, Method::GET, "/v1/status", &[]).await, ok);
    assert_eq!(status(&app, Method::GET, "/v1/status", &["chrome-extension://a"]).await, ok);
    assert_eq!(status(&app, Method::GET, "/v1/ws", &["moz-extension://b"]).await, ok);

    let forbidden = StatusCode::FORBIDDEN;
    assert_eq!(status(&app, Method::GET, "/v1/status", &["https://a.com"]).await, forbidden);
    assert_eq!(status(&app, Method::OPTIONS, "/v1/status", &["https://a.com"]).await, forbidden);
    assert_eq!(status(&app, Method::GET, "/v1/ws", &["https://a.com"]).await, forbidden);
    assert_eq!(status(&app, Method::GET, "/v1/nowhere", &["null"]).await, forbidden);
    // Two, even if one is admitted: no browser sends that.
    let two = ["chrome-extension://a", "https://a.com"];
    assert_eq!(status(&app, Method::GET, "/v1/status", &two).await, forbidden);

    let series = |transport| {
        format!("hergmes_rejections_total{{transport=\"{transport}\",code=\"origin_not_allowed\"}}")
    };
    assert_eq!(metrics.count(&series("rest")), 4);
    assert_eq!(metrics.count(&series("ws")), 1);
}

#[tokio::test]
async fn the_refusal_is_the_api_error_body() {
    let app = app(policy(&[]), Metrics::default());
    let request = Request::get("/v1/status")
        .header(ORIGIN, "https://a.com")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"], "origin_not_allowed");
}

#[tokio::test]
async fn with_any_origin_the_guard_is_not_there() {
    let metrics = Metrics::default();
    let app = app(OriginPolicy::Any, metrics.clone());
    let origins = ["https://a.com", "null"];
    assert_eq!(status(&app, Method::GET, "/v1/status", &origins).await, StatusCode::OK);
}
