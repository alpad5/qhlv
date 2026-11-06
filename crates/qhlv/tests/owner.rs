//! End-to-end check of owner tokens and the activity-extends-to-cap flag against
//! a throwaway local redis-server (port 6391). Requires `redis-server` on PATH.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use tower::ServiceExt;

struct Redis(Child);
impl Drop for Redis {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

async fn start(extends_to_cap: bool) -> (Redis, axum::Router, redis::aio::ConnectionManager) {
    let child = Command::new("redis-server")
        .args(["--port", "6391", "--save", "", "--appendonly", "no"])
        .stdout(Stdio::null())
        .spawn()
        .expect("redis-server on PATH");
    let guard = Redis(child);
    let client = redis::Client::open("redis://127.0.0.1:6391/").unwrap();
    let mut con = None;
    for _ in 0..50 {
        if let Ok(c) = redis::aio::ConnectionManager::new(client.clone()).await {
            con = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let con = con.expect("redis did not start");

    let config = qhlv::Config {
        inactivity_ttl_secs: 20,
        hard_cap_secs: 30,
        max_payload_bytes: 4096,
        activity_extends_to_cap: extends_to_cap,
    };
    let state = qhlv::AppState::new(
        con.clone(),
        config,
        Arc::new(qhlv::GeoSpace::from_env()),
        Arc::new(|_: &Value| Ok(())),
    );
    (guard, qhlv::router(state), con)
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    if let Some(t) = token {
        req = req.header("x-owner-token", t);
    }
    let req = req
        .body(match body {
            Some(b) => Body::from(b.to_string()),
            None => Body::empty(),
        })
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn create_body(owned: Option<bool>) -> Value {
    let mut b = json!({"payload": {"content": "hi"}, "locator": {"lat": 40.0, "lng": -3.0}});
    if let Some(o) = owned {
        b["owned"] = json!(o);
    }
    b
}

#[tokio::test]
async fn owner_token_and_ttl_flag() {
    let (_redis, app, mut con) = start(true).await;

    // Unowned (hlv-style) object: no token in the response, cannot be edited/deleted.
    let (s, plain) = call(&app, "POST", "/objects", None, Some(create_body(None))).await;
    assert_eq!(s, StatusCode::OK);
    assert!(plain.get("owner_token").is_none());
    let plain_id = plain["id"].as_str().unwrap();
    let (s, _) = call(&app, "DELETE", &format!("/objects/{plain_id}"), Some("x"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // Owned object: token returned once, never stored in clear.
    let (s, owned) = call(&app, "POST", "/objects", None, Some(create_body(Some(true)))).await;
    assert_eq!(s, StatusCode::OK);
    let id = owned["id"].as_str().unwrap().to_string();
    let token = owned["owner_token"].as_str().unwrap().to_string();
    assert_eq!(token.len(), 64);
    let stored: String = redis::cmd("HGET")
        .arg(format!("object:{id}"))
        .arg("owner_hash")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_ne!(stored, token);

    // Wrong or missing token is forbidden; right token edits.
    let uri = format!("/objects/{id}");
    let new_body = json!({"payload": {"content": "edited"}});
    let (s, _) = call(&app, "PUT", &uri, Some("wrong"), Some(new_body.clone())).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _) = call(&app, "PUT", &uri, None, Some(new_body.clone())).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, edited) = call(&app, "PUT", &uri, Some(&token), Some(new_body)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(edited["payload"]["content"], "edited");
    assert!(edited.get("owner_token").is_none());

    // Feed never leaks the token.
    let (_, feed) = call(
        &app,
        "POST",
        "/feed",
        None,
        Some(json!({"lat": 40.0, "lng": -3.0, "radius_km": 5.0})),
    )
    .await;
    assert!(feed.to_string().find("owner").is_none());

    // First activity extends life straight to the cap (flag on): TTL ~30 > 20.
    let ttl_before: i64 = redis::cmd("TTL").arg(format!("object:{id}")).query_async(&mut con).await.unwrap();
    assert!(ttl_before <= 20);
    let (s, _) = call(
        &app,
        "POST",
        &format!("/objects/{id}/activities"),
        None,
        Some(json!({"payload": {"content": "contact"}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let ttl_after: i64 = redis::cmd("TTL").arg(format!("object:{id}")).query_async(&mut con).await.unwrap();
    assert!((29..=30).contains(&ttl_after), "ttl_after = {ttl_after}");

    // Delete: right token removes it, then it is gone.
    let (s, _) = call(&app, "DELETE", &uri, Some("wrong"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _) = call(&app, "DELETE", &uri, Some(&token), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, _) = call(&app, "DELETE", &uri, Some(&token), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = call(&app, "PUT", &uri, Some(&token), Some(json!({"payload": {}}))).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let exists: bool = redis::cmd("EXISTS").arg(format!("object:{id}")).query_async(&mut con).await.unwrap();
    assert!(!exists);
}
