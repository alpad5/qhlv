//! # qhlv — core engine
//!
//! *que hablen los vecinos* — "may the neighbours speak."
//!
//! The reusable core for **anonymous, ephemeral, proximity-scoped, real-time**
//! apps. An app supplies three things and gets a full HTTP + WebSocket server:
//!
//! 1. a [`Config`] (TTLs and a payload-size backstop),
//! 2. a [`Space`] — how objects are placed and who is "near" whom (use the
//!    built-in [`GeoSpace`], or implement the trait for e.g. a semantic space),
//! 3. a [`Validator`] — a closure that checks the app's opaque payload.
//!
//! ```no_run
//! use std::sync::Arc;
//! # async fn run() {
//! let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
//! let con = redis::aio::ConnectionManager::new(redis).await.unwrap();
//!
//! let state = qhlv::AppState::new(
//!     con,
//!     qhlv::Config::from_env(),
//!     Arc::new(qhlv::GeoSpace::from_env()),
//!     Arc::new(|payload: &serde_json::Value| {
//!         match payload.get("content").and_then(|c| c.as_str()) {
//!             Some(s) if s.len() <= 300 => Ok(()),
//!             _ => Err("content missing or too long".into()),
//!         }
//!     }),
//! );
//!
//! qhlv::spawn_sweep(state.clone());
//! let app = qhlv::router(state);
//! // axum::serve(listener, app).await ...
//! # }
//! ```

mod config;
mod error;
mod models;
mod owner;
mod routes;
mod store;

pub mod space;
pub mod ws;

use axum::routing::{get, post, put};
use axum::Router;
use redis::aio::ConnectionManager;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

pub use config::Config;
pub use error::{Error, Result};
pub use models::{object_view, Activity, CreateActivity, CreateObject, Object};
pub use space::{GeoSpace, Space};
pub use store::{
    add_activity, get_activities, get_object, get_objects, save_object, sweep_expired,
};
pub use ws::{ClientMap, WsEvent};

/// A closure that validates an app's opaque payload. `Err(message)` becomes a 422.
pub type Validator = Arc<dyn Fn(&Value) -> std::result::Result<(), String> + Send + Sync>;

/// Shared server state. Cheap to clone (all fields are `Arc`s or clonable handles).
#[derive(Clone)]
pub struct AppState {
    pub redis: ConnectionManager,
    pub clients: ClientMap,
    pub config: Arc<Config>,
    pub space: Arc<dyn Space>,
    pub validate: Validator,
}

impl AppState {
    pub fn new(
        redis: ConnectionManager,
        config: Config,
        space: Arc<dyn Space>,
        validate: Validator,
    ) -> Self {
        Self {
            redis,
            clients: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(config),
            space,
            validate,
        }
    }
}

/// Build the qhlv HTTP + WebSocket router, ready to mount or serve.
///
/// Routes: `POST /objects`, `PUT|DELETE /objects/:id` (owned objects only),
/// `POST /feed`, `POST|GET /objects/:id/activities`, `GET /ws`.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/objects", post(routes::objects::create))
        .route(
            "/objects/:id",
            put(routes::objects::update).delete(routes::objects::delete),
        )
        .route("/feed", post(routes::objects::feed))
        .route(
            "/objects/:id/activities",
            post(routes::activities::create).get(routes::activities::list),
        )
        .route("/ws", get(ws::ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Spawn the background expiry sweep: every 30s it reconciles the space index
/// against Redis and broadcasts `object_expired` for anything that has died, so
/// clients drop stale objects without a refresh. Returns the task handle.
pub fn spawn_sweep(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let mut con = state.redis.clone();
            match store::sweep_expired(&mut con, state.space.as_ref()).await {
                Ok(expired) => {
                    for object_id in expired {
                        ws::broadcast_all(&state.clients, WsEvent::ObjectExpired { object_id })
                            .await;
                    }
                }
                Err(e) => tracing::warn!("sweep_expired error: {e}"),
            }
        }
    })
}
