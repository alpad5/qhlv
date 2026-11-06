//! The `Space` abstraction: the pluggable notion of "where objects live and who
//! is near whom." Geo is one implementation; a semantic (embedding) space is
//! another. Everything geo-specific in the original app — coordinate fuzzing,
//! `GEOADD`/`GEOSEARCH`, the haversine fan-out test — lives behind this trait.
//!
//! Points and viewports are opaque `serde_json::Value` to the core, exactly like
//! the app payload. Only the active `Space` deserializes and interprets them, so
//! the engine never needs to know whether "near" means metres or cosine distance.

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use serde_json::Value;

use crate::error::Result;

pub mod geo;
pub use geo::GeoSpace;

#[async_trait]
pub trait Space: Send + Sync {
    /// Derive the object's stored point from the client's raw locator + payload.
    /// This is where any privacy transform lives (geo fuzzes here; a semantic
    /// space would embed the text here). May be async — embedding is a network call.
    async fn locate(&self, locator: Value, payload: &Value) -> Result<Value>;

    /// Add an object's point to the space's index so it can be found by `query`.
    async fn index(&self, con: &mut ConnectionManager, id: &str, point: &Value) -> Result<()>;

    /// Remove an object from the index (called when it expires).
    async fn remove(&self, con: &mut ConnectionManager, id: &str) -> Result<()>;

    /// The feed: return the ids of objects matching a viewport — the nearest
    /// neighbours within threshold, filtered by any metadata facets (hybrid query).
    async fn query(&self, con: &mut ConnectionManager, viewport: &Value) -> Result<Vec<String>>;

    /// Every id currently in the index. Used by the expiry sweep to reconcile the
    /// index against objects Redis has already dropped.
    async fn all_indexed_ids(&self, con: &mut ConnectionManager) -> Result<Vec<String>>;

    /// The WebSocket fan-out test: is `point` inside `viewport`? Called per
    /// connected client on every new object/activity, so it must be cheap and sync.
    fn matches(&self, viewport: &Value, point: &Value) -> bool;

    /// Project a stored point down to what is safe to return to clients. Defaults
    /// to returning it unchanged (fine for geo — coordinates are already fuzzed).
    /// A semantic space overrides this to strip the raw embedding vector.
    fn public_point(&self, point: &Value) -> Value {
        point.clone()
    }
}
