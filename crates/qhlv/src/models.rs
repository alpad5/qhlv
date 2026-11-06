//! The two core object types (payload-generic) and request/response shapes.
//!
//! The engine owns *lifecycle* (id, timestamps, TTL, the point in the space);
//! the app owns the *payload* (opaque JSON) and the *point*'s interpretation
//! (via its `Space`). Neither the engine nor the wire format cares what the
//! payload or point actually contain.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::space::Space;

/// A root object: something posted into the space, with a lifetime.
///
/// `point` is opaque to the core — only the active `Space` interprets it
/// (geo: fuzzed lat/lng; semantic: an embedding vector + facets).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Object {
    pub id: String,
    pub point: Value,
    pub created_at: i64,    // unix seconds
    pub expires_at: i64,    // unix seconds (hard cap: created_at + hard_cap_secs)
    pub last_activity: i64, // unix seconds (resets on each activity)
    pub activity_count: u64,
    pub payload: Value,
}

/// An activity on an object (a reply, reaction, etc). Posting one resets the
/// parent object's inactivity timer, keeping it alive. Activities are not
/// independently indexed in the space — they live and die with their parent.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Activity {
    pub id: String,
    pub object_id: String,
    pub created_at: i64,
    pub payload: Value,
}

/// Body of `POST /objects`.
#[derive(Debug, Deserialize)]
pub struct CreateObject {
    /// App-defined content. Passed to the app's validator and stored verbatim.
    pub payload: Value,
    /// Space-defined locator (geo: `{lat,lng,noise_sigma?}`; semantic: `{text,...}`).
    /// Passed to `Space::locate` to derive the stored point.
    pub locator: Value,
    /// Opt in to owner-token control. When true the response carries a one-time
    /// `owner_token` and the object can later be edited or deleted with it.
    #[serde(default)]
    pub owned: bool,
}

/// Body of `PUT /objects/:id`: replaces the payload of an owned object.
#[derive(Debug, Deserialize)]
pub struct UpdateObject {
    pub payload: Value,
}

/// Body of `POST /objects/:id/activities`.
#[derive(Debug, Deserialize)]
pub struct CreateActivity {
    pub payload: Value,
}

/// Build the client-facing JSON view of an object.
///
/// The stored `point` is passed through `Space::public_point` first, so a space
/// can strip anything private (e.g. a raw embedding vector) before it leaves the
/// server. The geo space returns its (already fuzzed) coordinates unchanged.
pub fn object_view(obj: &Object, space: &dyn Space) -> Value {
    json!({
        "id": obj.id,
        "point": space.public_point(&obj.point),
        "created_at": obj.created_at,
        "expires_at": obj.expires_at,
        "last_activity": obj.last_activity,
        "activity_count": obj.activity_count,
        "payload": obj.payload,
    })
}
