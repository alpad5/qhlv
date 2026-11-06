//! All Redis operations for object/activity lifecycle. Purely payload- and
//! space-agnostic: the object record stores the opaque `payload` and `point`
//! blobs verbatim, and spatial indexing is delegated to the active `Space`.
//!
//! Redis layout (one server per app, so key names are fixed and un-namespaced):
//!   object:{id}            → hash with TTL (lifecycle fields + payload + point)
//!   object:{id}:activities → list with TTL

use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Result;
use crate::models::{Activity, Object};
use crate::space::Space;

/// Current unix time in seconds.
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn object_key(id: &str) -> String {
    format!("object:{id}")
}

fn activities_key(id: &str) -> String {
    format!("object:{id}:activities")
}

/// Store a new object as a hash with the configured inactivity TTL. The point is
/// added to the space index separately by the caller (via `Space::index`), which
/// keeps the store free of any geo/vector specifics.
pub async fn save_object(
    con: &mut ConnectionManager,
    obj: &Object,
    inactivity_ttl_secs: i64,
) -> Result<()> {
    save_object_owned(con, obj, inactivity_ttl_secs, None).await
}

/// Like [`save_object`], optionally recording the SHA-256 hash of an owner token
/// in the same write so an owned object is never visible without its owner.
pub async fn save_object_owned(
    con: &mut ConnectionManager,
    obj: &Object,
    inactivity_ttl_secs: i64,
    owner_hash: Option<&str>,
) -> Result<()> {
    let key = object_key(&obj.id);
    let mut fields = vec![
        ("id", obj.id.clone()),
        ("point", serde_json::to_string(&obj.point)?),
        ("payload", serde_json::to_string(&obj.payload)?),
        ("created_at", obj.created_at.to_string()),
        ("expires_at", obj.expires_at.to_string()),
        ("last_activity", obj.last_activity.to_string()),
        ("activity_count", obj.activity_count.to_string()),
    ];
    if let Some(hash) = owner_hash {
        fields.push(("owner_hash", hash.to_string()));
    }
    redis::pipe()
        .hset_multiple(&key, &fields)
        .expire(&key, inactivity_ttl_secs)
        .ignore()
        .query_async::<_, ()>(con)
        .await?;
    Ok(())
}

/// Atomically append an activity and reset the parent's inactivity TTL, bounded
/// by the hard cap. Returns false if the object no longer exists (expired/gone).
///
/// The TTL maths runs inside a Lua script so the "is it still alive? / how much
/// life is left under the hard cap?" check and the TTL write happen as one step,
/// with no race against expiry.
pub async fn add_activity(
    con: &mut ConnectionManager,
    activity: &Activity,
    inactivity_ttl_secs: i64,
    hard_cap_secs: i64,
) -> Result<bool> {
    let thread_key = object_key(&activity.object_id);
    let comments_key = activities_key(&activity.object_id);

    let script = Script::new(
        r#"
        local object_key = KEYS[1]
        local activities_key = KEYS[2]
        local activity_json = ARGV[1]
        local now = tonumber(ARGV[2])
        local inactivity_ttl = tonumber(ARGV[3])
        local max_lifetime = tonumber(ARGV[4])

        if redis.call('EXISTS', object_key) == 0 then
            return 0
        end

        local created_at = tonumber(redis.call('HGET', object_key, 'created_at'))
        local hard_expires_at = created_at + max_lifetime
        local remaining = hard_expires_at - now

        if remaining <= 0 then
            return 0
        end

        local new_ttl = math.min(inactivity_ttl, remaining)
        redis.call('HSET', object_key, 'last_activity', now)
        redis.call('HINCRBY', object_key, 'activity_count', 1)
        redis.call('EXPIRE', object_key, new_ttl)
        redis.call('RPUSH', activities_key, activity_json)
        redis.call('EXPIRE', activities_key, new_ttl)

        return 1
        "#,
    );

    let activity_json = serde_json::to_string(activity)?;
    let result: i64 = script
        .key(&thread_key)
        .key(&comments_key)
        .arg(&activity_json)
        .arg(now())
        .arg(inactivity_ttl_secs)
        .arg(hard_cap_secs)
        .invoke_async(con)
        .await?;

    Ok(result == 1)
}

/// Result of an owner-checked write.
#[derive(Debug, PartialEq, Eq)]
pub enum OwnerWrite {
    Done,
    /// The object is gone (expired or never existed).
    NotFound,
    /// The object has no owner, or the token hash does not match.
    Forbidden,
}

/// Lua prelude shared by owner-checked writes. KEYS[1] is the object key and
/// ARGV[1] the presented token hash. Doing the checks and the write in one
/// script means an expired object is never resurrected by a late HSET and the
/// owner check cannot race the write.
const OWNER_GUARD: &str = r#"
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    local stored = redis.call('HGET', KEYS[1], 'owner_hash')
    if (not stored) or stored ~= ARGV[1] then return 2 end
"#;

fn owner_outcome(code: i64) -> OwnerWrite {
    match code {
        1 => OwnerWrite::Done,
        2 => OwnerWrite::Forbidden,
        _ => OwnerWrite::NotFound,
    }
}

/// Replace an owned object's payload. Lifecycle fields and TTL are untouched.
pub async fn update_payload_owned(
    con: &mut ConnectionManager,
    id: &str,
    owner_hash: &str,
    payload: &Value,
) -> Result<OwnerWrite> {
    let script = Script::new(&format!(
        "{OWNER_GUARD} redis.call('HSET', KEYS[1], 'payload', ARGV[2]) return 1"
    ));
    let code: i64 = script
        .key(object_key(id))
        .arg(owner_hash)
        .arg(serde_json::to_string(payload)?)
        .invoke_async(con)
        .await?;
    Ok(owner_outcome(code))
}

/// Delete an owned object and its activities. The caller removes it from the
/// space index.
pub async fn delete_owned(
    con: &mut ConnectionManager,
    id: &str,
    owner_hash: &str,
) -> Result<OwnerWrite> {
    let script = Script::new(&format!(
        "{OWNER_GUARD} redis.call('DEL', KEYS[1], KEYS[2]) return 1"
    ));
    let code: i64 = script
        .key(object_key(id))
        .key(activities_key(id))
        .arg(owner_hash)
        .invoke_async(con)
        .await?;
    Ok(owner_outcome(code))
}

/// Fetch a single object by id, or `None` if its key is gone (expired).
pub async fn get_object(con: &mut ConnectionManager, id: &str) -> Result<Option<Object>> {
    let key = object_key(id);
    let fields: Vec<String> = con.hgetall(&key).await?;
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(parse_object_fields(&fields))
}

/// Fetch several objects by id, skipping any that have expired. Preserves order.
pub async fn get_objects(con: &mut ConnectionManager, ids: &[String]) -> Result<Vec<Object>> {
    let mut objects = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(obj) = get_object(con, id).await? {
            objects.push(obj);
        }
    }
    Ok(objects)
}

/// Fetch all activities on an object, oldest first.
pub async fn get_activities(con: &mut ConnectionManager, object_id: &str) -> Result<Vec<Activity>> {
    let key = activities_key(object_id);
    let raw: Vec<String> = con.lrange(&key, 0, -1).await?;
    Ok(raw
        .iter()
        .filter_map(|s| serde_json::from_str(s).ok())
        .collect())
}

/// Reconcile the space index against Redis: find objects whose key has already
/// expired, remove their (now stale) index entries, and return their ids so the
/// caller can broadcast `object_expired` to connected clients.
pub async fn sweep_expired(con: &mut ConnectionManager, space: &dyn Space) -> Result<Vec<String>> {
    let ids = space.all_indexed_ids(con).await?;
    let mut expired = Vec::new();
    for id in ids {
        let exists: bool = con.exists(object_key(&id)).await?;
        if !exists {
            space.remove(con, &id).await?;
            expired.push(id);
        }
    }
    Ok(expired)
}

/// Rebuild an `Object` from a Redis `HGETALL` field/value stream.
fn parse_object_fields(fields: &[String]) -> Option<Object> {
    let mut map = std::collections::HashMap::new();
    let mut iter = fields.iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        map.insert(k.as_str(), v.as_str());
    }

    let parse_json = |s: &str| -> Option<Value> { serde_json::from_str(s).ok() };

    Some(Object {
        id: map.get("id")?.to_string(),
        point: parse_json(map.get("point")?)?,
        payload: parse_json(map.get("payload")?)?,
        created_at: map.get("created_at")?.parse().ok()?,
        expires_at: map.get("expires_at")?.parse().ok()?,
        last_activity: map.get("last_activity")?.parse().ok()?,
        activity_count: map.get("activity_count")?.parse().ok()?,
    })
}
