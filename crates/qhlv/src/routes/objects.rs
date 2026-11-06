//! `POST /objects` (create) and `POST /feed` (query within a viewport).

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::models::{object_view, CreateObject, Object, UpdateObject};
use crate::owner;
use crate::store::OwnerWrite;
use crate::ws::{self, WsEvent};
use crate::{store, AppState};

pub async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateObject>,
) -> Result<Json<Value>> {
    validate_payload(&state, &body.payload)?;

    let now = store::now();
    // Derive the stored point (fuzz / embed) via the active space.
    let point = state.space.locate(body.locator, &body.payload).await?;

    let obj = Object {
        id: Uuid::new_v4().to_string(),
        point,
        created_at: now,
        expires_at: now + state.config.hard_cap_secs,
        last_activity: now,
        activity_count: 0,
        payload: body.payload,
    };

    // Opt-in ownership: keep only the token's hash; the token itself is returned
    // once, below, and never broadcast.
    let token = body.owned.then(owner::new_token);
    let owner_hash = token.as_deref().map(owner::hash_token);

    let mut con = state.redis.clone();
    store::save_object_owned(
        &mut con,
        &obj,
        state.config.inactivity_ttl_secs,
        owner_hash.as_deref(),
    )
    .await?;
    state.space.index(&mut con, &obj.id, &obj.point).await?;

    // Fan out to nearby clients, and return the public view to the poster.
    let mut view = object_view(&obj, state.space.as_ref());
    ws::broadcast(
        &state.clients,
        state.space.as_ref(),
        &obj.point,
        WsEvent::ObjectCreated { data: view.clone() },
    )
    .await;

    if let Some(token) = token {
        view["owner_token"] = json!(token);
    }
    Ok(Json(view))
}

/// `PUT /objects/:id` — replace the payload of an owned object (header
/// `x-owner-token`). Point, lifetime and activities are unchanged.
pub async fn update(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<UpdateObject>,
) -> Result<Json<Value>> {
    let owner_hash = owner::hash_from_headers(&headers)?;
    validate_payload(&state, &body.payload)?;

    let mut con = state.redis.clone();
    match store::update_payload_owned(&mut con, &id, &owner_hash, &body.payload).await? {
        OwnerWrite::Done => {}
        OwnerWrite::NotFound => return Err(Error::NotFound),
        OwnerWrite::Forbidden => return Err(Error::Forbidden),
    }

    // Re-read for the response; None only if it expired in the meantime.
    let obj = store::get_object(&mut con, &id).await?.ok_or(Error::NotFound)?;
    Ok(Json(object_view(&obj, state.space.as_ref())))
}

/// `DELETE /objects/:id` — remove an owned object (header `x-owner-token`).
/// Clients see it as an ordinary `object_expired` event.
pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    let owner_hash = owner::hash_from_headers(&headers)?;

    let mut con = state.redis.clone();
    match store::delete_owned(&mut con, &id, &owner_hash).await? {
        OwnerWrite::Done => {}
        OwnerWrite::NotFound => return Err(Error::NotFound),
        OwnerWrite::Forbidden => return Err(Error::Forbidden),
    }

    state.space.remove(&mut con, &id).await?;
    ws::broadcast_all(&state.clients, WsEvent::ObjectExpired { object_id: id }).await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn feed(
    State(state): State<AppState>,
    Json(viewport): Json<Value>,
) -> Result<Json<Vec<Value>>> {
    let mut con = state.redis.clone();
    let ids = state.space.query(&mut con, &viewport).await?;
    let objects = store::get_objects(&mut con, &ids).await?;
    let views = objects
        .iter()
        .map(|o| object_view(o, state.space.as_ref()))
        .collect();
    Ok(Json(views))
}

/// Run the app's validator, then the core's size backstop.
pub(crate) fn validate_payload(state: &AppState, payload: &Value) -> Result<()> {
    (state.validate)(payload).map_err(Error::Validation)?;
    let size = serde_json::to_vec(payload)?.len();
    if size > state.config.max_payload_bytes {
        return Err(Error::Validation(format!(
            "payload too large ({size} > {} bytes)",
            state.config.max_payload_bytes
        )));
    }
    Ok(())
}
