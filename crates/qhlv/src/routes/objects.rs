//! `POST /objects` (create) and `POST /feed` (query within a viewport).

use axum::{extract::State, Json};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::models::{object_view, CreateObject, Object};
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

    let mut con = state.redis.clone();
    store::save_object(&mut con, &obj, state.config.inactivity_ttl_secs).await?;
    state.space.index(&mut con, &obj.id, &obj.point).await?;

    // Fan out to nearby clients, and return the public view to the poster.
    let view = object_view(&obj, state.space.as_ref());
    ws::broadcast(
        &state.clients,
        state.space.as_ref(),
        &obj.point,
        WsEvent::ObjectCreated { data: view.clone() },
    )
    .await;

    Ok(Json(view))
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
