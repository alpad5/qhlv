//! `POST /objects/:id/activities` (reply) and `GET /objects/:id/activities`.

use axum::{
    extract::{Path, State},
    Json,
};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::models::{Activity, CreateActivity};
use crate::routes::objects::validate_payload;
use crate::ws::{self, WsEvent};
use crate::{store, AppState};

pub async fn create(
    State(state): State<AppState>,
    Path(object_id): Path<String>,
    Json(body): Json<CreateActivity>,
) -> Result<Json<Value>> {
    validate_payload(&state, &body.payload)?;

    let activity = Activity {
        id: Uuid::new_v4().to_string(),
        object_id: object_id.clone(),
        created_at: store::now(),
        payload: body.payload,
    };

    let mut con = state.redis.clone();
    // Atomically append + reset the parent's inactivity timer; false ⇒ object gone.
    // add_activity sets TTL = min(window, life left under the cap), so passing the
    // hard cap as the window makes the activity extend straight to the cap.
    let window = if state.config.activity_extends_to_cap {
        state.config.hard_cap_secs
    } else {
        state.config.inactivity_ttl_secs
    };
    let saved = store::add_activity(
        &mut con,
        &activity,
        window,
        state.config.hard_cap_secs,
    )
    .await?;
    if !saved {
        return Err(Error::NotFound);
    }

    // Look up the parent's point so the activity fans out to the same neighbourhood.
    if let Some(obj) = store::get_object(&mut con, &object_id).await? {
        ws::broadcast(
            &state.clients,
            state.space.as_ref(),
            &obj.point,
            WsEvent::ActivityAdded {
                object_id: object_id.clone(),
                data: serde_json::to_value(&activity)?,
            },
        )
        .await;
    }

    Ok(Json(serde_json::to_value(&activity)?))
}

pub async fn list(
    State(state): State<AppState>,
    Path(object_id): Path<String>,
) -> Result<Json<Vec<Activity>>> {
    let mut con = state.redis.clone();
    let activities = store::get_activities(&mut con, &object_id).await?;
    Ok(Json(activities))
}
