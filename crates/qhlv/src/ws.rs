//! WebSocket handling: the client registry and the real-time fan-out.
//!
//! A client connects and sends one registration message — an opaque viewport
//! (interpreted by the active `Space`). On every new object/activity the server
//! asks the space `matches(viewport, point)` for each client and pushes to those
//! whose viewport contains the event. Nothing here knows what "near" means.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::sink::SinkExt;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::space::Space;
use crate::AppState;

/// A connected client and its last-registered viewport (opaque JSON).
/// `viewport` is `Null` until the client sends its first registration message.
pub struct WsClient {
    pub viewport: Value,
    pub tx: mpsc::UnboundedSender<String>,
}

pub type ClientMap = Arc<RwLock<HashMap<Uuid, WsClient>>>;

/// Outbound events pushed from server to client. Names are the generic protocol
/// vocabulary — `object`/`activity`, not any app's `thread`/`comment`.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsEvent {
    ObjectCreated { data: Value },
    ActivityAdded { object_id: String, data: Value },
    ObjectExpired { object_id: String },
}

/// Push an event to every client whose viewport contains `point`, per the space.
pub async fn broadcast(clients: &ClientMap, space: &dyn Space, point: &Value, event: WsEvent) {
    let json = match serde_json::to_string(&event) {
        Ok(s) => s,
        Err(_) => return,
    };
    let map = clients.read().await;
    for client in map.values() {
        if !client.viewport.is_null() && space.matches(&client.viewport, point) {
            let _ = client.tx.send(json.clone());
        }
    }
}

/// Push an event to every connected client regardless of location. Used for
/// `object_expired`: any client could have the object in view, and one that
/// doesn't will simply no-op the removal on their end.
pub async fn broadcast_all(clients: &ClientMap, event: WsEvent) {
    let json = match serde_json::to_string(&event) {
        Ok(s) => s,
        Err(_) => return,
    };
    let map = clients.read().await;
    for client in map.values() {
        let _ = client.tx.send(json.clone());
    }
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.clients))
}

async fn handle_socket(socket: WebSocket, clients: ClientMap) {
    let id = Uuid::new_v4();
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let (sink, stream): (SplitSink<WebSocket, Message>, SplitStream<WebSocket>) = socket.split();

    // Register immediately with an empty viewport; it fills in on first message.
    clients.write().await.insert(
        id,
        WsClient {
            viewport: Value::Null,
            tx,
        },
    );

    let send_task = tokio::spawn(run_sender(rx, sink));
    run_receiver(stream, &clients, id).await;

    clients.write().await.remove(&id);
    send_task.abort();
}

async fn run_sender(
    mut rx: mpsc::UnboundedReceiver<String>,
    mut sink: SplitSink<WebSocket, Message>,
) {
    while let Some(msg) = rx.recv().await {
        if sink.send(Message::Text(msg)).await.is_err() {
            break;
        }
    }
}

async fn run_receiver(mut stream: SplitStream<WebSocket>, clients: &ClientMap, id: Uuid) {
    while let Some(result) = stream.next().await {
        let Ok(msg) = result else { break };
        if let Message::Text(text) = msg {
            // Any JSON object is accepted as a viewport; the space validates shape
            // lazily when it evaluates `matches`. Non-JSON messages are ignored.
            if let Ok(viewport) = serde_json::from_str::<Value>(&text) {
                if let Some(client) = clients.write().await.get_mut(&id) {
                    client.viewport = viewport;
                }
            }
        }
    }
}
