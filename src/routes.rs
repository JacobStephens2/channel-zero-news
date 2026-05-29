//! REST handlers: the small HTTP surface that bootstraps a realtime session.
//! Everything live happens over the WebSocket; these endpoints only create
//! rooms, validate join codes, and expose the prompt catalog.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Serialize;

use crate::app::AppState;
use crate::error::AppError;
use crate::state::PromptSet;

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "service": "channel-zero" }))
}

#[derive(Serialize)]
pub struct CreateRoomResponse {
    pub code: String,
    /// Secret — the host presents this in `JoinRoom` to gain host authority.
    pub host_token: String,
}

/// Create a room seeded with the current prompt catalog and start its actor.
pub async fn create_room(
    State(state): State<AppState>,
) -> Result<Json<CreateRoomResponse>, AppError> {
    if state.prompts.is_empty() {
        return Err(AppError::Internal(
            "no prompt sets configured; cannot create a room".into(),
        ));
    }
    let room = state
        .registry
        .create_room((*state.prompts).clone())
        .await;
    Ok(Json(CreateRoomResponse {
        code: room.code,
        host_token: room.host_token,
    }))
}

#[derive(Serialize)]
pub struct RoomInfoResponse {
    pub exists: bool,
}

/// Join validation: confirm a code maps to a live room before opening a socket.
pub async fn room_info(
    State(state): State<AppState>,
    Path(code): Path<String>,
) -> Json<RoomInfoResponse> {
    let exists = state.registry.get(&code).await.is_some();
    Json(RoomInfoResponse { exists })
}

/// Expose the prompt-set catalog (used by host/prompt-management UIs).
pub async fn list_prompt_sets(State(state): State<AppState>) -> Json<Vec<PromptSet>> {
    Json((*state.prompts).clone())
}
