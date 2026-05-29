//! Shared application state and router construction.

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

use crate::registry::Registry;
use crate::state::PromptSet;
use crate::{routes, ws};

/// State shared across all handlers. Cheap to clone (everything is `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    /// The prompt-set catalog handed to each new room.
    ///
    /// M4 seeds this in memory; M5 replaces the source with the Postgres-backed
    /// catalog so prompt management persists.
    pub prompts: Arc<Vec<PromptSet>>,
}

impl AppState {
    pub fn new(prompts: Vec<PromptSet>) -> Self {
        Self {
            registry: Registry::new(),
            prompts: Arc::new(prompts),
        }
    }
}

/// Build the full application router.
///
/// * `GET  /health`            — liveness
/// * `POST /api/rooms`         — create a room, returns `{code, host_token}`
/// * `GET  /api/rooms/:code`   — join validation: `{exists, phase?}`
/// * `GET  /api/prompt-sets`   — the prompt-set catalog
/// * `GET  /ws`                — the realtime game socket
/// * `/`                       — static test client (./static)
pub fn build_router(state: AppState, static_dir: &str) -> Router {
    let api = Router::new()
        .route("/rooms", post(routes::create_room))
        .route("/rooms/:code", get(routes::room_info))
        .route("/prompt-sets", get(routes::list_prompt_sets));

    Router::new()
        .route("/health", get(routes::health))
        .route("/ws", get(ws::ws_handler))
        .nest("/api", api)
        .fallback_service(ServeDir::new(static_dir))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
