//! Shared application state and router construction.

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

use crate::db::Db;
use crate::error::AppError;
use crate::registry::Registry;
use crate::state::PromptSet;
use crate::{routes, ws};

/// State shared across all handlers. Cheap to clone (everything is `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    /// The Postgres-backed persistence layer. `None` runs the game fully in
    /// memory (used by integration tests and DB-less runs).
    pub db: Option<Db>,
    /// Prompt catalog used only when `db` is `None`.
    seed_prompts: Arc<Vec<PromptSet>>,
}

impl AppState {
    /// In-memory mode: prompt catalog from a seed, no persistence.
    pub fn in_memory(prompts: Vec<PromptSet>) -> Self {
        Self {
            registry: Registry::new(),
            db: None,
            seed_prompts: Arc::new(prompts),
        }
    }

    /// Database-backed mode: prompt catalog and durable artifacts in Postgres.
    pub fn with_db(db: Db) -> Self {
        Self {
            registry: Registry::new(),
            db: Some(db),
            seed_prompts: Arc::new(Vec::new()),
        }
    }

    /// The current prompt-set catalog (from Postgres, or the in-memory seed).
    pub async fn prompt_catalog(&self) -> Result<Vec<PromptSet>, AppError> {
        match &self.db {
            Some(db) => db
                .load_prompt_sets()
                .await
                .map_err(|e| AppError::Internal(format!("loading prompt sets: {e}"))),
            None => Ok((*self.seed_prompts).clone()),
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
