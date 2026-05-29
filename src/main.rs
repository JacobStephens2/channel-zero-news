//! The Channel 0 News — realtime backend.
//!
//! Milestone 1: prove the stack. Axum + Tokio serving a health route and a
//! WebSocket echo endpoint. Later milestones layer the typed protocol, the
//! per-room actor model, persistence, and the real game flow on top.

use std::net::SocketAddr;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() {
    init_tracing();

    let app = router();

    let addr: SocketAddr = std::env::var("CHANNEL_ZERO_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3471".to_string())
        .parse()
        .expect("CHANNEL_ZERO_ADDR must be a valid socket address");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    tracing::info!("channel-zero listening on http://{addr}");

    axum::serve(listener, app)
        .await
        .expect("server crashed");
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,channel_zero=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// The application router. Kept in its own function so integration tests and
/// later milestones can mount it without spinning up a real listener.
fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ws/echo", get(ws_echo_upgrade))
        .layer(TraceLayer::new_for_http())
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "service": "channel-zero" }))
}

async fn ws_echo_upgrade(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(ws_echo)
}

/// Minimal echo handler: bounce every text/binary frame back to the sender.
/// Replaced in milestone 4 by the real per-connection game handler.
async fn ws_echo(mut socket: WebSocket) {
    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            Message::Text(text) => {
                if socket.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
            Message::Binary(bin) => {
                if socket.send(Message::Binary(bin)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            // Ping/Pong are handled by axum automatically.
            _ => {}
        }
    }
}
