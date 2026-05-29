//! The Channel 0 News — realtime backend binary.
//!
//! Wires the room registry, REST surface, and WebSocket game endpoint into one
//! Axum app. Live game state lives in memory (the room actors); only prompt
//! sets, final responses, and archives are persisted (the M5 db layer).

use std::net::SocketAddr;

use channel_zero::app::{build_router, AppState};
use channel_zero::state::PromptSet;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() {
    init_tracing();

    // M4: seed the prompt catalog in memory. M5 loads it from Postgres
    // (migrated from the live MySQL `tblPrompts`).
    let prompts = seed_prompt_sets();
    let state = AppState::new(prompts);

    let static_dir = std::env::var("CHANNEL_ZERO_STATIC").unwrap_or_else(|_| "static".to_string());
    let app = build_router(state, &static_dir);

    let addr: SocketAddr = std::env::var("CHANNEL_ZERO_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3471".to_string())
        .parse()
        .expect("CHANNEL_ZERO_ADDR must be a valid socket address");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    tracing::info!("channel-zero listening on http://{addr}");

    axum::serve(listener, app).await.expect("server crashed");
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,channel_zero=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// A small in-memory prompt catalog so the full flow is exercisable before the
/// database lands in M5.
fn seed_prompt_sets() -> Vec<PromptSet> {
    let mk = |id: i64, name: &str, prompts: &[&str]| PromptSet {
        id,
        name: name.into(),
        author: "Eric".into(),
        prompts: prompts.iter().map(|s| s.to_string()).collect(),
    };
    vec![
        mk(
            1,
            "Top Story",
            &[
                "Our top story tonight: ",
                "In local news, ",
                "And in a stunning development, ",
                "Experts are now saying that ",
                "Meanwhile, across town, ",
                "In sports, ",
                "And finally, the weather: ",
            ],
        ),
        mk(
            2,
            "Special Report",
            &[
                "This just in: ",
                "Authorities are warning that ",
                "Witnesses on the scene reported ",
                "The mayor released a statement saying ",
                "In a related story, ",
                "Health officials recommend ",
                "Looking ahead to tomorrow, ",
            ],
        ),
    ]
}
