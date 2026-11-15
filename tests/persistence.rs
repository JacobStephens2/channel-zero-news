//! Database-backed persistence test. Drives admitted Room connections and asserts
//! that finalized responses and archives land in Postgres, while live state
//! stays in memory.
//!
//! Skipped automatically when `DATABASE_URL` is not set, so the rest of the
//! suite stays hermetic. Run with:
//!   set -a; . ./.env; set +a; cargo test --test persistence -- --nocapture

use channel_zero::db::Db;
use channel_zero::protocol::{ClientMsg, Role, ServerMsg};
use channel_zero::registry::Registry;

#[path = "common/connection.rs"]
#[allow(dead_code)]
mod connection;
use connection::{join, Connection};

#[tokio::test]
async fn final_responses_and_archive_persist_to_postgres() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set; skipping persistence test");
        return;
    };

    let db = Db::connect(&url).await.expect("connect");
    db.migrate().await.expect("migrate");
    let prompts = db.load_prompt_sets().await.expect("load prompts");
    assert!(!prompts.is_empty(), "need migrated prompt sets");

    let pool = sqlx::PgPool::connect(&url).await.expect("pool");

    let registry = Registry::new();
    let room = registry
        .create_room(prompts, Some(db.clone()))
        .await
        .expect("create room");
    let code = room.code.clone();
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;

    // Host builds the roster and opens submissions.
    for name in ["Ann", "Bo"] {
        host.send(ClientMsg::AddPlayer { name: name.into() }).await;
    }
    host.send(ClientMsg::StartCollecting).await;
    host.barrier().await;

    // Each player joins, learns their prompt count from the assignment, submits.
    for name in ["Ann", "Bo"] {
        let mut player = Connection::admit(&registry, join(&room, Role::Player, Some(name))).await;
        let ServerMsg::Assignment { prompts, .. } = player.recv().await else {
            panic!("expected assignment")
        };
        player
            .send(ClientMsg::SubmitResponses {
                responses: vec!["resp".to_string(); prompts.len()],
                signoff: "and that's the way it is".into(),
            })
            .await;
        player.barrier().await;
    }

    // Start the game -> final responses persist.
    host.send(ClientMsg::StartGame).await;
    // Ping/Pong crosses the same actor queue and remains the completion
    // barrier for all preceding durable effects, including best-effort writes.
    host.barrier().await;

    let final_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM responses WHERE room_code = $1")
            .bind(&code)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(final_count, 2, "two finalized responses should persist");

    // Archive -> rows move to the archive, live responses cleared.
    host.send(ClientMsg::ArchiveRound).await;
    host.barrier().await;

    let archived_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM response_archive WHERE room_code = $1")
            .bind(&code)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(archived_count, 2, "two archived rows expected");

    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM responses WHERE room_code = $1")
        .bind(&code)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0, "live responses cleared after archive");

    // Spot-check a persisted archive row carries prompts + responses.
    let (prompts_len, responses_len): (i32, i32) = sqlx::query_as(
        "SELECT cardinality(prompts), cardinality(responses) \
         FROM response_archive WHERE room_code = $1 LIMIT 1",
    )
    .bind(&code)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(prompts_len > 0 && responses_len > 0);
    assert_eq!(prompts_len, responses_len);

    // Clean up this test's rows.
    sqlx::query("DELETE FROM response_archive WHERE room_code = $1")
        .bind(&code)
        .execute(&pool)
        .await
        .unwrap();
}
