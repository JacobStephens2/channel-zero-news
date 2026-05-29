//! Database-backed persistence test. Drives a room's actor directly and asserts
//! that finalized responses and archives land in Postgres, while live state
//! stays in memory.
//!
//! Skipped automatically when `DATABASE_URL` is not set, so the rest of the
//! suite stays hermetic. Run with:
//!   set -a; . ./.env; set +a; cargo test --test persistence -- --nocapture

use channel_zero::db::Db;
use channel_zero::protocol::{ClientMsg, Role, ServerMsg};
use channel_zero::registry::Registry;
use channel_zero::room::{Command, RoomHandle};
use tokio::sync::mpsc;

/// Dispatch an intent as a given role/name with a throwaway direct channel.
async fn fire(handle: &RoomHandle, intent: ClientMsg, role: Role, name: Option<&str>) {
    let (tx, _rx) = mpsc::unbounded_channel();
    handle
        .dispatch(Command {
            intent,
            role,
            name: name.map(String::from),
            direct: tx,
        })
        .await
        .unwrap();
}

/// Send a Ping and await the Pong. Because the actor processes commands (and
/// awaits their durable effects) strictly in order, a returned Pong guarantees
/// every previously dispatched command — and its persistence — has completed.
async fn barrier(handle: &RoomHandle) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    handle
        .dispatch(Command {
            intent: ClientMsg::Ping,
            role: Role::Display,
            name: None,
            direct: tx,
        })
        .await
        .unwrap();
    loop {
        match rx.recv().await {
            Some(ServerMsg::Pong) => return,
            Some(_) => continue,
            None => panic!("actor closed before pong"),
        }
    }
}

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
    let room = registry.create_room(prompts, Some(db.clone())).await;
    let code = room.code.clone();
    let handle = registry.get(&code).await.unwrap();

    // Host builds the roster and opens submissions.
    for name in ["Ann", "Bo"] {
        fire(&handle, ClientMsg::AddPlayer { name: name.into() }, Role::Host, None).await;
    }
    fire(&handle, ClientMsg::StartCollecting, Role::Host, None).await;

    // Each player joins, learns their prompt count from the assignment, submits.
    for name in ["Ann", "Bo"] {
        let (tx, mut rx) = mpsc::unbounded_channel();
        handle
            .dispatch(Command {
                intent: ClientMsg::JoinRoom {
                    code: code.clone(),
                    role: Role::Player,
                    name: Some(name.into()),
                    token: None,
                },
                role: Role::Player,
                name: Some(name.into()),
                direct: tx,
            })
            .await
            .unwrap();

        let prompt_count = loop {
            match rx.recv().await {
                Some(ServerMsg::Assignment { prompts, .. }) => break prompts.len(),
                Some(_) => continue,
                None => panic!("closed before assignment"),
            }
        };

        fire(
            &handle,
            ClientMsg::SubmitResponses {
                responses: vec!["resp".to_string(); prompt_count],
                signoff: "and that's the way it is".into(),
            },
            Role::Player,
            Some(name),
        )
        .await;
    }

    // Start the game -> final responses persist.
    fire(&handle, ClientMsg::StartGame, Role::Host, None).await;
    barrier(&handle).await;

    let final_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM responses WHERE room_code = $1")
        .bind(&code)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(final_count, 2, "two finalized responses should persist");

    // Archive -> rows move to the archive, live responses cleared.
    fire(&handle, ClientMsg::ArchiveRound, Role::Host, None).await;
    barrier(&handle).await;

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
