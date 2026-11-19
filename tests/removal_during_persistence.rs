//! Removal must stop a room even while PostgreSQL is waiting on a table lock.
//! Uses a disposable database supplied in DATABASE_URL; skips without it.

use std::time::Duration;

use channel_zero::client_address::ClientAddress;
use channel_zero::db::Db;
use channel_zero::protocol::{ClientMsg, Role, ServerMsg};
use channel_zero::registry::Registry;
use channel_zero::state::PromptSet;

#[path = "common/connection.rs"]
#[allow(dead_code)]
mod connection;
use connection::{join, Connection};

#[tokio::test]
async fn removal_interrupts_a_blocked_durable_effect() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set; skipping persistence removal test");
        return;
    };
    let db = Db::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    let observer = sqlx::PgPool::connect(&url).await.unwrap();
    let mut lock = observer.begin().await.unwrap();
    sqlx::query("LOCK TABLE responses IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();

    let registry = Registry::new();
    let created = registry
        .create_room(
            ClientAddress::NO_PEER,
            vec![PromptSet {
                id: 1,
                name: "Set".into(),
                author: "Author".into(),
                prompts: vec!["Prompt".into()],
            }],
            Some(db),
        )
        .await
        .unwrap();
    let handle = registry.get(&created.code).await.unwrap();
    let mut host = Connection::admit(&registry, join(&created, Role::Host, None)).await;
    host.send(ClientMsg::AddPlayer { name: "Ann".into() }).await;
    host.send(ClientMsg::StartCollecting).await;
    host.barrier().await;
    let mut player = Connection::admit(&registry, join(&created, Role::Player, Some("Ann"))).await;
    assert!(matches!(player.recv().await, ServerMsg::Assignment { .. }));
    player
        .send(ClientMsg::SubmitResponses {
            responses: vec!["Response".into()],
            signoff: "Signoff".into(),
        })
        .await;
    player.barrier().await;
    host.send(ClientMsg::StartGame).await;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock' AND query = $1)",
            )
            .bind("DELETE FROM responses WHERE room_code = $1")
            .fetch_one(&observer)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the actor never reached its locked persistence write");

    assert!(registry.remove(&created.code).await);
    assert!(registry.get(&created.code).await.is_none());
    let stopped_before_unlock = tokio::time::timeout(Duration::from_secs(1), handle.closed()).await;
    // Always release the database lock before asserting, including on failure.
    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle.closed())
        .await
        .unwrap();
    assert!(
        stopped_before_unlock.is_ok(),
        "removed actor remained alive until PostgreSQL unlocked"
    );
}
