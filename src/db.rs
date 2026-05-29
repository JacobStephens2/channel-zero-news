//! Persistence layer (SQLx + Postgres).
//!
//! The boundary is deliberate: **live game state never touches the database** —
//! it lives in the per-room actors. Only durable artifacts are persisted here:
//!
//! * the prompt-set catalog (read at room creation / prompt management),
//! * each room's finalized responses (written when a game starts), and
//! * the append-only archive of past rounds (written on archive).

use rand::Rng;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};

use crate::state::{ArchivedEntry, PromptSet};

/// A cheap-to-clone handle to the connection pool.
#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

#[derive(FromRow)]
struct PromptRow {
    id: i64,
    name: Option<String>,
    author: Option<String>,
    prompts: Vec<String>,
}

impl Db {
    /// Connect and verify the pool.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    /// Run embedded migrations (the `./migrations` directory).
    pub async fn migrate(&self) -> Result<(), sqlx::migrate::MigrateError> {
        sqlx::migrate!("./migrations").run(&self.pool).await
    }

    /// The active (non-archived) prompt catalog, ordered by id — mirrors the
    /// original `WHERE archived_at IS NULL ORDER BY id`.
    pub async fn load_prompt_sets(&self) -> Result<Vec<PromptSet>, sqlx::Error> {
        let rows: Vec<PromptRow> = sqlx::query_as(
            "SELECT id, name, author, prompts \
             FROM prompt_sets WHERE archived_at IS NULL ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| PromptSet {
                id: r.id,
                name: r.name.unwrap_or_default(),
                author: r.author.unwrap_or_default(),
                prompts: r.prompts,
            })
            .collect())
    }

    /// Persist a room's finalized responses, replacing any previous set for that
    /// room (called when the host starts the game).
    pub async fn save_final_responses(
        &self,
        room_code: &str,
        entries: &[ArchivedEntry],
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM responses WHERE room_code = $1")
            .bind(room_code)
            .execute(&mut *tx)
            .await?;
        for e in entries {
            sqlx::query(
                "INSERT INTO responses \
                 (room_code, name, partner, prompt_set_id, prompts, responses, signoff) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(room_code)
            .bind(&e.name)
            .bind(&e.partner)
            .bind(e.prompt_set_id)
            .bind(&e.prompts)
            .bind(&e.responses)
            .bind(&e.signoff)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Copy a round's responses into the archive under a fresh batch id, then
    /// clear the room's live responses — mirrors the original
    /// `archive_current_responses` (insert-select then delete, transactional).
    /// Returns the archive batch id.
    pub async fn archive_round(
        &self,
        room_code: &str,
        entries: &[ArchivedEntry],
    ) -> Result<String, sqlx::Error> {
        let batch_id = random_batch_id();
        let mut tx = self.pool.begin().await?;
        for e in entries {
            sqlx::query(
                "INSERT INTO response_archive \
                 (archive_batch_id, room_code, name, partner, prompt_set_id, prompts, responses, signoff) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(&batch_id)
            .bind(room_code)
            .bind(&e.name)
            .bind(&e.partner)
            .bind(e.prompt_set_id)
            .bind(&e.prompts)
            .bind(&e.responses)
            .bind(&e.signoff)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("DELETE FROM responses WHERE room_code = $1")
            .bind(room_code)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(batch_id)
    }
}

fn random_batch_id() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 16] = rng.gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
