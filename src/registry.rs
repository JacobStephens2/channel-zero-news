//! In-memory registry of live rooms, keyed by join code.
//!
//! Rooms live entirely in memory (only prompt sets, final responses, and
//! archives are persisted — see the M5 db layer). The registry maps a join code
//! to its [`RoomHandle`] behind an `RwLock`, since lookups vastly outnumber the
//! rare create/remove. Per-room state itself is *not* locked here — it is owned
//! by the room's actor task.

use std::collections::HashMap;
use std::sync::Arc;

use rand::Rng;
use tokio::sync::RwLock;

use crate::db::Db;
use crate::room::{spawn_room, RoomHandle};
use crate::state::PromptSet;

/// Characters used for join codes: unambiguous uppercase letters/digits.
const CODE_ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
const CODE_LEN: usize = 4;

#[derive(Default)]
pub struct Registry {
    rooms: RwLock<HashMap<String, RoomHandle>>,
}

/// What a freshly created room hands back to the host: the public join code and
/// the secret host token.
#[derive(Debug, Clone)]
pub struct NewRoom {
    pub code: String,
    pub host_token: String,
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Create a room with the given prompt-set catalog and start its actor.
    /// Generates a unique join code and a secret host token. `db` is handed to
    /// the room actor for durable effects (`None` to disable persistence).
    pub async fn create_room(&self, prompt_sets: Vec<PromptSet>, db: Option<Db>) -> NewRoom {
        let host_token = random_token();
        let mut rooms = self.rooms.write().await;
        let code = loop {
            let candidate = random_code();
            if !rooms.contains_key(&candidate) {
                break candidate;
            }
        };
        let handle = spawn_room(code.clone(), prompt_sets, host_token.clone(), db);
        rooms.insert(code.clone(), handle);
        NewRoom { code, host_token }
    }

    /// Look up a live room by join code.
    pub async fn get(&self, code: &str) -> Option<RoomHandle> {
        self.rooms.read().await.get(code).cloned()
    }

    /// Remove a room from the registry (its actor stops once all handles drop).
    pub async fn remove(&self, code: &str) -> bool {
        self.rooms.write().await.remove(code).is_some()
    }

    pub async fn count(&self) -> usize {
        self.rooms.read().await.len()
    }
}

fn random_code() -> String {
    let mut rng = rand::thread_rng();
    (0..CODE_LEN)
        .map(|_| CODE_ALPHABET[rng.gen_range(0..CODE_ALPHABET.len())] as char)
        .collect()
}

fn random_token() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 24] = rng.gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sets() -> Vec<PromptSet> {
        vec![PromptSet {
            id: 1,
            name: "Set".into(),
            author: "Eric".into(),
            prompts: vec!["a ".into()],
        }]
    }

    #[tokio::test]
    async fn create_and_lookup() {
        let reg = Registry::new();
        let room = reg.create_room(sets(), None).await;
        assert_eq!(room.code.len(), CODE_LEN);
        assert!(reg.get(&room.code).await.is_some());
        assert!(reg.get("ZZZZ").await.is_none());
        assert_eq!(reg.count().await, 1);
    }

    #[tokio::test]
    async fn codes_are_unique_across_many_rooms() {
        let reg = Registry::new();
        let mut codes = std::collections::HashSet::new();
        for _ in 0..50 {
            let room = reg.create_room(sets(), None).await;
            assert!(codes.insert(room.code), "duplicate code generated");
        }
        assert_eq!(reg.count().await, 50);
    }
}
