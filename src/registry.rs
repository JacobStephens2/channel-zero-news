//! In-memory registry of live rooms, keyed by join code.
//!
//! Rooms live entirely in memory (only prompt sets, final responses, and
//! archives are persisted — see the M5 db layer). The registry maps a join code
//! to its [`RoomHandle`] behind an `RwLock`, since lookups vastly outnumber the
//! rare create/remove. Per-room state itself is *not* locked here: it is owned
//! by the room's actor task.
//!
//! Creation retains at most [`MAX_LIVE_ROOMS`] rooms and draws at most
//! [`MAX_JOIN_CODE_ATTEMPTS`] join codes. Past either limit it retains nothing.
//! A room with no socket for [`IDLE_WITHOUT_SOCKET`] is dropped through the
//! same map removal as [`Registry::remove`], which ends its actor once nothing
//! else holds the handle.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rand::Rng;

use crate::db::Db;
use crate::room::{spawn_room, RoomHandle};
use crate::state::PromptSet;

/// Characters used for join codes: unambiguous uppercase letters/digits.
const CODE_ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
const CODE_LEN: usize = 4;

/// Live rooms [`Registry::create_room`] will retain. One more creation returns
/// [`CreateRoomError::AtCapacity`] and leaves the map unchanged.
///
/// 128 is far below the 31^4 join-code space and more concurrent parties than
/// this process is for. Rooms with no socket are removed after
/// [`IDLE_WITHOUT_SOCKET`], so the cap bounds rooms that actually have clients.
pub const MAX_LIVE_ROOMS: usize = 128;

/// Join-code draws taken while holding the rooms lock. If none is free,
/// creation returns [`CreateRoomError::NoJoinCode`] and retains nothing.
pub const MAX_JOIN_CODE_ATTEMPTS: usize = 32;

/// How long a room may have zero sockets before it is removed.
///
/// The retention proof waits one second after creates with no socket and
/// requires the registry count to be 0. Half a second leaves schedule margin
/// inside that window. The host client opens its socket immediately after
/// `POST /api/rooms`.
pub const IDLE_WITHOUT_SOCKET: Duration = Duration::from_millis(500);

/// First idle generation armed at insert. Later empty intervals bump it so an
/// in-flight sleep cannot remove a room that has since gained a socket or
/// started a newer empty interval.
const INITIAL_IDLE_GENERATION: u64 = 1;

#[derive(Default)]
pub struct Registry {
    rooms: RwLock<HashMap<String, RetainedRoom>>,
}

struct RetainedRoom {
    handle: RoomHandle,
    sockets: usize,
    idle_generation: u64,
}

/// What a freshly created room hands back to the host: the public join code and
/// the secret host token.
#[derive(Debug, Clone)]
pub struct NewRoom {
    pub code: String,
    pub host_token: String,
}

/// Why [`Registry::create_room`] retained nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CreateRoomError {
    /// [`MAX_LIVE_ROOMS`] rooms are already retained.
    #[error("too many live rooms")]
    AtCapacity,
    /// No free join code turned up within [`MAX_JOIN_CODE_ATTEMPTS`].
    #[error("could not allocate a join code")]
    NoJoinCode,
}

/// Held for as long as one WebSocket is attached to a room. Dropping it,
/// including when the connection task ends, starts the idle deadline once no
/// socket remains.
#[must_use = "dropping this marks the socket closed"]
pub struct LiveSocket {
    registry: Arc<Registry>,
    code: String,
}

impl Drop for LiveSocket {
    fn drop(&mut self) {
        self.registry.socket_closed(&self.code);
    }
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Create a room with the given prompt-set catalog and start its actor.
    /// Generates a unique join code and a secret host token. `db` is handed to
    /// the room actor for durable effects (`None` to disable persistence).
    ///
    /// Retains nothing when the live-room cap is already met or when no free
    /// join code is drawn within [`MAX_JOIN_CODE_ATTEMPTS`]. The new room
    /// starts with no socket, so [`IDLE_WITHOUT_SOCKET`] removes it unless a
    /// socket attaches first.
    pub async fn create_room(
        self: &Arc<Self>,
        prompt_sets: Vec<PromptSet>,
        db: Option<Db>,
    ) -> Result<NewRoom, CreateRoomError> {
        self.create_room_drawing(prompt_sets, db, random_code).await
    }

    async fn create_room_drawing(
        self: &Arc<Self>,
        prompt_sets: Vec<PromptSet>,
        db: Option<Db>,
        mut draw: impl FnMut() -> String,
    ) -> Result<NewRoom, CreateRoomError> {
        let (code, host_token) = {
            let mut rooms = self.write();
            if rooms.len() >= MAX_LIVE_ROOMS {
                return Err(CreateRoomError::AtCapacity);
            }
            let code = allocate_code(&rooms, MAX_JOIN_CODE_ATTEMPTS, &mut draw)
                .ok_or(CreateRoomError::NoJoinCode)?;
            let host_token = random_token();
            let handle = spawn_room(code.clone(), prompt_sets, host_token.clone(), db);
            rooms.insert(
                code.clone(),
                RetainedRoom {
                    handle,
                    sockets: 0,
                    idle_generation: INITIAL_IDLE_GENERATION,
                },
            );
            (code, host_token)
        };
        self.arm_idle(code.clone(), INITIAL_IDLE_GENERATION);
        Ok(NewRoom { code, host_token })
    }

    /// Look up a live room by join code. Does not count as a socket.
    pub async fn get(&self, code: &str) -> Option<RoomHandle> {
        self.read().get(code).map(|room| room.handle.clone())
    }

    /// Attach one socket to a live room. `None` when the code is not retained.
    /// The returned guard is that socket: drop it when the connection ends.
    pub async fn open_socket(self: &Arc<Self>, code: &str) -> Option<(RoomHandle, LiveSocket)> {
        let mut rooms = self.write();
        let room = rooms.get_mut(code)?;
        if room.sockets == 0 {
            // Invalidate the empty-interval sleep armed while no socket was open.
            room.idle_generation = room.idle_generation.wrapping_add(1);
        }
        room.sockets += 1;
        let handle = room.handle.clone();
        Some((
            handle,
            LiveSocket {
                registry: Arc::clone(self),
                code: code.to_string(),
            },
        ))
    }

    /// Remove a room from the registry (its actor stops once all handles drop).
    pub async fn remove(&self, code: &str) -> bool {
        self.remove_room(code, |_| true)
    }

    pub async fn count(&self) -> usize {
        self.read().len()
    }

    fn socket_closed(self: &Arc<Self>, code: &str) {
        let generation = {
            let mut rooms = self.write();
            let Some(room) = rooms.get_mut(code) else {
                return;
            };
            if room.sockets == 0 {
                return;
            }
            room.sockets -= 1;
            if room.sockets > 0 {
                return;
            }
            room.idle_generation = room.idle_generation.wrapping_add(1);
            room.idle_generation
        };
        self.arm_idle(code.to_string(), generation);
    }

    fn arm_idle(self: &Arc<Self>, code: String, generation: u64) {
        let registry = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::time::sleep(IDLE_WITHOUT_SOCKET).await;
            let Some(registry) = registry.upgrade() else {
                return;
            };
            registry.reap_idle(&code, generation);
        });
    }

    /// Idle removal goes through [`Self::remove_room`], the same map removal
    /// [`Self::remove`] uses, and only while this empty interval is still current.
    fn reap_idle(&self, code: &str, generation: u64) {
        self.remove_room(code, |room| {
            room.sockets == 0 && room.idle_generation == generation
        });
    }

    fn remove_room(&self, code: &str, allow: impl FnOnce(&RetainedRoom) -> bool) -> bool {
        let mut rooms = self.write();
        let allowed = rooms.get(code).is_some_and(allow);
        if allowed {
            rooms.remove(code).is_some()
        } else {
            false
        }
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, RetainedRoom>> {
        self.rooms
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, RetainedRoom>> {
        self.rooms
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn allocate_code(
    rooms: &HashMap<String, RetainedRoom>,
    attempts: usize,
    mut draw: impl FnMut() -> String,
) -> Option<String> {
    for _ in 0..attempts {
        let candidate = draw();
        if !rooms.contains_key(&candidate) {
            return Some(candidate);
        }
    }
    None
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
    use std::time::Duration;
    use tokio::sync::broadcast;

    fn sets() -> Vec<PromptSet> {
        vec![PromptSet {
            id: 1,
            name: "Set".into(),
            author: "Eric".into(),
            prompts: vec!["a ".into()],
        }]
    }

    async fn until_removed(reg: &Registry, code: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if reg.get(code).await.is_none() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("room {code} was still retained with no socket");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn create_and_lookup() {
        let reg = Registry::new();
        let room = reg.create_room(sets(), None).await.unwrap();
        assert_eq!(room.code.len(), CODE_LEN);
        assert!(reg.get(&room.code).await.is_some());
        assert!(reg.get("ZZZZ").await.is_none());
        assert_eq!(reg.count().await, 1);
    }

    #[tokio::test]
    async fn codes_are_unique_across_many_rooms() {
        let reg = Registry::new();
        let mut codes = std::collections::HashSet::new();
        let mut sockets = Vec::new();
        for _ in 0..50 {
            let room = reg.create_room(sets(), None).await.unwrap();
            assert!(codes.insert(room.code.clone()), "duplicate code generated");
            sockets.push(reg.open_socket(&room.code).await.unwrap());
        }
        assert_eq!(reg.count().await, 50);
    }

    #[tokio::test]
    async fn create_room_retains_at_most_the_live_room_cap() {
        let reg = Registry::new();
        let mut sockets = Vec::new();
        let mut first = None;
        for _ in 0..MAX_LIVE_ROOMS {
            let room = reg.create_room(sets(), None).await.unwrap();
            first.get_or_insert_with(|| room.code.clone());
            sockets.push(reg.open_socket(&room.code).await.unwrap());
        }
        let err = reg.create_room(sets(), None).await.unwrap_err();
        assert_eq!(err, CreateRoomError::AtCapacity);
        assert_eq!(reg.count().await, MAX_LIVE_ROOMS);
        assert!(reg.get(first.as_deref().unwrap()).await.is_some());
    }

    #[tokio::test]
    async fn join_code_attempts_are_bounded_and_a_miss_retains_nothing() {
        let reg = Registry::new();
        let created = reg
            .create_room_drawing(sets(), None, || "ABCD".into())
            .await
            .unwrap();
        assert_eq!(created.code, "ABCD");

        let mut draws = 0;
        let err = reg
            .create_room_drawing(sets(), None, || {
                draws += 1;
                "ABCD".into()
            })
            .await
            .unwrap_err();
        assert_eq!(err, CreateRoomError::NoJoinCode);
        assert_eq!(draws, MAX_JOIN_CODE_ATTEMPTS);
        assert_eq!(reg.count().await, 1);
        assert!(reg.get("ABCD").await.is_some());
    }

    #[tokio::test]
    async fn the_last_join_code_attempt_is_kept() {
        let reg = Registry::new();
        reg.create_room_drawing(sets(), None, || "TAKEN".into())
            .await
            .unwrap();
        let mut draws = 0;
        let room = reg
            .create_room_drawing(sets(), None, || {
                draws += 1;
                if draws < MAX_JOIN_CODE_ATTEMPTS {
                    "TAKEN".into()
                } else {
                    "FREE1".into()
                }
            })
            .await
            .unwrap();
        assert_eq!(room.code, "FREE1");
        assert_eq!(draws, MAX_JOIN_CODE_ATTEMPTS);
        assert_eq!(reg.count().await, 2);
    }

    #[tokio::test]
    async fn remove_drops_the_registry_handle_so_the_actor_ends() {
        let reg = Registry::new();
        let created = reg.create_room(sets(), None).await.unwrap();
        let handle = reg.get(&created.code).await.unwrap();
        let mut events = handle.subscribe();
        drop(handle);
        assert!(reg.remove(&created.code).await);
        assert_actor_ended(&mut events).await;
    }

    #[tokio::test]
    async fn a_room_with_no_socket_is_removed_and_its_actor_ends() {
        let reg = Registry::new();
        let created = reg.create_room(sets(), None).await.unwrap();
        let handle = reg.get(&created.code).await.unwrap();
        let mut events = handle.subscribe();
        drop(handle);
        until_removed(&reg, &created.code).await;
        assert_eq!(reg.count().await, 0);
        assert_actor_ended(&mut events).await;
    }

    #[tokio::test]
    async fn an_open_socket_keeps_the_room_until_the_deadline_after_it_closes() {
        let reg = Registry::new();
        let created = reg.create_room(sets(), None).await.unwrap();
        let code = created.code.clone();
        let (_handle, socket) = reg.open_socket(&code).await.unwrap();
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(300)).await;
        assert!(
            reg.get(&code).await.is_some(),
            "an open socket should keep the room"
        );

        drop(socket);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            reg.get(&code).await.is_some(),
            "the room stays until the idle deadline"
        );

        until_removed(&reg, &code).await;
    }

    #[tokio::test]
    async fn the_room_stays_while_any_socket_is_open() {
        let reg = Registry::new();
        let created = reg.create_room(sets(), None).await.unwrap();
        let code = created.code.clone();
        let (_first, first) = reg.open_socket(&code).await.unwrap();
        let (_second, second) = reg.open_socket(&code).await.unwrap();
        drop(first);
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(300)).await;
        assert!(reg.get(&code).await.is_some());
        drop(second);
        until_removed(&reg, &code).await;
    }

    async fn assert_actor_ended(events: &mut broadcast::Receiver<crate::protocol::ServerMsg>) {
        tokio::select! {
            ended = events.recv() => {
                assert!(
                    matches!(ended, Err(broadcast::error::RecvError::Closed)),
                    "actor dropped its broadcast sender, got {ended:?}"
                );
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                panic!("room actor was still running after registry removal");
            }
        }
    }
}
