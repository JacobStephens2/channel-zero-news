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
//!
//! Until its host join is acknowledged ([`LiveSocket::host_joined`]), a room is
//! removed at its creation deadline, [`IDLE_WITHOUT_SOCKET`] after creation,
//! even with other sockets attached. After that any socket keeps it, and it is
//! removed [`IDLE_WITHOUT_SOCKET`] after the last one closes. Both deadlines go
//! through the same map removal as [`Registry::remove`], which stops the room's
//! actor even while connections still hold its handle; each of them then sends
//! its socket `unknown_room` and closes it. Retention is tracked per room
//! instance, so a removed room's late socket closes and deadlines never touch a
//! new room that reuses its join code.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rand::Rng;
use tokio::sync::oneshot;

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
/// this process is for. Rooms whose host never joins are removed at their
/// creation deadline and rooms with no socket after [`IDLE_WITHOUT_SOCKET`], so
/// the cap bounds hosted rooms that actually have clients.
pub const MAX_LIVE_ROOMS: usize = 128;

/// Join-code draws taken while holding the rooms lock. If none is free,
/// creation returns [`CreateRoomError::NoJoinCode`] and retains nothing.
pub const MAX_JOIN_CODE_ATTEMPTS: usize = 32;

/// How long a room may have zero sockets before it is removed. It is also the
/// creation deadline: a room whose host join is not acknowledged this long
/// after creation is removed, whatever sockets are attached.
///
/// The retention proof waits one second after creates with no socket and
/// requires the registry count to be 0. Half a second leaves schedule margin
/// inside that window. The host client opens its socket and joins immediately
/// after `POST /api/rooms`.
pub const IDLE_WITHOUT_SOCKET: Duration = Duration::from_millis(500);

#[derive(Default)]
pub struct Registry {
    rooms: RwLock<HashMap<String, RetainedRoom>>,
    /// Serial number for the next created room, see [`RoomInstance`].
    next_serial: AtomicU64,
}

struct RetainedRoom {
    handle: RoomHandle,
    /// Unique among the rooms this registry has created.
    serial: u64,
    sockets: usize,
    /// Set once the room acknowledges a host join. Until then the creation
    /// deadline removes the room whatever sockets are attached.
    host_joined: bool,
    /// Bumped whenever the socket count leaves or reaches zero, so an
    /// in-flight idle sleep cannot remove a room that has since gained a
    /// socket or started a newer empty interval.
    idle_generation: u64,
    /// Never sent: dropping it with this entry stops the room's actor.
    _stop: oneshot::Sender<()>,
}

impl RetainedRoom {
    /// Whether this entry is `room`, not a later room with its join code.
    fn is(&self, room: &RoomInstance) -> bool {
        self.serial == room.serial
    }
}

/// One created room: its join code and serial number. Socket closes and
/// deadlines carry the instance they belong to, so once a room is removed
/// they never act on a new room that reuses its join code.
#[derive(Clone)]
struct RoomInstance {
    code: String,
    serial: u64,
}

/// What an armed sleep checks when it wakes, [`IDLE_WITHOUT_SOCKET`] after
/// it was armed.
#[derive(Clone, Copy)]
enum Deadline {
    /// Armed at creation: removes the room unless its host join was
    /// acknowledged, even with sockets attached.
    Creation,
    /// Armed when the last socket closed: removes the room unless a socket
    /// has opened since.
    Idle(u64),
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
/// socket remains. It belongs to one room instance: once that room is removed,
/// dropping it changes nothing, even if a new room has taken the join code.
#[must_use = "dropping this marks the socket closed"]
pub struct LiveSocket {
    registry: Arc<Registry>,
    room: RoomInstance,
}

impl LiveSocket {
    /// Record that the room acknowledged this socket's host join. From then
    /// on any socket keeps the room, which until now its creation deadline
    /// removes even with sockets attached.
    pub fn host_joined(&self) {
        self.registry.host_joined(&self.room);
    }
}

impl Drop for LiveSocket {
    fn drop(&mut self) {
        self.registry.socket_closed(&self.room);
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
    /// join code is drawn within [`MAX_JOIN_CODE_ATTEMPTS`]. The new room is
    /// removed [`IDLE_WITHOUT_SOCKET`] after creation unless a host join is
    /// acknowledged first ([`LiveSocket::host_joined`]).
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
        let (room, host_token) = {
            let mut rooms = self.write();
            if rooms.len() >= MAX_LIVE_ROOMS {
                return Err(CreateRoomError::AtCapacity);
            }
            let code = allocate_code(&rooms, MAX_JOIN_CODE_ATTEMPTS, &mut draw)
                .ok_or(CreateRoomError::NoJoinCode)?;
            let host_token = random_token();
            let serial = self.next_serial.fetch_add(1, Ordering::Relaxed);
            let (stop, stopped) = oneshot::channel();
            let handle = spawn_room(
                code.clone(),
                prompt_sets,
                host_token.clone(),
                db,
                async move {
                    // Resolves with an error once the entry drops `stop`.
                    let _ = stopped.await;
                },
            );
            rooms.insert(
                code.clone(),
                RetainedRoom {
                    handle,
                    serial,
                    sockets: 0,
                    host_joined: false,
                    idle_generation: 0,
                    _stop: stop,
                },
            );
            (RoomInstance { code, serial }, host_token)
        };
        let code = room.code.clone();
        self.arm(room, Deadline::Creation);
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
                room: RoomInstance {
                    code: code.to_string(),
                    serial: room.serial,
                },
            },
        ))
    }

    /// Remove a room from the registry and stop its actor, even while
    /// connections still hold its handle: [`RoomHandle::closed`] resolves.
    pub async fn remove(&self, code: &str) -> bool {
        self.remove_room(code, |_| true)
    }

    pub async fn count(&self) -> usize {
        self.read().len()
    }

    fn host_joined(&self, room: &RoomInstance) {
        let mut rooms = self.write();
        if let Some(retained) = retained_mut(&mut rooms, room) {
            retained.host_joined = true;
        }
    }

    fn socket_closed(self: &Arc<Self>, room: &RoomInstance) {
        let generation = {
            let mut rooms = self.write();
            let Some(retained) = retained_mut(&mut rooms, room) else {
                return;
            };
            retained.sockets -= 1;
            if retained.sockets > 0 {
                return;
            }
            retained.idle_generation = retained.idle_generation.wrapping_add(1);
            retained.idle_generation
        };
        self.arm(room.clone(), Deadline::Idle(generation));
    }

    fn arm(self: &Arc<Self>, room: RoomInstance, deadline: Deadline) {
        let registry = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::time::sleep(IDLE_WITHOUT_SOCKET).await;
            let Some(registry) = registry.upgrade() else {
                return;
            };
            registry.reap(&room, deadline);
        });
    }

    /// Deadline removal goes through [`Self::remove_room`], the same map
    /// removal [`Self::remove`] uses, and only while the room it was armed
    /// for is retained and the deadline still holds.
    fn reap(&self, room: &RoomInstance, deadline: Deadline) {
        self.remove_room(&room.code, |retained| {
            retained.is(room)
                && match deadline {
                    Deadline::Creation => !retained.host_joined,
                    Deadline::Idle(generation) => {
                        retained.sockets == 0 && retained.idle_generation == generation
                    }
                }
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

/// The entry for `room`'s join code, while it is still that room.
fn retained_mut<'a>(
    rooms: &'a mut HashMap<String, RetainedRoom>,
    room: &RoomInstance,
) -> Option<&'a mut RetainedRoom> {
    rooms
        .get_mut(&room.code)
        .filter(|retained| retained.is(room))
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
                panic!("room {code} was still retained past its deadline");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Attach a socket and record its acknowledged host join, as the
    /// connection handler does once the room answers a host with `joined`.
    async fn open_host_socket(reg: &Arc<Registry>, code: &str) -> (RoomHandle, LiveSocket) {
        let (handle, socket) = reg.open_socket(code).await.unwrap();
        socket.host_joined();
        (handle, socket)
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
            sockets.push(open_host_socket(&reg, &room.code).await);
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
            sockets.push(open_host_socket(&reg, &room.code).await);
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
    async fn a_room_whose_host_never_joins_is_removed_with_a_socket_attached_and_its_actor_stops() {
        let reg = Registry::new();
        let created = reg.create_room(sets(), None).await.unwrap();
        // Like a connection, the test holds its own handle while its socket
        // stays attached.
        let (handle, _socket) = reg.open_socket(&created.code).await.unwrap();
        until_removed(&reg, &created.code).await;
        assert_eq!(reg.count().await, 0);
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test]
    async fn a_removed_rooms_late_socket_close_leaves_a_new_room_with_its_code_retained() {
        let reg = Registry::new();
        reg.create_room_drawing(sets(), None, || "ABCD".into())
            .await
            .unwrap();
        // No host join, so the creation deadline removes it with this socket
        // still attached.
        let (_removed_handle, removed_socket) = reg.open_socket("ABCD").await.unwrap();
        until_removed(&reg, "ABCD").await;

        reg.create_room_drawing(sets(), None, || "ABCD".into())
            .await
            .unwrap();
        let (_handle, _socket) = open_host_socket(&reg, "ABCD").await;
        drop(removed_socket);
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(300)).await;
        assert!(
            reg.get("ABCD").await.is_some(),
            "the removed room's socket close was counted against the new room"
        );
    }

    #[tokio::test]
    async fn an_open_socket_keeps_the_room_until_the_deadline_after_it_closes() {
        let reg = Registry::new();
        let created = reg.create_room(sets(), None).await.unwrap();
        let code = created.code.clone();
        let (_handle, socket) = open_host_socket(&reg, &code).await;
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
        let (_first, first) = open_host_socket(&reg, &code).await;
        let (_second, second) = reg.open_socket(&code).await.unwrap();
        drop(first);
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(300)).await;
        assert!(reg.get(&code).await.is_some());
        drop(second);
        until_removed(&reg, &code).await;
    }

    /// The actor has stopped even though `handle` is still held.
    async fn assert_actor_stopped(handle: &RoomHandle) {
        tokio::time::timeout(Duration::from_secs(5), handle.closed())
            .await
            .expect("room actor was still running after its removal");
        let (direct, _replies) = crate::direct::channel();
        let ping = crate::room::Command {
            intent: crate::protocol::ClientMsg::Ping,
            role: crate::protocol::Role::Display,
            name: None,
            direct,
        };
        assert!(
            handle.dispatch(ping).await.is_err(),
            "a stopped room actor accepted a command"
        );
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
