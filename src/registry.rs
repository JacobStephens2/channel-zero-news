//! In-memory registry of live rooms, keyed by join code.
//!
//! Rooms live entirely in memory (only prompt sets, final responses, and
//! archives are persisted — see the M5 db layer). The registry maps a join code
//! to its [`RoomHandle`] behind an `RwLock`, since lookups vastly outnumber the
//! rare create/remove. Per-room state itself is *not* locked here: it is owned
//! by the room's actor task.
//!
//! Creation retains at most [`MAX_LIVE_ROOMS`] rooms, at most
//! [`MAX_ROOMS_PER_CLIENT`] of them created by one [`ClientAddress`], and draws
//! at most [`MAX_JOIN_CODE_ATTEMPTS`] join codes. Past any limit it retains
//! nothing.
//!
//! One watcher per retained room checks the first of three deadlines:
//!
//! * **Host join:** [`IDLE_WITHOUT_SOCKET`] after creation unless its host
//!   join is acknowledged before then ([`LiveSocket::host_joined`]), even
//!   with other sockets attached.
//! * **No socket:** after that any socket keeps it, until
//!   [`IDLE_WITHOUT_SOCKET`] after the last one closes.
//! * **Activity:** [`IDLE_WITHOUT_ACTIVITY`] after its creation or its last
//!   room activity, even with sockets attached. Room activity is an intent the
//!   room accepts and broadcasts as a state change; pings, joins that add no
//!   player and rejected intents are not activity.
//!
//! Every deadline goes through the same map removal as [`Registry::remove`],
//! which stops the room's actor even while connections still hold its handle;
//! each of them then sends its socket `unknown_room` and closes it. Retention,
//! activity included, is tracked per room instance, so a removed room's late
//! socket closes and deadlines never touch a new room that reuses its join
//! code.

use std::collections::HashMap;
use std::future::pending;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rand::Rng;
use tokio::sync::{oneshot, watch};
use tokio::time::{sleep_until, Instant};

use crate::client_address::ClientAddress;
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
/// creation deadline, rooms with no socket after [`IDLE_WITHOUT_SOCKET`] and
/// rooms without activity after [`IDLE_WITHOUT_ACTIVITY`], so the cap bounds
/// hosted rooms that have clients and are still in play.
pub const MAX_LIVE_ROOMS: usize = 128;

/// Live rooms one [`ClientAddress`] may have created. Its next creation returns
/// [`CreateRoomError::AtQuota`] and leaves the map unchanged; it can create
/// again once one of its rooms is removed.
pub const MAX_ROOMS_PER_CLIENT: usize = 8;

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

/// How long a room may go without room activity before it is removed,
/// whatever sockets are attached, counted from its creation or its last
/// activity. Room activity is an intent the room accepts and broadcasts as a
/// state change, so sockets that only ping or join without adding a player
/// do not keep a room.
///
/// Issue #15 sets 2 hours. A round in play changes state with each roster
/// change, submission and slide move; a room no one plays in holds one of the
/// [`MAX_LIVE_ROOMS`] slots for at most 2 hours after its last change,
/// however many sockets stay attached.
pub const IDLE_WITHOUT_ACTIVITY: Duration = Duration::from_secs(2 * 60 * 60);

#[derive(Default)]
pub struct Registry {
    rooms: RwLock<HashMap<String, RetainedRoom>>,
    /// Serial number for the next created room, see [`RoomInstance`].
    next_serial: AtomicU64,
}

struct RetainedRoom {
    handle: RoomHandle,
    creator: ClientAddress,
    /// Unique among the rooms this registry has created.
    serial: u64,
    sockets: usize,
    /// Set once the room acknowledges a host join. Until then the creation
    /// deadline removes the room whatever sockets are attached.
    host_joined: bool,
    /// Creation deadline before host acknowledgement, then the deadline for
    /// the current empty interval, if any. Dropping it cancels the watcher.
    socket_deadline: watch::Sender<Option<Instant>>,
    /// Never sent: dropping it with this entry stops the room's actor.
    _stop: oneshot::Sender<()>,
}

impl RetainedRoom {
    /// Whether this entry is `room`, not a later room with its join code.
    fn is(&self, room: &RoomInstance) -> bool {
        self.serial == room.serial
    }

    fn socket_opened(&mut self) {
        self.sockets += 1;
        if self.host_joined && self.sockets == 1 {
            self.socket_deadline.send_replace(None);
        }
    }

    fn acknowledge_host(&mut self) {
        if self.host_joined {
            return;
        }
        let creation_deadline = *self.socket_deadline.borrow();
        if creation_deadline.is_some_and(|deadline| Instant::now() < deadline) {
            self.host_joined = true;
            // The guard reporting this acknowledgement still counts.
            self.socket_deadline.send_replace(None);
        }
    }

    fn socket_closed(&mut self) {
        self.sockets -= 1;
        if self.host_joined && self.sockets == 0 {
            self.socket_deadline
                .send_replace(Some(Instant::now() + IDLE_WITHOUT_SOCKET));
        }
    }
}

/// One created room: its join code and serial number. Socket closes and
/// deadlines carry the instance they belong to, so once a room is removed
/// they never act on a new room that reuses its join code.
struct RoomInstance {
    code: String,
    serial: u64,
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
    /// The creating address already has [`MAX_ROOMS_PER_CLIENT`] live rooms.
    #[error("too many live rooms from this address")]
    AtQuota,
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
    /// Record that the room acknowledged this socket's host join before the
    /// creation deadline. From then on any socket clears the socket deadline.
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

    /// Create a room for `creator` with the given prompt-set catalog and start
    /// its actor. Generates a unique join code and a secret host token. `db` is
    /// handed to the room actor for durable effects (`None` to disable
    /// persistence).
    ///
    /// Retains nothing when the live-room cap is already met, when `creator`
    /// already has [`MAX_ROOMS_PER_CLIENT`] live rooms, or when no free join
    /// code is drawn within [`MAX_JOIN_CODE_ATTEMPTS`]. The cap is checked
    /// first, so a full registry returns [`CreateRoomError::AtCapacity`] to
    /// every address. The new room is removed [`IDLE_WITHOUT_SOCKET`] after
    /// creation unless a host join is acknowledged first
    /// ([`LiveSocket::host_joined`]).
    pub async fn create_room(
        self: &Arc<Self>,
        creator: ClientAddress,
        prompt_sets: Vec<PromptSet>,
        db: Option<Db>,
    ) -> Result<NewRoom, CreateRoomError> {
        self.create_room_drawing(creator, prompt_sets, db, random_code)
            .await
    }

    async fn create_room_drawing(
        self: &Arc<Self>,
        creator: ClientAddress,
        prompt_sets: Vec<PromptSet>,
        db: Option<Db>,
        mut draw: impl FnMut() -> String,
    ) -> Result<NewRoom, CreateRoomError> {
        let (room, host_token, socket_changes, activity) = {
            let mut rooms = self.write();
            if rooms.len() >= MAX_LIVE_ROOMS {
                return Err(CreateRoomError::AtCapacity);
            }
            let created = rooms.values().filter(|room| room.creator == creator);
            if created.count() >= MAX_ROOMS_PER_CLIENT {
                return Err(CreateRoomError::AtQuota);
            }
            let code = allocate_code(&rooms, MAX_JOIN_CODE_ATTEMPTS, &mut draw)
                .ok_or(CreateRoomError::NoJoinCode)?;
            let host_token = random_token();
            let serial = self.next_serial.fetch_add(1, Ordering::Relaxed);
            let created_at = Instant::now();
            let (socket_deadline, socket_changes) =
                watch::channel(Some(created_at + IDLE_WITHOUT_SOCKET));
            let (publish_activity, activity) = watch::channel(created_at);
            let (stop, stopped) = oneshot::channel();
            let handle = spawn_room(
                code.clone(),
                prompt_sets,
                host_token.clone(),
                db,
                publish_activity,
                async move {
                    // Resolves with an error once the entry drops `stop`.
                    let _ = stopped.await;
                },
            );
            rooms.insert(
                code.clone(),
                RetainedRoom {
                    handle,
                    creator,
                    serial,
                    sockets: 0,
                    host_joined: false,
                    socket_deadline,
                    _stop: stop,
                },
            );
            (
                RoomInstance { code, serial },
                host_token,
                socket_changes,
                activity,
            )
        };
        let code = room.code.clone();
        self.watch_deadlines(room, socket_changes, activity);
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
        room.socket_opened();
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
        Self::remove_room(&mut self.write(), code)
    }

    pub async fn count(&self) -> usize {
        self.read().len()
    }

    fn host_joined(&self, room: &RoomInstance) {
        let mut rooms = self.write();
        if let Some(retained) = retained_mut(&mut rooms, room) {
            retained.acknowledge_host();
        }
    }

    fn socket_closed(&self, room: &RoomInstance) {
        let mut rooms = self.write();
        if let Some(retained) = retained_mut(&mut rooms, room) {
            retained.socket_closed();
        }
    }

    fn watch_deadlines(
        self: &Arc<Self>,
        room: RoomInstance,
        mut socket_changes: watch::Receiver<Option<Instant>>,
        mut activity: watch::Receiver<Instant>,
    ) {
        let registry = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut activity_open = true;
            loop {
                let check = match registry.upgrade() {
                    Some(registry) => registry.recheck_deadlines(&room, &activity),
                    None => return,
                };
                let ControlFlow::Continue(deadline) = check else {
                    return;
                };
                tokio::select! {
                    changed = socket_changes.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    changed = activity.changed(), if activity_open => {
                        activity_open = changed.is_ok();
                    }
                    () = async {
                        match deadline {
                            Some(deadline) => sleep_until(deadline).await,
                            None => pending().await,
                        }
                    } => {}
                }
            }
        });
    }

    fn recheck_deadlines(
        &self,
        room: &RoomInstance,
        activity: &watch::Receiver<Instant>,
    ) -> ControlFlow<(), Option<Instant>> {
        let mut rooms = self.write();
        let Some(retained) = rooms.get(&room.code).filter(|retained| retained.is(room)) else {
            return ControlFlow::Break(());
        };
        // Read activity under the registry lock and keep the observation
        // borrowed through removal so a concurrent publish cannot race it.
        let observed = activity.borrow();
        let activity_deadline = activity
            .has_changed()
            .is_ok()
            .then(|| *observed + IDLE_WITHOUT_ACTIVITY);
        let socket_deadline = *retained.socket_deadline.borrow();
        let deadline = socket_deadline.into_iter().chain(activity_deadline).min();
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            Self::remove_room(&mut rooms, &room.code);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(deadline)
    }

    fn remove_room(rooms: &mut HashMap<String, RetainedRoom>, code: &str) -> bool {
        // Dropping the entry stops its actor and wakes its watcher to exit.
        rooms.remove(code).is_some()
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

    fn sets() -> Vec<PromptSet> {
        vec![PromptSet {
            id: 1,
            name: "Set".into(),
            author: "Eric".into(),
            prompts: vec!["a ".into()],
        }]
    }

    /// A distinct client address for each `n` below 256.
    fn address(n: usize) -> ClientAddress {
        let last = u8::try_from(n).expect("test addresses stay in 203.0.113.0/24");
        ClientAddress::key_for(std::net::IpAddr::from([203, 0, 113, last]))
    }

    /// Create a room for `creator` and hold an acknowledged host socket on it,
    /// so the idle deadline keeps the room while the guard lives.
    async fn create_with_socket(
        reg: &Arc<Registry>,
        creator: ClientAddress,
    ) -> (String, LiveSocket) {
        let room = reg.create_room(creator, sets(), None).await.unwrap();
        let (_handle, socket) = open_host_socket(reg, &room.code).await;
        (room.code, socket)
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
        let room = reg.create_room(address(0), sets(), None).await.unwrap();
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
        for n in 0..50 {
            let (code, socket) = create_with_socket(&reg, address(n)).await;
            assert!(codes.insert(code), "duplicate code generated");
            sockets.push(socket);
        }
        assert_eq!(reg.count().await, 50);
    }

    #[tokio::test]
    async fn create_room_retains_at_most_the_live_room_cap() {
        let reg = Registry::new();
        let mut sockets = Vec::new();
        let mut first = None;
        for n in 0..MAX_LIVE_ROOMS {
            let (code, socket) = create_with_socket(&reg, address(n)).await;
            first.get_or_insert(code);
            sockets.push(socket);
        }
        let err = reg
            .create_room(address(MAX_LIVE_ROOMS), sets(), None)
            .await
            .unwrap_err();
        assert_eq!(err, CreateRoomError::AtCapacity);
        assert_eq!(reg.count().await, MAX_LIVE_ROOMS);
        assert!(reg.get(first.as_deref().unwrap()).await.is_some());
    }

    #[tokio::test]
    async fn an_address_holds_at_most_its_quota_of_live_rooms_and_others_still_create() {
        let reg = Registry::new();
        let mut sockets = Vec::new();
        for _ in 0..MAX_ROOMS_PER_CLIENT {
            let (_code, socket) = create_with_socket(&reg, address(7)).await;
            sockets.push(socket);
        }
        let err = reg.create_room(address(7), sets(), None).await.unwrap_err();
        assert_eq!(err, CreateRoomError::AtQuota);
        assert_eq!(reg.count().await, MAX_ROOMS_PER_CLIENT);
        reg.create_room(address(8), sets(), None)
            .await
            .expect("another address is under its own quota");
    }

    #[tokio::test]
    async fn an_address_at_its_quota_creates_again_once_one_of_its_rooms_is_removed() {
        let reg = Registry::new();
        let mut codes = Vec::new();
        let mut sockets = Vec::new();
        for _ in 0..MAX_ROOMS_PER_CLIENT {
            let (code, socket) = create_with_socket(&reg, address(7)).await;
            codes.push(code);
            sockets.push(socket);
        }
        assert!(reg.remove(&codes[0]).await);
        reg.create_room(address(7), sets(), None)
            .await
            .expect("removing a room frees one place in the address's quota");
    }

    #[tokio::test]
    async fn at_the_live_room_cap_an_address_at_its_quota_still_gets_at_capacity() {
        let reg = Registry::new();
        let mut sockets = Vec::new();
        for n in 0..MAX_LIVE_ROOMS {
            let creator = address(n / MAX_ROOMS_PER_CLIENT);
            let (_code, socket) = create_with_socket(&reg, creator).await;
            sockets.push(socket);
        }
        let err = reg.create_room(address(0), sets(), None).await.unwrap_err();
        assert_eq!(err, CreateRoomError::AtCapacity);
    }

    #[tokio::test]
    async fn join_code_attempts_are_bounded_and_a_miss_retains_nothing() {
        let reg = Registry::new();
        let created = reg
            .create_room_drawing(address(0), sets(), None, || "ABCD".into())
            .await
            .unwrap();
        assert_eq!(created.code, "ABCD");

        let mut draws = 0;
        let err = reg
            .create_room_drawing(address(0), sets(), None, || {
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
        reg.create_room_drawing(address(0), sets(), None, || "TAKEN".into())
            .await
            .unwrap();
        let mut draws = 0;
        let room = reg
            .create_room_drawing(address(0), sets(), None, || {
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
    async fn remove_stops_the_actor_while_a_handle_is_held() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let handle = reg.get(&created.code).await.unwrap();
        assert!(reg.remove(&created.code).await);
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test]
    async fn a_room_with_no_socket_is_removed_and_its_actor_stops() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let handle = reg.get(&created.code).await.unwrap();
        until_removed(&reg, &created.code).await;
        assert_eq!(reg.count().await, 0);
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn the_creation_deadline_counts_from_creation_even_if_the_watcher_has_not_run() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let (handle, _socket) = reg.open_socket(&created.code).await.unwrap();

        // No spawned task has polled yet. Move time to the creation deadline.
        tokio::time::advance(IDLE_WITHOUT_SOCKET).await;
        tokio::time::timeout(Duration::from_millis(5), handle.closed())
            .await
            .expect("the creation deadline slid past 500 ms");
        assert!(reg.get(&created.code).await.is_none());
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test]
    async fn a_late_host_acknowledgement_cannot_cancel_creation_expiry() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let (handle, socket) = reg.open_socket(&created.code).await.unwrap();

        std::thread::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(25));
        socket.host_joined();

        tokio::time::timeout(Duration::from_secs(1), handle.closed())
            .await
            .expect("the late host acknowledgement kept the actor alive");
        assert!(reg.get(&created.code).await.is_none());
    }

    #[tokio::test]
    async fn a_room_whose_host_never_joins_is_removed_with_a_socket_attached_and_its_actor_stops() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        // Like a connection, the test holds its own handle while its socket
        // stays attached.
        let (handle, _socket) = reg.open_socket(&created.code).await.unwrap();
        until_removed(&reg, &created.code).await;
        assert_eq!(reg.count().await, 0);
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_removed_rooms_late_socket_close_leaves_a_new_room_with_its_code_retained() {
        let reg = Registry::new();
        reg.create_room_drawing(address(0), sets(), None, || "ABCD".into())
            .await
            .unwrap();
        // No host join, so the creation deadline removes it with this socket
        // still attached.
        let (removed_handle, removed_socket) = reg.open_socket("ABCD").await.unwrap();
        until_removed(&reg, "ABCD").await;
        assert_actor_stopped(&removed_handle).await;

        reg.create_room_drawing(address(0), sets(), None, || "ABCD".into())
            .await
            .unwrap();
        let (handle, socket) = open_host_socket(&reg, "ABCD").await;
        drop(removed_socket);
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(10)).await;
        assert!(
            reg.get("ABCD").await.is_some(),
            "the removed room's socket close was counted against the new room"
        );
        drop(socket);
        until_removed(&reg, "ABCD").await;
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_open_socket_keeps_the_room_until_the_deadline_after_it_closes() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let code = created.code;
        let (handle, socket) = open_host_socket(&reg, &code).await;
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(10)).await;
        assert!(
            reg.get(&code).await.is_some(),
            "an open socket should keep the room"
        );

        drop(socket);
        tokio::time::sleep(IDLE_WITHOUT_SOCKET - Duration::from_millis(10)).await;
        assert!(
            reg.get(&code).await.is_some(),
            "the room stays until the idle deadline"
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(reg.get(&code).await.is_none());
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn reopening_and_closing_again_gives_the_room_a_fresh_empty_interval() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let code = created.code;
        let (handle, first) = open_host_socket(&reg, &code).await;
        drop(first);
        let first_deadline = Instant::now() + IDLE_WITHOUT_SOCKET;

        tokio::time::sleep(IDLE_WITHOUT_SOCKET / 2).await;
        let (_, second) = reg.open_socket(&code).await.unwrap();
        drop(second);
        let second_deadline = Instant::now() + IDLE_WITHOUT_SOCKET;

        tokio::time::sleep_until(first_deadline + Duration::from_millis(10)).await;
        assert!(reg.get(&code).await.is_some(), "the first interval expired");
        tokio::time::sleep_until(second_deadline - Duration::from_millis(10)).await;
        assert!(
            reg.get(&code).await.is_some(),
            "the second interval is still active"
        );
        tokio::time::sleep_until(second_deadline + Duration::from_millis(10)).await;
        assert!(reg.get(&code).await.is_none());
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn the_room_stays_while_any_socket_is_open() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let code = created.code.clone();
        let (handle, first) = open_host_socket(&reg, &code).await;
        let (_second, second) = reg.open_socket(&code).await.unwrap();
        drop(first);
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(10)).await;
        assert!(reg.get(&code).await.is_some());
        drop(second);
        until_removed(&reg, &code).await;
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn the_socket_deadline_still_removes_a_hosted_room_after_its_actor_exits() {
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let code = created.code;
        let (handle, socket) = open_host_socket(&reg, &code).await;
        handle.actor.abort();
        assert_actor_stopped(&handle).await;
        tokio::time::sleep(IDLE_WITHOUT_ACTIVITY + Duration::from_secs(1)).await;
        assert!(reg.get(&code).await.is_some());

        drop(socket);
        tokio::time::sleep(IDLE_WITHOUT_SOCKET - Duration::from_millis(10)).await;
        assert!(reg.get(&code).await.is_some());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(reg.get(&code).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_published_activity_observation_at_the_deadline_wins_over_expiry() {
        let deadline = Instant::now() + IDLE_WITHOUT_ACTIVITY;
        let (publish_activity, activity) = watch::channel(Instant::now());
        // Keep the live watcher's own activity deadline just beyond the one
        // supplied to the recheck, so only the recheck decides this tie.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let reg = Registry::new();
        let created = reg.create_room(address(0), sets(), None).await.unwrap();
        let (_, socket) = open_host_socket(&reg, &created.code).await;

        tokio::time::sleep_until(deadline).await;
        let refreshed = Instant::now();
        publish_activity.send_replace(refreshed);
        assert_eq!(
            reg.recheck_deadlines(&socket.room, &activity),
            ControlFlow::Continue(Some(refreshed + IDLE_WITHOUT_ACTIVITY))
        );
        assert!(reg.get(&created.code).await.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_removed_rooms_socket_deadline_leaves_a_new_room_with_its_code_retained() {
        let reg = Registry::new();
        reg.create_room_drawing(address(0), sets(), None, || "ABCD".into())
            .await
            .unwrap();
        let (old_handle, socket) = open_host_socket(&reg, "ABCD").await;
        drop(socket);
        let old_deadline = Instant::now() + IDLE_WITHOUT_SOCKET;
        tokio::time::sleep(IDLE_WITHOUT_SOCKET / 2).await;
        assert!(reg.remove("ABCD").await);
        assert_actor_stopped(&old_handle).await;

        reg.create_room_drawing(address(0), sets(), None, || "ABCD".into())
            .await
            .unwrap();
        let new_deadline = Instant::now() + IDLE_WITHOUT_SOCKET;
        let handle = reg.get("ABCD").await.unwrap();
        tokio::time::sleep_until(old_deadline + Duration::from_millis(10)).await;
        assert!(reg.get("ABCD").await.is_some());
        tokio::time::sleep_until(new_deadline + Duration::from_millis(10)).await;
        assert!(reg.get("ABCD").await.is_none());
        assert_actor_stopped(&handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_removed_rooms_activity_deadline_leaves_a_new_room_with_its_code_retained() {
        let reg = Registry::new();
        reg.create_room_drawing(address(0), sets(), None, || "ABCD".into())
            .await
            .unwrap();
        let (old_handle, old_socket) = open_host_socket(&reg, "ABCD").await;
        tokio::time::sleep(IDLE_WITHOUT_ACTIVITY / 2).await;
        assert!(reg.remove("ABCD").await);
        assert_actor_stopped(&old_handle).await;
        drop(old_socket);

        reg.create_room_drawing(address(0), sets(), None, || "ABCD".into())
            .await
            .unwrap();
        let created = Instant::now();
        let (handle, _socket) = open_host_socket(&reg, "ABCD").await;

        // A second past the removed room's activity deadline.
        tokio::time::sleep(IDLE_WITHOUT_ACTIVITY / 2 + Duration::from_secs(1)).await;
        assert!(
            reg.get("ABCD").await.is_some(),
            "the removed room's activity deadline removed the new room"
        );
        // The new room's deadline counts from its own creation.
        tokio::time::sleep_until(created + IDLE_WITHOUT_ACTIVITY + Duration::from_secs(1)).await;
        assert!(reg.get("ABCD").await.is_none());
        assert_actor_stopped(&handle).await;
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
}
