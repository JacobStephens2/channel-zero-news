//! Per-room actor: the only owner of a room's [`GameState`].
//!
//! Concurrency model (the centerpiece of this project):
//!
//! * Each room is a single Tokio task that **owns** its `GameState`. Nothing
//!   else can touch the state, so there are no locks around it and no chance of
//!   a torn read/write — all mutation is serialized through one `mpsc` queue.
//! * Connections send [`Command`]s (a validated intent + the connection's
//!   server-held role/identity) into that queue.
//! * The actor reduces each command with the pure [`apply`] function, then fans
//!   the resulting **broadcast** events out to every subscriber via a
//!   `tokio::sync::broadcast` channel, and sends any **direct** (per-connection)
//!   events back over the requester's own fixed-capacity
//!   [direct reply channel](crate::direct). The actor never waits on that
//!   channel: one that overflows closes its connection instead.
//!
//! Keeping the reducer pure ([`apply`] takes `&mut GameState` and returns a
//! [`Reaction`]) means the entire command surface is unit-testable without
//! spinning up tasks or sockets.

use tokio::sync::{broadcast, mpsc};

use crate::db::Db;
use crate::direct::DirectSender;
use crate::protocol::{
    ClientMsg, ErrorCode, PlayerStatus, Role, RoomSnapshot, ServerMsg,
};
use crate::state::{ArchivedEntry, GameState, Phase, PromptSet, TransitionError};

/// Capacity of a room's command queue.
const COMMAND_QUEUE: usize = 64;
/// Capacity of a room's broadcast buffer (slow subscribers lag, never block).
const BROADCAST_BUFFER: usize = 256;

/// A validated intent handed to a room actor. `role`/`name` are established once
/// at join time and held server-side by the connection task, so the client
/// cannot forge them on later messages.
pub struct Command {
    pub intent: ClientMsg,
    pub role: Role,
    pub name: Option<String>,
    /// Channel for events meant only for the originating connection.
    pub direct: DirectSender,
}

/// A durable side effect for the actor to run against the database, produced by
/// the pure reducer but executed by the async actor loop (best-effort: a DB
/// failure is logged and never corrupts the in-memory game).
#[derive(Debug, PartialEq, Eq)]
pub enum Effect {
    /// Persist the round's finalized responses (on game start).
    SaveFinalResponses(Vec<ArchivedEntry>),
    /// Archive the round's responses (on archive).
    ArchiveRound(Vec<ArchivedEntry>),
}

/// The set of messages and effects a single command produces.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Reaction {
    /// Fanned out to every subscriber of the room.
    pub broadcasts: Vec<ServerMsg>,
    /// Sent only to the originating connection.
    pub direct: Vec<ServerMsg>,
    /// Durable side effects to run against the database.
    pub effects: Vec<Effect>,
}

impl Reaction {
    fn direct_only(msg: ServerMsg) -> Self {
        Reaction {
            broadcasts: vec![],
            direct: vec![msg],
            effects: vec![],
        }
    }
}

/// A cloneable handle to a running room actor.
#[derive(Clone)]
pub struct RoomHandle {
    pub code: String,
    pub host_token: String,
    cmd_tx: mpsc::Sender<Command>,
    events_tx: broadcast::Sender<ServerMsg>,
}

/// Returned when a room's actor task has stopped.
#[derive(Debug)]
pub struct RoomClosed;

impl RoomHandle {
    /// Subscribe to this room's broadcast event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<ServerMsg> {
        self.events_tx.subscribe()
    }

    /// Queue a command for the actor.
    pub async fn dispatch(&self, cmd: Command) -> Result<(), RoomClosed> {
        self.cmd_tx.send(cmd).await.map_err(|_| RoomClosed)
    }
}

/// Spawn a room actor and return a handle to it. `db` is `None` in tests and
/// no-database runs, in which case durable effects are simply skipped.
pub fn spawn_room(
    code: impl Into<String>,
    prompt_sets: Vec<PromptSet>,
    host_token: impl Into<String>,
    db: Option<Db>,
) -> RoomHandle {
    let code = code.into();
    let host_token = host_token.into();
    let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_QUEUE);
    let (events_tx, _events_rx) = broadcast::channel(BROADCAST_BUFFER);

    let state = GameState::new(code.clone(), prompt_sets);
    tokio::spawn(run(state, host_token.clone(), cmd_rx, events_tx.clone(), db));

    RoomHandle {
        code,
        host_token,
        cmd_tx,
        events_tx,
    }
}

/// The actor loop. Owns the state for the room's entire lifetime.
async fn run(
    mut state: GameState,
    host_token: String,
    mut cmd_rx: mpsc::Receiver<Command>,
    events_tx: broadcast::Sender<ServerMsg>,
    db: Option<Db>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        let reaction = apply(
            &mut state,
            &host_token,
            cmd.role,
            cmd.name.as_deref(),
            cmd.intent,
        );
        for msg in reaction.broadcasts {
            // Errs only when there are zero subscribers — fine to ignore.
            let _ = events_tx.send(msg);
        }
        for msg in reaction.direct {
            // Never waits, so one connection's full channel cannot hold up
            // the room; the overflow closes that connection instead.
            cmd.direct.send(msg);
        }
        for effect in reaction.effects {
            run_effect(&db, &state.code, effect).await;
        }
    }
    tracing::debug!(room = %state.code, "room actor stopped");
}

/// Execute a durable effect. Best-effort: failures are logged, never fatal to
/// the in-memory game.
async fn run_effect(db: &Option<Db>, code: &str, effect: Effect) {
    let Some(db) = db else { return };
    match effect {
        Effect::SaveFinalResponses(entries) => {
            if let Err(e) = db.save_final_responses(code, &entries).await {
                tracing::error!(room = %code, error = %e, "failed to persist final responses");
            }
        }
        Effect::ArchiveRound(entries) => match db.archive_round(code, &entries).await {
            Ok(batch) => tracing::info!(room = %code, batch = %batch, "archived round"),
            Err(e) => tracing::error!(room = %code, error = %e, "failed to archive round"),
        },
    }
}

// ---------------------------------------------------------------------------
// Pure reducer
// ---------------------------------------------------------------------------

/// Reduce a single command against the room state. Pure and side-effect free:
/// it mutates the state in place and returns the messages to emit. This is the
/// single place every protocol variant is handled — exhaustively, no catch-all.
pub fn apply(
    state: &mut GameState,
    host_token: &str,
    role: Role,
    name: Option<&str>,
    intent: ClientMsg,
) -> Reaction {
    match intent {
        ClientMsg::Ping => Reaction::direct_only(ServerMsg::Pong),

        ClientMsg::JoinRoom {
            role: requested,
            name: join_name,
            token,
            ..
        } => join(state, host_token, requested, join_name, token),

        ClientMsg::AddPlayer { name } => host_action(role, |s: &mut GameState| s.add_player(&name), state),

        ClientMsg::RemovePlayer { name } => {
            host_action(role, |s: &mut GameState| s.remove_player(&name), state)
        }

        ClientMsg::StartCollecting => {
            host_action(role, |s: &mut GameState| s.start_collecting(), state)
        }

        ClientMsg::SubmitResponses { responses, signoff } => {
            submit(state, role, name, responses, signoff)
        }

        ClientMsg::StartGame => match require_host(role) {
            Err(r) => r,
            Ok(()) => match state.start_game() {
                Ok(()) => Reaction {
                    broadcasts: vec![
                        ServerMsg::GameStarted {
                            total_slides: state.total_slides(),
                        },
                        slide_changed(state),
                        room_state(state),
                    ],
                    direct: vec![],
                    // The round's responses are now final — persist them.
                    effects: vec![Effect::SaveFinalResponses(state.round_entries())],
                },
                Err(e) => Reaction::direct_only(transition_error(e)),
            },
        },

        ClientMsg::AdvanceSlide => control_slide(state, role, name, |s| s.advance_slide()),
        ClientMsg::PrevSlide => control_slide(state, role, name, |s| s.prev_slide()),
        ClientMsg::GotoSlide { index } => control_slide(state, role, name, move |s| s.goto_slide(index)),

        ClientMsg::ArchiveRound => match require_host(role) {
            Err(r) => r,
            // The returned snapshot is what the persistence layer (M5) writes;
            // here we just transition and broadcast the new state.
            Ok(()) => match state.archive_round() {
                Ok(entries) => Reaction {
                    broadcasts: vec![room_state(state)],
                    direct: vec![],
                    effects: vec![Effect::ArchiveRound(entries)],
                },
                Err(e) => Reaction::direct_only(transition_error(e)),
            },
        },

        ClientMsg::NewRound => host_action(role, |s: &mut GameState| s.new_round(), state),
    }
}

/// Handle a join: validate the host token, self-register an unknown player, and
/// send the new connection its initial state (and, for a player, their
/// assignment) directly.
fn join(
    state: &mut GameState,
    host_token: &str,
    requested: Role,
    join_name: Option<String>,
    token: Option<String>,
) -> Reaction {
    if requested.is_host() && token.as_deref() != Some(host_token) {
        return Reaction::direct_only(ServerMsg::Error {
            code: ErrorCode::Forbidden,
            message: "invalid or missing host token".into(),
        });
    }

    // Self-registration: a player who is not on the roster yet is added while
    // the room is still in the lobby. `add_player` enforces the shared
    // name-length and roster-size limits; if it rejects that new name, the
    // join returns that error instead of succeeding. Joining a name already
    // on the roster, or any join that does not add a name, stays a normal
    // join. Once submissions are open the roster stays locked so the partner
    // ring and assignments stay consistent.
    let mut self_added = false;
    if requested == Role::Player {
        if let Some(n) = join_name.as_deref() {
            let on_roster = state.players.iter().any(|p| p.name == n);
            if !on_roster && state.phase == Phase::Lobby {
                match state.add_player(n) {
                    Ok(()) => self_added = true,
                    Err(e) => return Reaction::direct_only(transition_error(e)),
                }
            }
        }
    }

    let mut direct = vec![
        ServerMsg::Joined {
            role: requested,
            name: join_name.clone(),
        },
        room_state(state),
        progress(state),
    ];

    // While collecting, a player gets their partner + prompts to fill in.
    if requested == Role::Player && state.phase == Phase::Collecting {
        if let Some(n) = join_name.as_deref() {
            if let Some(assignment) = assignment(state, n) {
                direct.push(assignment);
            }
        }
    }

    // Reconnecting (or joining) mid-performance: send the current slide so the
    // client can render the stage immediately, not just on the next move.
    if state.phase == Phase::Performing {
        direct.push(slide_changed(state));
    }

    // If a new player just appeared on the roster, let everyone (host/display) see it.
    let broadcasts = if self_added {
        vec![room_state(state), progress(state)]
    } else {
        vec![]
    };

    Reaction {
        broadcasts,
        direct,
        effects: vec![],
    }
}

/// Run a host-only mutation that, on success, broadcasts room state + progress.
fn host_action<F>(role: Role, f: F, state: &mut GameState) -> Reaction
where
    F: FnOnce(&mut GameState) -> Result<(), TransitionError>,
{
    if let Err(r) = require_host(role) {
        return r;
    }
    match f(state) {
        Ok(()) => Reaction {
            broadcasts: vec![room_state(state), progress(state)],
            direct: vec![],
            effects: vec![],
        },
        Err(e) => Reaction::direct_only(transition_error(e)),
    }
}

/// May this connection move the carousel right now? The host always can; a
/// player may move only while they are the controller of the current slide.
fn may_control(state: &GameState, role: Role, name: Option<&str>) -> bool {
    if role.is_host() {
        return true;
    }
    if role == Role::Player {
        if let (Some(n), Some(controller)) = (name, state.current_controller()) {
            return n == controller;
        }
    }
    false
}

/// Run a carousel move (host or the current reader), broadcasting the change.
fn control_slide<F>(state: &mut GameState, role: Role, name: Option<&str>, f: F) -> Reaction
where
    F: FnOnce(&mut GameState) -> Result<usize, TransitionError>,
{
    if !may_control(state, role, name) {
        return Reaction::direct_only(ServerMsg::Error {
            code: ErrorCode::Forbidden,
            message: "only the host or the current reader can move the carousel".into(),
        });
    }
    match f(state) {
        Ok(_) => Reaction {
            broadcasts: vec![slide_changed(state), room_state(state)],
            direct: vec![],
            effects: vec![],
        },
        Err(e) => Reaction::direct_only(transition_error(e)),
    }
}

/// Handle a player's submission.
fn submit(
    state: &mut GameState,
    role: Role,
    name: Option<&str>,
    responses: Vec<String>,
    signoff: String,
) -> Reaction {
    // Only a joined player may submit, and only for their own name.
    let player_name = match (role, name) {
        (Role::Player, Some(n)) => n.to_string(),
        _ => {
            return Reaction::direct_only(ServerMsg::Error {
                code: ErrorCode::Forbidden,
                message: "only a joined player may submit responses".into(),
            })
        }
    };

    match state.submit_responses(&player_name, responses, signoff) {
        Ok(()) => Reaction {
            broadcasts: vec![progress(state), room_state(state)],
            direct: vec![],
            effects: vec![],
        },
        Err(e) => Reaction::direct_only(transition_error(e)),
    }
}

fn require_host(role: Role) -> Result<(), Reaction> {
    if role.is_host() {
        Ok(())
    } else {
        Err(Reaction::direct_only(ServerMsg::Error {
            code: ErrorCode::Forbidden,
            message: "this action is host-only".into(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Message builders
// ---------------------------------------------------------------------------

fn statuses(state: &GameState) -> Vec<PlayerStatus> {
    state
        .players
        .iter()
        .map(|p| PlayerStatus {
            name: p.name.clone(),
            submitted: p.submitted(),
        })
        .collect()
}

fn snapshot(state: &GameState) -> RoomSnapshot {
    RoomSnapshot {
        code: state.code.clone(),
        phase: state.phase,
        players: statuses(state),
        current_slide: state.current_slide,
        total_slides: state.total_slides(),
    }
}

fn room_state(state: &GameState) -> ServerMsg {
    ServerMsg::RoomState {
        room: snapshot(state),
    }
}

fn progress(state: &GameState) -> ServerMsg {
    ServerMsg::SubmissionProgress {
        submitted: state.submitted_count(),
        total: state.players.len(),
        players: statuses(state),
    }
}

fn slide_changed(state: &GameState) -> ServerMsg {
    ServerMsg::SlideChanged {
        index: state.current_slide,
        total_slides: state.total_slides(),
        slide: state
            .slide_at(state.current_slide)
            .unwrap_or(crate::state::Slide::Blank),
        controller: state.current_controller(),
    }
}

/// Build a player's targeted assignment message, if they have one.
fn assignment(state: &GameState, name: &str) -> Option<ServerMsg> {
    let player = state.players.iter().find(|p| p.name == name)?;
    let partner = player.partner.clone()?;
    let prompts = player
        .prompt_set_id
        .and_then(|id| state.prompt_set(id))
        .map(|s| s.prompts.clone())
        .unwrap_or_default();
    Some(ServerMsg::Assignment { partner, prompts })
}

fn transition_error(e: TransitionError) -> ServerMsg {
    ServerMsg::Error {
        code: ErrorCode::InvalidTransition,
        message: e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::direct;
    use crate::state::{Phase, PromptSet, TransitionError};
    use std::time::Duration;

    fn sets() -> Vec<PromptSet> {
        vec![PromptSet {
            id: 1,
            name: "Set".into(),
            author: "Eric".into(),
            prompts: vec!["a ".into(), "b ".into()],
        }]
    }

    fn host_join() -> (GameState, String) {
        (GameState::new("ABCD", sets()), "secret-token".to_string())
    }

    #[test]
    fn non_host_add_player_is_forbidden() {
        let (mut state, token) = host_join();
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("Ann"),
            ClientMsg::AddPlayer { name: "Bo".into() },
        );
        assert!(r.broadcasts.is_empty());
        assert!(matches!(
            r.direct.as_slice(),
            [ServerMsg::Error {
                code: ErrorCode::Forbidden,
                ..
            }]
        ));
        assert!(state.players.is_empty());
    }

    #[test]
    fn host_add_player_broadcasts_state() {
        let (mut state, token) = host_join();
        let r = apply(
            &mut state,
            &token,
            Role::Host,
            None,
            ClientMsg::AddPlayer { name: "Ann".into() },
        );
        assert_eq!(state.players.len(), 1);
        assert!(matches!(r.broadcasts[0], ServerMsg::RoomState { .. }));
        assert!(matches!(
            r.broadcasts[1],
            ServerMsg::SubmissionProgress { .. }
        ));
    }

    #[test]
    fn host_join_requires_valid_token() {
        let (mut state, token) = host_join();
        let bad = apply(
            &mut state,
            &token,
            Role::Host,
            None,
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Host,
                name: None,
                token: Some("wrong".into()),
            },
        );
        assert!(matches!(
            bad.direct.as_slice(),
            [ServerMsg::Error {
                code: ErrorCode::Forbidden,
                ..
            }]
        ));

        let good = apply(
            &mut state,
            &token,
            Role::Host,
            None,
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Host,
                name: None,
                token: Some(token.clone()),
            },
        );
        assert!(matches!(good.direct[0], ServerMsg::Joined { .. }));
    }

    #[test]
    fn player_join_gets_assignment() {
        let (mut state, token) = host_join();
        state.add_player("Ann").unwrap();
        state.add_player("Bo").unwrap();
        state.start_collecting().unwrap();

        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("Ann"),
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Player,
                name: Some("Ann".into()),
                token: None,
            },
        );
        assert!(r
            .direct
            .iter()
            .any(|m| matches!(m, ServerMsg::Assignment { .. })));
    }

    #[test]
    fn player_self_registers_in_lobby() {
        let (mut state, token) = host_join();
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("Zoe"),
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Player,
                name: Some("Zoe".into()),
                token: None,
            },
        );
        // The unknown name was added to the roster...
        assert!(state.players.iter().any(|p| p.name == "Zoe"));
        // ...the joiner is acknowledged...
        assert!(r.direct.iter().any(|m| matches!(m, ServerMsg::Joined { .. })));
        // ...and host/display screens are told about the new roster.
        assert!(r
            .broadcasts
            .iter()
            .any(|m| matches!(m, ServerMsg::RoomState { .. })));
    }

    #[test]
    fn player_join_returns_add_player_rejection_for_a_new_lobby_name() {
        let (mut state, token) = host_join();
        let name = "é".repeat(201);
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some(&name),
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Player,
                name: Some(name.clone()),
                token: None,
            },
        );
        assert!(state.players.is_empty());
        assert!(r.broadcasts.is_empty());
        assert_eq!(
            r.direct,
            vec![ServerMsg::Error {
                code: ErrorCode::InvalidTransition,
                message: TransitionError::NameTooLong.to_string(),
            }]
        );
    }

    #[test]
    fn player_join_returns_roster_full_for_a_new_lobby_name() {
        let (mut state, token) = host_join();
        for i in 0..64 {
            state.add_player(&format!("p{i}")).unwrap();
        }
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("newbie"),
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Player,
                name: Some("newbie".into()),
                token: None,
            },
        );
        assert_eq!(state.players.len(), 64);
        assert!(!state.players.iter().any(|p| p.name == "newbie"));
        assert!(r.broadcasts.is_empty());
        assert_eq!(
            r.direct,
            vec![ServerMsg::Error {
                code: ErrorCode::InvalidTransition,
                message: TransitionError::RosterFull.to_string(),
            }]
        );
    }

    #[test]
    fn player_join_of_an_existing_name_stays_successful_on_a_full_roster() {
        let (mut state, token) = host_join();
        for i in 0..64 {
            state.add_player(&format!("p{i}")).unwrap();
        }
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("p0"),
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Player,
                name: Some("p0".into()),
                token: None,
            },
        );
        assert_eq!(state.players.len(), 64);
        assert!(r.broadcasts.is_empty());
        assert!(matches!(
            r.direct.first(),
            Some(ServerMsg::Joined {
                role: Role::Player,
                name: Some(joined),
            }) if joined == "p0"
        ));
    }

    #[test]
    fn player_join_that_does_not_add_a_name_stays_successful() {
        let (mut state, token) = host_join();
        state.add_player("Ann").unwrap();
        state.start_collecting().unwrap();
        let name = "é".repeat(201);
        let before = state.players.len();
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some(&name),
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Player,
                name: Some(name.clone()),
                token: None,
            },
        );
        // Roster is locked, so a name `add_player` would reject still joins
        // without being added and without an error.
        assert_eq!(state.players.len(), before);
        assert!(!state.players.iter().any(|p| p.name == name));
        assert!(r.broadcasts.is_empty());
        assert!(matches!(
            r.direct.first(),
            Some(ServerMsg::Joined {
                role: Role::Player,
                name: Some(joined),
            }) if joined == &name
        ));
    }

    #[test]
    fn host_add_player_rejects_a_name_over_200_characters() {
        let (mut state, token) = host_join();
        let r = apply(
            &mut state,
            &token,
            Role::Host,
            None,
            ClientMsg::AddPlayer {
                name: "é".repeat(201),
            },
        );
        assert!(state.players.is_empty());
        assert!(r.broadcasts.is_empty());
        assert_eq!(
            r.direct,
            vec![ServerMsg::Error {
                code: ErrorCode::InvalidTransition,
                message: TransitionError::NameTooLong.to_string(),
            }]
        );
    }

    #[test]
    fn player_submit_rejects_a_response_over_200_characters() {
        let (mut state, token) = host_join();
        state.add_player("Ann").unwrap();
        state.add_player("Bo").unwrap();
        state.start_collecting().unwrap();
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("Ann"),
            ClientMsg::SubmitResponses {
                responses: vec!["fine".into(), "é".repeat(201)],
                signoff: "sign".into(),
            },
        );
        assert!(state.players.iter().all(|p| p.responses.is_none()));
        assert!(r.broadcasts.is_empty());
        assert_eq!(
            r.direct,
            vec![ServerMsg::Error {
                code: ErrorCode::InvalidTransition,
                message: TransitionError::ResponseTooLong.to_string(),
            }]
        );
    }

    #[test]
    fn player_submit_rejects_a_signoff_over_200_characters() {
        let (mut state, token) = host_join();
        state.add_player("Ann").unwrap();
        state.add_player("Bo").unwrap();
        state.start_collecting().unwrap();
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("Ann"),
            ClientMsg::SubmitResponses {
                responses: vec!["fine".into(), "fine".into()],
                signoff: "é".repeat(201),
            },
        );
        assert!(state.players.iter().all(|p| p.responses.is_none()));
        assert!(r.broadcasts.is_empty());
        assert_eq!(
            r.direct,
            vec![ServerMsg::Error {
                code: ErrorCode::InvalidTransition,
                message: TransitionError::SignoffTooLong.to_string(),
            }]
        );
    }

    #[test]
    fn host_add_player_rejects_a_65th_player() {
        let (mut state, token) = host_join();
        for i in 0..64 {
            state.add_player(&format!("p{i}")).unwrap();
        }
        let r = apply(
            &mut state,
            &token,
            Role::Host,
            None,
            ClientMsg::AddPlayer {
                name: "one-more".into(),
            },
        );
        assert_eq!(state.players.len(), 64);
        assert!(r.broadcasts.is_empty());
        assert_eq!(
            r.direct,
            vec![ServerMsg::Error {
                code: ErrorCode::InvalidTransition,
                message: TransitionError::RosterFull.to_string(),
            }]
        );
    }

    #[test]
    fn player_does_not_self_register_after_lobby() {
        let (mut state, token) = host_join();
        state.add_player("Ann").unwrap();
        state.start_collecting().unwrap();
        let r = apply(
            &mut state,
            &token,
            Role::Player,
            Some("Latecomer"),
            ClientMsg::JoinRoom {
                code: "ABCD".into(),
                role: Role::Player,
                name: Some("Latecomer".into()),
                token: None,
            },
        );
        // Roster is locked once submissions are open: no silent add, no broadcast.
        assert!(!state.players.iter().any(|p| p.name == "Latecomer"));
        assert!(r.broadcasts.is_empty());
    }

    #[test]
    fn reader_can_advance_their_own_slides() {
        let (mut state, token) = host_join();
        state.add_player("Ann").unwrap();
        state.add_player("Bo").unwrap();
        state.start_collecting().unwrap();
        for n in ["Ann", "Bo"] {
            let idx = state.players.iter().position(|p| p.name == n).unwrap();
            let id = state.players[idx].prompt_set_id.unwrap();
            let cnt = state.prompt_set(id).unwrap().prompts.len();
            state
                .submit_responses(n, vec!["x".into(); cnt], "s".into())
                .unwrap();
        }
        state.start_game().unwrap(); // slide 0 = rules

        let forbidden = |r: &Reaction| {
            matches!(
                r.direct.as_slice(),
                [ServerMsg::Error { code: ErrorCode::Forbidden, .. }]
            ) && r.broadcasts.is_empty()
        };

        // The first presenter (Bo, Ann's partner) controls the rules slide and
        // may start the show; a non-presenter cannot.
        assert_eq!(state.current_controller().as_deref(), Some("Bo"));
        let r = apply(&mut state, &token, Role::Player, Some("Ann"), ClientMsg::AdvanceSlide);
        assert!(forbidden(&r));
        let r = apply(&mut state, &token, Role::Player, Some("Bo"), ClientMsg::AdvanceSlide);
        assert!(r
            .broadcasts
            .iter()
            .any(|m| matches!(m, ServerMsg::SlideChanged { .. })));
        assert_eq!(state.current_slide, 1); // Bo's presenter intro

        // Bo keeps control of their own segment; Ann still cannot move it.
        let r = apply(&mut state, &token, Role::Player, Some("Ann"), ClientMsg::AdvanceSlide);
        assert!(forbidden(&r));
        let r = apply(&mut state, &token, Role::Player, Some("Bo"), ClientMsg::AdvanceSlide);
        assert!(!r.broadcasts.is_empty());
    }

    #[tokio::test]
    async fn two_subscribers_receive_the_same_broadcast() {
        let handle = spawn_room("ABCD", sets(), "tok", None);
        let mut a = handle.subscribe();
        let mut b = handle.subscribe();

        let (direct, _direct_rx) = direct::channel();
        handle
            .dispatch(Command {
                intent: ClientMsg::AddPlayer { name: "Ann".into() },
                role: Role::Host,
                name: None,
                direct,
            })
            .await
            .unwrap();

        let ma = a.recv().await.unwrap();
        let mb = b.recv().await.unwrap();
        assert_eq!(ma, mb);
        match ma {
            ServerMsg::RoomState { room } => {
                assert_eq!(room.players.len(), 1);
                assert_eq!(room.phase, Phase::Lobby);
            }
            other => panic!("expected RoomState, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn forbidden_action_replies_direct_not_broadcast() {
        let handle = spawn_room("ABCD", sets(), "tok", None);
        let mut bcast = handle.subscribe();
        let (direct, mut direct_rx) = direct::channel();

        handle
            .dispatch(Command {
                intent: ClientMsg::StartGame,
                role: Role::Player,
                name: Some("Ann".into()),
                direct,
            })
            .await
            .unwrap();

        // The originating connection gets a direct Forbidden error...
        let err = direct_rx.recv().await.unwrap();
        assert!(matches!(
            err,
            ServerMsg::Error {
                code: ErrorCode::Forbidden,
                ..
            }
        ));
        // ...and nothing is broadcast.
        assert!(bcast.try_recv().is_err());
    }

    /// Dispatch `intent` from a connection whose direct replies go to `direct`.
    async fn dispatch_from(
        handle: &RoomHandle,
        direct: &DirectSender,
        role: Role,
        name: Option<&str>,
        intent: ClientMsg,
    ) {
        handle
            .dispatch(Command {
                intent,
                role,
                name: name.map(String::from),
                direct: direct.clone(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_full_direct_reply_channel_does_not_stall_the_room() {
        let handle = spawn_room("ABCD", sets(), "tok", None);
        let mut events = handle.subscribe();

        // Never read: one more ping than the channel holds overflows it.
        let (stalled, _stalled_rx) = direct::channel();
        for _ in 0..=direct::DIRECT_REPLY_CAPACITY {
            dispatch_from(&handle, &stalled, Role::Display, None, ClientMsg::Ping).await;
        }

        let (other, mut other_rx) = direct::channel();
        dispatch_from(&handle, &other, Role::Display, None, ClientMsg::Ping).await;
        dispatch_from(
            &handle,
            &other,
            Role::Host,
            None,
            ClientMsg::AddPlayer { name: "Ann".into() },
        )
        .await;
        let served = async {
            assert_eq!(other_rx.recv().await, Some(ServerMsg::Pong));
            assert!(matches!(
                events.recv().await,
                Ok(ServerMsg::RoomState { room }) if room.players.len() == 1
            ));
        };
        tokio::time::timeout(Duration::from_secs(2), served)
            .await
            .expect("the room stopped serving other connections");
    }

    #[tokio::test]
    async fn no_reply_after_an_overflow_reaches_the_connection() {
        let handle = spawn_room("ABCD", sets(), "tok", None);
        let (stalled, mut stalled_rx) = direct::channel();
        let overflowed = stalled_rx.overflowed();
        for _ in 0..=direct::DIRECT_REPLY_CAPACITY {
            dispatch_from(&handle, &stalled, Role::Display, None, ClientMsg::Ping).await;
        }
        tokio::time::timeout(Duration::from_secs(2), overflowed)
            .await
            .expect("the overflow was not signalled");

        // Reading one reply frees a slot, but a reply sent after the dropped
        // pong must still not get through.
        assert_eq!(stalled_rx.recv().await, Some(ServerMsg::Pong));
        dispatch_from(&handle, &stalled, Role::Display, None, ClientMsg::Ping).await;
        drop(stalled);
        let mut delivered = 1;
        while let Some(reply) = stalled_rx.recv().await {
            assert_eq!(reply, ServerMsg::Pong);
            delivered += 1;
        }
        assert_eq!(delivered, direct::DIRECT_REPLY_CAPACITY);
    }

    #[tokio::test]
    async fn a_player_rejoining_mid_performance_gets_every_direct_reply() {
        let handle = spawn_room("ABCD", sets(), "tok", None);
        let (setup, _setup_rx) = direct::channel();
        let open_round = [
            ClientMsg::AddPlayer { name: "Ann".into() },
            ClientMsg::AddPlayer { name: "Bo".into() },
            ClientMsg::StartCollecting,
        ];
        for intent in open_round {
            dispatch_from(&handle, &setup, Role::Host, None, intent).await;
        }
        for name in ["Ann", "Bo"] {
            let intent = ClientMsg::SubmitResponses {
                responses: vec!["x".into(); 2],
                signoff: "s".into(),
            };
            dispatch_from(&handle, &setup, Role::Player, Some(name), intent).await;
        }
        dispatch_from(&handle, &setup, Role::Host, None, ClientMsg::StartGame).await;

        let (player, mut player_rx) = direct::channel();
        let rejoin = ClientMsg::JoinRoom {
            code: "ABCD".into(),
            role: Role::Player,
            name: Some("Ann".into()),
            token: None,
        };
        dispatch_from(&handle, &player, Role::Player, Some("Ann"), rejoin).await;
        drop(player);
        let mut replies = Vec::new();
        while let Some(reply) = player_rx.recv().await {
            replies.push(reply);
        }
        assert!(
            matches!(
                replies.as_slice(),
                [
                    ServerMsg::Joined { .. },
                    ServerMsg::RoomState { room },
                    ServerMsg::SubmissionProgress { submitted: 2, total: 2, .. },
                    ServerMsg::SlideChanged { index: 0, .. },
                ] if room.phase == Phase::Performing
            ),
            "{replies:?}"
        );
    }
}
