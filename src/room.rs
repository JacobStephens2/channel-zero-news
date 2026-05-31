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
//!   events back over the requester's own channel.
//!
//! Keeping the reducer pure ([`apply`] takes `&mut GameState` and returns a
//! [`Reaction`]) means the entire command surface is unit-testable without
//! spinning up tasks or sockets.

use tokio::sync::{broadcast, mpsc};

use crate::db::Db;
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
    pub direct: mpsc::UnboundedSender<ServerMsg>,
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
            // Errs only if the originating connection has gone away.
            let _ = cmd.direct.send(msg);
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

    // Self-registration: a player may join with any name — they don't have to be
    // pre-entered by the host. If they're not on the roster yet and the room is
    // still in the lobby, add them automatically. (Once submissions are open the
    // roster is locked so the partner ring / assignments stay consistent.)
    let mut self_added = false;
    if requested == Role::Player {
        if let Some(n) = join_name.as_deref() {
            let on_roster = state.players.iter().any(|p| p.name == n);
            if !on_roster && state.phase == Phase::Lobby && state.add_player(n).is_ok() {
                self_added = true;
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

    if requested == Role::Player {
        if let Some(n) = join_name.as_deref() {
            if let Some(assignment) = assignment(state, n) {
                direct.push(assignment);
            }
        }
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
    use crate::state::{Phase, PromptSet};

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
        state.start_game().unwrap(); // slide 0 = rules (host-only)

        let forbidden = |r: &Reaction| {
            matches!(
                r.direct.as_slice(),
                [ServerMsg::Error { code: ErrorCode::Forbidden, .. }]
            ) && r.broadcasts.is_empty()
        };

        // On the rules slide a player cannot advance...
        let r = apply(&mut state, &token, Role::Player, Some("Bo"), ClientMsg::AdvanceSlide);
        assert!(forbidden(&r));
        // ...the host moves to slide 1 (Bo's presenter intro)...
        let r = apply(&mut state, &token, Role::Host, None, ClientMsg::AdvanceSlide);
        assert!(!r.broadcasts.is_empty());
        assert_eq!(state.current_controller().as_deref(), Some("Bo"));
        // ...now Bo (the reader) can advance their own segment...
        let r = apply(&mut state, &token, Role::Player, Some("Bo"), ClientMsg::AdvanceSlide);
        assert!(r
            .broadcasts
            .iter()
            .any(|m| matches!(m, ServerMsg::SlideChanged { .. })));
        // ...but another player cannot move Bo's segment.
        let r = apply(&mut state, &token, Role::Player, Some("Ann"), ClientMsg::AdvanceSlide);
        assert!(forbidden(&r));
    }

    #[tokio::test]
    async fn two_subscribers_receive_the_same_broadcast() {
        let handle = spawn_room("ABCD", sets(), "tok", None);
        let mut a = handle.subscribe();
        let mut b = handle.subscribe();

        let (direct, _direct_rx) = mpsc::unbounded_channel();
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
        let (direct, mut direct_rx) = mpsc::unbounded_channel();

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
}
