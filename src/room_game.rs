//! The pure, authoritative Room game: construction plus one intent operation.
//!
//! All transitions, authorization, assignments, replay, slides and ordered
//! reaction construction live here. The caller supplies trusted connection
//! identity and owns delivery and best-effort execution of returned effects.
//! No mutable game representation or raw transition is exposed.

use serde::{Deserialize, Serialize};

use crate::protocol::{ClientMsg, ErrorCode, PlayerStatus, Role, RoomSnapshot, ServerMsg};
use crate::state::{
    ArchivedEntry, Phase, PromptSet, Slide, MAX_PLAYER_NAME_CHARS, MAX_ROSTER_PLAYERS,
    MAX_SUBMISSION_CHARS,
};

/// A durable side effect for the actor to run against the database, produced by
/// Room game but executed by the async actor loop (best-effort: a DB
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

/// Opaque authoritative rules for one room, independent of tasks, sockets and DBs.
/// Inspect observable state by issuing a join/replay intent, just like a connection.
pub struct RoomGame {
    state: GameState,
    host_token: String,
}

impl RoomGame {
    /// Capture the room's immutable code, prompt catalog and host credential.
    pub fn new(
        code: impl Into<String>,
        prompt_sets: Vec<PromptSet>,
        host_token: impl Into<String>,
    ) -> Self {
        Self {
            state: GameState::new(code, prompt_sets),
            host_token: host_token.into(),
        }
    }

    /// Handle one client intent with the actor's trusted role/name context.
    /// Joins use the requested identity and check host credentials on every replay;
    /// later intents use the trusted context. Ordered outputs own their data.
    pub fn apply(&mut self, role: Role, name: Option<&str>, intent: ClientMsg) -> Reaction {
        let state = &mut self.state;
        let host_token = &self.host_token;
        match intent {
            ClientMsg::Ping => Reaction::direct_only(ServerMsg::Pong),

            ClientMsg::JoinRoom {
                role: requested,
                name: join_name,
                token,
                ..
            } => join(state, host_token, requested, join_name, token),

            ClientMsg::AddPlayer { name } => {
                host_action(role, |s: &mut GameState| s.add_player(&name), state)
            }

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
            ClientMsg::GotoSlide { index } => {
                control_slide(state, role, name, move |s| s.goto_slide(index))
            }

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
}

/// A player's submitted answers, aligned to their assigned prompt set.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
struct Responses {
    /// One response per prompt in the assigned set (same length, same order).
    items: Vec<String>,
    /// The closing "...and remember:" line.
    signoff: String,
}

/// A roster entry. In the original PHP app a row in `tblResponses` *was* the
/// player; here a player carries their assignment and (once submitted) answers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Player {
    name: String,
    /// Assigned when the room moves to `Collecting`.
    prompt_set_id: Option<i64>,
    /// The teammate this player writes *for* (ring assignment). Set on collect.
    partner: Option<String>,
    /// `Some` once the player has submitted.
    responses: Option<Responses>,
}

impl Player {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            prompt_set_id: None,
            partner: None,
            responses: None,
        }
    }

    fn submitted(&self) -> bool {
        self.responses.is_some()
    }
}

/// Errors a transition can reject with. These map to a `ServerMsg::Error` over
/// the socket; nothing here panics.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum TransitionError {
    #[error("action not allowed in the {phase:?} phase")]
    WrongPhase { phase: Phase },
    #[error("the room has no players")]
    NoPlayers,
    #[error("no prompt sets are available to assign")]
    NoPromptSets,
    #[error("player '{0}' was not found in this room")]
    UnknownPlayer(String),
    #[error("a player named '{0}' is already in this room")]
    DuplicatePlayer(String),
    #[error("player name is longer than {MAX_PLAYER_NAME_CHARS} characters")]
    NameTooLong,
    #[error("the roster is limited to {MAX_ROSTER_PLAYERS} players")]
    RosterFull,
    #[error("a response is longer than {MAX_SUBMISSION_CHARS} characters")]
    ResponseTooLong,
    #[error("sign-off is longer than {MAX_SUBMISSION_CHARS} characters")]
    SignoffTooLong,
    #[error("not every player has submitted yet")]
    NotAllSubmitted,
    #[error("slide index {0} is out of range")]
    SlideOutOfRange(usize),
    #[error("expected {expected} responses but got {got}")]
    ResponseCountMismatch { expected: usize, got: usize },
    #[error("player '{0}' has no assigned prompt set")]
    NoAssignment(String),
}

/// The authoritative state of one room.
#[derive(Clone, Debug)]
struct GameState {
    code: String,
    phase: Phase,
    players: Vec<Player>,
    /// Prompt sets available to this room (loaded from the DB at room creation).
    prompt_sets: Vec<PromptSet>,
    /// Current carousel index while `Performing`.
    current_slide: usize,
}

impl GameState {
    fn new(code: impl Into<String>, prompt_sets: Vec<PromptSet>) -> Self {
        Self {
            code: code.into(),
            phase: Phase::Lobby,
            players: Vec::new(),
            prompt_sets,
            current_slide: 0,
        }
    }

    fn require_phase(&self, phase: Phase) -> Result<(), TransitionError> {
        if self.phase == phase {
            Ok(())
        } else {
            Err(TransitionError::WrongPhase { phase: self.phase })
        }
    }

    fn player_index(&self, name: &str) -> Option<usize> {
        self.players.iter().position(|p| p.name == name)
    }

    fn prompt_set(&self, id: i64) -> Option<&PromptSet> {
        self.prompt_sets.iter().find(|p| p.id == id)
    }

    // ----- roster management (Lobby only) -------------------------------------

    /// Add a player to the roster. Allowed only while in the Lobby so that the
    /// roster, and therefore the partner ring and assignments, is fixed once
    /// submissions open. The name is capped at [`MAX_PLAYER_NAME_CHARS`] and
    /// the roster at [`MAX_ROSTER_PLAYERS`]; host adds and player joins both
    /// come through here.
    fn add_player(&mut self, name: &str) -> Result<(), TransitionError> {
        self.require_phase(Phase::Lobby)?;
        if name.chars().count() > MAX_PLAYER_NAME_CHARS {
            return Err(TransitionError::NameTooLong);
        }
        if self.player_index(name).is_some() {
            return Err(TransitionError::DuplicatePlayer(name.to_string()));
        }
        if self.players.len() >= MAX_ROSTER_PLAYERS {
            return Err(TransitionError::RosterFull);
        }
        self.players.push(Player::new(name));
        Ok(())
    }

    fn remove_player(&mut self, name: &str) -> Result<(), TransitionError> {
        self.require_phase(Phase::Lobby)?;
        let idx = self
            .player_index(name)
            .ok_or_else(|| TransitionError::UnknownPlayer(name.to_string()))?;
        self.players.remove(idx);
        Ok(())
    }

    // ----- Lobby -> Collecting ------------------------------------------------

    /// Lock the roster, assign each player a prompt set (round-robin over the
    /// available sets) and their partner (the next player in the ring), and
    /// open submissions.
    fn start_collecting(&mut self) -> Result<(), TransitionError> {
        self.require_phase(Phase::Lobby)?;
        if self.players.is_empty() {
            return Err(TransitionError::NoPlayers);
        }
        if self.prompt_sets.is_empty() {
            return Err(TransitionError::NoPromptSets);
        }

        let n = self.players.len();
        let names: Vec<String> = self.players.iter().map(|p| p.name.clone()).collect();
        let set_ids: Vec<i64> = self.prompt_sets.iter().map(|s| s.id).collect();

        for (i, player) in self.players.iter_mut().enumerate() {
            player.prompt_set_id = Some(set_ids[i % set_ids.len()]);
            // Ring: each player writes for the next; the last writes for the first.
            player.partner = Some(names[(i + 1) % n].clone());
            player.responses = None;
        }

        self.phase = Phase::Collecting;
        Ok(())
    }

    // ----- submissions (Collecting) -------------------------------------------

    /// Record a player's responses. The number of response items must match the
    /// number of prompts in their assigned set. Each response and the sign-off
    /// are capped at [`MAX_SUBMISSION_CHARS`].
    fn submit_responses(
        &mut self,
        name: &str,
        items: Vec<String>,
        signoff: String,
    ) -> Result<(), TransitionError> {
        self.require_phase(Phase::Collecting)?;
        let idx = self
            .player_index(name)
            .ok_or_else(|| TransitionError::UnknownPlayer(name.to_string()))?;

        let set_id = self.players[idx]
            .prompt_set_id
            .ok_or_else(|| TransitionError::NoAssignment(name.to_string()))?;
        let expected = self
            .prompt_set(set_id)
            .map(|s| s.prompts.len())
            .unwrap_or(0);
        if items.len() != expected {
            return Err(TransitionError::ResponseCountMismatch {
                expected,
                got: items.len(),
            });
        }
        if items
            .iter()
            .any(|item| item.chars().count() > MAX_SUBMISSION_CHARS)
        {
            return Err(TransitionError::ResponseTooLong);
        }
        if signoff.chars().count() > MAX_SUBMISSION_CHARS {
            return Err(TransitionError::SignoffTooLong);
        }

        self.players[idx].responses = Some(Responses { items, signoff });
        Ok(())
    }

    fn submitted_count(&self) -> usize {
        self.players.iter().filter(|p| p.submitted()).count()
    }

    fn all_submitted(&self) -> bool {
        !self.players.is_empty() && self.players.iter().all(|p| p.submitted())
    }

    // ----- Collecting -> Performing -------------------------------------------

    fn start_game(&mut self) -> Result<(), TransitionError> {
        self.require_phase(Phase::Collecting)?;
        if !self.all_submitted() {
            return Err(TransitionError::NotAllSubmitted);
        }
        self.phase = Phase::Performing;
        self.current_slide = 0;
        Ok(())
    }

    // ----- carousel control (Performing) --------------------------------------

    fn total_slides(&self) -> usize {
        self.build_slides().len()
    }

    fn advance_slide(&mut self) -> Result<usize, TransitionError> {
        self.require_phase(Phase::Performing)?;
        let last = self.total_slides().saturating_sub(1);
        if self.current_slide < last {
            self.current_slide += 1;
        }
        Ok(self.current_slide)
    }

    fn prev_slide(&mut self) -> Result<usize, TransitionError> {
        self.require_phase(Phase::Performing)?;
        self.current_slide = self.current_slide.saturating_sub(1);
        Ok(self.current_slide)
    }

    fn goto_slide(&mut self, index: usize) -> Result<usize, TransitionError> {
        self.require_phase(Phase::Performing)?;
        if index >= self.total_slides() {
            return Err(TransitionError::SlideOutOfRange(index));
        }
        self.current_slide = index;
        Ok(self.current_slide)
    }

    /// Generate the full ordered slide deck from the current roster + answers.
    /// Mirrors the original `game.php` carousel: a rules slide, then per player
    /// a presenter intro, greeting, one slide per prompt, and a sign-off; then
    /// a credits slide and a trailing blank.
    fn build_slides(&self) -> Vec<Slide> {
        let mut slides = vec![Slide::Rules];

        for (i, player) in self.players.iter().enumerate() {
            let presenter = player.partner.clone().unwrap_or_default();
            let teleprompter = player.name.clone();

            slides.push(Slide::PresenterIntro {
                presenter: presenter.clone(),
                teleprompter,
                first: i == 0,
            });
            slides.push(Slide::Greeting {
                presenter: presenter.clone(),
            });

            let prompts = player
                .prompt_set_id
                .and_then(|id| self.prompt_set(id))
                .map(|s| s.prompts.clone())
                .unwrap_or_default();
            let responses = player.responses.clone().unwrap_or_default();

            for (j, prompt) in prompts.iter().enumerate() {
                let response = responses.items.get(j).cloned().unwrap_or_default();
                slides.push(Slide::Script {
                    reader: presenter.clone(),
                    text: format!("{prompt}{response}"),
                });
            }

            slides.push(Slide::Signoff {
                presenter: presenter.clone(),
                text: responses.signoff.clone(),
            });
        }

        slides.push(Slide::Credits);
        slides.push(Slide::Blank);
        slides
    }

    fn slide_at(&self, index: usize) -> Option<Slide> {
        self.build_slides().into_iter().nth(index)
    }

    /// The player allowed to advance the *current* slide: the presenter of their
    /// own segment (intro → greeting → scripts → sign-off). On the opening rules
    /// slide this is the first presenter, so they can start the show themselves.
    /// The closing credits / blank slides have no controller (host-only).
    fn current_controller(&self) -> Option<String> {
        match self.slide_at(self.current_slide)? {
            // The first presenter starts the show from the rules slide.
            Slide::Rules => self
                .slide_at(self.current_slide + 1)
                .and_then(|s| slide_controller(&s)),
            other => slide_controller(&other),
        }
    }

    // ----- archive + reset ----------------------------------------------------

    /// Snapshot every player's answers paired with the prompts they answered.
    /// This is the durable artifact the persistence layer writes — both for a
    /// round's finalized responses and for the archive.
    fn round_entries(&self) -> Vec<ArchivedEntry> {
        self.players
            .iter()
            .map(|p| {
                let prompts = p
                    .prompt_set_id
                    .and_then(|id| self.prompt_set(id))
                    .map(|s| s.prompts.clone())
                    .unwrap_or_default();
                let responses = p.responses.clone().unwrap_or_default();
                ArchivedEntry {
                    name: p.name.clone(),
                    partner: p.partner.clone(),
                    prompt_set_id: p.prompt_set_id,
                    prompts,
                    responses: responses.items,
                    signoff: responses.signoff,
                }
            })
            .collect()
    }

    /// Snapshot the round and move the room to `Archived`.
    fn archive_round(&mut self) -> Result<Vec<ArchivedEntry>, TransitionError> {
        self.require_phase(Phase::Performing)?;
        let entries = self.round_entries();
        self.phase = Phase::Archived;
        Ok(entries)
    }

    /// Start a fresh round: clear the roster and return to the Lobby. Allowed
    /// only after a round has been archived (mirrors archive-then-new-round).
    fn new_round(&mut self) -> Result<(), TransitionError> {
        self.require_phase(Phase::Archived)?;
        self.reset();
        Ok(())
    }

    /// Wipe the slate from any phase back to an empty Lobby (the host's
    /// "delete all" control).
    fn reset(&mut self) {
        self.players.clear();
        self.current_slide = 0;
        self.phase = Phase::Lobby;
    }
}

/// Who reads/controls a given slide, if anyone (the presenter of that segment).
fn slide_controller(slide: &Slide) -> Option<String> {
    match slide {
        Slide::Script { reader, .. } => Some(reader.clone()),
        Slide::Greeting { presenter }
        | Slide::Signoff { presenter, .. }
        | Slide::PresenterIntro { presenter, .. } => Some(presenter.clone()),
        Slide::Rules | Slide::Credits | Slide::Blank => None,
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
        slide: state.slide_at(state.current_slide).unwrap_or(Slide::Blank),
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
