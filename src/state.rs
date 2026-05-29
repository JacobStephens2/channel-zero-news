//! Authoritative per-room game state and its transition machine.
//!
//! This module is the single source of truth for one game. It is deliberately
//! free of any networking, async, or persistence concerns so that every valid
//! and invalid transition can be exercised by plain unit tests. The room actor
//! (see `game::room`) owns one of these and is the only thing allowed to mutate
//! it; WebSocket clients send *intents* that the actor validates against it.
//!
//! Phase machine:
//! ```text
//!   Lobby ──start_collecting──▶ Collecting ──start_game──▶ Performing ──archive_round──▶ Archived
//!     ▲                                                                                      │
//!     └──────────────────────────────── new_round / reset ───────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

/// Maximum number of prompts in a prompt set (mirrors `tblPrompts.prompt1..7`).
pub const MAX_PROMPTS: usize = 7;

/// A prompt set: a themed list of up to [`MAX_PROMPTS`] prompt lines, plus a
/// fixed sign-off prompt that always closes a player's segment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptSet {
    pub id: i64,
    pub name: String,
    pub author: String,
    /// Ordered prompt lines; `responses[i]` pairs with `prompts[i]`.
    pub prompts: Vec<String>,
}

/// A player's submitted answers, aligned to their assigned prompt set.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Responses {
    /// One response per prompt in the assigned set (same length, same order).
    pub items: Vec<String>,
    /// The closing "...and remember:" line.
    pub signoff: String,
}

/// A roster entry. In the original PHP app a row in `tblResponses` *was* the
/// player; here a player carries their assignment and (once submitted) answers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Player {
    pub name: String,
    /// Assigned when the room moves to `Collecting`.
    pub prompt_set_id: Option<i64>,
    /// The teammate this player writes *for* (ring assignment). Set on collect.
    pub partner: Option<String>,
    /// `Some` once the player has submitted.
    pub responses: Option<Responses>,
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

    pub fn submitted(&self) -> bool {
        self.responses.is_some()
    }
}

/// The four lifecycle phases of a room.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Lobby,
    Collecting,
    Performing,
    Archived,
}

/// A single carousel slide. The `reader` is the person performing the line
/// (the partner/presenter); the `teleprompter` advances the slides.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Slide {
    Rules,
    PresenterIntro {
        presenter: String,
        teleprompter: String,
        first: bool,
    },
    Greeting {
        presenter: String,
    },
    /// A prompt+response line read aloud by the presenter.
    Script {
        reader: String,
        text: String,
    },
    Signoff {
        presenter: String,
        text: String,
    },
    Credits,
    Blank,
}

/// One player's responses captured at archive time, paired with the prompts
/// they answered — the durable artifact persisted to the archive table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchivedEntry {
    pub name: String,
    pub partner: Option<String>,
    pub prompt_set_id: Option<i64>,
    pub prompts: Vec<String>,
    pub responses: Vec<String>,
    pub signoff: String,
}

/// Errors a transition can reject with. These map to a `ServerMsg::Error` over
/// the socket; nothing here panics.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransitionError {
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
pub struct GameState {
    pub code: String,
    pub phase: Phase,
    pub players: Vec<Player>,
    /// Prompt sets available to this room (loaded from the DB at room creation).
    pub prompt_sets: Vec<PromptSet>,
    /// Current carousel index while `Performing`.
    pub current_slide: usize,
}

impl GameState {
    pub fn new(code: impl Into<String>, prompt_sets: Vec<PromptSet>) -> Self {
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

    pub fn prompt_set(&self, id: i64) -> Option<&PromptSet> {
        self.prompt_sets.iter().find(|p| p.id == id)
    }

    // ----- roster management (Lobby only) -------------------------------------

    /// Add a player to the roster. Allowed only while in the Lobby so that the
    /// roster — and therefore the partner ring and assignments — is fixed once
    /// submissions open.
    pub fn add_player(&mut self, name: &str) -> Result<(), TransitionError> {
        self.require_phase(Phase::Lobby)?;
        if self.player_index(name).is_some() {
            return Err(TransitionError::DuplicatePlayer(name.to_string()));
        }
        self.players.push(Player::new(name));
        Ok(())
    }

    pub fn remove_player(&mut self, name: &str) -> Result<(), TransitionError> {
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
    pub fn start_collecting(&mut self) -> Result<(), TransitionError> {
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
    /// number of prompts in their assigned set.
    pub fn submit_responses(
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

        self.players[idx].responses = Some(Responses { items, signoff });
        Ok(())
    }

    pub fn submitted_count(&self) -> usize {
        self.players.iter().filter(|p| p.submitted()).count()
    }

    pub fn all_submitted(&self) -> bool {
        !self.players.is_empty() && self.players.iter().all(|p| p.submitted())
    }

    // ----- Collecting -> Performing -------------------------------------------

    pub fn start_game(&mut self) -> Result<(), TransitionError> {
        self.require_phase(Phase::Collecting)?;
        if !self.all_submitted() {
            return Err(TransitionError::NotAllSubmitted);
        }
        self.phase = Phase::Performing;
        self.current_slide = 0;
        Ok(())
    }

    // ----- carousel control (Performing) --------------------------------------

    pub fn total_slides(&self) -> usize {
        self.build_slides().len()
    }

    pub fn advance_slide(&mut self) -> Result<usize, TransitionError> {
        self.require_phase(Phase::Performing)?;
        let last = self.total_slides().saturating_sub(1);
        if self.current_slide < last {
            self.current_slide += 1;
        }
        Ok(self.current_slide)
    }

    pub fn prev_slide(&mut self) -> Result<usize, TransitionError> {
        self.require_phase(Phase::Performing)?;
        self.current_slide = self.current_slide.saturating_sub(1);
        Ok(self.current_slide)
    }

    pub fn goto_slide(&mut self, index: usize) -> Result<usize, TransitionError> {
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
    pub fn build_slides(&self) -> Vec<Slide> {
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

    pub fn slide_at(&self, index: usize) -> Option<Slide> {
        self.build_slides().into_iter().nth(index)
    }

    // ----- archive + reset ----------------------------------------------------

    /// Snapshot every player's answers (with the prompts they answered) and move
    /// the room to `Archived`. The returned entries are what the persistence
    /// layer writes to the durable archive table.
    pub fn archive_round(&mut self) -> Result<Vec<ArchivedEntry>, TransitionError> {
        self.require_phase(Phase::Performing)?;
        let entries = self
            .players
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
            .collect();
        self.phase = Phase::Archived;
        Ok(entries)
    }

    /// Start a fresh round: clear the roster and return to the Lobby. Allowed
    /// only after a round has been archived (mirrors archive-then-new-round).
    pub fn new_round(&mut self) -> Result<(), TransitionError> {
        self.require_phase(Phase::Archived)?;
        self.reset();
        Ok(())
    }

    /// Wipe the slate from any phase back to an empty Lobby (the host's
    /// "delete all" control).
    pub fn reset(&mut self) {
        self.players.clear();
        self.current_slide = 0;
        self.phase = Phase::Lobby;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sets() -> Vec<PromptSet> {
        vec![
            PromptSet {
                id: 1,
                name: "Set A".into(),
                author: "Eric".into(),
                prompts: vec!["In other news, ".into(), "Weather today: ".into()],
            },
            PromptSet {
                id: 2,
                name: "Set B".into(),
                author: "Eric".into(),
                prompts: vec!["Breaking: ".into(), "Sports update: ".into()],
            },
        ]
    }

    fn lobby_with(names: &[&str]) -> GameState {
        let mut g = GameState::new("ABCD", sets());
        for n in names {
            g.add_player(n).unwrap();
        }
        g
    }

    #[test]
    fn add_player_rejects_duplicates() {
        let mut g = lobby_with(&["Ann"]);
        assert_eq!(
            g.add_player("Ann"),
            Err(TransitionError::DuplicatePlayer("Ann".into()))
        );
    }

    #[test]
    fn cannot_add_player_outside_lobby() {
        let mut g = lobby_with(&["Ann", "Bo"]);
        g.start_collecting().unwrap();
        assert_eq!(
            g.add_player("Cy"),
            Err(TransitionError::WrongPhase {
                phase: Phase::Collecting
            })
        );
    }

    #[test]
    fn start_collecting_requires_players() {
        let mut g = GameState::new("ABCD", sets());
        assert_eq!(g.start_collecting(), Err(TransitionError::NoPlayers));
    }

    #[test]
    fn start_collecting_requires_prompt_sets() {
        let mut g = GameState::new("ABCD", vec![]);
        g.add_player("Ann").unwrap();
        assert_eq!(g.start_collecting(), Err(TransitionError::NoPromptSets));
    }

    #[test]
    fn start_collecting_assigns_partners_in_a_ring() {
        let mut g = lobby_with(&["Ann", "Bo", "Cy"]);
        g.start_collecting().unwrap();
        assert_eq!(g.phase, Phase::Collecting);
        assert_eq!(g.players[0].partner.as_deref(), Some("Bo"));
        assert_eq!(g.players[1].partner.as_deref(), Some("Cy"));
        assert_eq!(g.players[2].partner.as_deref(), Some("Ann"));
        // every player got a valid prompt set assignment
        for p in &g.players {
            assert!(p.prompt_set_id.is_some());
            assert!(g.prompt_set(p.prompt_set_id.unwrap()).is_some());
        }
    }

    #[test]
    fn cannot_start_collecting_twice() {
        let mut g = lobby_with(&["Ann"]);
        g.start_collecting().unwrap();
        assert_eq!(
            g.start_collecting(),
            Err(TransitionError::WrongPhase {
                phase: Phase::Collecting
            })
        );
    }

    #[test]
    fn submit_requires_collecting_phase() {
        let mut g = lobby_with(&["Ann"]);
        assert_eq!(
            g.submit_responses("Ann", vec![], String::new()),
            Err(TransitionError::WrongPhase { phase: Phase::Lobby })
        );
    }

    #[test]
    fn submit_validates_response_count() {
        let mut g = lobby_with(&["Ann", "Bo"]);
        g.start_collecting().unwrap();
        let set_id = g.players[0].prompt_set_id.unwrap();
        let expected = g.prompt_set(set_id).unwrap().prompts.len();
        let err = g
            .submit_responses("Ann", vec!["only one".into()], "remember".into())
            .unwrap_err();
        assert_eq!(
            err,
            TransitionError::ResponseCountMismatch {
                expected,
                got: 1
            }
        );
    }

    #[test]
    fn submit_unknown_player() {
        let mut g = lobby_with(&["Ann"]);
        g.start_collecting().unwrap();
        assert_eq!(
            g.submit_responses("Ghost", vec!["x".into(), "y".into()], "z".into()),
            Err(TransitionError::UnknownPlayer("Ghost".into()))
        );
    }

    #[test]
    fn full_happy_path() {
        let mut g = lobby_with(&["Ann", "Bo"]);
        g.start_collecting().unwrap();

        // not everyone has submitted yet
        assert_eq!(g.start_game(), Err(TransitionError::NotAllSubmitted));

        for name in ["Ann", "Bo"] {
            let id = g.players[g.player_index(name).unwrap()].prompt_set_id.unwrap();
            let count = g.prompt_set(id).unwrap().prompts.len();
            let items = vec!["resp".to_string(); count];
            g.submit_responses(name, items, "and that's the truth".into())
                .unwrap();
        }
        assert!(g.all_submitted());
        assert_eq!(g.submitted_count(), 2);

        g.start_game().unwrap();
        assert_eq!(g.phase, Phase::Performing);
        assert_eq!(g.current_slide, 0);

        // slide count: rules(1) + per player [intro, greeting, N prompts, signoff] + credits + blank
        let n_prompts = 2; // both sets have 2 prompts
        let expected = 1 + 2 * (3 + n_prompts) + 2;
        assert_eq!(g.total_slides(), expected);

        // carousel is bounded
        for _ in 0..1000 {
            g.advance_slide().unwrap();
        }
        assert_eq!(g.current_slide, expected - 1);
        g.prev_slide().unwrap();
        assert_eq!(g.current_slide, expected - 2);
        assert_eq!(
            g.goto_slide(expected + 5),
            Err(TransitionError::SlideOutOfRange(expected + 5))
        );
        g.goto_slide(0).unwrap();
        assert_eq!(g.current_slide, 0);

        // archive
        let archived = g.archive_round().unwrap();
        assert_eq!(archived.len(), 2);
        assert_eq!(g.phase, Phase::Archived);
        assert_eq!(archived[0].partner.as_deref(), Some("Bo"));

        // new round resets to an empty lobby
        g.new_round().unwrap();
        assert_eq!(g.phase, Phase::Lobby);
        assert!(g.players.is_empty());
    }

    #[test]
    fn advance_slide_rejected_outside_performing() {
        let mut g = lobby_with(&["Ann"]);
        assert_eq!(
            g.advance_slide(),
            Err(TransitionError::WrongPhase { phase: Phase::Lobby })
        );
        g.start_collecting().unwrap();
        assert_eq!(
            g.advance_slide(),
            Err(TransitionError::WrongPhase {
                phase: Phase::Collecting
            })
        );
    }

    #[test]
    fn cannot_start_game_from_lobby() {
        let mut g = lobby_with(&["Ann"]);
        assert_eq!(
            g.start_game(),
            Err(TransitionError::WrongPhase { phase: Phase::Lobby })
        );
    }

    #[test]
    fn archive_only_from_performing() {
        let mut g = lobby_with(&["Ann"]);
        assert!(matches!(
            g.archive_round(),
            Err(TransitionError::WrongPhase { .. })
        ));
    }

    #[test]
    fn new_round_only_from_archived() {
        let mut g = lobby_with(&["Ann"]);
        assert_eq!(
            g.new_round(),
            Err(TransitionError::WrongPhase { phase: Phase::Lobby })
        );
    }

    #[test]
    fn first_presenter_intro_is_flagged() {
        let mut g = lobby_with(&["Ann", "Bo"]);
        g.start_collecting().unwrap();
        let slides = g.build_slides();
        let first_intro = slides
            .iter()
            .find_map(|s| match s {
                Slide::PresenterIntro { first, .. } => Some(*first),
                _ => None,
            })
            .unwrap();
        assert!(first_intro);
    }
}
