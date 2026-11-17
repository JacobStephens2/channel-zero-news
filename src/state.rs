//! Shared game value types and limits.
//!
//! Authoritative rules and mutable representation are private to [`crate::room_game`].

use serde::{Deserialize, Serialize};

/// Maximum number of prompts in a prompt set (mirrors `tblPrompts.prompt1..7`).
pub const MAX_PROMPTS: usize = 7;

/// Maximum characters in a lobby player name. Counted as Unicode scalar
/// values, the same unit as the client's text-field character limit.
pub const MAX_PLAYER_NAME_CHARS: usize = 200;

/// Maximum players on one room's roster.
pub const MAX_ROSTER_PLAYERS: usize = 64;

/// Maximum characters in one submitted response or the sign-off. Matches the
/// client `maxlength="200"` on those fields. Counted as Unicode scalar values.
pub const MAX_SUBMISSION_CHARS: usize = 200;

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
