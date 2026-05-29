//! The typed wire protocol between clients and the server.
//!
//! Every message is a serde-tagged enum so JSON looks like
//! `{"type":"submit_responses","responses":[...],"signoff":"..."}`. The server
//! exhaustively matches [`ClientMsg`]; there is no catch-all that could silently
//! drop an intent. Clients send *intents* ([`ClientMsg`]); the server is the
//! sole authority and replies with *events* ([`ServerMsg`]).

use serde::{Deserialize, Serialize};

use crate::state::{Phase, Slide};

/// Which kind of client a socket is acting as. Permissions are enforced
/// server-side per role — never trusted from the UI.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// The control panel: the only role allowed to drive the game.
    Host,
    /// A participant filling out and submitting prompts.
    Player,
    /// The shared submissions / carousel screen (read-only).
    Display,
}

impl Role {
    /// Host-only actions are gated on this server-side.
    pub fn is_host(self) -> bool {
        matches!(self, Role::Host)
    }
}

/// Client → server intents.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    /// First message on every socket: identify the room and role (and, for a
    /// player, which name on the roster they are).
    JoinRoom {
        code: String,
        role: Role,
        #[serde(default)]
        name: Option<String>,
        /// Required when joining as [`Role::Host`]; ignored otherwise. Issued by
        /// the REST `create room` call so host authority is enforced server-side.
        #[serde(default)]
        token: Option<String>,
    },
    /// Host: add a name to the roster (Lobby only).
    AddPlayer { name: String },
    /// Host: remove a name from the roster (Lobby only).
    RemovePlayer { name: String },
    /// Host: lock the roster and open submissions.
    StartCollecting,
    /// Player: submit answers for their assigned prompt set.
    SubmitResponses {
        responses: Vec<String>,
        #[serde(default)]
        signoff: String,
    },
    /// Host: begin the performance once everyone has submitted.
    StartGame,
    /// Host: next carousel slide.
    AdvanceSlide,
    /// Host: previous carousel slide.
    PrevSlide,
    /// Host: jump to a specific slide (keeps late joiners / refreshes in sync).
    GotoSlide { index: usize },
    /// Host: archive the round's responses and end the game.
    ArchiveRound,
    /// Host: clear the slate and start a fresh round.
    NewRound,
    /// Liveness ping; server replies with [`ServerMsg::Pong`].
    Ping,
}

/// Per-player status broadcast to host/display screens. Deliberately does *not*
/// leak a player's partner or answers to everyone.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlayerStatus {
    pub name: String,
    pub submitted: bool,
}

/// A snapshot of the room sent on join and after any state change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoomSnapshot {
    pub code: String,
    pub phase: Phase,
    pub players: Vec<PlayerStatus>,
    pub current_slide: usize,
    pub total_slides: usize,
}

/// Stable machine-readable error codes for [`ServerMsg::Error`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The socket tried a host-only action without the host role.
    Forbidden,
    /// The action isn't valid in the room's current phase / state.
    InvalidTransition,
    /// The referenced room does not exist.
    UnknownRoom,
    /// The first message was not a well-formed `JoinRoom`.
    NotJoined,
    /// The payload could not be parsed.
    BadRequest,
}

/// Server → client events.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// Acknowledges a successful join.
    Joined {
        role: Role,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    /// Targeted (not broadcast): a player's partner and the prompts to fill in.
    Assignment {
        partner: String,
        prompts: Vec<String>,
    },
    /// Full room snapshot.
    RoomState { room: RoomSnapshot },
    /// Submission progress for the submissions screen.
    SubmissionProgress {
        submitted: usize,
        total: usize,
        players: Vec<PlayerStatus>,
    },
    /// The performance has begun.
    GameStarted { total_slides: usize },
    /// The carousel moved.
    SlideChanged {
        index: usize,
        total_slides: usize,
        slide: Slide,
    },
    /// A rejected intent or other error.
    Error { code: ErrorCode, message: String },
    /// Reply to [`ClientMsg::Ping`].
    Pong,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_msg_roundtrips_as_tagged_json() {
        let msg = ClientMsg::JoinRoom {
            code: "ABCD".into(),
            role: Role::Player,
            name: Some("Ann".into()),
            token: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"join_room\""));
        assert!(json.contains("\"role\":\"player\""));
        let back: ClientMsg = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn submit_defaults_missing_signoff() {
        let json = r#"{"type":"submit_responses","responses":["a","b"]}"#;
        let msg: ClientMsg = serde_json::from_str(json).unwrap();
        assert_eq!(
            msg,
            ClientMsg::SubmitResponses {
                responses: vec!["a".into(), "b".into()],
                signoff: String::new(),
            }
        );
    }

    #[test]
    fn unit_intents_parse() {
        for (json, expected) in [
            (r#"{"type":"start_game"}"#, ClientMsg::StartGame),
            (r#"{"type":"advance_slide"}"#, ClientMsg::AdvanceSlide),
            (r#"{"type":"archive_round"}"#, ClientMsg::ArchiveRound),
            (r#"{"type":"ping"}"#, ClientMsg::Ping),
        ] {
            let msg: ClientMsg = serde_json::from_str(json).unwrap();
            assert_eq!(msg, expected);
        }
    }

    #[test]
    fn server_error_serializes_code() {
        let msg = ServerMsg::Error {
            code: ErrorCode::Forbidden,
            message: "host only".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("\"code\":\"forbidden\""));
    }

    #[test]
    fn unknown_intent_type_is_rejected() {
        // No catch-all variant: an unknown intent fails to parse rather than
        // being silently accepted.
        let parsed: Result<ClientMsg, _> = serde_json::from_str(r#"{"type":"nuke"}"#);
        assert!(parsed.is_err());
    }
}
