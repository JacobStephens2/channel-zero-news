//! Deterministic traces through the same pure command interface as the Room actor.

use channel_zero::protocol::{ClientMsg, ErrorCode, PlayerStatus, Role, RoomSnapshot, ServerMsg};
use channel_zero::room_game::{Effect, Reaction, RoomGame};
use channel_zero::state::{ArchivedEntry, Phase, PromptSet, Slide};

fn catalog() -> Vec<PromptSet> {
    vec![
        PromptSet {
            id: 10,
            name: "Headlines".into(),
            author: "Eric".into(),
            prompts: vec!["News: ".into(), "Weather: ".into()],
        },
        PromptSet {
            id: 20,
            name: "Sports".into(),
            author: "Eric".into(),
            prompts: vec!["Sports: ".into()],
        },
    ]
}

fn room(phase: Phase, players: &[(&str, bool)], index: usize, total: usize) -> RoomSnapshot {
    RoomSnapshot {
        code: "ABCD".into(),
        phase,
        players: players
            .iter()
            .map(|(name, submitted)| PlayerStatus {
                name: (*name).into(),
                submitted: *submitted,
            })
            .collect(),
        current_slide: index,
        total_slides: total,
    }
}

fn state(snapshot: &RoomSnapshot) -> ServerMsg {
    ServerMsg::RoomState {
        room: snapshot.clone(),
    }
}

fn progress(snapshot: &RoomSnapshot) -> ServerMsg {
    ServerMsg::SubmissionProgress {
        submitted: snapshot.players.iter().filter(|p| p.submitted).count(),
        total: snapshot.players.len(),
        players: snapshot.players.clone(),
    }
}

fn state_progress(snapshot: &RoomSnapshot) -> Vec<ServerMsg> {
    vec![state(snapshot), progress(snapshot)]
}

fn join_intent(role: Role, name: Option<&str>, token: Option<&str>) -> ClientMsg {
    ClientMsg::JoinRoom {
        code: "ABCD".into(),
        role,
        name: name.map(str::to_owned),
        token: token.map(str::to_owned),
    }
}

fn replay(game: &mut RoomGame, role: Role, name: Option<&str>) -> Reaction {
    // Join identity comes from the intent, independently of the trusted context.
    game.apply(
        Role::Display,
        None,
        join_intent(role, name, role.is_host().then_some("secret-token")),
    )
}

fn join_messages(role: Role, name: Option<&str>, snapshot: &RoomSnapshot) -> Vec<ServerMsg> {
    vec![
        ServerMsg::Joined {
            role,
            name: name.map(str::to_owned),
        },
        state(snapshot),
        progress(snapshot),
    ]
}

fn expect_replay(
    game: &mut RoomGame,
    role: Role,
    name: Option<&str>,
    snapshot: &RoomSnapshot,
    tail: Vec<ServerMsg>,
) {
    let mut direct = join_messages(role, name, snapshot);
    direct.extend(tail);
    assert_eq!(
        replay(game, role, name),
        Reaction {
            direct,
            ..Reaction::default()
        }
    );
}

fn host(game: &mut RoomGame, intent: ClientMsg) -> Reaction {
    game.apply(Role::Host, None, intent)
}

fn expect_host_mutation(game: &mut RoomGame, intent: ClientMsg, snapshot: &RoomSnapshot) {
    assert_eq!(
        host(game, intent),
        Reaction {
            broadcasts: state_progress(snapshot),
            ..Reaction::default()
        }
    );
}

fn rejected(
    game: &mut RoomGame,
    role: Role,
    name: Option<&str>,
    intent: ClientMsg,
    code: ErrorCode,
    message: &str,
) {
    let before = replay(game, Role::Display, None);
    assert_eq!(
        game.apply(role, name, intent),
        Reaction {
            direct: vec![ServerMsg::Error {
                code,
                message: message.into()
            }],
            ..Reaction::default()
        }
    );
    assert_eq!(
        replay(game, Role::Display, None),
        before,
        "rejection changed replay"
    );
}

fn lobby(names: &[&str]) -> RoomGame {
    let mut game = RoomGame::new("ABCD", catalog(), "secret-token");
    for name in names {
        let reaction = host(
            &mut game,
            ClientMsg::AddPlayer {
                name: (*name).into(),
            },
        );
        assert!(reaction.direct.is_empty() && reaction.effects.is_empty());
        assert!(matches!(
            reaction.broadcasts.as_slice(),
            [
                ServerMsg::RoomState { .. },
                ServerMsg::SubmissionProgress { .. }
            ]
        ));
    }
    game
}

fn submit(game: &mut RoomGame, name: &str, responses: &[&str], signoff: &str) -> Reaction {
    game.apply(
        Role::Player,
        Some(name),
        ClientMsg::SubmitResponses {
            responses: responses.iter().map(|s| (*s).into()).collect(),
            signoff: signoff.into(),
        },
    )
}

fn collecting() -> RoomGame {
    let mut game = lobby(&["Ann", "Bo"]);
    expect_host_mutation(
        &mut game,
        ClientMsg::StartCollecting,
        &room(Phase::Collecting, &[("Ann", false), ("Bo", false)], 0, 12),
    );
    game
}

fn finalized_entries() -> Vec<ArchivedEntry> {
    vec![
        ArchivedEntry {
            name: "Ann".into(),
            partner: Some("Bo".into()),
            prompt_set_id: Some(10),
            prompts: vec!["News: ".into(), "Weather: ".into()],
            responses: vec!["cats".into(), "sun".into()],
            signoff: "stay curious".into(),
        },
        ArchivedEntry {
            name: "Bo".into(),
            partner: Some("Ann".into()),
            prompt_set_id: Some(20),
            prompts: vec!["Sports: ".into()],
            responses: vec!["goals".into()],
            signoff: "good night".into(),
        },
    ]
}

fn slide(index: usize, total: usize, slide: Slide, controller: Option<&str>) -> ServerMsg {
    ServerMsg::SlideChanged {
        index,
        total_slides: total,
        slide,
        controller: controller.map(str::to_owned),
    }
}

fn performing() -> RoomGame {
    let mut game = collecting();
    let ann_submitted = room(Phase::Collecting, &[("Ann", true), ("Bo", false)], 0, 12);
    assert_eq!(
        submit(&mut game, "Ann", &["cats", "sun"], "stay curious"),
        Reaction {
            broadcasts: vec![progress(&ann_submitted), state(&ann_submitted)],
            ..Reaction::default()
        }
    );
    let all_submitted = room(Phase::Collecting, &[("Ann", true), ("Bo", true)], 0, 12);
    assert_eq!(
        submit(&mut game, "Bo", &["goals"], "good night"),
        Reaction {
            broadcasts: vec![progress(&all_submitted), state(&all_submitted)],
            ..Reaction::default()
        }
    );
    let snapshot = room(Phase::Performing, &[("Ann", true), ("Bo", true)], 0, 12);
    assert_eq!(
        host(&mut game, ClientMsg::StartGame),
        Reaction {
            broadcasts: vec![
                ServerMsg::GameStarted { total_slides: 12 },
                slide(0, 12, Slide::Rules, Some("Bo")),
                state(&snapshot)
            ],
            direct: vec![],
            effects: vec![Effect::SaveFinalResponses(finalized_entries())],
        }
    );
    game
}

#[test]
fn ping_is_only_a_direct_pong_for_every_trusted_role() {
    let mut game = RoomGame::new("ABCD", vec![], "secret-token");
    for role in [Role::Host, Role::Player, Role::Display] {
        assert_eq!(
            game.apply(role, None, ClientMsg::Ping),
            Reaction {
                direct: vec![ServerMsg::Pong],
                ..Reaction::default()
            }
        );
    }
}

#[test]
fn host_tokens_are_checked_on_every_join_and_later_authority_uses_trusted_context() {
    let mut game = lobby(&[]);
    for _ in 0..2 {
        for token in [None, Some("wrong")] {
            rejected(
                &mut game,
                Role::Host,
                None,
                join_intent(Role::Host, None, token),
                ErrorCode::Forbidden,
                "invalid or missing host token",
            );
        }
        expect_replay(
            &mut game,
            Role::Host,
            None,
            &room(Phase::Lobby, &[], 0, 3),
            vec![],
        );
    }
    // A previous host join does not elevate a later trusted player context.
    rejected(
        &mut game,
        Role::Player,
        Some("Ann"),
        ClientMsg::AddPlayer { name: "Bo".into() },
        ErrorCode::Forbidden,
        "this action is host-only",
    );
    expect_host_mutation(
        &mut game,
        ClientMsg::AddPlayer { name: "Ann".into() },
        &room(Phase::Lobby, &[("Ann", false)], 0, 6),
    );
}

#[test]
fn roster_commands_preserve_names_order_and_exact_mutation_events() {
    let mut game = lobby(&[]);
    for name in ["", " Ann ", "ann"] {
        // Blank, whitespace and case remain literal identities: no new validation.
        let reaction = replay(&mut game, Role::Player, Some(name));
        assert_eq!(
            reaction.direct.first(),
            Some(&ServerMsg::Joined {
                role: Role::Player,
                name: Some(name.into())
            })
        );
        assert_eq!(reaction.broadcasts, reaction.direct[1..]);
        assert!(reaction.effects.is_empty());
    }
    let snapshot = room(
        Phase::Lobby,
        &[("", false), (" Ann ", false), ("ann", false)],
        0,
        12,
    );
    expect_replay(&mut game, Role::Display, None, &snapshot, vec![]);
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::AddPlayer { name: "ann".into() },
        ErrorCode::InvalidTransition,
        "a player named 'ann' is already in this room",
    );
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::RemovePlayer { name: "Ann".into() },
        ErrorCode::InvalidTransition,
        "player 'Ann' was not found in this room",
    );
    expect_host_mutation(
        &mut game,
        ClientMsg::RemovePlayer {
            name: " Ann ".into(),
        },
        &room(Phase::Lobby, &[("", false), ("ann", false)], 0, 9),
    );
    expect_host_mutation(
        &mut game,
        ClientMsg::AddPlayer { name: "Bo".into() },
        &room(
            Phase::Lobby,
            &[("", false), ("ann", false), ("Bo", false)],
            0,
            12,
        ),
    );
}

#[test]
fn name_limit_counts_unicode_scalars_for_host_adds_and_self_registration() {
    for self_register in [false, true] {
        let mut game = lobby(&[]);
        let name = "📰".repeat(200);
        let snapshot = room(Phase::Lobby, &[(&name, false)], 0, 6);
        if self_register {
            assert_eq!(
                replay(&mut game, Role::Player, Some(&name)),
                Reaction {
                    broadcasts: state_progress(&snapshot),
                    direct: join_messages(Role::Player, Some(&name), &snapshot),
                    effects: vec![],
                }
            );
        } else {
            expect_host_mutation(
                &mut game,
                ClientMsg::AddPlayer { name: name.clone() },
                &snapshot,
            );
        }
        let too_long = "📰".repeat(201);
        let intent = if self_register {
            join_intent(Role::Player, Some(&too_long), None)
        } else {
            ClientMsg::AddPlayer { name: too_long }
        };
        rejected(
            &mut game,
            Role::Host,
            None,
            intent,
            ErrorCode::InvalidTransition,
            "player name is longer than 200 characters",
        );
        expect_replay(&mut game, Role::Display, None, &snapshot, vec![]);
    }
}

#[test]
fn full_roster_rejects_new_names_but_allows_existing_or_missing_join_names() {
    let names: Vec<String> = (0..64).map(|i| format!("p{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut game = lobby(&refs);
    let players: Vec<(&str, bool)> = refs.iter().map(|n| (*n, false)).collect();
    let snapshot = room(Phase::Lobby, &players, 0, 195);
    for intent in [
        ClientMsg::AddPlayer { name: "new".into() },
        join_intent(Role::Player, Some("new"), None),
    ] {
        rejected(
            &mut game,
            Role::Host,
            None,
            intent,
            ErrorCode::InvalidTransition,
            "the roster is limited to 64 players",
        );
    }
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::AddPlayer { name: "p0".into() },
        ErrorCode::InvalidTransition,
        "a player named 'p0' is already in this room",
    );
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::AddPlayer {
            name: "é".repeat(201),
        },
        ErrorCode::InvalidTransition,
        "player name is longer than 200 characters",
    );
    expect_replay(&mut game, Role::Player, Some("p0"), &snapshot, vec![]);
    expect_replay(&mut game, Role::Player, None, &snapshot, vec![]);
    expect_host_mutation(
        &mut game,
        ClientMsg::RemovePlayer { name: "p0".into() },
        &room(Phase::Lobby, &players[1..], 0, 192),
    );
    let reaction = replay(&mut game, Role::Player, Some("replacement"));
    assert!(reaction
        .direct
        .iter()
        .all(|m| !matches!(m, ServerMsg::Error { .. })));
    assert_eq!(reaction.broadcasts, reaction.direct[1..]);
}

#[test]
fn assignments_follow_roster_ring_and_catalog_round_robin() {
    let mut game = lobby(&["Ann", "Bo", "Cy"]);
    expect_replay(
        &mut game,
        Role::Player,
        Some("Ann"),
        &room(
            Phase::Lobby,
            &[("Ann", false), ("Bo", false), ("Cy", false)],
            0,
            12,
        ),
        vec![],
    );
    let snapshot = room(
        Phase::Collecting,
        &[("Ann", false), ("Bo", false), ("Cy", false)],
        0,
        17,
    );
    expect_host_mutation(&mut game, ClientMsg::StartCollecting, &snapshot);
    for (name, partner, prompts) in [
        ("Ann", "Bo", vec!["News: ", "Weather: "]),
        ("Bo", "Cy", vec!["Sports: "]),
        ("Cy", "Ann", vec!["News: ", "Weather: "]),
    ] {
        expect_replay(
            &mut game,
            Role::Player,
            Some(name),
            &snapshot,
            vec![ServerMsg::Assignment {
                partner: partner.into(),
                prompts: prompts.into_iter().map(str::to_owned).collect(),
            }],
        );
    }
    for role in [Role::Host, Role::Display] {
        expect_replay(&mut game, role, Some("Ann"), &snapshot, vec![]);
    }
    let too_long = "é".repeat(201);
    for name in [None, Some("Latecomer"), Some(too_long.as_str())] {
        expect_replay(&mut game, Role::Player, name, &snapshot, vec![]);
    }
}

#[test]
fn absent_catalog_rejects_without_mutation_and_player_validation_keeps_precedence() {
    let mut game = RoomGame::new("ABCD", vec![], "secret-token");
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::StartCollecting,
        ErrorCode::InvalidTransition,
        "the room has no players",
    );
    expect_host_mutation(
        &mut game,
        ClientMsg::AddPlayer { name: "Ann".into() },
        &room(Phase::Lobby, &[("Ann", false)], 0, 6),
    );
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::StartCollecting,
        ErrorCode::InvalidTransition,
        "no prompt sets are available to assign",
    );
}

#[test]
fn zero_prompt_single_player_round_has_self_assignment_and_signoff() {
    let mut game = RoomGame::new(
        "ABCD",
        vec![PromptSet {
            id: 30,
            name: "Sign-off only".into(),
            author: "Eric".into(),
            prompts: vec![],
        }],
        "secret-token",
    );
    let lobby = room(Phase::Lobby, &[("Solo", false)], 0, 6);
    assert_eq!(
        replay(&mut game, Role::Player, Some("Solo")),
        Reaction {
            broadcasts: state_progress(&lobby),
            direct: join_messages(Role::Player, Some("Solo"), &lobby),
            effects: vec![],
        }
    );
    let collecting = room(Phase::Collecting, &[("Solo", false)], 0, 6);
    expect_host_mutation(&mut game, ClientMsg::StartCollecting, &collecting);
    expect_replay(
        &mut game,
        Role::Player,
        Some("Solo"),
        &collecting,
        vec![ServerMsg::Assignment {
            partner: "Solo".into(),
            prompts: vec![],
        }],
    );
    let submitted = room(Phase::Collecting, &[("Solo", true)], 0, 6);
    assert_eq!(
        submit(&mut game, "Solo", &[], "bye"),
        Reaction {
            broadcasts: vec![progress(&submitted), state(&submitted)],
            ..Reaction::default()
        }
    );
    let entries = vec![ArchivedEntry {
        name: "Solo".into(),
        partner: Some("Solo".into()),
        prompt_set_id: Some(30),
        prompts: vec![],
        responses: vec![],
        signoff: "bye".into(),
    }];
    let start = host(&mut game, ClientMsg::StartGame);
    assert_eq!(
        start.effects,
        vec![Effect::SaveFinalResponses(entries.clone())]
    );
    assert_eq!(
        start.broadcasts,
        vec![
            ServerMsg::GameStarted { total_slides: 6 },
            slide(0, 6, Slide::Rules, Some("Solo")),
            state(&room(Phase::Performing, &[("Solo", true)], 0, 6))
        ]
    );
    assert!(start.direct.is_empty());
    let moved = host(&mut game, ClientMsg::GotoSlide { index: 3 });
    assert_eq!(
        moved.broadcasts,
        vec![
            slide(
                3,
                6,
                Slide::Signoff {
                    presenter: "Solo".into(),
                    text: "bye".into()
                },
                Some("Solo")
            ),
            state(&room(Phase::Performing, &[("Solo", true)], 3, 6))
        ]
    );
    assert!(moved.direct.is_empty() && moved.effects.is_empty());
    assert_eq!(
        host(&mut game, ClientMsg::ArchiveRound),
        Reaction {
            broadcasts: vec![state(&room(Phase::Archived, &[("Solo", true)], 3, 6))],
            direct: vec![],
            effects: vec![Effect::ArchiveRound(entries)],
        }
    );
}

#[test]
fn submission_rejections_preserve_accepted_answers_and_resubmission_updates_them() {
    let mut game = collecting();
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::StartGame,
        ErrorCode::InvalidTransition,
        "not every player has submitted yet",
    );
    let limit = "📰".repeat(200);
    let submitted = room(Phase::Collecting, &[("Ann", true), ("Bo", false)], 0, 12);
    assert_eq!(
        submit(&mut game, "Ann", &["old", "answers"], "old signoff"),
        Reaction {
            broadcasts: vec![progress(&submitted), state(&submitted)],
            ..Reaction::default()
        }
    );
    assert_eq!(
        submit(&mut game, "Ann", &[&limit, "updated"], &limit),
        Reaction {
            broadcasts: vec![progress(&submitted), state(&submitted)],
            ..Reaction::default()
        }
    );
    for (responses, signoff, message) in [
        (
            vec!["é".repeat(201)],
            "é".repeat(201),
            "expected 2 responses but got 1",
        ),
        (
            vec!["fine".into(), "é".repeat(201)],
            "é".repeat(201),
            "a response is longer than 200 characters",
        ),
        (
            vec!["fine".into(), "fine".into()],
            "é".repeat(201),
            "sign-off is longer than 200 characters",
        ),
    ] {
        rejected(
            &mut game,
            Role::Player,
            Some("Ann"),
            ClientMsg::SubmitResponses { responses, signoff },
            ErrorCode::InvalidTransition,
            message,
        );
    }
    rejected(
        &mut game,
        Role::Player,
        Some("Ghost"),
        ClientMsg::SubmitResponses {
            responses: vec![],
            signoff: String::new(),
        },
        ErrorCode::InvalidTransition,
        "player 'Ghost' was not found in this room",
    );
    rejected(
        &mut game,
        Role::Host,
        None,
        ClientMsg::StartGame,
        ErrorCode::InvalidTransition,
        "not every player has submitted yet",
    );
    let all = room(Phase::Collecting, &[("Ann", true), ("Bo", true)], 0, 12);
    assert_eq!(
        submit(&mut game, "Bo", &["goals"], "good night"),
        Reaction {
            broadcasts: vec![progress(&all), state(&all)],
            ..Reaction::default()
        }
    );
    let mut expected = finalized_entries();
    expected[0].responses = vec![limit.clone(), "updated".into()];
    expected[0].signoff = limit;
    let start = host(&mut game, ClientMsg::StartGame);
    assert_eq!(
        start.effects,
        vec![Effect::SaveFinalResponses(expected.clone())]
    );
    assert_eq!(
        host(&mut game, ClientMsg::ArchiveRound).effects,
        vec![Effect::ArchiveRound(expected.clone())]
    );
    host(&mut game, ClientMsg::NewRound);
    assert_eq!(start.effects, vec![Effect::SaveFinalResponses(expected)]);
}

#[test]
fn full_round_preserves_deck_replay_archive_and_owned_effect_snapshots() {
    let mut game = performing();
    let deck = vec![
        (Slide::Rules, Some("Bo")),
        (
            Slide::PresenterIntro {
                presenter: "Bo".into(),
                teleprompter: "Ann".into(),
                first: true,
            },
            Some("Bo"),
        ),
        (
            Slide::Greeting {
                presenter: "Bo".into(),
            },
            Some("Bo"),
        ),
        (
            Slide::Script {
                reader: "Bo".into(),
                text: "News: cats".into(),
            },
            Some("Bo"),
        ),
        (
            Slide::Script {
                reader: "Bo".into(),
                text: "Weather: sun".into(),
            },
            Some("Bo"),
        ),
        (
            Slide::Signoff {
                presenter: "Bo".into(),
                text: "stay curious".into(),
            },
            Some("Bo"),
        ),
        (
            Slide::PresenterIntro {
                presenter: "Ann".into(),
                teleprompter: "Bo".into(),
                first: false,
            },
            Some("Ann"),
        ),
        (
            Slide::Greeting {
                presenter: "Ann".into(),
            },
            Some("Ann"),
        ),
        (
            Slide::Script {
                reader: "Ann".into(),
                text: "Sports: goals".into(),
            },
            Some("Ann"),
        ),
        (
            Slide::Signoff {
                presenter: "Ann".into(),
                text: "good night".into(),
            },
            Some("Ann"),
        ),
        (Slide::Credits, None),
        (Slide::Blank, None),
    ];
    for (index, (content, controller)) in deck.into_iter().enumerate() {
        let snapshot = room(Phase::Performing, &[("Ann", true), ("Bo", true)], index, 12);
        let changed = slide(index, 12, content, controller);
        assert_eq!(
            host(&mut game, ClientMsg::GotoSlide { index }),
            Reaction {
                broadcasts: vec![changed.clone(), state(&snapshot)],
                ..Reaction::default()
            }
        );
        for (role, name) in [
            (Role::Host, None),
            (Role::Player, Some("Ann")),
            (Role::Player, Some("unknown")),
            (Role::Display, None),
        ] {
            expect_replay(&mut game, role, name, &snapshot, vec![changed.clone()]);
        }
    }
    let archived_snapshot = room(Phase::Archived, &[("Ann", true), ("Bo", true)], 11, 12);
    let archive = host(&mut game, ClientMsg::ArchiveRound);
    assert_eq!(
        archive,
        Reaction {
            broadcasts: vec![state(&archived_snapshot)],
            direct: vec![],
            effects: vec![Effect::ArchiveRound(finalized_entries())],
        }
    );
    for role in [Role::Host, Role::Player, Role::Display] {
        expect_replay(&mut game, role, Some("Ann"), &archived_snapshot, vec![]);
    }
    expect_host_mutation(
        &mut game,
        ClientMsg::NewRound,
        &room(Phase::Lobby, &[], 0, 3),
    );
    expect_replay(
        &mut game,
        Role::Display,
        None,
        &room(Phase::Lobby, &[], 0, 3),
        vec![],
    );
    expect_host_mutation(
        &mut game,
        ClientMsg::AddPlayer { name: "Cy".into() },
        &room(Phase::Lobby, &[("Cy", false)], 0, 6),
    );
    assert_eq!(
        archive.effects,
        vec![Effect::ArchiveRound(finalized_entries())]
    );
}

#[test]
fn navigation_checks_rules_reader_handoff_host_override_and_saturation() {
    let mut game = performing();
    let forbidden = "only the host or the current reader can move the carousel";
    for (role, name) in [
        (Role::Display, Some("Bo")),
        (Role::Player, Some("Ann")),
        (Role::Player, None),
    ] {
        rejected(
            &mut game,
            role,
            name,
            ClientMsg::AdvanceSlide,
            ErrorCode::Forbidden,
            forbidden,
        );
    }
    let snapshot = room(Phase::Performing, &[("Ann", true), ("Bo", true)], 0, 12);
    assert_eq!(
        host(&mut game, ClientMsg::PrevSlide),
        Reaction {
            broadcasts: vec![slide(0, 12, Slide::Rules, Some("Bo")), state(&snapshot)],
            ..Reaction::default()
        }
    );
    for index in 1..=6 {
        let moved = game.apply(Role::Player, Some("Bo"), ClientMsg::AdvanceSlide);
        assert!(moved.direct.is_empty() && moved.effects.is_empty());
        assert!(
            matches!(&moved.broadcasts[..], [ServerMsg::SlideChanged { index: actual, .. }, ServerMsg::RoomState { room }] if *actual == index && room.current_slide == index)
        );
    }
    rejected(
        &mut game,
        Role::Player,
        Some("Bo"),
        ClientMsg::PrevSlide,
        ErrorCode::Forbidden,
        forbidden,
    );
    let handoff = game.apply(Role::Player, Some("Ann"), ClientMsg::AdvanceSlide);
    assert_eq!(
        handoff,
        Reaction {
            broadcasts: vec![
                slide(
                    7,
                    12,
                    Slide::Greeting {
                        presenter: "Ann".into()
                    },
                    Some("Ann")
                ),
                state(&room(
                    Phase::Performing,
                    &[("Ann", true), ("Bo", true)],
                    7,
                    12
                ))
            ],
            ..Reaction::default()
        }
    );
    for index in [12, usize::MAX] {
        rejected(
            &mut game,
            Role::Host,
            None,
            ClientMsg::GotoSlide { index },
            ErrorCode::InvalidTransition,
            &format!("slide index {index} is out of range"),
        );
    }
    host(&mut game, ClientMsg::GotoSlide { index: 11 });
    for _ in 0..3 {
        assert_eq!(
            host(&mut game, ClientMsg::AdvanceSlide),
            Reaction {
                broadcasts: vec![
                    slide(11, 12, Slide::Blank, None),
                    state(&room(
                        Phase::Performing,
                        &[("Ann", true), ("Bo", true)],
                        11,
                        12
                    ))
                ],
                ..Reaction::default()
            }
        );
    }
    for index in [10, 11] {
        host(&mut game, ClientMsg::GotoSlide { index });
        for name in ["Ann", "Bo"] {
            rejected(
                &mut game,
                Role::Player,
                Some(name),
                ClientMsg::PrevSlide,
                ErrorCode::Forbidden,
                forbidden,
            );
        }
    }
    let previous = host(&mut game, ClientMsg::PrevSlide);
    assert_eq!(
        previous,
        Reaction {
            broadcasts: vec![
                slide(10, 12, Slide::Credits, None),
                state(&room(
                    Phase::Performing,
                    &[("Ann", true), ("Bo", true)],
                    10,
                    12
                ))
            ],
            ..Reaction::default()
        }
    );
}

#[test]
fn phase_and_authorization_rejections_keep_error_precedence_and_replay() {
    let mut game = lobby(&["Ann", "Bo"]);
    for phase in [
        Phase::Lobby,
        Phase::Collecting,
        Phase::Performing,
        Phase::Archived,
    ] {
        let wrong_phase = format!("action not allowed in the {phase:?} phase");
        let mut invalid = vec![
            ClientMsg::StartCollecting,
            ClientMsg::StartGame,
            ClientMsg::ArchiveRound,
            ClientMsg::NewRound,
            ClientMsg::AddPlayer { name: "Cy".into() },
            ClientMsg::RemovePlayer {
                name: "Ghost".into(),
            },
        ];
        invalid.retain(|intent| {
            !matches!(
                (phase, intent),
                (
                    Phase::Lobby,
                    ClientMsg::StartCollecting
                        | ClientMsg::AddPlayer { .. }
                        | ClientMsg::RemovePlayer { .. }
                ) | (Phase::Collecting, ClientMsg::StartGame)
                    | (Phase::Performing, ClientMsg::ArchiveRound)
                    | (Phase::Archived, ClientMsg::NewRound)
            )
        });
        for intent in invalid {
            rejected(
                &mut game,
                Role::Host,
                None,
                intent,
                ErrorCode::InvalidTransition,
                &wrong_phase,
            );
        }
        for intent in [
            ClientMsg::AddPlayer {
                name: "é".repeat(201),
            },
            ClientMsg::RemovePlayer {
                name: "Ghost".into(),
            },
            ClientMsg::StartCollecting,
            ClientMsg::StartGame,
            ClientMsg::ArchiveRound,
            ClientMsg::NewRound,
        ] {
            for role in [Role::Display, Role::Player] {
                rejected(
                    &mut game,
                    role,
                    Some("Ann"),
                    intent.clone(),
                    ErrorCode::Forbidden,
                    "this action is host-only",
                );
            }
        }
        for (role, name) in [
            (Role::Host, Some("Ann")),
            (Role::Display, Some("Ann")),
            (Role::Player, None),
        ] {
            rejected(
                &mut game,
                role,
                name,
                ClientMsg::SubmitResponses {
                    responses: vec![],
                    signoff: String::new(),
                },
                ErrorCode::Forbidden,
                "only a joined player may submit responses",
            );
        }
        if phase != Phase::Collecting {
            rejected(
                &mut game,
                Role::Player,
                Some("Ghost"),
                ClientMsg::SubmitResponses {
                    responses: vec![],
                    signoff: String::new(),
                },
                ErrorCode::InvalidTransition,
                &wrong_phase,
            );
        }
        if phase != Phase::Performing {
            for intent in [
                ClientMsg::AdvanceSlide,
                ClientMsg::PrevSlide,
                ClientMsg::GotoSlide { index: usize::MAX },
            ] {
                rejected(
                    &mut game,
                    Role::Host,
                    None,
                    intent,
                    ErrorCode::InvalidTransition,
                    &wrong_phase,
                );
            }
        }
        // Token validation also precedes phase concerns during every replay.
        rejected(
            &mut game,
            Role::Host,
            None,
            join_intent(Role::Host, None, None),
            ErrorCode::Forbidden,
            "invalid or missing host token",
        );
        match phase {
            Phase::Lobby => {
                host(&mut game, ClientMsg::StartCollecting);
            }
            Phase::Collecting => {
                submit(&mut game, "Ann", &["cats", "sun"], "stay curious");
                submit(&mut game, "Bo", &["goals"], "good night");
                host(&mut game, ClientMsg::StartGame);
            }
            Phase::Performing => {
                host(&mut game, ClientMsg::ArchiveRound);
            }
            Phase::Archived => {
                // All previous rejections preserved the accepted answers.
                expect_replay(
                    &mut game,
                    Role::Display,
                    None,
                    &room(Phase::Archived, &[("Ann", true), ("Bo", true)], 0, 12),
                    vec![],
                );
                host(&mut game, ClientMsg::NewRound);
            }
        }
    }
    expect_replay(
        &mut game,
        Role::Display,
        None,
        &room(Phase::Lobby, &[], 0, 3),
        vec![],
    );
}
