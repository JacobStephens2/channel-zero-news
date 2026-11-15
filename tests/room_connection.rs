//! Behavioral tests of the Room connection runner against the real registry and
//! actor, with controlled standard Stream/Sink transport adapters.

use std::{sync::Arc, time::Duration};

use channel_zero::{
    connection::Incoming,
    protocol::{ClientMsg, ErrorCode, Role, ServerMsg},
    registry::{NewRoom, Registry, IDLE_WITHOUT_SOCKET},
    state::{Phase, PromptSet, MAX_PLAYER_NAME_CHARS, MAX_ROSTER_PLAYERS},
};

#[path = "common/connection.rs"]
mod connection;
use connection::{bounded, join, Connection};

async fn room() -> (Arc<Registry>, NewRoom) {
    let registry = Registry::new();
    let room = registry
        .create_room(
            vec![PromptSet {
                id: 1,
                name: "News".into(),
                author: "Eric".into(),
                prompts: vec!["headline".into()],
            }],
            None,
        )
        .await
        .unwrap();
    (registry, room)
}

fn assert_error(message: ServerMsg, expected: ErrorCode) {
    assert!(
        matches!(message, ServerMsg::Error { code, .. } if code == expected),
        "{message:?}"
    );
}

fn roster(message: ServerMsg, names: &[&str]) {
    let ServerMsg::RoomState { room } = message else {
        panic!("{message:?}")
    };
    assert_eq!(
        room.players
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        names
    );
}

#[tokio::test]
async fn admission_preserves_first_frame_distinctions_and_terminates_rejections() {
    let (registry, room) = room().await;
    for (frame, expected) in [
        (Incoming::NonText, None),
        (
            Incoming::MalformedText("invalid JSON".into()),
            Some(ErrorCode::NotJoined),
        ),
        (
            Incoming::Intent(ClientMsg::Ping),
            Some(ErrorCode::NotJoined),
        ),
        (
            Incoming::Intent(ClientMsg::JoinRoom {
                code: "missing".into(),
                role: Role::Display,
                name: None,
                token: None,
            }),
            Some(ErrorCode::UnknownRoom),
        ),
        (
            Incoming::Intent(ClientMsg::JoinRoom {
                code: room.code.clone(),
                role: Role::Host,
                name: None,
                token: Some("bad".into()),
            }),
            Some(ErrorCode::Forbidden),
        ),
    ] {
        let mut connection = Connection::start(&registry);
        connection.frame(frame).await;
        if let Some(expected) = expected {
            assert_error(connection.recv().await, expected);
        }
        connection.ended().await;
    }
    removed(&registry, &room.code).await;
}

#[tokio::test]
async fn conflicting_replays_do_not_change_identity_roster_or_subscription() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut player = Connection::admit(&registry, join(&room, Role::Player, Some("Ann"))).await;
    for intent in [
        ClientMsg::JoinRoom {
            code: "other".into(),
            role: Role::Player,
            name: Some("Ann".into()),
            token: None,
        },
        join(&room, Role::Host, Some("Ann")),
        join(&room, Role::Player, Some("Bo")),
        join(&room, Role::Player, None),
    ] {
        player.send(intent).await;
        assert_error(player.recv().await, ErrorCode::Forbidden);
        player.barrier().await;
    }
    player
        .send(ClientMsg::AddPlayer {
            name: "forged".into(),
        })
        .await;
    assert_error(player.recv().await, ErrorCode::Forbidden);
    host.send(ClientMsg::AddPlayer { name: "Bo".into() }).await;
    roster(player.recv().await, &["Ann", "Bo"]);
    host.barrier().await;
}

#[tokio::test]
async fn matching_replays_preserve_host_authority_and_ignore_non_host_tokens() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut bad_token = join(&room, Role::Host, None);
    if let ClientMsg::JoinRoom { token, .. } = &mut bad_token {
        *token = None;
    }
    host.send(bad_token).await;
    assert_error(host.recv().await, ErrorCode::Forbidden);
    host.send(ClientMsg::AddPlayer { name: "Ann".into() }).await;
    roster(host.recv().await, &["Ann"]);
    host.barrier().await;
    host.send(join(&room, Role::Host, None)).await;
    assert!(matches!(
        host.recv().await,
        ServerMsg::Joined {
            role: Role::Host,
            ..
        }
    ));
    roster(host.recv().await, &["Ann"]);
    assert!(matches!(
        host.recv().await,
        ServerMsg::SubmissionProgress { total: 1, .. }
    ));
    let mut display_join = join(&room, Role::Display, None);
    if let ClientMsg::JoinRoom { token, .. } = &mut display_join {
        *token = Some("ignored".into());
    }
    let mut display = Connection::admit(&registry, display_join.clone()).await;
    display.send(display_join).await;
    assert!(matches!(
        display.recv().await,
        ServerMsg::Joined {
            role: Role::Display,
            ..
        }
    ));
    roster(display.recv().await, &["Ann"]);
    assert!(matches!(
        display.recv().await,
        ServerMsg::SubmissionProgress { total: 1, .. }
    ));
    display.barrier().await;
}

#[tokio::test]
async fn admission_snapshot_precedes_mutations_queued_behind_its_cut() {
    let (registry, room) = room().await;
    let host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut player = Connection::start(&registry);
    player.send(join(&room, Role::Player, Some("Ann"))).await;
    assert!(matches!(player.blocked().await, ServerMsg::Joined { .. }));
    // The actor has acknowledged Ann while her first output write is blocked.
    host.send(ClientMsg::AddPlayer { name: "Bo".into() }).await;
    player.release(true).await;
    roster(player.recv().await, &["Ann"]);
    assert!(matches!(
        player.recv().await,
        ServerMsg::SubmissionProgress { total: 1, .. }
    ));
    roster(player.recv().await, &["Ann", "Bo"]);
    assert!(matches!(
        player.recv().await,
        ServerMsg::SubmissionProgress { total: 2, .. }
    ));
    player.barrier().await;
}

#[tokio::test]
async fn replay_discards_old_backlog_and_finishes_prefix_before_new_broadcasts() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut display = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    // Occupy output with an ordinary reply, then ready both streams behind it.
    display.send(ClientMsg::Ping).await;
    assert_eq!(display.blocked().await, ServerMsg::Pong);
    host.send(ClientMsg::AddPlayer { name: "Ann".into() }).await;
    host.barrier().await;
    display
        .frame(Incoming::MalformedText("before replay".into()))
        .await;
    display.send(join(&room, Role::Display, None)).await;
    // A later input parse error proves the replay was forwarded before release:
    // errors are direct and keep their position relative to queued snapshots.
    display
        .frame(Incoming::MalformedText("after replay".into()))
        .await;
    host.send(ClientMsg::AddPlayer { name: "Bo".into() }).await;
    host.barrier().await;
    display.release(true).await;
    assert_error(display.recv().await, ErrorCode::BadRequest);
    // Depending on actor scheduling, the second parse error can precede the
    // complete snapshot or follow it; it never permits a broadcast inside it.
    let mut next = display.recv().await;
    if matches!(next, ServerMsg::Error { .. }) {
        assert_error(next, ErrorCode::BadRequest);
        next = display.recv().await;
    }
    assert!(matches!(next, ServerMsg::Joined { .. }), "{next:?}");
    let snapshot = display.recv().await;
    let ServerMsg::RoomState { room: snapshot } = snapshot else {
        panic!("expected snapshot")
    };
    assert_eq!(
        snapshot
            .players
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        ["Ann"]
    );
    let mut progress = display.recv().await;
    if matches!(progress, ServerMsg::Error { .. }) {
        assert_error(progress, ErrorCode::BadRequest);
        progress = display.recv().await;
    }
    assert!(
        matches!(progress, ServerMsg::SubmissionProgress { total, .. } if total == snapshot.players.len())
    );
    // A new mutation after observing the replay must resume live broadcasts.
    host.send(ClientMsg::AddPlayer { name: "Cy".into() }).await;
    let mut last = snapshot.players.len();
    loop {
        match display.recv().await {
            ServerMsg::RoomState { room } => {
                assert!(room.players.len() >= last, "older state after replay");
                last = room.players.len();
                if last == 3 {
                    break;
                }
            }
            ServerMsg::Error { code, .. } => assert_eq!(code, ErrorCode::BadRequest),
            ServerMsg::SubmissionProgress { .. } => {}
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test]
async fn blocked_output_overflow_terminates_and_another_connection_remains_responsive() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut slow = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    slow.send(ClientMsg::Ping).await;
    assert_eq!(slow.blocked().await, ServerMsg::Pong);
    // Eight three-message snapshots plus eight pongs occupy exactly 32 slots.
    for _ in 0..8 {
        slow.send(join(&room, Role::Display, None)).await;
    }
    for _ in 0..8 {
        slow.send(ClientMsg::Ping).await;
    }
    host.barrier().await;
    assert!(
        !slow.task.is_finished(),
        "metadata consumed actual-message capacity"
    );
    slow.send(ClientMsg::Ping).await;
    slow.ended().await;
    host.barrier().await;
    host.send(ClientMsg::AddPlayer { name: "Ann".into() }).await;
    roster(host.recv().await, &["Ann"]);
    host.barrier().await;
    host.input.take();
    host.ended().await;
    removed(&registry, &room.code).await;
}

#[tokio::test]
async fn parser_overflow_does_not_drain_queued_replies_into_ready_output() {
    use futures_util::{sink, stream, StreamExt};
    let (registry, room) = room().await;
    let frames = std::iter::once(Ok::<_, ()>(Incoming::Intent(join(
        &room,
        Role::Display,
        None,
    ))))
    .chain((0..33).map(|_| Ok(Incoming::MalformedText("invalid".into()))));
    let incoming = stream::iter(frames).chain(stream::pending());
    let mut delivered = Vec::new();
    let outgoing = sink::unfold(&mut delivered, |messages, message| async {
        messages.push(message);
        Ok::<_, ()>(messages)
    });
    bounded(channel_zero::connection::run(registry, incoming, outgoing)).await;
    assert_eq!(
        delivered,
        vec![ServerMsg::Joined {
            role: Role::Display,
            name: None
        }],
        "overflow drained replies after losing one"
    );
}

#[tokio::test]
async fn both_admitted_subscribers_receive_broadcasts_and_errors_stay_direct() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut display = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    display.send(ClientMsg::StartGame).await;
    assert_error(display.recv().await, ErrorCode::Forbidden);
    // Pong is a barrier: any erroneously broadcast error would precede it.
    host.send(ClientMsg::Ping).await;
    assert_eq!(host.recv().await, ServerMsg::Pong);
    host.send(ClientMsg::AddPlayer { name: "Ann".into() }).await;
    let a = host.recv().await;
    let b = display.recv().await;
    assert_eq!(a, b);
    roster(a, &["Ann"]);
}

#[tokio::test]
async fn same_identity_replay_acquires_assignment_and_current_performance_slide() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut ann = Connection::admit(&registry, join(&room, Role::Player, Some("Ann"))).await;
    let mut bo = Connection::admit(&registry, join(&room, Role::Player, Some("Bo"))).await;
    host.send(ClientMsg::StartCollecting).await;
    host.until(|m| matches!(m, ServerMsg::RoomState { room } if room.phase == Phase::Collecting))
        .await;
    for (player, name, partner) in [(&mut ann, "Ann", "Bo"), (&mut bo, "Bo", "Ann")] {
        player.send(join(&room, Role::Player, Some(name))).await;
        player
            .until(|m| matches!(m, ServerMsg::Joined { .. }))
            .await;
        assert!(
            matches!(player.recv().await, ServerMsg::RoomState { room } if room.phase == Phase::Collecting)
        );
        assert!(matches!(
            player.recv().await,
            ServerMsg::SubmissionProgress { total: 2, .. }
        ));
        assert_eq!(
            player.recv().await,
            ServerMsg::Assignment {
                partner: partner.into(),
                prompts: vec!["headline".into()]
            }
        );
        player
            .send(ClientMsg::SubmitResponses {
                responses: vec!["news".into()],
                signoff: "bye".into(),
            })
            .await;
        player.barrier().await;
    }
    host.send(ClientMsg::StartGame).await;
    host.until(|m| matches!(m, ServerMsg::GameStarted { .. }))
        .await;
    ann.send(join(&room, Role::Player, Some("Ann"))).await;
    ann.until(|m| matches!(m, ServerMsg::Joined { .. })).await;
    assert!(
        matches!(ann.recv().await, ServerMsg::RoomState { room } if room.phase == Phase::Performing)
    );
    assert!(matches!(
        ann.recv().await,
        ServerMsg::SubmissionProgress {
            submitted: 2,
            total: 2,
            ..
        }
    ));
    assert!(matches!(
        ann.recv().await,
        ServerMsg::SlideChanged { index: 0, .. }
    ));
    ann.barrier().await;
}

#[tokio::test]
async fn registration_limits_and_permissive_non_registering_joins_are_preserved() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut invalid = Connection::start(&registry);
    invalid
        .send(join(
            &room,
            Role::Player,
            Some(&"a".repeat(MAX_PLAYER_NAME_CHARS + 1)),
        ))
        .await;
    assert_error(invalid.recv().await, ErrorCode::InvalidTransition);
    invalid.ended().await;
    for i in 0..MAX_ROSTER_PLAYERS {
        host.send(ClientMsg::AddPlayer {
            name: format!("Player {i}"),
        })
        .await;
        // Drain as we go to retain all production broadcast events.
        host.until(|m| matches!(m, ServerMsg::SubmissionProgress { total, .. } if *total == i + 1))
            .await;
    }
    let mut full = Connection::start(&registry);
    full.send(join(&room, Role::Player, Some("extra"))).await;
    assert_error(full.recv().await, ErrorCode::InvalidTransition);
    full.ended().await;
    let mut existing =
        Connection::admit(&registry, join(&room, Role::Player, Some("Player 0"))).await;
    existing.barrier().await;
    let mut unnamed = Connection::admit(&registry, join(&room, Role::Player, None)).await;
    unnamed.barrier().await;
    host.send(ClientMsg::StartCollecting).await;
    host.barrier().await;
    let mut outsider =
        Connection::admit(&registry, join(&room, Role::Player, Some("outsider"))).await;
    outsider.send(ClientMsg::Ping).await;
    assert_eq!(
        outsider.recv().await,
        ServerMsg::Pong,
        "non-roster player should have no assignment"
    );
}

#[tokio::test]
async fn lagged_broadcasts_are_skipped_and_live_delivery_resumes() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut display = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    display.send(ClientMsg::Ping).await;
    assert_eq!(display.blocked().await, ServerMsg::Pong);
    // 300 events exceed the unchanged 256-event broadcast capacity.
    for _ in 0..75 {
        for intent in [
            ClientMsg::AddPlayer {
                name: "temporary".into(),
            },
            ClientMsg::RemovePlayer {
                name: "temporary".into(),
            },
        ] {
            host.send(intent).await;
            host.until(|m| matches!(m, ServerMsg::SubmissionProgress { .. }))
                .await;
        }
    }
    display.release(true).await;
    host.send(ClientMsg::AddPlayer {
        name: "latest".into(),
    })
    .await;
    display.until(|m| matches!(m, ServerMsg::RoomState { room } if room.players.iter().any(|p| p.name == "latest"))).await;
    display.barrier().await;
}

async fn removed(registry: &Registry, code: &str) {
    bounded(async {
        loop {
            if registry.get(code).await.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn input_closure_cancels_blocked_output_and_releases_guard() {
    let (registry, room) = room().await;
    let mut connection = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    connection.send(ClientMsg::Ping).await;
    assert_eq!(connection.blocked().await, ServerMsg::Pong);
    connection.input.take();
    connection.ended().await;
    removed(&registry, &room.code).await;
}

#[tokio::test]
async fn output_failure_cancels_pending_input_and_releases_guard() {
    let (registry, room) = room().await;
    let mut connection = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    connection.send(ClientMsg::Ping).await;
    assert_eq!(connection.blocked().await, ServerMsg::Pong);
    connection.release(false).await;
    connection.ended().await;
    assert!(connection.input.as_ref().unwrap().is_closed());
    removed(&registry, &room.code).await;
}

#[tokio::test]
async fn transport_error_cancels_blocked_output_and_releases_guard() {
    let (registry, room) = room().await;
    let mut connection = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    connection.send(ClientMsg::Ping).await;
    assert_eq!(connection.blocked().await, ServerMsg::Pong);
    connection.input_result(Err(())).await;
    connection.ended().await;
    removed(&registry, &room.code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_parser_errors_do_not_end_replay_prefixes() {
    let (registry, room) = room().await;
    let mut host = Connection::admit(&registry, join(&room, Role::Host, None)).await;
    let mut display = Connection::admit(&registry, join(&room, Role::Display, None)).await;
    for i in 0..32 {
        display.send(join(&room, Role::Display, None)).await;
        display
            .frame(Incoming::MalformedText("interleaved".into()))
            .await;
        host.send(ClientMsg::AddPlayer {
            name: format!("Player {i}"),
        })
        .await;
        host.barrier().await;
        let mut prefix = 0;
        let mut errors = 0;
        let mut snapshot_players = 0;
        let mut saw_latest = false;
        while prefix < 3 || errors == 0 || !saw_latest {
            match display.recv().await {
                ServerMsg::Error { code, .. } => {
                    assert_eq!(code, ErrorCode::BadRequest);
                    errors += 1;
                }
                ServerMsg::Joined { .. } => {
                    assert_eq!(prefix, 0);
                    prefix = 1;
                }
                ServerMsg::RoomState { room } if prefix == 1 => {
                    snapshot_players = room.players.len();
                    saw_latest = snapshot_players == i + 1;
                    prefix = 2;
                }
                ServerMsg::SubmissionProgress { total, .. } if prefix == 2 => {
                    assert_eq!(total, snapshot_players);
                    prefix = 3;
                }
                ServerMsg::RoomState { room } if prefix == 3 => {
                    assert!(room.players.len() >= snapshot_players);
                    saw_latest = room.players.len() == i + 1;
                }
                ServerMsg::SubmissionProgress { .. } if prefix == 3 => {}
                other => panic!("broadcast overtook snapshot prefix: {other:?}"),
            }
        }
        display.barrier().await;
    }
}

#[tokio::test]
async fn cancellation_during_admission_or_after_join_releases_guard() {
    for admitted in [false, true] {
        let (registry, room) = room().await;
        let mut connection = if admitted {
            Connection::admit(&registry, join(&room, Role::Display, None)).await
        } else {
            let mut connection = Connection::start(&registry);
            connection.send(join(&room, Role::Display, None)).await;
            assert!(matches!(
                connection.blocked().await,
                ServerMsg::Joined { .. }
            ));
            connection
        };
        tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(50)).await;
        assert!(registry.get(&room.code).await.is_some());
        connection.task.abort();
        assert!(bounded(&mut connection.task)
            .await
            .unwrap_err()
            .is_cancelled());
        removed(&registry, &room.code).await;
    }
}
