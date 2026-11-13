//! End-to-end test of the full game flow over real WebSockets:
//! create room (REST) -> host adds players -> open submissions -> players
//! submit -> start game -> advance slides -> archive. Exercises host/player/
//! display roles and server-side host enforcement against the live router.

use channel_zero::app::{build_router, AppState};
use channel_zero::protocol::{ClientMsg, Role, ServerMsg};
use channel_zero::state::{Phase, PromptSet, Slide};
use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

#[path = "common/mod.rs"]
mod common;

use common::recv_until;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn prompt_sets() -> Vec<PromptSet> {
    vec![PromptSet {
        id: 1,
        name: "Test".into(),
        author: "Eric".into(),
        prompts: vec!["one ".into(), "two ".into(), "three ".into()],
    }]
}

async fn spawn_server() -> String {
    let state = AppState::in_memory(prompt_sets());
    let app = build_router(state, "static");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("{addr}")
}

#[tokio::test]
async fn client_native_modules_are_served_without_a_build_step() {
    let addr = spawn_server().await;
    let http = reqwest::Client::new();
    let index = http.get(format!("http://{addr}/")).send().await.unwrap();
    assert!(index.status().is_success());
    let html = index.text().await.unwrap();
    assert!(html.contains("<script type=\"module\">"));
    for (path, source) in [
        (
            "room-session.mjs",
            include_str!("../static/room-session.mjs"),
        ),
        (
            "player-round-participation.mjs",
            include_str!("../static/player-round-participation.mjs"),
        ),
        (
            "performance-stage.mjs",
            include_str!("../static/performance-stage.mjs"),
        ),
    ] {
        assert!(html.contains(&format!("from \"./{path}\"")));
        let module = http
            .get(format!("http://{addr}/{path}"))
            .send()
            .await
            .unwrap();
        assert!(module.status().is_success(), "{path}");
        let content_type = module.headers()["content-type"].to_str().unwrap();
        assert!(
            content_type.starts_with("text/javascript")
                || content_type.starts_with("application/javascript"),
            "{path}: {content_type}"
        );
        assert_eq!(module.text().await.unwrap(), source, "{path}");
    }
}

async fn send(ws: &mut Ws, msg: &ClientMsg) {
    ws.send(Message::Text(serde_json::to_string(msg).unwrap()))
        .await
        .unwrap();
}

async fn connect(addr: &str) -> Ws {
    let (ws, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    ws
}

#[tokio::test]
async fn full_game_over_websocket() {
    let addr = spawn_server().await;
    let http = reqwest::Client::new();

    // ---- create a room over REST ----
    let created: serde_json::Value = http
        .post(format!("http://{addr}/api/rooms"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let code = created["code"].as_str().unwrap().to_string();
    let host_token = created["host_token"].as_str().unwrap().to_string();

    // ---- host joins ----
    let mut host = connect(&addr).await;
    send(
        &mut host,
        &ClientMsg::JoinRoom {
            code: code.clone(),
            role: Role::Host,
            name: None,
            token: Some(host_token.clone()),
        },
    )
    .await;
    let joined = recv_until(&mut host, |m| matches!(m, ServerMsg::Joined { .. })).await;
    assert!(matches!(joined, ServerMsg::Joined { role: Role::Host, .. }));

    // ---- host builds the roster ----
    for name in ["Ann", "Bo"] {
        send(&mut host, &ClientMsg::AddPlayer { name: name.into() }).await;
    }
    // wait until both players are visible
    recv_until(&mut host, |m| {
        matches!(m, ServerMsg::RoomState { room } if room.players.len() == 2)
    })
    .await;

    // ---- display screen joins (read-only) ----
    let mut display = connect(&addr).await;
    send(
        &mut display,
        &ClientMsg::JoinRoom {
            code: code.clone(),
            role: Role::Display,
            name: None,
            token: None,
        },
    )
    .await;
    recv_until(&mut display, |m| matches!(m, ServerMsg::Joined { .. })).await;

    // ---- a display screen must NOT be able to drive the game ----
    send(&mut display, &ClientMsg::StartCollecting).await;
    let err = recv_until(&mut display, |m| matches!(m, ServerMsg::Error { .. })).await;
    assert!(
        matches!(err, ServerMsg::Error { code, .. } if code == channel_zero::protocol::ErrorCode::Forbidden)
    );

    // ---- host opens submissions ----
    send(&mut host, &ClientMsg::StartCollecting).await;
    recv_until(&mut host, |m| {
        matches!(m, ServerMsg::RoomState { room } if room.phase == Phase::Collecting)
    })
    .await;

    // ---- players join and submit ----
    for name in ["Ann", "Bo"] {
        let mut player = connect(&addr).await;
        send(
            &mut player,
            &ClientMsg::JoinRoom {
                code: code.clone(),
                role: Role::Player,
                name: Some(name.into()),
                token: None,
            },
        )
        .await;
        // player receives their assignment (partner + prompts)
        let assignment =
            recv_until(&mut player, |m| matches!(m, ServerMsg::Assignment { .. })).await;
        let prompt_count = match assignment {
            ServerMsg::Assignment { prompts, .. } => prompts.len(),
            _ => unreachable!(),
        };
        assert_eq!(prompt_count, 3);
        send(
            &mut player,
            &ClientMsg::SubmitResponses {
                responses: vec!["aa".into(), "bb".into(), "cc".into()],
                signoff: "stay classy".into(),
            },
        )
        .await;
        // confirm the server accepted it (no error before progress)
        recv_until(&mut player, |m| {
            matches!(m, ServerMsg::SubmissionProgress { .. } | ServerMsg::RoomState { .. })
        })
        .await;
    }

    // ---- display sees everyone submitted ----
    recv_until(&mut display, |m| {
        matches!(m, ServerMsg::SubmissionProgress { submitted, total, .. } if submitted == total && *total == 2)
    })
    .await;

    // ---- host starts the game ----
    send(&mut host, &ClientMsg::StartGame).await;
    let started = recv_until(&mut host, |m| matches!(m, ServerMsg::GameStarted { .. })).await;
    let total_slides = match started {
        ServerMsg::GameStarted { total_slides } => total_slides,
        _ => unreachable!(),
    };
    // rules(1) + per player [intro, greeting, 3 prompts, signoff]=6 + credits + blank
    assert_eq!(total_slides, 1 + 2 * 6 + 2);

    // the display should see the first slide (the rules)
    let slide0 = recv_until(&mut display, |m| {
        matches!(m, ServerMsg::SlideChanged { index, .. } if *index == 0)
    })
    .await;
    assert!(matches!(
        slide0,
        ServerMsg::SlideChanged { slide: Slide::Rules, .. }
    ));

    // ---- host advances the carousel; display follows ----
    send(&mut host, &ClientMsg::AdvanceSlide).await;
    recv_until(&mut display, |m| {
        matches!(m, ServerMsg::SlideChanged { index: 1, .. })
    })
    .await;

    // ---- host archives the round ----
    send(&mut host, &ClientMsg::ArchiveRound).await;
    recv_until(&mut host, |m| {
        matches!(m, ServerMsg::RoomState { room } if room.phase == Phase::Archived)
    })
    .await;
}

#[tokio::test]
async fn host_join_with_bad_token_is_rejected() {
    let addr = spawn_server().await;
    let http = reqwest::Client::new();
    let created: serde_json::Value = http
        .post(format!("http://{addr}/api/rooms"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let code = created["code"].as_str().unwrap().to_string();

    let mut host = connect(&addr).await;
    send(
        &mut host,
        &ClientMsg::JoinRoom {
            code,
            role: Role::Host,
            name: None,
            token: Some("totally-wrong".into()),
        },
    )
    .await;
    let err = recv_until(&mut host, |m| matches!(m, ServerMsg::Error { .. })).await;
    assert!(
        matches!(err, ServerMsg::Error { code, .. } if code == channel_zero::protocol::ErrorCode::Forbidden)
    );
}

#[tokio::test]
async fn joining_unknown_room_errors() {
    let addr = spawn_server().await;
    let mut ws = connect(&addr).await;
    send(
        &mut ws,
        &ClientMsg::JoinRoom {
            code: "ZZZZ".into(),
            role: Role::Display,
            name: None,
            token: None,
        },
    )
    .await;
    let err = recv_until(&mut ws, |m| matches!(m, ServerMsg::Error { .. })).await;
    assert!(
        matches!(err, ServerMsg::Error { code, .. } if code == channel_zero::protocol::ErrorCode::UnknownRoom)
    );
}
