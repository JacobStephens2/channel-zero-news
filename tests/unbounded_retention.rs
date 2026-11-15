//! Live-room retention: a cap, removal of a room whose host never joins, and
//! removal when no socket is attached.
//!
//! The no-socket case is the security reproduction for anonymous
//! `POST /api/rooms`: three empty creates, no WebSocket, one second later the
//! registry count is 0. The HTTP body does not carry the count.

use std::time::Duration;

use channel_zero::app::{build_router, AppState};
use channel_zero::protocol::{ClientMsg, ErrorCode, Role, ServerMsg};
use channel_zero::registry::{IDLE_WITHOUT_SOCKET, MAX_LIVE_ROOMS};
use channel_zero::state::PromptSet;
use futures_util::{SinkExt, StreamExt};
use tokio::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[path = "common/mod.rs"]
mod common;

use common::{create_room, join, recv_until, send, Ws};

fn prompt_sets() -> Vec<PromptSet> {
    vec![PromptSet {
        id: 1,
        name: "Top Story".into(),
        author: "Eric".into(),
        prompts: vec!["Our top story tonight: ".into()],
    }]
}

struct Running {
    addr: String,
    state: AppState,
}

async fn spawn_server() -> Running {
    let state = AppState::in_memory(prompt_sets());
    let app = build_router(state.clone(), "static");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let http = http();
    let health_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(response) = http.get(format!("http://{addr}/health")).send().await {
            if response.status().is_success() {
                break;
            }
        }
        if tokio::time::Instant::now() >= health_deadline {
            panic!("loopback server did not accept on {addr}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    Running {
        addr: addr.to_string(),
        state,
    }
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client")
}

fn display_join(code: &str) -> ClientMsg {
    ClientMsg::JoinRoom {
        code: code.to_string(),
        role: Role::Display,
        name: None,
        token: None,
    }
}

/// The server has closed `ws`: the next read ends the stream instead of
/// returning a frame.
async fn assert_closed(ws: &mut Ws) {
    let next = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("the socket was still open");
    assert!(
        matches!(next, None | Some(Err(_)) | Some(Ok(Message::Close(_)))),
        "expected the socket to close, got {next:?}"
    );
}

#[tokio::test]
async fn three_empty_posts_are_reaped_without_a_websocket() {
    let running = spawn_server().await;
    let http = http();

    for n in 1..=3 {
        let response = http
            .post(format!("http://{}/api/rooms", running.addr))
            .body("")
            .send()
            .await
            .unwrap_or_else(|e| panic!("POST {n} failed: {e}"));
        let status = response.status();
        let body: serde_json::Value = response
            .json()
            .await
            .unwrap_or_else(|e| panic!("POST {n} body: {e}"));
        assert!(status.is_success(), "POST {n} status {status} body {body}");
        assert!(
            body.get("count").is_none(),
            "HTTP body included count: {body}"
        );
        assert!(body.get("code").is_some(), "POST {n} missing code: {body}");
        assert!(
            body.get("host_token").is_some(),
            "POST {n} missing host_token: {body}"
        );
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    let count = running.state.registry.count().await;
    assert_eq!(
        count, 0,
        "missing reaper: registry.count() is still {count} after three anonymous creates with no socket"
    );
}

#[tokio::test]
async fn a_joined_socket_keeps_the_room_and_closing_it_lets_the_deadline_remove_it() {
    let running = spawn_server().await;
    let http = http();
    let created: serde_json::Value = http
        .post(format!("http://{}/api/rooms", running.addr))
        .body("")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let code = created["code"].as_str().unwrap().to_string();
    let host_token = created["host_token"].as_str().unwrap().to_string();

    let (mut ws, _) = connect_async(format!("ws://{}/ws", running.addr))
        .await
        .unwrap();
    let join = ClientMsg::JoinRoom {
        code: code.clone(),
        role: Role::Host,
        name: None,
        token: Some(host_token),
    };
    ws.send(Message::Text(serde_json::to_string(&join).unwrap()))
        .await
        .unwrap();
    recv_until(&mut ws, |msg| {
        matches!(
            msg,
            ServerMsg::Joined {
                role: Role::Host,
                ..
            }
        )
    })
    .await;

    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        running.state.registry.count().await,
        1,
        "a joined socket should still be retained after the idle deadline"
    );
    ws.send(Message::Text(
        serde_json::to_string(&ClientMsg::Ping).unwrap(),
    ))
    .await
    .unwrap();
    recv_until(&mut ws, |msg| matches!(msg, ServerMsg::Pong)).await;

    ws.close(None).await.unwrap();
    drop(ws);
    // Close handling plus the idle deadline, with margin for the connection task.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(running.state.registry.count().await, 0);
    assert!(running.state.registry.get(&code).await.is_none());
}

#[tokio::test]
async fn a_room_whose_host_never_joins_is_removed_at_its_creation_deadline_and_its_socket_closed() {
    let running = spawn_server().await;
    let created_at = Instant::now();
    let (code, _host_token) = create_room(&running.addr).await;
    // A non-host join succeeds before the deadline but does not keep the room.
    let mut display = join(&running.addr, &display_join(&code)).await;

    let error = recv_until(&mut display, |msg| matches!(msg, ServerMsg::Error { .. })).await;
    assert!(
        matches!(
            error,
            ServerMsg::Error {
                code: ErrorCode::UnknownRoom,
                ..
            }
        ),
        "expected unknown_room once the room was removed, got {error:?}"
    );
    assert!(
        created_at.elapsed() >= IDLE_WITHOUT_SOCKET,
        "the room was removed before its creation deadline"
    );
    assert_closed(&mut display).await;
    assert!(running.state.registry.get(&code).await.is_none());
}

#[tokio::test]
async fn a_hosted_room_stays_while_a_non_host_socket_remains_after_the_host_leaves() {
    let running = spawn_server().await;
    let (code, host_token) = create_room(&running.addr).await;
    let host_join = ClientMsg::JoinRoom {
        code: code.clone(),
        role: Role::Host,
        name: None,
        token: Some(host_token),
    };
    let mut host = join(&running.addr, &host_join).await;
    let mut display = join(&running.addr, &display_join(&code)).await;

    host.close(None).await.unwrap();
    drop(host);
    // Past the creation deadline, and past the idle deadline the host's close
    // would have started had it been the last socket.
    tokio::time::sleep(IDLE_WITHOUT_SOCKET + Duration::from_millis(300)).await;
    assert!(
        running.state.registry.get(&code).await.is_some(),
        "the display's socket should keep a room whose host joined"
    );
    send(&mut display, &ClientMsg::Ping).await;
    recv_until(&mut display, |msg| matches!(msg, ServerMsg::Pong)).await;
}

#[tokio::test]
async fn create_past_the_cap_returns_unavailable_and_retains_nothing() {
    let running = spawn_server().await;
    let mut sockets = Vec::new();
    for _ in 0..MAX_LIVE_ROOMS {
        let room = running
            .state
            .registry
            .create_room(prompt_sets(), None)
            .await
            .expect("room under the cap");
        let (handle, socket) = running
            .state
            .registry
            .open_socket(&room.code)
            .await
            .expect("socket under the cap");
        // As the connection handler does once a host join is acknowledged.
        socket.host_joined();
        sockets.push((handle, socket));
    }
    assert_eq!(running.state.registry.count().await, MAX_LIVE_ROOMS);

    let response = http()
        .post(format!("http://{}/api/rooms", running.addr))
        .body("")
        .send()
        .await
        .expect("POST at cap");
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("error body");
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], "unavailable");
    assert!(
        body.get("code").is_none(),
        "rejected create still returned a code: {body}"
    );
    assert_eq!(running.state.registry.count().await, MAX_LIVE_ROOMS);
    drop(sockets);
}
