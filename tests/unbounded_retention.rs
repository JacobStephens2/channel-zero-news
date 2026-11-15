//! Live-room retention: a cap, a quota per client address, and removal when no
//! socket is attached.
//!
//! The no-socket case is the security reproduction for anonymous
//! `POST /api/rooms`: three empty creates, no WebSocket, one second later the
//! registry count is 0. The HTTP body does not carry the count.
//!
//! The quota tests serve the router as the binary does, with peer addresses,
//! and send what the local Apache forwards. The others serve it without peer
//! addresses, so all their creates share one quota.

use std::net::IpAddr;
use std::time::Duration;

use channel_zero::app::{self, build_router, AppState};
use channel_zero::client_address::ClientAddress;
use channel_zero::protocol::{ClientMsg, Role, ServerMsg};
use channel_zero::registry::MAX_LIVE_ROOMS;
use channel_zero::state::PromptSet;
use futures_util::SinkExt;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[path = "common/mod.rs"]
mod common;

use common::recv_until;

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

enum Peers {
    /// Plain `axum::serve`: requests carry no peer address.
    Unknown,
    /// [`app::serve`], as the binary serves.
    Known,
}

async fn spawn_server(peers: Peers) -> Running {
    let state = AppState::in_memory(prompt_sets());
    let router = build_router(state.clone(), "static");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        match peers {
            Peers::Unknown => axum::serve(listener, router).await,
            Peers::Known => app::serve(listener, router).await,
        }
        .expect("serve");
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

/// `POST /api/rooms` carrying `forwarded_for` as its `X-Forwarded-For`. Bytes,
/// because Apache forwards a client's own header bytes unchanged.
async fn create_from(
    http: &reqwest::Client,
    url: &str,
    forwarded_for: &[u8],
) -> (reqwest::StatusCode, serde_json::Value) {
    let shown = forwarded_for.escape_ascii();
    let response = http
        .post(url)
        .header("x-forwarded-for", forwarded_for)
        .send()
        .await
        .unwrap_or_else(|e| panic!("create from {shown} failed: {e}"));
    let status = response.status();
    let body = response
        .json()
        .await
        .unwrap_or_else(|e| panic!("create from {shown} body: {e}"));
    (status, body)
}

#[tokio::test]
async fn three_empty_posts_are_reaped_without_a_websocket() {
    let running = spawn_server(Peers::Unknown).await;
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
    let running = spawn_server(Peers::Unknown).await;
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
async fn create_past_the_cap_returns_unavailable_and_retains_nothing() {
    let running = spawn_server(Peers::Unknown).await;
    let mut sockets = Vec::new();
    for n in 0..MAX_LIVE_ROOMS {
        // One room per address, so no address reaches its own quota.
        let creator = ClientAddress::key_for(IpAddr::from([10, 0, 0, u8::try_from(n).unwrap()]));
        let room = running
            .state
            .registry
            .create_room(creator, prompt_sets(), None)
            .await
            .expect("room under the cap");
        sockets.push(
            running
                .state
                .registry
                .open_socket(&room.code)
                .await
                .expect("socket under the cap"),
        );
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

#[tokio::test]
async fn one_forwarded_address_creates_eight_rooms_then_gets_429_while_another_still_creates() {
    let running = spawn_server(Peers::Known).await;
    let http = http();
    let url = format!("http://{}/api/rooms", running.addr);
    let mut sockets = Vec::new();
    for n in 1..=8 {
        // The client's own leading entry changes; the one Apache appends does not.
        let forwarded_for = format!("198.51.100.{n}, 203.0.113.7");
        let (status, body) = create_from(&http, &url, forwarded_for.as_bytes()).await;
        assert!(
            status.is_success(),
            "create {n} status {status} body {body}"
        );
        let code = body["code"].as_str().expect("created code");
        sockets.push(
            running
                .state
                .registry
                .open_socket(code)
                .await
                .expect("live room"),
        );
    }

    let (status, body) = create_from(&http, &url, b"198.51.100.9, 203.0.113.7").await;
    assert_eq!(
        status,
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "body {body}"
    );
    assert_eq!(body["error"], "too_many_rooms");
    assert!(
        body.get("code").is_none(),
        "rejected create returned a code: {body}"
    );
    assert_eq!(running.state.registry.count().await, 8);

    let (status, body) = create_from(&http, &url, b"203.0.113.8").await;
    assert!(
        status.is_success(),
        "other address status {status} body {body}"
    );
    assert!(
        body.get("code").is_some(),
        "other address missing code: {body}"
    );
    drop(sockets);
}

#[tokio::test]
async fn bytes_a_client_sends_before_the_entry_apache_appends_do_not_lift_its_quota() {
    let running = spawn_server(Peers::Known).await;
    let http = http();
    let url = format!("http://{}/api/rooms", running.addr);
    let mut sockets = Vec::new();
    for n in 1..=8 {
        let (status, body) = create_from(&http, &url, b"203.0.113.7").await;
        assert!(
            status.is_success(),
            "create {n} status {status} body {body}"
        );
        let code = body["code"].as_str().expect("created code");
        sockets.push(
            running
                .state
                .registry
                .open_socket(code)
                .await
                .expect("live room"),
        );
    }

    // Apache forwards the client's own header bytes unchanged, non-ASCII
    // included, and appends `, 203.0.113.7` after them on the same line.
    let lines: [&[u8]; 3] = [
        b"198.51.100.9, 203.0.113.7",
        b"\xff, 203.0.113.7",
        "é, 203.0.113.7".as_bytes(),
    ];
    for forwarded_for in lines {
        let (status, body) = create_from(&http, &url, forwarded_for).await;
        assert_eq!(
            status,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "{} body {body}",
            forwarded_for.escape_ascii()
        );
    }
    assert_eq!(running.state.registry.count().await, 8);
    drop(sockets);
}
