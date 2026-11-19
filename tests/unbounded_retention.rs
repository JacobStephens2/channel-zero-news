//! Live-room retention: a cap, a quota per client address, removal of a room
//! whose host never joins, and removal when no socket is attached.
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
use channel_zero::protocol::{ClientMsg, ErrorCode, Role, ServerMsg};
use channel_zero::registry::{IDLE_WITHOUT_SOCKET, MAX_LIVE_ROOMS};
use channel_zero::state::{PromptSet, MAX_PLAYER_NAME_CHARS, MAX_ROSTER_PLAYERS};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;
use tokio_tungstenite::{
    client_async, connect_async_with_config, tungstenite::Message, MaybeTlsStream,
};

#[path = "common/mod.rs"]
mod common;

use common::{create_room, http, join, recv_until, send, Ws};

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

/// Bytes asked for each accepted socket's send buffer by
/// [`serve_with_small_send_buffers`], and for a stalled client's receive
/// buffer. Linux doubles the request and stops autotuning the buffer.
const SMALL_SOCKET_BUFFER: u32 = 4096;

async fn spawn_server(peers: Peers) -> Running {
    let state = AppState::in_memory(prompt_sets());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    Running {
        addr: serve(listener, state.clone(), peers).await,
        state,
    }
}

/// Serve `state` on a second loopback listener whose accepted sockets inherit
/// a send buffer of a few kilobytes, so a client of this address that stops
/// reading stalls its connection's writes after a few frames rather than a
/// few megabytes. Returns the address.
async fn serve_with_small_send_buffers(state: AppState) -> String {
    let socket = TcpSocket::new_v4().expect("socket");
    socket
        .set_send_buffer_size(SMALL_SOCKET_BUFFER)
        .expect("send buffer");
    socket
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("bind loopback");
    serve(socket.listen(1024).expect("listen"), state, Peers::Unknown).await
}

/// Serve the router over `state` on `listener` once it answers `/health`, and
/// return its address.
async fn serve(listener: TcpListener, state: AppState, peers: Peers) -> String {
    let router = build_router(state, "static");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        match peers {
            // No Nagle delay, so request-reply loops stay well inside deadlines.
            Peers::Unknown => axum::serve(listener, router).tcp_nodelay(true).await,
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
    addr.to_string()
}

fn host_join(code: &str, host_token: &str) -> ClientMsg {
    ClientMsg::JoinRoom {
        code: code.to_string(),
        role: Role::Host,
        name: None,
        token: Some(host_token.to_string()),
    }
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
    let (code, host_token) = create_room(&running.addr).await;
    let mut ws = join(&running.addr, &host_join(&code, &host_token)).await;

    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        running.state.registry.count().await,
        1,
        "a joined socket should still be retained after the idle deadline"
    );
    send(&mut ws, &ClientMsg::Ping).await;
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
    let running = spawn_server(Peers::Unknown).await;
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
    let running = spawn_server(Peers::Unknown).await;
    let (code, host_token) = create_room(&running.addr).await;
    let mut host = join(&running.addr, &host_join(&code, &host_token)).await;
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

/// A distinct player name whose JSON is as long as a name gets: control
/// characters, each escaped to six bytes.
fn bulky_name(i: usize) -> String {
    format!("{i:02}") + &"\u{1}".repeat(MAX_PLAYER_NAME_CHARS - 2)
}

/// Player joins after which the stalled display's buffers are certainly full.
/// Each join broadcasts the whole roster twice, about 2.4 KB per player with
/// [`bulky_name`]s, so 16 joins send it over 300 KB.
const JOINS_TO_STALL: usize = 16;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_rooms_socket_is_closed_even_when_its_client_stopped_reading() {
    let running = spawn_server(Peers::Unknown).await;
    let stalling_addr = serve_with_small_send_buffers(running.state.clone()).await;
    let (code, _host_token) = create_room(&running.addr).await;
    // Every connection to the room holds a sender of this channel, so it
    // reports Closed only once the actor and all of those connections end.
    let mut room_events = running.state.registry.get(&code).await.unwrap().subscribe();

    // A display that stops reading once its join is acknowledged.
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(SMALL_SOCKET_BUFFER).unwrap();
    let stream = socket
        .connect(stalling_addr.parse().unwrap())
        .await
        .unwrap();
    let (mut stalled, _) = client_async(
        format!("ws://{stalling_addr}/ws"),
        MaybeTlsStream::Plain(stream),
    )
    .await
    .unwrap();
    send(&mut stalled, &display_join(&code)).await;
    recv_until(&mut stalled, |m| matches!(m, ServerMsg::Joined { .. })).await;

    // Non-host self-registrations until the creation deadline; each one
    // broadcasts the whole roster twice to the display. The player socket
    // reads each acknowledgement before its next join.
    let (mut players, _) =
        connect_async_with_config(format!("ws://{}/ws", running.addr), None, true)
            .await
            .unwrap();
    let mut acknowledged = 0;
    'joins: for i in 0..MAX_ROSTER_PLAYERS {
        let player_join = ClientMsg::JoinRoom {
            code: code.clone(),
            role: Role::Player,
            name: Some(bulky_name(i)),
            token: None,
        };
        let frame = Message::Text(serde_json::to_string(&player_join).unwrap());
        if players.send(frame).await.is_err() {
            break;
        }
        loop {
            match tokio::time::timeout(Duration::from_secs(2), players.next()).await {
                Ok(Some(Ok(Message::Text(text)))) if text.starts_with("{\"type\":\"joined\"") => {
                    acknowledged += 1;
                    break;
                }
                Ok(Some(Ok(Message::Text(text)))) if text.contains("\"unknown_room\"") => {
                    break 'joins;
                }
                Ok(Some(Ok(_))) => continue,
                _ => break 'joins,
            }
        }
    }
    assert!(
        acknowledged >= JOINS_TO_STALL,
        "precondition: only {acknowledged} joins before the creation deadline"
    );
    let drain = tokio::spawn(async move { while let Some(Ok(_)) = players.next().await {} });

    // The creation deadline removes the room with the display attached.
    let removal_deadline = Instant::now() + Duration::from_secs(2);
    while running.state.registry.get(&code).await.is_some() {
        assert!(
            Instant::now() < removal_deadline,
            "the room was not removed at its creation deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A socket whose client stopped reading cannot take unknown_room, but must
    // still be closed.
    let all_closed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Err(RecvError::Closed) = room_events.recv().await {
                return;
            }
        }
    })
    .await;
    assert!(
        all_closed.is_ok(),
        "a connection to the removed room was still open 3 s after its removal \
         ({acknowledged} joins before the deadline)"
    );
    drop(stalled);
    let _ = drain.await;
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
        let token = body["host_token"].as_str().expect("host token");
        sockets.push(join(&running.addr, &host_join(code, token)).await);
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
        let token = body["host_token"].as_str().expect("host token");
        sockets.push(join(&running.addr, &host_join(code, token)).await);
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
