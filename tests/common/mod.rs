//! Shared helpers for the integration tests. Cargo does not compile this
//! directory on its own; each test crate includes it with `#[path]`.

use std::time::Duration;

use channel_zero::protocol::{ClientMsg, ServerMsg};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Read frames until one satisfies `pred`, returning it. Fails on timeout.
pub async fn recv_until<F>(ws: &mut Ws, mut pred: F) -> ServerMsg
where
    F: FnMut(&ServerMsg) -> bool,
{
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!("timed out waiting for a matching server message"),
            frame = ws.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        let msg: ServerMsg = serde_json::from_str(&text)
                            .unwrap_or_else(|e| panic!("bad server msg {text:?}: {e}"));
                        if pred(&msg) {
                            return msg;
                        }
                    }
                    Some(Ok(_)) => {}
                    other => panic!("socket closed early: {other:?}"),
                }
            }
        }
    }
}

pub async fn send(ws: &mut Ws, msg: &ClientMsg) {
    ws.send(Message::Text(serde_json::to_string(msg).unwrap()))
        .await
        .unwrap();
}

pub async fn connect(addr: &str) -> Ws {
    let (ws, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    ws
}

/// An HTTP client whose requests fail after two seconds instead of hanging.
pub fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client")
}

/// Create a room over REST, returning its join code and host token.
pub async fn create_room(addr: &str) -> (String, String) {
    let created: serde_json::Value = http()
        .post(format!("http://{addr}/api/rooms"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        created["code"].as_str().unwrap().to_string(),
        created["host_token"].as_str().unwrap().to_string(),
    )
}

/// Connect and send `msg`, a `JoinRoom`, returning the socket once the server
/// acknowledges the role it asked for.
pub async fn join(addr: &str, msg: &ClientMsg) -> Ws {
    let ClientMsg::JoinRoom { role, .. } = msg else {
        panic!("join needs a JoinRoom message, got {msg:?}");
    };
    let mut ws = connect(addr).await;
    send(&mut ws, msg).await;
    let joined = recv_until(&mut ws, |m| matches!(m, ServerMsg::Joined { .. })).await;
    assert!(
        matches!(joined, ServerMsg::Joined { role: acked, .. } if acked == *role),
        "asked to join as {role:?}, got {joined:?}"
    );
    ws
}
