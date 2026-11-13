//! Shared helpers for the integration tests. Cargo does not compile this
//! directory on its own; each test crate includes it with `#[path]`.

use std::time::Duration;

use channel_zero::protocol::ServerMsg;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

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
