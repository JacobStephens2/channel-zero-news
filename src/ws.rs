//! WebSocket upgrade and the per-connection handler.
//!
//! Each socket is one client (host, player, or display). The connection's role
//! and identity are established once — at join — by the room actor, then held
//! here in server memory and stamped onto every subsequent [`Command`]. The
//! client never re-asserts its role, so it cannot escalate privilege mid-session.
//!
//! Two halves run concurrently:
//! * **inbound**: read frames → parse [`ClientMsg`] → dispatch to the room actor
//! * **outbound**: merge the room's broadcast stream with this connection's
//!   direct reply channel → write frames
//!
//! A dropped connection simply stops sending intents. Because the authoritative
//! state lives in the actor and only mutates on explicit intents, a player
//! disconnecting mid-submission cannot corrupt the room — they can reconnect
//! with the same name and submit again.

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc};

use crate::app::AppState;
use crate::protocol::{ClientMsg, ErrorCode, Role, ServerMsg};
use crate::room::{Command, RoomHandle};

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sink, mut stream) = socket.split();

    // ---- 1. first frame must be a JoinRoom --------------------------------
    let first = match stream.next().await {
        Some(Ok(Message::Text(t))) => t,
        // closed or non-text before joining: nothing to clean up.
        _ => return,
    };

    let join_msg: ClientMsg = match serde_json::from_str(&first) {
        Ok(m @ ClientMsg::JoinRoom { .. }) => m,
        _ => {
            let _ = send_one(
                &mut sink,
                &ServerMsg::Error {
                    code: ErrorCode::NotJoined,
                    message: "first message must be join_room".into(),
                },
            )
            .await;
            return;
        }
    };

    let code = match &join_msg {
        ClientMsg::JoinRoom { code, .. } => code.clone(),
        _ => unreachable!(),
    };

    let handle: RoomHandle = match state.registry.get(&code).await {
        Some(h) => h,
        None => {
            let _ = send_one(
                &mut sink,
                &ServerMsg::Error {
                    code: ErrorCode::UnknownRoom,
                    message: format!("no room with code {code}"),
                },
            )
            .await;
            return;
        }
    };

    // Subscribe before joining so no broadcast between join and loop is missed.
    let mut events = handle.subscribe();
    let (direct_tx, mut direct_rx) = mpsc::unbounded_channel::<ServerMsg>();

    // Forward the join to the actor; it validates the host token and replies
    // with Joined (+ initial state) or an Error over the direct channel.
    if handle
        .dispatch(Command {
            intent: join_msg,
            role: Role::Display, // placeholder; actor reads role from the intent
            name: None,
            direct: direct_tx.clone(),
        })
        .await
        .is_err()
    {
        return; // room actor gone
    }

    // ---- 2. learn our adopted identity from the actor's first reply -------
    let (role, name) = match direct_rx.recv().await {
        Some(ServerMsg::Joined { role, name }) => {
            let _ = send_one(&mut sink, &ServerMsg::Joined { role, name: name.clone() }).await;
            (role, name)
        }
        Some(other @ ServerMsg::Error { .. }) => {
            let _ = send_one(&mut sink, &other).await;
            return; // join rejected (e.g. bad host token)
        }
        _ => return,
    };

    // ---- 3. outbound pump: broadcast + direct -> socket ------------------
    let outbound = tokio::spawn(async move {
        loop {
            tokio::select! {
                ev = events.recv() => match ev {
                    Ok(msg) => {
                        if send_one(&mut sink, &msg).await.is_err() {
                            break;
                        }
                    }
                    // A slow client missed messages: keep going with the latest.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                direct = direct_rx.recv() => match direct {
                    Some(msg) => {
                        if send_one(&mut sink, &msg).await.is_err() {
                            break;
                        }
                    }
                    None => break, // inbound dropped the sender: connection closing
                },
            }
        }
    });

    // ---- 4. inbound loop: socket -> actor --------------------------------
    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => match serde_json::from_str::<ClientMsg>(&text) {
                Ok(intent) => {
                    let cmd = Command {
                        intent,
                        role,
                        name: name.clone(),
                        direct: direct_tx.clone(),
                    };
                    if handle.dispatch(cmd).await.is_err() {
                        break; // room gone
                    }
                }
                Err(e) => {
                    // Report the parse failure to this client only.
                    let _ = direct_tx.send(ServerMsg::Error {
                        code: ErrorCode::BadRequest,
                        message: format!("could not parse message: {e}"),
                    });
                }
            },
            Message::Close(_) => break,
            // Ping/Pong/Binary: ignore (axum auto-answers protocol pings).
            _ => {}
        }
    }

    // Inbound ended: dropping direct_tx closes the outbound pump.
    drop(direct_tx);
    let _ = outbound.await;
    tracing::debug!(%code, ?role, "connection closed");
}

/// Serialize and send a single server message. Returns `Err` if the socket is
/// closed so callers can stop pumping.
async fn send_one(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &ServerMsg,
) -> Result<(), ()> {
    let text = match serde_json::to_string(msg) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to serialize server message");
            return Ok(());
        }
    };
    sink.send(Message::Text(text)).await.map_err(|_| ())
}
