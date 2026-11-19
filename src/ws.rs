//! WebSocket upgrade and frame/JSON conversion for the Room connection runner.

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::IntoResponse,
};
use futures_util::{future, SinkExt, StreamExt};

use crate::app::AppState;
use crate::connection::{self, Incoming};
use crate::protocol::{ClientMsg, ServerMsg};

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (sink, stream) = socket.split();
    let incoming = stream
        .take_while(|frame| future::ready(!matches!(frame, Ok(Message::Close(_)))))
        .map(|frame| {
            frame.map(|frame| match frame {
                Message::Text(text) => match serde_json::from_str::<ClientMsg>(&text) {
                    Ok(intent) => Incoming::Intent(intent),
                    Err(error) => Incoming::MalformedText(error.to_string()),
                },
                _ => Incoming::NonText,
            })
        });
    let outgoing = sink.with(|message: ServerMsg| {
        future::ready(Ok::<_, axum::Error>(Message::Text(
            serde_json::to_string(&message).expect("server event JSON"),
        )))
    });
    connection::run(state.registry, incoming, outgoing).await;
}
