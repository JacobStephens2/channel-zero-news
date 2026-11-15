//! One socket's Room connection: admission, acknowledged identity, snapshot
//! prefixes, bounded delivery and termination. Transport adapters supply only
//! framed events and a server-event sink; the actor owns game state.

use std::sync::Arc;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::{broadcast, mpsc::error::TryRecvError};

use crate::direct::{self, DirectReceiver, DirectSender};
use crate::protocol::{ClientMsg, ErrorCode, Role, ServerMsg};
use crate::registry::Registry;
use crate::room::{Command, Delivery, RoomHandle};

/// Transport-neutral frame classification. Stream end or an error ends input;
/// non-text frames end initial admission and are ignored after admission.
pub enum Incoming {
    Intent(ClientMsg),
    MalformedText(String),
    NonText,
}

struct Identity {
    code: String,
    role: Role,
    name: Option<String>,
}

impl Identity {
    fn conflicts(&self, intent: &ClientMsg) -> bool {
        matches!(intent, ClientMsg::JoinRoom { code, role, name, .. }
            if code != &self.code || role != &self.role || name != &self.name)
    }
}

/// Run one attempt. Completion or cancellation drops both adapters and the
/// registry's socket guard. No forwarding task outlives this future.
pub async fn run<I, O, E>(registry: Arc<Registry>, incoming: I, outgoing: O)
where
    I: Stream<Item = Result<Incoming, E>>,
    O: Sink<ServerMsg>,
{
    futures_util::pin_mut!(incoming, outgoing);
    let join = match incoming.next().await {
        Some(Ok(Incoming::Intent(join @ ClientMsg::JoinRoom { .. }))) => join,
        Some(Ok(Incoming::Intent(_) | Incoming::MalformedText(_))) => {
            let _ = outgoing
                .send(error(
                    ErrorCode::NotJoined,
                    "first message must be join_room",
                ))
                .await;
            return;
        }
        _ => return,
    };
    let ClientMsg::JoinRoom { code, .. } = &join else {
        unreachable!()
    };
    let Some((handle, _socket)) = registry.open_socket(code).await else {
        let _ = outgoing
            .send(error(
                ErrorCode::UnknownRoom,
                format!("no room with code {code}"),
            ))
            .await;
        return;
    };
    let (direct_tx, mut direct_rx) = direct::channel();
    let overflowed = direct_rx.overflowed();
    let session = async {
        // The actor reads initial identity from JoinRoom, never these dispatch
        // fields. Only its successful acknowledgement establishes authority.
        if handle
            .dispatch(Command {
                intent: join,
                role: Role::Display,
                name: None,
                direct: direct_tx.clone(),
            })
            .await
            .is_err()
        {
            return;
        }
        let Some(first) = direct_rx.recv().await else {
            return;
        };
        let identity = match &first.message {
            ServerMsg::Joined { role, name } => Identity {
                code: handle.code.clone(),
                role: *role,
                name: name.clone(),
            },
            ServerMsg::Error { .. } => {
                let _ = outgoing.send(first.message).await;
                return;
            }
            _ => return,
        };
        let Some(events) = first.snapshot_start else {
            return;
        };
        // Preserve handshake backpressure: do not read later input before the
        // first acknowledgement has been written.
        if outgoing.send(first.message).await.is_err() {
            return;
        }
        tokio::select! {
            biased;
            () = forward_input(&mut incoming, &handle, &identity, direct_tx) => {},
            () = deliver(direct_rx, events, &mut outgoing) => {},
        }
    };
    tokio::select! {
        // Sticky overflow wins even when another terminal event is ready.
        biased;
        () = overflowed => {},
        () = handle.closed() => {},
        () = session => {},
    }
}

async fn forward_input<I, E>(
    incoming: &mut I,
    handle: &RoomHandle,
    identity: &Identity,
    direct: DirectSender<Delivery>,
) where
    I: Stream<Item = Result<Incoming, E>> + Unpin,
{
    while let Some(Ok(frame)) = incoming.next().await {
        if direct.is_overflowed() {
            return;
        }
        match frame {
            Incoming::Intent(intent) if identity.conflicts(&intent) => {
                direct.send(Delivery::ordinary(error(
                    ErrorCode::Forbidden,
                    "join identity cannot change",
                )));
            }
            Incoming::Intent(intent) => {
                if handle
                    .dispatch(Command {
                        intent,
                        role: identity.role,
                        name: identity.name.clone(),
                        direct: direct.clone(),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Incoming::MalformedText(reason) => direct.send(Delivery::ordinary(error(
                ErrorCode::BadRequest,
                format!("could not parse message: {reason}"),
            ))),
            Incoming::NonText => {}
        }
        if direct.is_overflowed() {
            return;
        }
    }
}

async fn deliver<O>(
    mut direct: DirectReceiver<Delivery>,
    mut events: broadcast::Receiver<ServerMsg>,
    outgoing: &mut O,
) where
    O: Sink<ServerMsg> + Unpin,
{
    // Joined has been sent; the rest of its snapshot prefix is still pending.
    let mut snapshot = true;
    let mut selected_broadcast = None;
    loop {
        if direct.is_overflowed() {
            return;
        }
        let reply = match direct.try_recv() {
            Ok(reply) => Some(reply),
            Err(TryRecvError::Disconnected) => return,
            Err(TryRecvError::Empty) if snapshot => direct.recv().await,
            Err(TryRecvError::Empty) => {
                if let Some(message) = selected_broadcast.take() {
                    // Rechecked direct readiness on this iteration, immediately
                    // before beginning the selected broadcast's write.
                    if direct.is_overflowed() || outgoing.send(message).await.is_err() {
                        return;
                    }
                    continue;
                }
                tokio::select! {
                    biased;
                    reply = direct.recv() => reply,
                    event = events.recv() => {
                        match event {
                            Ok(message) => selected_broadcast = Some(message),
                            Err(broadcast::error::RecvError::Lagged(_)) => {},
                            Err(broadcast::error::RecvError::Closed) => return,
                        }
                        // A broadcast can become ready after the direct branch
                        // was polled. Loop to check direct readiness again.
                        continue;
                    }
                }
            }
        };
        let Some(reply) = reply else { return };
        if let Some(subscription) = reply.snapshot_start {
            events = subscription;
            selected_broadcast = None;
            snapshot = true;
        }
        if reply.snapshot_complete {
            snapshot = false;
        }
        if direct.is_overflowed() || outgoing.send(reply.message).await.is_err() {
            return;
        }
    }
}

fn error(code: ErrorCode, message: impl Into<String>) -> ServerMsg {
    ServerMsg::Error {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{sink, stream};
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn actor_termination_cancels_a_blocked_write_and_releases_the_guard() {
        let registry = Registry::new();
        let room = registry.create_room(vec![], None).await.unwrap();
        let actor = registry.get(&room.code).await.unwrap().actor;
        let incoming = stream::once(async {
            Ok::<_, ()>(Incoming::Intent(ClientMsg::JoinRoom {
                code: room.code.clone(),
                role: Role::Display,
                name: None,
                token: None,
            }))
        })
        .chain(stream::pending());
        let (writing, mut writes) = mpsc::channel(1);
        let (release, allowed) = oneshot::channel::<()>();
        let outgoing = sink::unfold((writing, allowed), |(writing, allowed), message| async {
            writing.send(message).await.map_err(|_| ())?;
            allowed.await.map_err(|_| ())?;
            Ok::<_, ()>((writing, oneshot::channel().1))
        });
        let attempt = run(Arc::clone(&registry), incoming, outgoing);
        tokio::pin!(attempt);
        tokio::select! {
            biased;
            _ = &mut attempt => panic!("runner ended before its first write"),
            message = writes.recv() => assert!(matches!(message, Some(ServerMsg::Joined { .. }))),
        }
        actor.abort();
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut attempt)
            .await
            .expect("actor closure did not cancel write");
        drop(release);
        // The input and output adapters have been dropped despite blocked I/O.
        tokio::time::sleep(
            crate::registry::IDLE_WITHOUT_SOCKET + std::time::Duration::from_millis(100),
        )
        .await;
        assert!(registry.get(&room.code).await.is_none());
    }
}
