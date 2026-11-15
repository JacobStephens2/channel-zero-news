//! Controlled transport adapters for the production Room connection runner.

use std::{future::Future, sync::Arc, time::Duration};

use channel_zero::{
    connection::{self, Incoming},
    protocol::{ClientMsg, Role, ServerMsg},
    registry::{NewRoom, Registry},
};
use futures_util::{sink, stream};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

pub async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("connection timed out")
}

pub fn join(room: &NewRoom, role: Role, name: Option<&str>) -> ClientMsg {
    ClientMsg::JoinRoom {
        code: room.code.clone(),
        role,
        name: name.map(String::from),
        token: role.is_host().then(|| room.host_token.clone()),
    }
}

pub struct Connection {
    pub input: Option<mpsc::Sender<(Result<Incoming, ()>, oneshot::Sender<()>)>>,
    writes: mpsc::Receiver<ServerMsg>,
    permits: mpsc::Sender<bool>,
    pub task: JoinHandle<()>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Connection {
    pub fn start(registry: &Arc<Registry>) -> Self {
        let (input, incoming) = mpsc::channel(256);
        let (writing, writes) = mpsc::channel(1);
        let (permits, allowed) = mpsc::channel(1);
        let incoming = stream::unfold(incoming, |mut rx| async {
            rx.recv()
                .await
                .map(|(frame, consumed): (_, oneshot::Sender<()>)| {
                    let _ = consumed.send(());
                    (frame, rx)
                })
        });
        // Each flush announces the attempted write and then waits for the test
        // to release it. Dropping input or failing a write remains observable.
        let outgoing = sink::unfold(
            (writing, allowed),
            |(writing, mut allowed), message: ServerMsg| async move {
                writing.send(message).await.map_err(|_| ())?;
                match allowed.recv().await {
                    Some(true) => Ok((writing, allowed)),
                    _ => Err(()),
                }
            },
        );
        let task = tokio::spawn(connection::run(Arc::clone(registry), incoming, outgoing));
        Self {
            input: Some(input),
            writes,
            permits,
            task,
        }
    }

    pub async fn frame(&self, frame: Incoming) {
        self.input_result(Ok(frame)).await;
    }

    pub async fn input_result(&self, frame: Result<Incoming, ()>) {
        let (consumed, acknowledged) = oneshot::channel();
        bounded(self.input.as_ref().unwrap().send((frame, consumed)))
            .await
            .expect("input closed");
        bounded(acknowledged).await.expect("frame not consumed");
    }

    pub async fn send(&self, intent: ClientMsg) {
        self.frame(Incoming::Intent(intent)).await;
    }

    pub async fn recv(&mut self) -> ServerMsg {
        self.permits.send(true).await.unwrap();
        bounded(self.writes.recv()).await.expect("output closed")
    }

    pub async fn blocked(&mut self) -> ServerMsg {
        bounded(self.writes.recv()).await.expect("output closed")
    }

    pub async fn release(&self, success: bool) {
        self.permits.send(success).await.unwrap();
    }

    pub async fn until(&mut self, predicate: impl Fn(&ServerMsg) -> bool) -> ServerMsg {
        loop {
            let message = self.recv().await;
            if predicate(&message) {
                return message;
            }
        }
    }

    pub async fn admit(registry: &Arc<Registry>, intent: ClientMsg) -> Self {
        let mut connection = Self::start(registry);
        connection.send(intent).await;
        assert!(matches!(connection.recv().await, ServerMsg::Joined { .. }));
        assert!(matches!(
            connection.recv().await,
            ServerMsg::RoomState { .. }
        ));
        assert!(matches!(
            connection.recv().await,
            ServerMsg::SubmissionProgress { .. }
        ));
        connection
    }

    pub async fn barrier(&mut self) {
        self.send(ClientMsg::Ping).await;
        self.until(|m| matches!(m, ServerMsg::Pong)).await;
    }

    pub async fn ended(&mut self) {
        bounded(&mut self.task).await.expect("runner panicked");
    }
}
