//! A connection's direct reply channel: the replies meant only for that
//! connection (its join acknowledgement and initial state, its assignment,
//! errors, pong), as opposed to the room's broadcast stream.
//!
//! The channel holds at most [`DIRECT_REPLY_CAPACITY`] replies and sending
//! never waits, so a connection that stops reading cannot stall the room actor
//! or the room's other connections. A reply that finds the channel full
//! overflows it: that reply and every later one are dropped, and
//! [`DirectReceiver::overflowed`] resolves so the connection handler closes the
//! socket. Replies queued before the overflow stay in order.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::watch;

use crate::protocol::ServerMsg;

/// Replies one connection's channel holds before it overflows.
///
/// A join is the largest set of direct replies: joined, room state, progress,
/// and the assignment or the current slide. 32 holds eight of those behind a
/// socket write that has not finished.
pub const DIRECT_REPLY_CAPACITY: usize = 32;

/// Create one connection's direct reply channel.
pub fn channel() -> (DirectSender, DirectReceiver) {
    let (tx, rx) = mpsc::channel(DIRECT_REPLY_CAPACITY);
    let overflowed = Arc::new(watch::Sender::new(false));
    (
        DirectSender {
            tx,
            overflowed: Arc::clone(&overflowed),
        },
        DirectReceiver { rx, overflowed },
    )
}

/// The sending half. The connection clones it into every
/// [`crate::room::Command`] it dispatches.
#[derive(Clone)]
pub struct DirectSender {
    tx: mpsc::Sender<ServerMsg>,
    overflowed: Arc<watch::Sender<bool>>,
}

impl DirectSender {
    /// Queue a reply without waiting. A reply that finds the channel full
    /// overflows it, and every reply after an overflow is dropped too. A closed
    /// channel means the connection has already gone, so the reply is dropped.
    pub fn send(&self, msg: ServerMsg) {
        // `send_if_modified` holds the watch lock for the whole closure, so for
        // every clone the overflow check, the enqueue and the overflow mark are
        // one step: a slot freed after an overflow cannot let a later reply in.
        self.overflowed.send_if_modified(|overflowed| {
            if *overflowed {
                return false;
            }
            *overflowed = matches!(self.tx.try_send(msg), Err(TrySendError::Full(_)));
            *overflowed
        });
    }
}

/// The receiving half, owned by the connection handler.
pub struct DirectReceiver {
    rx: mpsc::Receiver<ServerMsg>,
    overflowed: Arc<watch::Sender<bool>>,
}

impl DirectReceiver {
    /// The next reply, in the order they were sent. `None` once every sender
    /// is gone and the queued replies are delivered.
    pub async fn recv(&mut self) -> Option<ServerMsg> {
        self.rx.recv().await
    }

    /// Resolves once a reply has overflowed this channel. It does not borrow
    /// the receiver, so the handler can still wait on it after moving the
    /// receiver into its outbound task.
    pub fn overflowed(&self) -> impl Future<Output = ()> + Send + 'static {
        let overflowed = Arc::clone(&self.overflowed);
        async move {
            // `overflowed` keeps the sender alive, so `wait_for` cannot fail:
            // it returns only once the flag is set.
            let mut flag = overflowed.subscribe();
            let _ = flag.wait_for(|&set| set).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ErrorCode;
    use futures_util::FutureExt;

    fn numbered(i: usize) -> ServerMsg {
        ServerMsg::Error {
            code: ErrorCode::BadRequest,
            message: format!("reply {i}"),
        }
    }

    #[tokio::test]
    async fn replies_from_the_overflow_on_never_arrive() {
        let (tx, mut rx) = channel();
        let overflowed = rx.overflowed();
        // One more reply than the channel holds, so the last one overflows it.
        for i in 0..=DIRECT_REPLY_CAPACITY {
            tx.send(numbered(i));
        }
        let signalled = overflowed.now_or_never().is_some();
        assert!(signalled, "the overflowing send did not signal it");

        // Reading frees a slot, but a reply sent after the overflow must not
        // take it: only the replies queued before it arrive, in order.
        assert_eq!(rx.recv().await, Some(numbered(0)));
        tx.send(numbered(DIRECT_REPLY_CAPACITY + 1));
        drop(tx);
        let mut rest = Vec::new();
        while let Some(reply) = rx.recv().await {
            rest.push(reply);
        }
        let queued: Vec<_> = (1..DIRECT_REPLY_CAPACITY).map(numbered).collect();
        assert_eq!(rest, queued);
    }
}
