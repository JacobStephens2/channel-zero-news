//! Per-room actor: scheduling, delivery and durable-effect execution for Room game.
//!
//! Concurrency model (the centerpiece of this project):
//!
//! * Each room is a single Tokio task that **owns** its opaque `RoomGame`. Nothing
//!   else can touch the state, so there are no locks around it and no chance of
//!   a torn read/write — all mutation is serialized through one `mpsc` queue.
//! * Connections send [`Command`]s (a validated intent + the connection's
//!   server-held role/identity) into that queue.
//! * The actor calls [`RoomGame::apply`] for each command, then fans
//!   the resulting **broadcast** events out to every subscriber via a
//!   `tokio::sync::broadcast` channel, and sends any **direct** (per-connection)
//!   events back over the requester's own fixed-capacity
//!   [direct reply channel](crate::direct). The actor never waits on that
//!   channel: one that overflows closes its connection instead.
//!
//! The registry stops the actor on removal, including pending durable effects.
//! The actor publishes accepted activity for the registry's retention policy.
//!
//! Rules and projection belong to [`RoomGame`]; the actor sees only its reactions.

use std::future::Future;

use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Instant;

use crate::db::Db;
use crate::direct::DirectSender;
use crate::protocol::{ClientMsg, Role, ServerMsg};
use crate::room_game::{Effect, RoomGame};
use crate::state::PromptSet;

/// Capacity of a room's command queue.
const COMMAND_QUEUE: usize = 64;
/// Capacity of a room's broadcast buffer (slow subscribers lag, never block).
const BROADCAST_BUFFER: usize = 256;

/// A validated intent handed to a room actor. `role`/`name` are established once
/// at join time and held server-side by the connection task, so the client
/// cannot forge them on later messages.
pub(crate) struct Command {
    pub intent: ClientMsg,
    pub role: Role,
    pub name: Option<String>,
    /// Channel for events meant only for the originating connection.
    pub direct: DirectSender<Delivery>,
}

/// Runtime-only snapshot boundaries. Each envelope still occupies exactly one
/// actual-message slot; neither metadata field is part of the wire protocol.
pub(crate) struct Delivery {
    pub message: ServerMsg,
    pub snapshot_start: Option<broadcast::Receiver<ServerMsg>>,
    pub snapshot_complete: bool,
}

impl Delivery {
    pub(crate) fn ordinary(message: ServerMsg) -> Self {
        Self {
            message,
            snapshot_start: None,
            snapshot_complete: false,
        }
    }
}

/// A cloneable handle to a running room actor.
#[derive(Clone)]
pub struct RoomHandle {
    pub(crate) code: String,
    cmd_tx: mpsc::Sender<Command>,
    #[cfg(test)]
    pub(crate) actor: tokio::task::AbortHandle,
}

/// Returned when a room's actor task has stopped.
#[derive(Debug)]
pub struct RoomClosed;

impl RoomHandle {
    /// Queue a command for the actor.
    pub(crate) async fn dispatch(&self, cmd: Command) -> Result<(), RoomClosed> {
        self.cmd_tx.send(cmd).await.map_err(|_| RoomClosed)
    }

    /// Resolves once the actor has stopped, including on registry removal.
    pub async fn closed(&self) {
        self.cmd_tx.closed().await;
    }
}

/// Spawn a room actor and return a handle to it. `db` is `None` in tests and
/// no-database runs, in which case durable effects are simply skipped.
/// Resolving `stop` ends the actor and drops queued commands and pending effects.
/// `activity_tx` publishes state changes before their broadcasts are enqueued.
pub(crate) fn spawn_room(
    code: impl Into<String>,
    prompt_sets: Vec<PromptSet>,
    host_token: impl Into<String>,
    db: Option<Db>,
    activity_tx: watch::Sender<Instant>,
    stop: impl Future<Output = ()> + Send + 'static,
) -> RoomHandle {
    let code = code.into();
    let host_token = host_token.into();
    let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_QUEUE);
    let (events_tx, _events_rx) = broadcast::channel(BROADCAST_BUFFER);

    let game = RoomGame::new(code.clone(), prompt_sets, host_token);
    let _task = tokio::spawn(run(
        game,
        code.clone(),
        cmd_rx,
        events_tx.clone(),
        activity_tx,
        db,
        stop,
    ));

    RoomHandle {
        code,
        cmd_tx,
        #[cfg(test)]
        actor: _task.abort_handle(),
    }
}
/// The actor loop. Owns the state for the room's entire lifetime.
async fn run(
    mut game: RoomGame,
    code: String,
    mut cmd_rx: mpsc::Receiver<Command>,
    events_tx: broadcast::Sender<ServerMsg>,
    activity_tx: watch::Sender<Instant>,
    db: Option<Db>,
    stop: impl Future<Output = ()>,
) {
    tokio::pin!(stop);
    'commands: loop {
        let next = tokio::select! {
            // A stopped room takes no further command, even one already queued.
            biased;
            () = &mut stop => None,
            cmd = cmd_rx.recv() => cmd,
        };
        let Some(cmd) = next else { break };
        let reaction = game.apply(cmd.role, cmd.name.as_deref(), cmd.intent);
        // Pings, plain joins and rejected intents only reply to their sender.
        if !reaction.broadcasts.is_empty() {
            activity_tx.send_replace(Instant::now());
        }
        for msg in reaction.broadcasts {
            // Errs only when there are zero subscribers — fine to ignore.
            let _ = events_tx.send(msg);
        }
        // The cut follows self-registration broadcasts and precedes every later
        // command. Queue the complete prefix synchronously at that same cut.
        let snapshot = matches!(reaction.direct.first(), Some(ServerMsg::Joined { .. }));
        let mut subscription = snapshot.then(|| events_tx.subscribe());
        let reply_count = reaction.direct.len();
        for (index, msg) in reaction.direct.into_iter().enumerate() {
            // Never waits, so one connection's full channel cannot hold up
            // the room; the overflow closes that connection instead.
            cmd.direct.send(Delivery {
                message: msg,
                snapshot_start: subscription.take(),
                snapshot_complete: snapshot && index + 1 == reply_count,
            });
        }
        for effect in reaction.effects {
            // A stalled write cannot keep a removed actor or its sockets alive.
            tokio::select! {
                biased;
                () = &mut stop => break 'commands,
                () = run_effect(&db, &code, effect) => {}
            }
        }
    }
    tracing::debug!(room = %code, "room actor stopped");
}

/// Execute a durable effect. Best-effort: failures are logged, never fatal to
/// the in-memory game.
async fn run_effect(db: &Option<Db>, code: &str, effect: Effect) {
    let Some(db) = db else { return };
    match effect {
        Effect::SaveFinalResponses(entries) => {
            if let Err(e) = db.save_final_responses(code, &entries).await {
                tracing::error!(room = %code, error = %e, "failed to persist final responses");
            }
        }
        Effect::ArchiveRound(entries) => match db.archive_round(code, &entries).await {
            Ok(batch) => tracing::info!(room = %code, batch = %batch, "archived round"),
            Err(e) => tracing::error!(room = %code, error = %e, "failed to archive round"),
        },
    }
}
