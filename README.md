# The Channel 0 News — realtime backend

A re-platform of the original PHP/MySQL party game into a push-based realtime
multiplayer backend in **Rust (Axum + WebSockets)**. The game flow is preserved
exactly; the transport moved from HTTP polling to authoritative server state
broadcast over WebSockets.


## History

This is a **re-platform** of the original PHP/MySQL app (HTTP polling) into a
push-based realtime backend. The historical source is archived at
[channel-zero-news-php](https://github.com/JacobStephens2/channel-zero-news-php).
Live game: [channelzeronews.app](https://channelzeronews.app).

## The game

A host enters participant names and opens submissions. Each player picks their
name, sees who they're writing *for* (a ring: each writes for the next), and
fills out a prompt set's prompts plus a sign-off. Once everyone has submitted,
the host starts the performance: a slide carousel where each player's partner
reads their script while that player drives the teleprompter. Afterwards the
host archives the round so the responses are retained but not reused.

## Architecture

```
channel-zero/
├── migrations/          SQLx migrations (prompt_sets, responses, response_archive)
└── src/
    ├── main.rs          binary: wire DB + router, serve
    ├── app.rs           AppState + router construction
    ├── routes.rs        REST: create/join room, prompt sets
    ├── ws.rs            WebSocket upgrade + per-connection handler
    ├── room.rs          per-room actor (owns state) + broadcast fan-out + pure reducer
    ├── registry.rs      in-memory map of live rooms by join code
    ├── state.rs         GameState machine (lobby→collecting→performing→archived)
    ├── protocol.rs      serde message enums (client intents ↔ server events)
    ├── db.rs            SQLx persistence
    └── error.rs         typed error → HTTP/WS responses
```

### Concurrency model (the centerpiece)

Each room is a **single Tokio task that owns its `GameState`** — there are no
locks around game state, and all mutation is serialized through one `mpsc`
command queue, so torn reads/writes are impossible. Connections send *intents*;
the actor reduces each with a pure function and:

* fans **broadcast** events (room state, submission progress, slide changes) out
  to every subscriber via a `tokio::sync::broadcast` channel, and
* sends **direct** events (your assignment, errors, pong) back to just the
  originating connection.

The server is the single source of truth. Clients render state and send intents;
they never send state.

### Roles & authority

Host, player, and display screen are all WebSocket clients distinguished by
**role**. A connection's role/identity is established once at join (the host
must present the secret token issued by `POST /api/rooms`) and held in
server memory thereafter — the client cannot re-assert or escalate it.
Host-only actions (open submissions, start, advance slides, archive) are
enforced in the actor, not just the UI.

### Persistence boundary

> **Live game state lives in memory; the database holds durable artifacts only.**

* **In memory** (room actors): the roster, partner ring, prompt assignments,
  submitted answers, current slide — everything that changes during a game.
* **In Postgres**: the prompt-set catalog, each room's *finalized* responses
  (written when the game starts), and the append-only archive of past rounds
  (written on archive, which also clears the live responses — mirroring the
  original insert-select-then-delete).

A dropped connection cannot corrupt a room: state only changes on explicit
intents, so a player can disconnect mid-submission and reconnect to resubmit.
Database writes are best-effort side effects — a DB failure is logged and never
breaks the in-memory game.

## REST surface

| Method | Path                 | Purpose                                   |
|--------|----------------------|-------------------------------------------|
| GET    | `/health`            | liveness                                  |
| POST   | `/api/rooms`         | create a room → `{ code, host_token }`    |
| GET    | `/api/rooms/:code`   | join validation → `{ exists }`            |
| GET    | `/api/prompt-sets`   | the prompt-set catalog                    |
| GET    | `/ws`                | the realtime game socket                  |
| `/`    | (static)             | the test client                           |

## Protocol

All WebSocket messages are serde-tagged JSON. Client→server intents
(`join_room`, `add_player`, `start_collecting`, `submit_responses`,
`start_game`, `advance_slide`/`prev_slide`/`goto_slide`, `archive_round`,
`new_round`, `ping`) and server→client events (`joined`, `assignment`,
`room_state`, `submission_progress`, `game_started`, `slide_changed`, `error`,
`pong`) are exhaustively matched — there is no catch-all that silently drops a
message.

## Development

```bash
cp .env.example .env          # set DATABASE_URL
cargo test                    # unit + in-memory WS e2e
set -a; . ./.env; set +a
cargo test --test persistence # DB-backed persistence (skipped if no DATABASE_URL)
cargo run                     # serves on 0.0.0.0:3471
```

### Database setup (Postgres)

```bash
createdb channelzero
# migrations run automatically on startup; or apply ./migrations manually
```

The original MySQL `tblPrompts` was migrated into `prompt_sets` (the seven
`prompt1..7` columns normalized into a `TEXT[]`), preserving `archived_at`.

## Credits

Original game design & writing: Eric. Web development: Jacob. Realtime
re-platform: Rust/Axum.
