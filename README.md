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
    ├── direct.rs        per-connection direct reply channel (fixed capacity)
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
  originating connection, over that connection's fixed-capacity direct reply
  channel. The actor never waits on it: a connection whose channel overflows
  is closed instead, so a client that stops reading cannot stall the room or
  stay open after missing a reply.

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

Client interface and entry-wiring tests use Node 22 or newer, with no package
manager or client build step:

```bash
node --experimental-vm-modules --test tests/*.test.mjs
cargo test --locked
```

The VM modules flag lets the wiring harness execute the actual inline client
module and its three allowed imports against controlled browser adapters. Session
tests drive the public `connect`, `restore`, `send`, and read-only `current`
interface in `static/room-session.mjs`. The existing static router serves all three
native modules directly; `tests/full_flow.rs` checks their production imports,
URLs, source bodies, and JavaScript content types. The database test skips its
DB assertions when `DATABASE_URL` is unset.

`static/player-round-participation.mjs` owns Player round participation. The entry
constructs it once with the document and a sender that delegates to Room session,
then forwards session views to `updateSession(view)` and accepted server events
to `handle(message)`. The module owns assignment replay, form rendering and draft
DOM, response/sign-off submission, server-confirmed acknowledgement, and prompt
collapse. Entering a new lobby or starting a replacement connection clears those
together. Identical assignments within a round preserve drafts and manual
reopening; a successful replay is limited to one per round, and empty prompt sets
still support sign-off submission. `tests/player-round-participation.test.mjs`
exercises that interface with a controlled document and recording sender; entry
tests verify its wiring through the real Room session. Roster/progress display,
errors, and host round-management controls stay in the entry. Room session retains
identity and connection ownership, and the room actor retains authoritative game state.

`static/performance-stage.mjs` owns Performance stage. The entry constructs one
instance with document/window adapters and a sender delegated to Room session,
then forwards session views to `updateSession(view)` and accepted events to
`handle(message)`. Those are its only public operations. The module coordinates
slide content/index, server-assigned controller eligibility, role presentation,
display dismissal, stage classes, bounded font fitting, resize and local stage
controls, and all carousel buttons/arrow keys. It uses acknowledged identity,
independent of pending join-form edits. A connecting session resets presentation
together; terminal sessions retain the last slide and local viewport controls,
while carousel input requires a joined acknowledged host or player controller.
Slides are processed in delivery order, including before a performing snapshot;
leaving performing clears controller eligibility and re-arms display entry for
the next performance. There is no client deck cache or authoritative game state.
Player round participation remains independent. Deterministic interface tests in
`tests/performance-stage.test.mjs` cover all slide kinds/roles, event ordering,
session and round transitions, carousel intents and font-dependent fit/resize.
Entry tests retain real Room session regressions for host navigation, player
controller handoff, display restore/dismissal/resize and obsolete socket events.

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
