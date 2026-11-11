import assert from "node:assert/strict";
import { test } from "node:test";
import { createRoomSession } from "../static/room-session.mjs";
import { ControlledSocket, memoryStorage } from "./client-adapters.mjs";

const ann = { code: "AAAA", role: "player", name: "Ann" };
const bo = { code: "BBBB", role: "player", name: "Bo" };
const joined = (name = "Ann", role = "player") => ({ type: "joined", role, name });

function harness(storage = memoryStorage()) {
  const sockets = [], changes = [], messages = [], errors = [];
  const session = createRoomSession({
    createSocket() { const socket = new ControlledSocket(); sockets.push(socket); return socket; },
    storage,
    onChange: (view) => changes.push(view),
    onMessage: (msg) => messages.push(msg),
    onError: (error) => errors.push(error),
  });
  return { session, sockets, changes, messages, errors, storage };
}

test("replaced callbacks are inert before and after the replacement joins", () => {
  const h = harness();
  h.session.connect(ann);
  const old = h.sockets[0];
  h.session.connect(bo);
  const current = h.sockets[1];
  assert.equal(old.closed, true);
  for (const acknowledged of [false, true]) {
    if (acknowledged) { current.open(); current.message(joined("Bo")); }
    const before = { view: h.session.current, changes: h.changes.length,
      messages: h.messages.length, errors: h.errors.length, operations: h.storage.operations.length };
    old.open();
    old.message(joined());
    old.message({ type: "error", code: "unknown_room", message: "gone" });
    old.message({ type: "assignment", partner: "Old", prompts: ["stale"] });
    old.message({ type: "slide_changed", index: 8 });
    old.error(); old.close();
    assert.deepEqual({ view: h.session.current, changes: h.changes.length,
      messages: h.messages.length, errors: h.errors.length, operations: h.storage.operations.length }, before);
    assert.equal(old.sent.length, 0);
    assert.equal(current.sent.length, acknowledged ? 1 : 0);
  }
  assert.deepEqual(h.storage.saved(), { ...bo, hostToken: null });
});

test("captures normalized credentials, adopts Joined identity and replays on that socket", () => {
  const h = harness();
  const input = { code: " aaaa ", role: "player", name: " Ann ", hostToken: "not-a-host" };
  assert.equal(h.session.send({ type: "ping" }), false);
  h.session.connect(input);
  input.code = "BBBB"; input.role = "display"; input.name = "Bo";
  assert.equal(h.session.send({ type: "ping" }), false);
  const socket = h.sockets[0]; socket.open();
  const initialJoin = { type: "join_room", code: "AAAA", role: "player", name: "Ann", token: null };
  assert.deepEqual(socket.sent, [initialJoin]);
  assert.equal(h.session.current.state, "awaiting_joined");
  assert.equal(h.session.send({ type: "ping" }), false);
  assert.equal(h.storage.saved(), null);
  socket.message(joined("Anne"));
  assert.equal(h.session.current.name, "Anne");
  assert.throws(() => { h.session.current.name = "Bo"; }, TypeError);
  assert.deepEqual(h.storage.saved(), { code: "AAAA", role: "player", name: "Anne", hostToken: null });
  assert.equal(h.session.send({ type: "join_room", name: "Bo" }), false);
  assert.equal(h.session.send({ type: "join_room", code: "BBBB" }), false);
  assert.equal(h.session.send({ type: "join_room", role: "host" }), false);
  assert.equal(h.session.send({ type: "join_room", token: "secret" }), false);
  assert.equal(h.session.send({ type: "join_room" }), true);
  assert.deepEqual(socket.sent[1], { ...initialJoin, name: "Anne" });
  const changes = h.changes.length, writes = h.storage.operations.length;
  socket.message(joined("Anne"));
  assert.equal(h.changes.length, changes);
  assert.equal(h.storage.operations.length, writes);
  socket.message(joined("Bo"));
  assert.equal(h.session.current.name, "Anne");
  assert.equal(h.session.send({ type: "ping" }), true);
  assert.equal(h.sockets.length, 1);
  assert.deepEqual(socket.sent.at(-1), { type: "ping" });
});

test("invalid replacement credentials leave a joined connection and recovery intact", () => {
  const h = harness(); h.session.connect(ann);
  const socket = h.sockets[0]; socket.open(); socket.message(joined());
  const view = h.session.current;
  for (const credentials of [null, {}, { ...ann, code: " " }, { ...ann, role: "admin" },
    { ...ann, name: " " }, { code: "AAAA", role: "host" }]) {
    assert.equal(h.session.connect(credentials), false);
    assert.equal(h.session.current, view);
    assert.equal(socket.closed, false);
  }
  assert.equal(h.sockets.length, 1);
  assert.deepEqual(h.storage.saved(), { ...ann, hostToken: null });
  assert.equal(h.session.send({ type: "ping" }), true);
});

test("refresh restores each role, saves only acknowledgement, and retains recovery on disconnect", () => {
  for (const identity of [ann, { code: "HOST", role: "host", name: null, hostToken: "secret" },
    { code: "SHOW", role: "display", name: null }]) {
    const saved = { ...identity, hostToken: identity.hostToken ?? null };
    const storage = memoryStorage(JSON.stringify(saved));
    const h = harness(storage);
    assert.equal(storage.operations.length, 0);
    assert.equal(h.session.restore(), true);
    assert.deepEqual(storage.operations.map(([operation]) => operation), ["read"]);
    h.sockets[0].open();
    assert.deepEqual(h.sockets[0].sent[0], { type: "join_room", code: identity.code,
      role: identity.role, name: identity.name, token: identity.hostToken ?? null });
    h.sockets[0].message(joined(identity.name, identity.role));
    assert.equal(h.session.current.state, "joined");
    h.sockets[0].close();
    assert.equal(h.session.current.state, "disconnected");
    assert.deepEqual(storage.saved(), saved);
    assert.equal(h.session.send({ type: "ping" }), false);
  }
});

test("invalid recovery is discarded quietly and storage failures never block joining", () => {
  for (const record of ["{", "null", "[]", '"AAAA"', '{}', JSON.stringify({ ...ann, name: 7 }),
    JSON.stringify({ ...ann, role: "admin" }), JSON.stringify({ code: "AAAA", role: "host" })]) {
    const h = harness(memoryStorage(record));
    assert.equal(h.session.restore(), false);
    assert.equal(h.storage.saved(), null);
    assert.equal(h.errors.length, 0);
    assert.equal(h.sockets.length, 0);
  }
  for (const operation of ["getItem", "setItem", "removeItem"]) {
    const storage = memoryStorage("{");
    storage[operation] = () => { throw new Error("storage unavailable"); };
    const h = harness(storage);
    assert.equal(h.session.restore(), false);
    assert.equal(h.session.connect(ann), true);
    h.sockets[0].open(); h.sockets[0].message(joined());
    assert.equal(h.session.current.state, "joined");
    assert.equal(h.errors.length, 0);
  }
});

test("unknown rooms clear only matching recovery and are silent only before restored acknowledgement", () => {
  const unknown = { type: "error", code: "unknown_room", message: "gone" };
  const saved = JSON.stringify({ ...ann, hostToken: null });
  const restored = harness(memoryStorage(saved)); restored.session.restore();
  restored.sockets[0].open(); restored.sockets[0].message(unknown);
  assert.equal(restored.storage.saved(), null);
  assert.deepEqual(restored.messages, []);
  const manual = harness(memoryStorage(saved)); manual.session.connect(ann);
  manual.sockets[0].open(); manual.sockets[0].message(unknown);
  assert.equal(manual.storage.saved(), null);
  assert.deepEqual(manual.messages, [unknown]);
  const replacement = harness(memoryStorage(saved)); replacement.session.connect(bo);
  replacement.sockets[0].open(); replacement.sockets[0].message(unknown);
  assert.deepEqual(replacement.storage.saved(), { ...ann, hostToken: null });
  assert.deepEqual(replacement.messages, [unknown]);
  const active = harness(memoryStorage(saved)); active.session.restore();
  active.sockets[0].open(); active.sockets[0].message(joined()); active.sockets[0].message(unknown);
  assert.deepEqual(active.messages, [unknown]);
});

test("terminal transport callbacks stop delivery and sends without overwriting a failure", () => {
  for (const terminal of ["close", "error"]) {
    const h = harness(); h.session.connect(ann);
    const socket = h.sockets[0]; socket.open(); socket.message(joined());
    socket[terminal]();
    const view = h.session.current, notifications = h.changes.length;
    const operations = h.storage.operations.length;
    assert.equal(view.state, terminal === "error" ? "failed" : "disconnected");
    socket.open(); socket.message({ type: "assignment", partner: "Old", prompts: [] });
    socket.message({ type: "error", code: "unknown_room" }); socket.raw("{");
    socket.message(joined()); socket.error(); socket.close();
    assert.equal(h.session.current, view);
    assert.equal(h.session.send({ type: "ping" }), false);
    assert.equal(h.messages.length, 0);
    assert.equal(h.changes.length, notifications);
    assert.equal(h.storage.operations.length, operations);
    assert.deepEqual(h.storage.saved(), { ...ann, hostToken: null });
  }
});

test("malformed messages are reported before rendering and game events require acknowledgement", () => {
  const h = harness(); h.session.connect(ann);
  const socket = h.sockets[0]; socket.open();
  socket.message({ type: "assignment", partner: "Early", prompts: [] });
  assert.deepEqual(h.messages, []);
  for (const raw of ["{", "null", "[]", "7", '{}']) socket.raw(raw);
  socket.message({ type: "joined", role: "admin" });
  assert.equal(h.errors.length, 6);
  assert.equal(h.session.current.acknowledged, false);
  socket.message(joined());
  socket.message({ type: "pong" });
  assert.deepEqual(h.messages, [{ type: "pong" }]);
  const error = { type: "error", code: "forbidden", message: "host only" };
  socket.message(error);
  assert.deepEqual(h.messages.at(-1), error);
  assert.equal(h.session.current.state, "joined");
});

test("denied joins remain visible and never persist an unacknowledged identity", () => {
  const h = harness(); h.session.connect({ code: "HOST", role: "host", hostToken: "wrong" });
  const socket = h.sockets[0]; socket.open();
  const error = { type: "error", code: "forbidden", message: "invalid token" };
  socket.message(error);
  assert.deepEqual(h.messages, [error]);
  assert.equal(h.session.current.state, "failed");
  socket.close(); socket.message(joined(null, "host"));
  assert.equal(h.session.current.state, "failed");
  assert.equal(h.storage.saved(), null);
});

test("socket construction and send failures are terminal client errors", () => {
  const errors = [];
  const session = createRoomSession({ createSocket() { throw new Error("unavailable"); },
    storage: memoryStorage(), onError: (error) => errors.push(error) });
  assert.equal(session.connect(ann), false);
  assert.equal(session.current.state, "failed");
  assert.equal(errors.length, 1);
  const h = harness(); h.session.connect(ann);
  const socket = h.sockets[0]; socket.open(); socket.message(joined());
  socket.send = () => { throw new Error("send failed"); };
  assert.equal(h.session.send({ type: "ping" }), false);
  assert.equal(h.session.current.state, "failed");
});
