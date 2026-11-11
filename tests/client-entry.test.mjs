import assert from "node:assert/strict";
import { test } from "node:test";
import { loadClient } from "./client-entry-harness.mjs";
import { memoryStorage } from "./client-adapters.mjs";

const room = (phase, submitted = false) => ({ type: "room_state", room: {
  code: "AAAA", phase, current_slide: 0, total_slides: 10,
  players: [{ name: "Ann", submitted }, { name: "Bo", submitted: false }],
} });

test("player form edits cannot change captured join, replay, submissions or reader controls", async () => {
  const h = await loadClient();
  h.element("code").value = " aaaa "; h.element("pname").value = " Ann ";
  await h.click("joinBtn");
  const socket = h.sockets[0];
  await h.editRole("display");
  h.element("pname").value = "Bo"; h.element("code").value = "BBBB";
  socket.open();
  assert.deepEqual(socket.sent[0], { type: "join_room", code: "AAAA", role: "player", name: "Ann", token: null });
  socket.message({ type: "joined", role: "player", name: "Ann" });
  assert.equal(h.element("playerPanel").classList.contains("hidden"), false);
  assert.equal(h.element("logPanel").classList.contains("hidden"), true);
  socket.message(room("collecting"));
  assert.deepEqual(socket.sent[1], socket.sent[0]);
  socket.message({ type: "assignment", partner: "Bo", prompts: ["Tonight", "Weather"] });
  h.textareas()[0].value = "News"; h.textareas()[1].value = "Rain";
  h.element("signoff").value = "Good night";
  await h.click("submitBtn");
  assert.deepEqual(socket.sent.at(-1), { type: "submit_responses", responses: ["News", "Rain"], signoff: "Good night" });
  socket.message(room("collecting", true));
  assert.equal(h.element("promptsBody").classList.contains("hidden"), true);
  socket.message(room("performing", true));
  socket.message({ type: "slide_changed", index: 2, total_slides: 10, controller: "Ann",
    slide: { kind: "script", reader: "Ann", text: "Read these lines" } });
  assert.equal(h.element("readerControls").classList.contains("hidden"), false);
  assert.equal(h.element("slidePanel").classList.contains("full"), true);
  assert.equal(h.document.body.classList.contains("stage-mode"), false);
  assert.match(h.element("slide").innerHTML, /Read these lines/);
  await h.editRole("host");
  await h.document.dispatch("keydown", { key: "ArrowRight" });
  assert.deepEqual(socket.sent.at(-1), { type: "advance_slide" });
  socket.message({ type: "joined", role: "player", name: "Ann" });
  assert.equal(h.element("promptsBody").classList.contains("hidden"), true);
  assert.equal(h.element("readerControls").classList.contains("hidden"), false);
  assert.equal(h.sockets.length, 1);
  assert.deepEqual(h.storage.saved(), { code: "AAAA", role: "player", name: "Ann", hostToken: null });
});

test("host creation connects with returned credentials despite pending form edits", async () => {
  let respond;
  const response = new Promise((resolve) => { respond = resolve; });
  const h = await loadClient({ fetch: () => response });
  await h.editRole("host");
  const creation = h.click("createBtn");
  await h.editRole("player");
  h.element("code").value = "FAKE"; h.element("pname").value = "Bo";
  respond({ ok: true, json: async () => ({ code: "HOST", host_token: "secret" }) });
  await creation;
  const socket = h.sockets[0]; socket.open();
  assert.deepEqual(socket.sent, [{ type: "join_room", code: "HOST", role: "host", name: null, token: "secret" }]);
  assert.equal(h.storage.saved(), null);
  socket.message({ type: "joined", role: "host" });
  assert.equal(h.element("hostPanel").classList.contains("hidden"), false);
  assert.equal(h.element("playerPanel").classList.contains("hidden"), true);
  assert.equal(h.element("roomPill").textContent, "Room HOST");
  h.element("newPlayer").value = "Ann"; await h.click("addBtn");
  assert.deepEqual(socket.sent.at(-1), { type: "add_player", name: "Ann" });
  await h.click("collectBtn");
  assert.deepEqual(socket.sent.at(-1), { type: "start_collecting" });
  assert.deepEqual(h.storage.saved(), { code: "HOST", role: "host", name: null, hostToken: "secret" });
});

test("display refresh restores through the session and retains stage sizing and controls", async () => {
  const storage = memoryStorage(JSON.stringify({ code: "SHOW", role: "display", name: null, hostToken: null }));
  const h = await loadClient({ storage });
  assert.equal(h.element("code").value, "SHOW");
  assert.equal(h.element("role").value, "display");
  const socket = h.sockets[0]; socket.open(); socket.message({ type: "joined", role: "display" });
  await h.editRole("host");
  socket.message(room("performing"));
  socket.message({ type: "slide_changed", index: 2, total_slides: 10,
    slide: { kind: "script", reader: "Ann", text: "Display lines" }, controller: "Ann" });
  assert.equal(h.document.body.classList.contains("stage-mode"), true);
  assert.match(h.element("slide").innerHTML, /Display lines/);
  assert.match(h.element("slide").style.fontSize, /^\d+px$/);
  assert.equal(h.element("hostPanel").classList.contains("hidden"), true);
  assert.equal(h.element("readerControls").classList.contains("hidden"), true);
  const sent = socket.sent.length;
  await h.document.dispatch("keydown", { key: "ArrowRight" });
  assert.equal(socket.sent.length, sent);
  await h.click("exitStage");
  assert.equal(h.document.body.classList.contains("stage-mode"), false);
  assert.equal(h.element("enterStage").classList.contains("hidden"), false);
  await h.click("enterStage");
  assert.equal(h.document.body.classList.contains("stage-mode"), true);
  socket.close();
  assert.equal(h.element("conn").textContent, "disconnected");
  assert.equal(h.element("roomPill").classList.contains("hidden"), true);
  assert.equal(storage.saved().code, "SHOW");
});

test("obsolete callbacks cannot change current UI, recovery or replacement sends", async () => {
  const h = await loadClient();
  h.element("code").value = "AAAA"; h.element("pname").value = "Ann";
  await h.click("joinBtn");
  const old = h.sockets[0]; old.open(); old.message({ type: "joined", role: "player", name: "Ann" });
  h.element("code").value = "BBBB"; h.element("pname").value = "Bo";
  await h.click("joinBtn");
  const current = h.sockets[1]; current.open(); current.message({ type: "joined", role: "player", name: "Bo" });
  const saved = h.storage.saved(), log = h.element("log").textContent;
  old.open(); old.message({ type: "error", code: "unknown_room", message: "stale" });
  old.message({ type: "assignment", partner: "Old", prompts: ["stale"] });
  old.message({ type: "slide_changed", index: 9 }); old.error(); old.close();
  assert.equal(h.element("conn").textContent, "connected");
  assert.equal(h.element("roomPill").textContent, "Room BBBB");
  assert.equal(h.element("roomPill").classList.contains("hidden"), false);
  assert.equal(h.element("log").textContent, log);
  assert.equal(current.sent.length, 1);
  assert.deepEqual(h.storage.saved(), saved);
  assert.deepEqual(h.alerts, []);
});
