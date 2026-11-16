import assert from "node:assert/strict";
import { test } from "node:test";
import { deferredRoomRequest, loadClient } from "./client-entry-harness.mjs";
import { deferred, measureSlide, memoryStorage } from "./client-adapters.mjs";

const room = (phase, submitted = false) => ({ type: "room_state", room: {
  code: "AAAA", phase, current_slide: 0, total_slides: 10,
  players: [{ name: "Ann", submitted }, { name: "Bo", submitted: false }],
} });

test("late creation cannot replace an acknowledged manual player connection", async () => {
  const response = deferred();
  const h = await loadClient({ fetch: () => response.promise });
  await h.editRole("host");
  const creation = h.click("createBtn");
  await h.editRole("player");
  h.element("code").value = "PLAY"; h.element("pname").value = "Ann";
  await h.click("joinBtn");
  const player = h.sockets[0]; player.open(); player.message({ type: "joined", role: "player", name: "Ann" });
  const operations = h.storage.operations.length, log = h.element("log").textContent;
  response.resolve({ ok: true, json: async () => ({ code: "HOST", host_token: "secret" }) });
  await creation;
  assert.equal(player.closed, false);
  assert.equal(h.sockets.length, 1);
  assert.equal(h.element("conn").textContent, "connected");
  assert.equal(h.element("code").value, "PLAY");
  assert.equal(h.element("hostCreds").textContent, "");
  assert.equal(h.element("hostCreds").classList.contains("hidden"), true);
  assert.equal(h.element("log").textContent, log);
  assert.equal(h.storage.operations.length, operations);
  assert.deepEqual(h.alerts, []);
});

function presentation(h) {
  const ids = ["conn", "roomPill", "hostCreds", "phasePill", "progressLine", "playerList",
    "promptFields", "partnerLine", "slide", "slideIdx", "log", "hostPanel", "playerPanel", "readerControls"];
  return { elements: ids.map((id) => { const el = h.element(id); return {
    id, text: el.textContent, html: el.innerHTML, hidden: el.classList.contains("hidden"),
  }; }), code: h.element("code").value, drafts: h.textareas().map((el) => el.value),
    signoff: h.document.getElementById("signoff")?.value,
    stage: h.document.body.classList.contains("stage-mode"),
    full: h.element("slidePanel").classList.contains("full"),
    font: h.element("slide").style.fontSize, alerts: [...h.alerts],
    operations: [...h.storage.operations],
    sockets: h.sockets.map((socket) => ({ closed: socket.closed, sent: [...socket.sent] })) };
}

async function joinPlayer(h) {
  h.element("code").value = "PLAY"; h.element("pname").value = "Ann";
  await h.click("joinBtn");
  const socket = h.sockets.at(-1); socket.open(); socket.message({ type: "joined", role: "player", name: "Ann" });
  socket.message(room("collecting"));
  socket.message({ type: "assignment", partner: "Bo", prompts: ["Tonight"] });
  h.textareas()[0].value = "Unsent news"; h.element("signoff").value = "Good night";
  return socket;
}

test("pending creation keeps draft submissions and accepted events usable and resets only on success", async () => {
  const request = deferredRoomRequest(), h = await loadClient({ fetch: request.fetch });
  const player = await joinPlayer(h);
  await h.editRole("host");
  const before = presentation(h), creation = h.click("createBtn");
  assert.deepEqual(presentation(h), before);
  request.respond(); await request.decoding.promise;
  assert.deepEqual(presentation(h), before, "headers alone do not replace the game");
  await h.click("submitBtn");
  assert.deepEqual(player.sent.at(-1), { type: "submit_responses", responses: ["Unsent news"], signoff: "Good night" });
  player.message({ type: "pong" });
  assert.match(h.element("log").textContent, /pong/);
  assert.equal(h.textareas()[0].value, "Unsent news");
  request.body.resolve({ code: " host ", host_token: "secret" });
  await creation;
  assert.equal(player.closed, true);
  assert.equal(h.sockets.length, 2);
  assert.equal(h.element("conn").textContent, "connecting…");
  assert.equal(h.element("code").value, "HOST");
  assert.equal(h.element("promptFields").innerHTML, "");
  assert.equal(h.element("phasePill").textContent, "—");
  assert.deepEqual(h.storage.saved(), { code: "PLAY", role: "player", name: "Ann", hostToken: null });
});

const httpFailures = {
  "request rejection": (request) => request.response.reject(new Error("secret request detail")),
  "unsuccessful HTTP response": (request) => request.respond(false),
  "body rejection": (request) => { request.respond(); request.body.reject(new Error("secret body detail")); },
  "non-object response": (request) => { request.respond(); request.body.resolve(null); },
  "array response": (request) => { request.respond(); request.body.resolve([]); },
  "invalid code": (request) => { request.respond(); request.body.resolve({ code: 9, host_token: "secret" }); },
  "invalid token": (request) => { request.respond(); request.body.resolve({ code: "HOST", host_token: " " }); },
};

for (const [failure, fail] of Object.entries(httpFailures)) {
  test(`current ${failure} reports once while preserving the active game`, async () => {
    const request = deferredRoomRequest(), h = await loadClient({ fetch: request.fetch });
    const player = await joinPlayer(h);
    player.message(room("performing"));
    player.message({ type: "slide_changed", index: 2, total_slides: 10, controller: "Ann",
      slide: { kind: "script", reader: "Ann", text: "Current performance" } });
    await h.editRole("host");
    const before = presentation(h), creation = h.click("createBtn");
    assert.deepEqual(presentation(h), before);
    fail(request); await creation;
    const after = presentation(h);
    assert.deepEqual(after, { ...before, alerts: ["create failed"], elements: before.elements.map((el) =>
      el.id === "log" ? { ...el, text: h.element("log").textContent } : el) });
    assert.equal(h.element("log").textContent.split("create failed").length, 2);
    assert.doesNotMatch(h.element("log").textContent, /secret/);
    await h.click("rNext");
    assert.deepEqual(player.sent.at(-1), { type: "advance_slide" });
    player.message({ type: "pong" });
    assert.match(h.element("log").textContent, /pong/);
  });
}

test("synchronous fetch errors are handled by the delegated create click", async () => {
  const h = await loadClient({ fetch() { throw new Error("private failure"); } });
  await h.editRole("host");
  await h.click("createBtn");
  assert.deepEqual(h.alerts, ["create failed"]);
  assert.equal(h.sockets.length, 0);
  assert.equal(h.storage.operations.length, 1, "only initial restoration accessed storage");
  assert.equal(h.element("hostCreds").textContent, "");
});

test("stale HTTP responses and JSON bodies cannot change newer player or display presentation", async () => {
  for (const role of ["player", "display"]) for (const decodedHeaders of [false, true]) {
    for (const acknowledged of [false, true]) for (const result of ["success", "malformed", "rejection"]) {
      const request = deferredRoomRequest(), h = await loadClient({ fetch: request.fetch });
      await h.editRole("host");
      const creation = h.click("createBtn");
      if (decodedHeaders) { request.respond(); await request.decoding.promise; }
      await h.editRole(role);
      h.element("code").value = "NEW"; h.element("pname").value = "Ann";
      await h.click("joinBtn");
      const socket = h.sockets[0];
      if (acknowledged) { socket.open(); socket.message({ type: "joined", role, name: role === "player" ? "Ann" : null }); }
      const before = presentation(h);
      if (result === "rejection") {
        (decodedHeaders ? request.body : request.response).reject(new Error("stale private detail"));
      } else {
        request.respond();
        request.body.resolve(result === "success" ? { code: "HOST", host_token: "secret" } : { code: "HOST", host_token: " " });
      }
      await creation;
      assert.deepEqual(presentation(h), before, `${role}, headers=${decodedHeaders}, joined=${acknowledged}, ${result}`);
    }
  }
});

test("two creates obey invocation order with independent HTTP headers and JSON bodies", async () => {
  for (const olderFirst of [false, true]) for (const decodedHeaders of [false, true]) {
    const older = deferredRoomRequest(), newer = deferredRoomRequest(), requests = [older, newer];
    const h = await loadClient({ fetch: () => requests.shift().fetch() });
    await h.editRole("host");
    const first = h.click("createBtn");
    if (decodedHeaders) { older.respond(); await older.decoding.promise; }
    const second = h.click("createBtn");
    const finishOlder = async () => {
      older.respond(); older.body.resolve({ code: "OLD", host_token: "old-secret" }); await first;
    };
    if (olderFirst) {
      const before = presentation(h); await finishOlder(); assert.deepEqual(presentation(h), before);
    }
    newer.respond(); newer.body.resolve({ code: "NEW", host_token: "new-secret" }); await second;
    const socket = h.sockets[0]; socket.open(); socket.message({ type: "joined", role: "host" });
    const before = presentation(h);
    if (!olderFirst) { await finishOlder(); assert.deepEqual(presentation(h), before); }
    assert.equal(h.sockets.length, 1);
    assert.equal(h.element("code").value, "NEW");
    assert.deepEqual(socket.sent[0], { type: "join_room", code: "NEW", role: "host", name: null, token: "new-secret" });
    assert.deepEqual(h.storage.saved(), { code: "NEW", role: "host", name: null, hostToken: "new-secret" });
  }
});

test("accepted host notice renders normalized code as text and later edits remain pending", async () => {
  const h = await loadClient({ fetch: async () => ({ ok: true,
    json: async () => ({ code: " <b>host</b> ", host_token: "private-token" }) }) });
  await h.editRole("host"); await h.click("createBtn");
  assert.equal(h.element("code").value, "<B>HOST</B>");
  assert.equal(h.element("hostCreds").textContent, "Room <B>HOST</B>. Players join with this code. Host token held in this tab.");
  assert.equal(h.element("hostCreds").innerHTML, "");
  assert.equal(h.element("hostCreds").classList.contains("hidden"), false);
  assert.doesNotMatch(h.element("log").textContent, /private-token|REST|host_token/);
  h.element("code").value = "PENDING";
  const host = h.sockets[0]; host.open(); host.message({ type: "joined", role: "host" });
  assert.equal(h.element("code").value, "PENDING");
  assert.match(h.element("hostCreds").textContent, /<B>HOST<\/B>/);
  assert.doesNotMatch(h.element("hostCreds").textContent, /private-token|host_token|hostToken/);
  await h.editRole("display");
  await h.click("joinBtn");
  assert.equal(h.element("hostCreds").classList.contains("hidden"), true);
  assert.equal(h.element("hostCreds").textContent, "");
});

test("restored hosts get the accepted host notice without a creation request", async () => {
  const saved = { code: " host ", role: "host", name: null, hostToken: "private-token" };
  const h = await loadClient({ storage: memoryStorage(JSON.stringify(saved)),
    fetch() { throw new Error("restoration must not create a room"); } });
  assert.equal(h.element("code").value, "HOST");
  assert.equal(h.element("hostCreds").classList.contains("hidden"), false);
  assert.equal(h.element("hostCreds").textContent, "Room HOST. Players join with this code. Host token held in this tab.");
  h.element("code").value = "PENDING";
  h.sockets[0].open(); h.sockets[0].message({ type: "joined", role: "host" });
  assert.equal(h.element("code").value, "PENDING");
  assert.doesNotMatch(h.element("hostCreds").textContent, /private-token/);
  assert.deepEqual(h.alerts, []);
});

test("one player connection acquires fresh assignments across consecutive rounds", async () => {
  const h = await loadClient();
  h.element("code").value = "AAAA"; h.element("pname").value = "Ann";
  await h.click("joinBtn");
  const socket = h.sockets[0]; socket.open();
  socket.message({ type: "joined", role: "player", name: "Ann" });
  for (const [partner, prompts] of [["Bo", ["Tonight"]], ["Cy", ["Weather"]], ["Cy", ["Weather"]]]) {
    const joins = socket.sent.length;
    socket.message(room("collecting"));
    assert.equal(socket.sent.length, joins + 1, "each round requests its assignment");
    socket.message({ type: "assignment", partner, prompts });
    assert.match(h.element("partnerLine").innerHTML, new RegExp(partner));
    assert.equal(h.textareas()[0].value, "");
    h.textareas()[0].value = "Round draft"; h.element("signoff").value = "Good night";
    socket.message(room("collecting", true));
    socket.message(room("performing", true)); socket.message(room("archived", true));
    socket.message({ ...room("lobby"), room: { ...room("lobby").room, players: [] } });
    assert.equal(h.element("promptFields").innerHTML, "", "lobby clears the previous draft");
    assert.equal(h.element("submitBtn").classList.contains("hidden"), true);
    assert.equal(h.element("submittedTag").classList.contains("hidden"), true);
    socket.message(room("lobby"));
  }
  assert.equal(h.sockets.length, 1);
});

test("duplicate assignment delivery preserves typing and a confirmed form's manual reopening", async () => {
  const h = await loadClient();
  h.element("code").value = "AAAA"; h.element("pname").value = "Ann";
  await h.click("joinBtn");
  const socket = h.sockets[0]; socket.open();
  socket.message({ type: "joined", role: "player", name: "Ann" });
  socket.message(room("collecting"));
  const assignment = { type: "assignment", partner: "Bo", prompts: ["Tonight", "Weather"] };
  socket.message(assignment);
  h.textareas()[0].value = "News"; h.textareas()[1].value = "Rain";
  h.element("signoff").value = "Good night";
  socket.message(assignment);
  assert.deepEqual(h.textareas().map((field) => field.value), ["News", "Rain"]);
  assert.equal(h.element("signoff").value, "Good night");
  await h.click("submitBtn");
  assert.equal(h.element("promptsBody").classList.contains("hidden"), false);
  assert.equal(h.element("submittedTag").classList.contains("hidden"), true);
  socket.message({ type: "error", code: "invalid_input", message: "rejected" });
  assert.deepEqual(h.alerts, ["invalid_input: rejected"]);
  assert.equal(h.element("promptsBody").classList.contains("hidden"), false);
  assert.equal(h.element("submittedTag").classList.contains("hidden"), true);
  socket.message({ type: "submission_progress", submitted: 1, total: 2, players: room("collecting", true).room.players });
  assert.equal(h.element("promptsBody").classList.contains("hidden"), true);
  await h.click("promptsToggle");
  socket.message({ type: "joined", role: "player", name: "Ann" });
  socket.message(assignment); socket.message(room("collecting", true));
  socket.message({ type: "submission_progress", submitted: 1, total: 2, players: room("collecting", true).room.players });
  assert.equal(h.element("promptsBody").classList.contains("hidden"), false);
  assert.equal(h.element("submittedTag").classList.contains("hidden"), false);
  assert.equal(h.element("signoff").value, "Good night");
  await h.click("submitBtn");
  assert.deepEqual(socket.sent.at(-1), { type: "submit_responses", responses: ["News", "Rain"], signoff: "Good night" });
  socket.message(room("collecting"));
  assert.equal(h.element("promptsBody").classList.contains("hidden"), false);
  assert.equal(h.element("submittedTag").classList.contains("hidden"), true);
});

test("zero-prompt assignments remain present and replacement clears participation before joining", async () => {
  const h = await loadClient();
  h.element("code").value = "AAAA"; h.element("pname").value = "Ann";
  await h.click("joinBtn");
  const socket = h.sockets[0]; socket.open();
  socket.message({ type: "joined", role: "player", name: "Ann" });
  socket.message(room("collecting"));
  socket.message({ type: "assignment", partner: "Bo", prompts: [] });
  h.element("signoff").value = "Stay classy";
  socket.message(room("collecting"));
  assert.equal(socket.sent.length, 2);
  await h.click("submitBtn");
  assert.deepEqual(socket.sent.at(-1), { type: "submit_responses", responses: [], signoff: "Stay classy" });
  socket.message(room("collecting", true));
  h.element("pname").value = "Bo";
  await h.click("joinBtn");
  assert.equal(h.element("promptFields").innerHTML, "");
  assert.equal(h.document.getElementById("signoff"), null);
  assert.equal(h.element("submitBtn").classList.contains("hidden"), true);
  assert.equal(h.element("submittedTag").classList.contains("hidden"), true);
  await h.click("submitBtn");
  const replacement = h.sockets[1]; replacement.open();
  replacement.message({ type: "joined", role: "player", name: "Bo" });
  replacement.message(room("collecting"));
  assert.deepEqual(replacement.sent.at(-1), { type: "join_room", code: "AAAA", role: "player", name: "Bo", token: null });
  replacement.message({ type: "assignment", partner: "Ann", prompts: [] });
  assert.equal(h.element("signoff").value, "");
  socket.message({ type: "assignment", partner: "Stale", prompts: ["Old"] });
  assert.match(h.element("partnerLine").innerHTML, /Ann/);
  assert.equal(h.textareas().length, 0);
});

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
  socket.message({ type: "submission_progress", submitted: 1, total: 2,
    players: [{ name: "Ann", submitted: false }, { name: "Bo", submitted: true }] });
  assert.equal(h.element("submittedTag").classList.contains("hidden"), true, "pending name Bo cannot drive Ann's status");
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
  socket.message({ type: "slide_changed", index: 3, total_slides: 10, controller: "Bo",
    slide: { kind: "script", reader: "Bo", text: "Bo's private lines" } });
  socket.message(room("performing", true));
  assert.equal(h.element("readerControls").classList.contains("hidden"), true);
  assert.equal(h.element("slidePanel").classList.contains("full"), false);
  assert.match(h.element("slide").innerHTML, /Bo.*is reading/);
  assert.doesNotMatch(h.element("slide").innerHTML, /private lines/);
  const afterHandoff = socket.sent.length;
  await h.click("rNext"); await h.click("rPrev");
  await h.document.dispatch("keydown", { key: "ArrowRight" });
  assert.equal(socket.sent.length, afterHandoff);
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
  const beforeSlides = socket.sent.length;
  socket.message({ type: "game_started", total_slides: 10 });
  socket.message({ type: "slide_changed", index: 0, total_slides: 10, controller: "Ann", slide: { kind: "rules" } });
  socket.message(room("performing"));
  await h.editRole("player");
  await h.click("nextBtn"); await h.click("prevBtn");
  await h.document.dispatch("keydown", { key: "ArrowRight" });
  await h.document.dispatch("keydown", { key: "ArrowLeft" });
  assert.deepEqual(socket.sent.slice(beforeSlides), [
    { type: "advance_slide" }, { type: "prev_slide" }, { type: "advance_slide" }, { type: "prev_slide" },
  ]);
  socket.close();
  await h.click("nextBtn"); await h.click("prevBtn");
  await h.document.dispatch("keydown", { key: "ArrowRight" });
  assert.equal(socket.sent.length, beforeSlides + 4);
  assert.deepEqual(h.storage.saved(), { code: "HOST", role: "host", name: null, hostToken: "secret" });
});

test("display refresh restores through the session and retains stage sizing and controls", async () => {
  const storage = memoryStorage(JSON.stringify({ code: "SHOW", role: "display", name: null, hostToken: null }));
  const h = await loadClient({ storage });
  assert.equal(h.element("code").value, "SHOW");
  assert.equal(h.element("role").value, "display");
  const { bounds } = measureSlide(h.element("slide"), { height: 240 });
  const socket = h.sockets[0]; socket.open(); socket.message({ type: "joined", role: "display" });
  await h.editRole("host");
  socket.message(room("performing"));
  socket.message({ type: "slide_changed", index: 2, total_slides: 10,
    slide: { kind: "script", reader: "Ann", text: "Display lines" }, controller: "Ann" });
  assert.equal(h.document.body.classList.contains("stage-mode"), true);
  assert.match(h.element("slide").innerHTML, /Display lines/);
  assert.equal(h.element("slide").style.fontSize, "120px");
  bounds.height = 360;
  await h.window.dispatch("resize");
  assert.equal(h.element("slide").style.fontSize, "180px");
  assert.equal(h.element("hostPanel").classList.contains("hidden"), true);
  assert.equal(h.element("readerControls").classList.contains("hidden"), true);
  const sent = socket.sent.length;
  await h.document.dispatch("keydown", { key: "ArrowRight" });
  assert.equal(socket.sent.length, sent);
  await h.click("exitStage");
  assert.equal(h.document.body.classList.contains("stage-mode"), false);
  assert.equal(h.element("enterStage").classList.contains("hidden"), false);
  socket.message({ type: "slide_changed", index: 3, total_slides: 10,
    slide: { kind: "credits" }, controller: null });
  socket.message(room("performing")); socket.message(room("performing"));
  assert.equal(h.document.body.classList.contains("stage-mode"), false);
  assert.equal(h.element("slide").style.fontSize, "");
  await h.click("enterStage");
  assert.equal(h.document.body.classList.contains("stage-mode"), true);
  socket.message(room("archived")); socket.message(room("lobby")); socket.message(room("collecting"));
  socket.message({ type: "game_started", total_slides: 10 });
  socket.message({ type: "slide_changed", index: 0, total_slides: 10, slide: { kind: "rules" }, controller: "Ann" });
  assert.equal(h.document.body.classList.contains("stage-mode"), true);
  socket.message(room("performing"));
  socket.close();
  assert.equal(h.element("conn").textContent, "disconnected");
  assert.equal(h.element("roomPill").classList.contains("hidden"), true);
  assert.equal(storage.saved().code, "SHOW");
  bounds.height = 200;
  await h.window.dispatch("resize");
  assert.equal(h.element("slide").style.fontSize, "100px");
  await h.click("exitStage"); await h.click("enterStage");
  assert.equal(h.document.body.classList.contains("stage-mode"), true);
});

test("obsolete callbacks cannot change current UI, recovery or replacement sends", async () => {
  const h = await loadClient();
  h.element("code").value = "AAAA"; h.element("pname").value = "Ann";
  await h.click("joinBtn");
  const old = h.sockets[0]; old.open(); old.message({ type: "joined", role: "player", name: "Ann" });
  old.message(room("performing"));
  old.message({ type: "slide_changed", index: 2, total_slides: 10, controller: "Ann",
    slide: { kind: "script", reader: "Ann", text: "Previous performance" } });
  h.element("code").value = "BBBB"; h.element("pname").value = "Bo";
  await h.click("joinBtn");
  assert.match(h.element("slide").textContent, /carousel will appear/);
  assert.equal(h.element("readerControls").classList.contains("hidden"), true);
  assert.equal(h.element("slidePanel").classList.contains("full"), false);
  old.message({ type: "game_started", total_slides: 99 });
  old.message(room("performing"));
  assert.equal(h.element("slideIdx").textContent, "—");
  const current = h.sockets[1]; current.open(); current.message({ type: "joined", role: "player", name: "Bo" });
  const saved = h.storage.saved(), log = h.element("log").textContent;
  old.open(); old.message({ type: "error", code: "unknown_room", message: "stale" });
  old.message({ type: "assignment", partner: "Old", prompts: ["stale"] });
  old.message({ type: "slide_changed", index: 9, total_slides: 10, controller: "Bo",
    slide: { kind: "script", reader: "Bo", text: "Obsolete" } }); old.error(); old.close();
  assert.match(h.element("slide").textContent, /carousel will appear/);
  assert.equal(h.element("slideIdx").textContent, "—");
  await h.click("rNext");
  assert.equal(h.element("conn").textContent, "connected");
  assert.equal(h.element("roomPill").textContent, "Room BBBB");
  assert.equal(h.element("roomPill").classList.contains("hidden"), false);
  assert.equal(h.element("log").textContent, log);
  assert.equal(current.sent.length, 1);
  assert.deepEqual(h.storage.saved(), saved);
  assert.deepEqual(h.alerts, []);
});
