import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { createPlayerRoundParticipation } from "../static/player-round-participation.mjs";
import { controlledDocument } from "./client-adapters.mjs";

const html = await readFile(new URL("../static/index.html", import.meta.url), "utf8");
const player = { state: "joined", acknowledged: true, code: "AAAA", role: "player", name: "Ann" };
const players = (submitted = false) => [{ name: "Ann", submitted }, { name: "Bo", submitted: false }];
const room = (phase, roster = players()) => ({ type: "room_state", room: { phase, players: roster } });
const assignment = (partner = "Bo", prompts = ["Tonight", "Weather"]) => ({ type: "assignment", partner, prompts });
const progress = (submitted) => ({ type: "submission_progress", players: players(submitted) });

function setup(results = []) {
  const dom = controlledDocument(html), sent = [];
  const participation = createPlayerRoundParticipation({ document: dom.document,
    send(intent) { sent.push(intent); return results.shift() ?? true; } });
  participation.updateSession(player);
  return { ...dom, ...participation, sent,
    hidden: (id) => dom.element(id).classList.contains("hidden") };
}

test("assignment acquisition requires the acknowledged collecting player on the roster", async () => {
  const h = setup();
  for (const view of [
    { ...player, state: "idle", acknowledged: false },
    { ...player, state: "awaiting_joined", acknowledged: false },
    { ...player, acknowledged: false },
    { ...player, role: "host", name: null },
    { ...player, role: "display", name: null },
  ]) {
    h.updateSession(view); h.handle(room("collecting")); h.handle(assignment());
    await h.click("submitBtn");
    assert.equal(h.element("promptFields").innerHTML, "");
  }
  h.updateSession(player);
  for (const phase of ["lobby", "performing", "archived"]) h.handle(room(phase));
  h.handle(room("collecting", [{ name: "Bo", submitted: false }]));
  h.handle(assignment());
  // A received assignment is present even without an earlier replay request.
  assert.equal(h.hidden("submitBtn"), false);
  assert.deepEqual(h.sent, []);
});

test("failed replay sends retry on a later snapshot; successful replay never loops", () => {
  const h = setup([false, true]);
  h.handle(room("collecting"));
  assert.deepEqual(h.sent, [{ type: "join_room" }]);
  h.handle(progress(false));
  assert.equal(h.sent.length, 1);
  h.handle(room("collecting"));
  assert.deepEqual(h.sent, [{ type: "join_room" }, { type: "join_room" }]);
  h.updateSession(player);
  for (let i = 0; i < 5; i++) h.handle(room("collecting"));
  assert.equal(h.sent.length, 2);
});

test("duplicate assignments preserve drafts, acknowledgement and a manually reopened form", async () => {
  const h = setup(); h.handle(room("collecting")); h.handle(assignment());
  h.textareas()[0].value = "Headlines"; h.textareas()[1].value = "Rain";
  h.element("signoff").value = "Good night";
  h.handle(assignment());
  assert.deepEqual(h.textareas().map((field) => field.value), ["Headlines", "Rain"]);
  assert.equal(h.element("signoff").value, "Good night");
  h.handle(progress(true));
  assert.equal(h.hidden("promptsBody"), true);
  await h.click("promptsToggle");
  h.handle(assignment()); h.handle(progress(true)); h.handle(room("collecting", players(true)));
  h.updateSession(player);
  assert.equal(h.hidden("promptsBody"), false);
  assert.equal(h.hidden("submittedTag"), false);
  assert.equal(h.element("signoff").value, "Good night");
  h.textareas().reverse();
  await h.click("submitBtn");
  assert.deepEqual(h.sent.at(-1), { type: "submit_responses", responses: ["Headlines", "Rain"], signoff: "Good night" });
});

test("a changed partner or ordered prompt list replaces the form", () => {
  const h = setup(); h.handle(room("collecting")); h.handle(assignment());
  for (const next of [assignment("Cy"), assignment("Cy", ["Weather", "Tonight"]), assignment("Cy", ["Weather"])]) {
    h.textareas()[0].value = "Old draft"; h.element("signoff").value = "Old sign-off";
    h.handle(next);
    assert.equal(h.textareas()[0].value, "");
    assert.equal(h.element("signoff").value, "");
    assert.equal(h.textareas().length, next.prompts.length);
  }
});

test("a zero-prompt assignment is present and submits a sign-off with no responses", async () => {
  const h = setup(); h.handle(room("collecting", [])); h.handle(assignment("Bo", []));
  h.element("signoff").value = "Stay classy";
  h.handle(room("collecting")); h.handle(assignment("Bo", []));
  assert.equal(h.sent.length, 0, "an assignment suppresses replay even without a prior successful send");
  assert.equal(h.hidden("submitBtn"), false);
  assert.equal(h.element("signoff").value, "Stay classy");
  await h.click("submitBtn");
  assert.deepEqual(h.sent.at(-1), { type: "submit_responses", responses: [], signoff: "Stay classy" });
});

test("clicks and rejections do not acknowledge; server statuses control collapse and reopening", async () => {
  const h = setup(); h.handle(room("collecting")); h.handle(assignment());
  await h.click("submitBtn");
  h.handle({ type: "error", code: "invalid_input", message: "rejected" });
  assert.equal(h.hidden("submittedTag"), true);
  assert.equal(h.hidden("promptsBody"), false);
  h.handle(room("collecting", players(true)));
  assert.equal(h.hidden("submittedTag"), false);
  assert.equal(h.hidden("promptsBody"), true);
  h.handle(assignment("Cy"));
  assert.equal(h.hidden("promptsBody"), true, "new assignments honor confirmed submission");
  h.handle(progress(false));
  assert.equal(h.hidden("submittedTag"), true);
  assert.equal(h.hidden("promptsBody"), false);
  h.handle(progress(true));
  assert.equal(h.hidden("promptsBody"), true);
  h.handle(room("collecting", []));
  assert.equal(h.hidden("submittedTag"), true);
  assert.equal(h.hidden("promptsBody"), false);
});

test("two new lobbies clear round participation, including equal assignments in a later round", async () => {
  const h = setup();
  for (const next of [assignment(), assignment("Cy", ["Next"]), assignment("Cy", ["Next"])]) {
    const sent = h.sent.length;
    h.handle(room("collecting"));
    assert.equal(h.sent.length, sent + 1);
    h.handle(next);
    assert.equal(h.textareas()[0].value, "");
    h.textareas()[0].value = "Old draft"; h.element("signoff").value = "Old sign-off";
    h.handle(progress(true)); await h.click("promptsToggle");
    h.handle(room("performing", players(true))); h.handle(room("archived", players(true)));
    h.handle(room("lobby", [])); h.handle(room("lobby"));
    assert.equal(h.element("promptFields").innerHTML, "");
    assert.equal(h.document.getElementById("signoff"), null);
    assert.equal(h.hidden("submitBtn"), true);
    assert.equal(h.hidden("submittedTag"), true);
    assert.equal(h.hidden("promptsToggle"), true);
    assert.equal(h.hidden("promptsBody"), false);
    await h.click("submitBtn");
    assert.equal(h.sent.length, sent + 1);
  }
});

test("connecting clears participation; matching session updates preserve it", async () => {
  const h = setup(); h.handle(room("collecting")); h.handle(assignment());
  h.textareas()[0].value = "News"; h.element("signoff").value = "Good night";
  h.updateSession(player); h.handle(assignment());
  assert.equal(h.textareas()[0].value, "News");
  h.updateSession({ ...player, state: "connecting", acknowledged: false });
  assert.equal(h.element("promptFields").innerHTML, "");
  assert.equal(h.hidden("submitBtn"), true);
  assert.equal(h.hidden("submittedTag"), true);
  await h.click("submitBtn");
  assert.deepEqual(h.sent, [{ type: "join_room" }]);
  h.updateSession(player); h.handle(room("collecting")); h.handle(assignment());
  assert.deepEqual(h.sent, [{ type: "join_room" }, { type: "join_room" }]);
  assert.equal(h.textareas()[0].value, "");
  h.updateSession({ ...player, state: "disconnected" });
  await h.click("submitBtn");
  assert.equal(h.sent.length, 2);
});

test("partner and prompts stay escaped and retain the existing field limits", () => {
  const h = setup(); h.handle(room("collecting"));
  h.handle(assignment('<img src="x"> & \'', ["<script> & \"", ""]));
  assert.match(h.element("partnerLine").innerHTML, /&lt;img src=&quot;x&quot;&gt; &amp; &#39;/);
  assert.match(h.element("promptFields").innerHTML, /&lt;script&gt; &amp; &quot;/);
  assert.match(h.element("promptFields").innerHTML, /<label>Prompt 2<\/label>/);
  assert.equal([...h.element("promptFields").innerHTML.matchAll(/maxlength="200"/g)].length, 3);
});
