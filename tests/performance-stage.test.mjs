import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { createPerformanceStage } from "../static/performance-stage.mjs";
import { ControlledElement, controlledDocument, measureSlide } from "./client-adapters.mjs";

const html = await readFile(new URL("../static/index.html", import.meta.url), "utf8");
const view = (role = "player", name = "Ann", state = "joined") =>
  ({ state, acknowledged: state === "joined", code: "SHOW", role, name });
const room = (phase = "performing", index = 0) => ({ type: "room_state", room: {
  phase, current_slide: index, total_slides: 10,
} });
const slide = (content = { kind: "rules" }, controller = "Ann", index = 0) =>
  ({ type: "slide_changed", index, total_slides: 10, slide: content, controller });

function setup(session = view()) {
  const dom = controlledDocument(html), window = new ControlledElement(), sent = [];
  const stage = createPerformanceStage({ document: dom.document, window,
    send: (intent) => { sent.push(intent); return true; } });
  stage.updateSession(session);
  return { ...dom, window, sent, stage,
    hidden: (id) => dom.element(id).classList.contains("hidden"),
    wide: () => dom.element("slidePanel").classList.contains("full"),
    inStage: () => dom.document.body.classList.contains("stage-mode"),
  };
}

test("rules use the acknowledged controller without inventing a spoken reader", async () => {
  const h = setup();
  h.stage.handle(room("collecting"));
  h.stage.handle({ type: "game_started", total_slides: 10 });
  assert.equal(h.element("slideIdx").textContent, "0 / 10");
  h.stage.handle(slide());
  assert.match(h.element("slide").innerHTML, /The rules/);
  assert.doesNotMatch(h.element("slide").innerHTML, /YOUR TURN TO READ|is reading/);
  assert.equal(h.hidden("readerControls"), false);
  await h.click("rNext");
  assert.deepEqual(h.sent, [{ type: "advance_slide" }]);
  h.stage.handle(room());
  assert.equal(h.wide(), true);
  assert.equal(h.hidden("readerControls"), false);
});

const cases = [
  [{ kind: "rules" }, "Ann", null,
    "<div><strong>The rules</strong><br>When it's your turn to read, DO NOT LAUGH.<br>The host advances the slides.</div>"],
  [{ kind: "presenter_intro", presenter: "Ann", teleprompter: "Bo", first: true }, "Ann", null,
    "To start us off, <strong>Ann</strong> presents, with Bo on teleprompter."],
  [{ kind: "presenter_intro", presenter: "Bo", teleprompter: "Ann", first: false }, "Ann", null,
    "Next up: <strong>Bo</strong> reads, with Ann on teleprompter."],
  [{ kind: "greeting", presenter: "Ann" }, "Ann", "Ann",
    "Good evening! I'm <strong>Ann</strong>, and this is today's Channel 0 News."],
  [{ kind: "script", reader: "Bo", text: "News & weather" }, "Ann", "Bo", "News &amp; weather"],
  [{ kind: "signoff", presenter: "Ann", text: "Stay kind" }, "Ann", "Ann",
    "That's all for today. This is Ann signing off, and remember: Stay kind"],
  [{ kind: "credits" }, null, null, "<strong>Thanks for playing!</strong>"],
  [{ kind: "blank" }, null, null, ""],
];

for (const [content, controller, reader, expected] of cases) {
  for (const [role, name] of [["host", null], ["display", null], ["player", "Ann"], ["player", "Cy"]]) {
    test(`${role}${name ? " " + name : ""} presents ${content.kind}${content.first === false ? " (next)" : ""}`, () => {
      const h = setup(view(role, name));
      h.stage.handle(room());
      h.stage.handle(slide(content, controller, 3));
      const isController = role === "player" && name === controller;
      let expectedHtml = expected;
      if (role === "display" || isController) expectedHtml = `<div class="stage-content">${expected}</div>`;
      if (isController && reader) expectedHtml = '<div class="badge">YOUR TURN TO READ 🎤</div>' + expectedHtml;
      if (role === "host" && reader) expectedHtml = `<p class="muted" style="margin:0 0 8px">🎤 ${reader} is reading</p>` + expected;
      if (role === "player" && !isController && reader)
        expectedHtml = `<div class="reading-now">🎤 <strong>${reader}</strong> is reading…</div>`;
      assert.equal(h.element("slide").innerHTML, expectedHtml);
      assert.equal(h.element("slideIdx").textContent, "3 / 10");
      assert.equal(h.element("slide").classList.contains("muted"), false);
      assert.equal(h.hidden("readerControls"), !isController);
      assert.equal(h.wide(), role !== "player" || isController);
      assert.equal(h.inStage(), role === "display");
    });
  }
}

test("all dynamic slide text and reading banners escape HTML", () => {
  const unsafe = `<>&"'`, escaped = "&lt;&gt;&amp;&quot;&#39;";
  for (const role of ["host", "display", "player"]) {
    const h = setup(view(role, "Ann"));
    h.stage.handle(room());
    for (const content of [
      { kind: "presenter_intro", presenter: unsafe, teleprompter: unsafe, first: true },
      { kind: "presenter_intro", presenter: unsafe, teleprompter: unsafe, first: false },
      { kind: "greeting", presenter: unsafe },
      { kind: "script", reader: unsafe, text: unsafe },
      { kind: "signoff", presenter: unsafe, text: unsafe },
    ]) {
      for (const controller of ["Ann", "Other"]) {
        h.stage.handle(slide(content, controller));
        assert.ok(h.element("slide").innerHTML.includes(escaped));
        assert.ok(!h.element("slide").innerHTML.includes(unsafe));
      }
    }
  }
});

test("start and navigation preserve slides delivered ahead of room snapshots", () => {
  for (const role of ["host", "player", "display"]) {
    for (const hasSnapshot of [false, true]) {
      const h = setup(view(role));
      if (hasSnapshot) h.stage.handle(room("collecting"));
      h.stage.handle({ type: "game_started", total_slides: 10 });
      h.stage.handle(slide());
      assert.match(h.element("slide").innerHTML, /The rules/);
      h.stage.handle(room());
      assert.match(h.element("slide").innerHTML, /The rules/);
      h.stage.handle(slide({ kind: "script", reader: "Ann", text: "Live news" }, "Ann", 2));
      h.stage.handle(room("performing", 2));
      assert.match(h.element("slide").innerHTML, /Live news/);
      assert.equal(h.element("slideIdx").textContent, "2 / 10");
      assert.equal(h.hidden("readerControls"), role !== "player");
    }
  }
});

test("performance join accepts the current slide after room state and progress", () => {
  const h = setup(view("display", null));
  h.stage.handle(room("performing", 4));
  assert.equal(h.element("slideIdx").textContent, "4 / 10");
  assert.equal(h.inStage(), false, "a snapshot alone does not enter stage mode");
  h.stage.handle({ type: "submission_progress", submitted: 2, total: 2, players: [] });
  h.stage.handle(slide({ kind: "greeting", presenter: "Bo" }, "Bo", 4));
  assert.match(h.element("slide").innerHTML, /Good evening/);
  assert.equal(h.inStage(), true);
  const content = h.element("slide").innerHTML;
  for (const type of ["pong", "assignment", "error", "unrelated"]) h.stage.handle({ type });
  assert.equal(h.element("slide").innerHTML, content);
  assert.equal(h.element("slideIdx").textContent, "4 / 10");
});

test("display dismissal survives slides and repeated snapshots, and the next round re-arms entry", async () => {
  const h = setup(view("display", null));
  h.stage.handle(room()); h.stage.handle(slide());
  await h.click("exitStage");
  for (let index = 1; index <= 3; index++) {
    h.stage.handle(slide({ kind: "greeting", presenter: "Ann" }, "Ann", index));
    h.stage.handle(room("performing", index));
    h.stage.handle(room("performing", index));
    assert.equal(h.inStage(), false);
    assert.equal(h.hidden("enterStage"), false);
    assert.equal(h.element("slide").style.fontSize, "");
  }
  await h.click("enterStage");
  assert.equal(h.inStage(), true);
  assert.equal(h.hidden("exitStage"), false);
  assert.equal(h.hidden("enterStage"), true);
  await h.click("exitStage");
  for (const phase of ["archived", "lobby", "collecting"]) {
    const content = h.element("slide").innerHTML;
    h.stage.handle(room(phase, 0));
    assert.equal(h.inStage(), false);
    assert.equal(h.wide(), false);
    assert.equal(h.hidden("readerControls"), true);
    assert.equal(h.hidden("enterStage"), true);
    assert.equal(h.element("slide").innerHTML, content);
    assert.equal(h.element("slideIdx").textContent, "0 / 10");
  }
  h.stage.handle({ type: "game_started", total_slides: 10 });
  h.stage.handle(slide());
  assert.equal(h.inStage(), true);
  h.stage.handle(room());
  assert.equal(h.wide(), true);
  h.stage.handle(room("archived"));
  assert.equal(h.inStage(), false);
  assert.equal(h.element("slide").style.fontSize, "");
});

test("all carousel inputs use acknowledged identity, follow handoffs and clear on leaving performance", async () => {
  const h = setup();
  h.element("role").value = "host"; h.element("pname").value = "Bo";
  const inputs = async () => {
    for (const id of ["nextBtn", "prevBtn", "rNext", "rPrev"]) await h.click(id);
    await h.document.dispatch("keydown", { key: "ArrowRight" });
    await h.document.dispatch("keydown", { key: "ArrowLeft" });
    await h.document.dispatch("keydown", { key: "Enter" });
  };
  const intents = ["advance_slide", "prev_slide", "advance_slide", "prev_slide", "advance_slide", "prev_slide"]
    .map((type) => ({ type }));
  h.stage.handle(room());
  h.stage.handle(slide({ kind: "script", reader: "Bo", text: "Ann's controls" }, "Ann"));
  await inputs();
  assert.deepEqual(h.sent, intents);
  for (const [content, controller] of [
    [{ kind: "script", reader: "Ann", text: "Bo's controls" }, "Bo"],
    [{ kind: "credits" }, null], [{ kind: "blank" }, null],
  ]) {
    h.stage.handle(slide(content, controller));
    assert.equal(h.hidden("readerControls"), true);
    assert.equal(h.wide(), false);
    await inputs();
    assert.deepEqual(h.sent, intents);
  }
  const missingController = slide({ kind: "blank" });
  delete missingController.controller;
  h.stage.handle(missingController);
  await inputs();
  assert.deepEqual(h.sent, intents);
  h.stage.handle(slide()); h.stage.handle(room("collecting"));
  await inputs();
  assert.deepEqual(h.sent, intents);
  assert.equal(h.hidden("readerControls"), true);
  assert.equal(h.wide(), false);
  h.stage.updateSession(view("host", null));
  await inputs();
  assert.deepEqual(h.sent, [...intents, ...intents], "host phase enforcement stays on the server");
  h.stage.updateSession(view("display", null));
  await inputs();
  assert.deepEqual(h.sent, [...intents, ...intents]);
});

for (const state of ["idle", "connecting", "awaiting_joined", "failed", "disconnected"]) {
  test(`${state} sessions emit no carousel intents even with a recording sender`, async () => {
    for (const role of ["host", "player", "display"]) {
      const h = setup(view(role));
      h.stage.handle(room()); h.stage.handle(slide());
      h.stage.updateSession({ ...view(role), state });
      for (const id of ["nextBtn", "prevBtn", "rNext", "rPrev"]) await h.click(id);
      await h.document.dispatch("keydown", { key: "ArrowRight" });
      await h.document.dispatch("keydown", { key: "ArrowLeft" });
      assert.deepEqual(h.sent, []);
    }
  });
}

test("unacknowledged events and input cannot change the stage", async () => {
  const h = setup({ ...view("host", null), acknowledged: false });
  h.stage.handle(room()); h.stage.handle(slide());
  assert.equal(h.element("slideIdx").textContent, "—");
  assert.equal(h.wide(), false);
  assert.match(h.element("slide").textContent, /carousel will appear/);
  await h.click("nextBtn");
  await h.document.dispatch("keydown", { key: "ArrowRight" });
  assert.deepEqual(h.sent, []);
});

for (const state of ["failed", "disconnected"]) {
  test(`${state} retains the last presentation and local display controls and resize`, async () => {
    const h = setup(view("display", null));
    const { bounds } = measureSlide(h.element("slide"), { height: 200 });
    h.stage.handle(room()); h.stage.handle(slide());
    const content = h.element("slide").innerHTML;
    h.stage.updateSession(view("display", null));
    assert.equal(h.element("slide").style.fontSize, "100px");
    h.stage.updateSession({ ...view("display", null), state });
    assert.equal(h.element("slide").innerHTML, content);
    assert.equal(h.element("slideIdx").textContent, "0 / 10");
    assert.equal(h.wide(), true);
    assert.equal(h.inStage(), true);
    bounds.height = 300;
    await h.window.dispatch("resize");
    assert.equal(h.element("slide").style.fontSize, "150px");
    await h.click("exitStage");
    assert.equal(h.hidden("enterStage"), false);
    assert.equal(h.element("slide").style.fontSize, "");
    await h.click("enterStage");
    assert.equal(h.inStage(), true);
    assert.equal(h.element("slide").style.fontSize, "150px");
    h.stage.handle(slide({ kind: "blank" }, null));
    assert.equal(h.element("slide").innerHTML, content);
  });
}

test("matching updates retain player controls; replacement resets the entire presentation and dismissal", async () => {
  const h = setup();
  h.stage.handle(room()); h.stage.handle(slide());
  h.stage.updateSession(view());
  assert.equal(h.hidden("readerControls"), false);
  h.stage.updateSession({ ...view(), state: "disconnected" });
  assert.equal(h.hidden("readerControls"), false);
  h.stage.updateSession(view("display", null, "connecting"));
  h.stage.updateSession(view("display", null));
  h.stage.handle(room()); h.stage.handle(slide());
  await h.click("exitStage");
  for (const role of ["player", "display"]) {
    h.stage.updateSession({ ...view(role, "Bo", "connecting"), code: "NEXT" });
    assert.equal(h.element("slideIdx").textContent, "—");
    assert.match(h.element("slide").textContent, /carousel will appear/);
    assert.equal(h.element("slide").classList.contains("muted"), true);
    assert.equal(h.inStage(), false);
    assert.equal(h.wide(), false);
    assert.equal(h.hidden("readerControls"), true);
    assert.equal(h.hidden("enterStage"), true);
    assert.equal(h.hidden("exitStage"), true);
    assert.equal(h.element("slide").style.fontSize, "");
    h.stage.updateSession(view(role, "Bo", "awaiting_joined"));
    h.stage.updateSession(view(role, "Bo"));
    await h.click("rNext");
    assert.deepEqual(h.sent, [], "replacement clears the previous controller");
    h.stage.handle(slide({ kind: "script", reader: "Bo", text: "Replacement" }, "Bo"));
    h.stage.handle(room());
    assert.equal(h.inStage(), role === "display", "replacement clears dismissal");
    assert.equal(h.hidden("readerControls"), role !== "player");
  }
  assert.notEqual(h.element("slide").style.fontSize, "");
  h.stage.updateSession(view("display", null, "connecting"));
  assert.equal(h.element("slide").style.fontSize, "", "replacement clears a fitted stage");
  assert.equal(h.inStage(), false);
  assert.equal(h.wide(), false);
  h.stage.updateSession(view("display", null));
  h.stage.handle(slide());
  assert.equal(h.wide(), false, "replacement clears the old performing phase");
});

for (const [description, bounds, expected] of [
  ["height limited", { height: 200, heightPerPixel: 2 }, "100px"],
  ["width limited", { width: 150, widthPerPixel: 2 }, "75px"],
  ["all fitting", { height: 10000, width: 10000 }, "320px"],
  ["minimum fallback", { height: 1, width: 1 }, "24px"],
]) {
  test(`display fit is ${description}, bounded, and resized from font-dependent measurements`, async () => {
    const h = setup(view("display", null));
    const measurements = measureSlide(h.element("slide"), bounds);
    h.stage.handle(room());
    measurements.probes.length = 0;
    h.stage.handle(slide());
    assert.equal(h.element("slide").style.fontSize, expected);
    assert.ok(measurements.probes.length > 0 && measurements.probes.length <= 9);
    assert.ok(measurements.probes.every((font) => font >= 24 && font <= 320));
    measurements.bounds.height = 180;
    measurements.bounds.width = 120;
    measurements.bounds.heightPerPixel = 2;
    measurements.bounds.widthPerPixel = 1;
    measurements.probes.length = 0;
    await h.window.dispatch("resize");
    assert.equal(h.element("slide").style.fontSize, "90px");
    assert.ok(measurements.probes.length <= 9);
    measurements.bounds.height = 100;
    h.stage.handle(slide({ kind: "credits" }, null));
    assert.equal(h.element("slide").style.fontSize, "50px", "a new slide fits again");
    await h.click("exitStage");
    assert.equal(h.element("slide").style.fontSize, "");
    measurements.bounds.height = 160;
    await h.window.dispatch("resize");
    assert.equal(h.element("slide").style.fontSize, "");
    await h.click("enterStage");
    assert.equal(h.element("slide").style.fontSize, "80px");
  });
}
