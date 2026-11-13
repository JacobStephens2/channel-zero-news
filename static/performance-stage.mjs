// One tab's performance presentation. Room session supplies acknowledged identity
// and accepted events; the server owns phase, slides, and controller assignment.
export function createPerformanceStage({ document, window, send }) {
  const $ = (id) => document.getElementById(id);
  let session = null, phase = null, controller = null, dismissed = false;
  const joined = () => session?.state === "joined" && session.acknowledged;
  const controlling = () => session?.role === "player" && !!session.name && session.name === controller;

  function updateWidth() {
    $("slidePanel").classList.toggle("full", phase === "performing" &&
      (session?.role === "host" || session?.role === "display" || controlling()));
  }

  function updateStageControls() {
    const inStage = document.body.classList.contains("stage-mode");
    $("exitStage").classList.toggle("hidden", !inStage);
    $("enterStage").classList.toggle("hidden",
      !(session?.role === "display" && phase === "performing" && !inStage));
  }

  // Pick the largest font that fits both dimensions, with a legible minimum
  // fallback when even 24px overflows. Nine probes cover the 24–320px range.
  function fitStage() {
    const slide = $("slide");
    if (!document.body.classList.contains("stage-mode")) { slide.style.fontSize = ""; return; }
    const fits = () => slide.scrollHeight <= slide.clientHeight && slide.scrollWidth <= slide.clientWidth;
    let lo = 24, hi = 320, best = lo;
    for (let i = 0; i < 9 && lo <= hi; i++) {
      const mid = Math.floor((lo + hi) / 2);
      slide.style.fontSize = mid + "px";
      if (fits()) { best = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    slide.style.fontSize = best + "px";
  }

  function setStage(on) {
    document.body.classList.toggle("stage-mode", on);
    updateStageControls();
    fitStage();
  }

  function reset() {
    phase = null; controller = null; dismissed = false;
    $("slide").textContent = "The carousel will appear here once the game starts.";
    $("slide").classList.add("muted");
    $("slideIdx").textContent = "—";
    $("readerControls").classList.add("hidden");
    updateWidth();
    setStage(false);
  }

  function updateSession(view) {
    session = view;
    if (view.state === "connecting") reset();
  }

  function renderSlide(message) {
    const { role } = session, slide = message.slide;
    const reader = readerOf(slide), content = slideContentHtml(slide);
    controller = message.controller || null;
    $("slideIdx").textContent = `${message.index} / ${message.total_slides}`;
    const el = $("slide");
    el.classList.remove("muted");
    if (role === "display") {
      el.innerHTML = `<div class="stage-content">${content}</div>`;
    } else if (role === "player") {
      if (controlling()) {
        const badge = reader ? `<div class="badge">YOUR TURN TO READ 🎤</div>` : "";
        el.innerHTML = `${badge}<div class="stage-content">${content}</div>`;
      } else if (reader) {
        el.innerHTML = `<div class="reading-now">🎤 <strong>${escapeHtml(reader)}</strong> is reading…</div>`;
      } else {
        el.innerHTML = content;
      }
    } else {
      el.innerHTML = (reader ? `<p class="muted" style="margin:0 0 8px">🎤 ${escapeHtml(reader)} is reading</p>` : "") + content;
    }
    $("readerControls").classList.toggle("hidden", !controlling());
    updateWidth();
    setStage(role === "display" && !dismissed);
  }

  function handle(message) {
    if (!joined()) return;
    switch (message.type) {
      case "room_state":
        phase = message.room.phase;
        $("slideIdx").textContent = `${message.room.current_slide} / ${message.room.total_slides}`;
        if (phase !== "performing") {
          controller = null; dismissed = false;
          $("readerControls").classList.add("hidden");
        }
        updateWidth();
        setStage(phase === "performing" && document.body.classList.contains("stage-mode"));
        break;
      case "game_started": $("slideIdx").textContent = `0 / ${message.total_slides}`; break;
      case "slide_changed": renderSlide(message); break;
    }
  }

  function navigate(type) {
    if (joined() && (session.role === "host" || controlling())) send({ type });
  }
  for (const id of ["prevBtn", "rPrev"]) $(id).addEventListener("click", () => navigate("prev_slide"));
  for (const id of ["nextBtn", "rNext"]) $(id).addEventListener("click", () => navigate("advance_slide"));
  document.addEventListener("keydown", (event) => {
    if (event.key === "ArrowRight") navigate("advance_slide");
    if (event.key === "ArrowLeft") navigate("prev_slide");
  });
  $("exitStage").addEventListener("click", () => { dismissed = true; setStage(false); });
  $("enterStage").addEventListener("click", () => { dismissed = false; setStage(true); });
  window.addEventListener("resize", fitStage);
  reset();
  return Object.freeze({ updateSession, handle });
}

function readerOf(slide) {
  switch (slide.kind) {
    case "script": return slide.reader;
    case "greeting":
    case "signoff": return slide.presenter;
    default: return null;
  }
}

function slideContentHtml(slide) {
  switch (slide.kind) {
    case "rules": return "<div><strong>The rules</strong><br>When it's your turn to read, DO NOT LAUGH.<br>The host advances the slides.</div>";
    case "presenter_intro":
      return slide.first
        ? `To start us off, <strong>${escapeHtml(slide.presenter)}</strong> presents, with ${escapeHtml(slide.teleprompter)} on teleprompter.`
        : `Next up: <strong>${escapeHtml(slide.presenter)}</strong> reads, with ${escapeHtml(slide.teleprompter)} on teleprompter.`;
    case "greeting": return `Good evening! I'm <strong>${escapeHtml(slide.presenter)}</strong>, and this is today's Channel 0 News.`;
    case "script": return escapeHtml(slide.text);
    case "signoff": return `That's all for today. This is ${escapeHtml(slide.presenter)} signing off, and remember: ${escapeHtml(slide.text)}`;
    case "credits": return "<strong>Thanks for playing!</strong>";
    case "blank": return "";
    default: return "?";
  }
}

function escapeHtml(value) {
  return (value ?? "").replace(/[&<>"']/g, (character) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[character]));
}
