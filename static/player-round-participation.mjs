// One acknowledged player's round lifecycle. Room session owns identity and
// transport; the server owns the roster and submission status. Drafts live only
// in this module's form DOM and disappear on a new lobby or connection.
export function createPlayerRoundParticipation({ document, send }) {
  const $ = (id) => document.getElementById(id);
  let session = null, phase = null, assignment = null, replaySent = false;
  let submitted = false, promptsOpen = true;
  const isPlayer = () => session?.state === "joined" && session.acknowledged && session.role === "player";

  function present() {
    $("promptsBody").classList.toggle("hidden", !promptsOpen);
    $("promptsToggle").classList.toggle("hidden", !submitted);
    $("promptsToggle").textContent = promptsOpen ? "▾" : "▸";
    $("submittedTag").classList.toggle("hidden", !submitted);
  }

  function reset() {
    assignment = null; replaySent = false; submitted = false; promptsOpen = true;
    $("promptFields").innerHTML = "";
    $("partnerLine").textContent = "Join to receive your assignment.";
    $("submitBtn").classList.add("hidden");
    present();
  }

  function updateSession(view) {
    session = view;
    if (view.state === "connecting") { phase = null; reset(); }
  }

  function updateSubmission(players) {
    const confirmed = !!players.find((player) => player.name === session.name)?.submitted;
    if (confirmed && !submitted) promptsOpen = false;
    if (!confirmed) promptsOpen = true;
    submitted = confirmed;
    present();
  }

  function renderAssignment(message) {
    if (phase !== "collecting") return;
    if (assignment?.partner === message.partner && assignment.prompts.length === message.prompts.length &&
      assignment.prompts.every((prompt, index) => prompt === message.prompts[index])) return;
    assignment = { partner: message.partner, prompts: [...message.prompts] };
    $("partnerLine").innerHTML = `You're writing for <strong>${escapeHtml(message.partner)}</strong>. Fill in each line:`;
    $("promptFields").innerHTML = message.prompts.map((prompt, index) =>
      `<label>${escapeHtml(prompt) || "Prompt " + (index + 1)}</label><textarea data-i="${index}" maxlength="200"></textarea>`
    ).join("") + `<label>…and remember:</label><textarea id="signoff" maxlength="200"></textarea>`;
    $("submitBtn").classList.remove("hidden");
    promptsOpen = !submitted;
    present();
  }

  function handle(message) {
    if (!isPlayer()) return;
    switch (message.type) {
      case "room_state": {
        const room = message.room;
        if (room.phase === "lobby" && phase !== "lobby") reset();
        phase = room.phase;
        updateSubmission(room.players);
        if (phase === "collecting" && !assignment && !replaySent &&
          room.players.some((player) => player.name === session.name)) {
          replaySent = send({ type: "join_room" });
        }
        break;
      }
      case "submission_progress": updateSubmission(message.players); break;
      case "assignment": renderAssignment(message); break;
    }
  }

  $("submitBtn").addEventListener("click", () => {
    if (!isPlayer() || phase !== "collecting" || !assignment) return;
    const responses = [...document.querySelectorAll("#promptFields textarea[data-i]")]
      .sort((a, b) => a.dataset.i - b.dataset.i).map((textarea) => textarea.value);
    send({ type: "submit_responses", responses, signoff: $("signoff").value });
  });
  $("promptsToggle").addEventListener("click", () => { promptsOpen = !promptsOpen; present(); });
  reset();
  return Object.freeze({ updateSession, handle });
}

function escapeHtml(value) {
  return (value ?? "").replace(/[&<>"']/g, (character) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[character]));
}
