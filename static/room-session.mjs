const SESSION_KEY = "cz_session";

function capture(credentials) {
  if (!credentials || typeof credentials.code !== "string" || !credentials.code.trim())
    throw new Error("enter a room code");
  const { role } = credentials;
  if (!["host", "player", "display"].includes(role)) throw new Error("choose a valid role");
  const name = role === "player" && typeof credentials.name === "string" ? credentials.name.trim() : null;
  if (role === "player" && !name) throw new Error("enter your name");
  const hostToken = role === "host" ? credentials.hostToken : null;
  if (role === "host" && (typeof hostToken !== "string" || !hostToken.trim()))
    throw new Error("a host token is required");
  return { code: credentials.code.trim().toUpperCase(), role, name, hostToken };
}

const joinMessage = ({ code, role, name, hostToken }) =>
  ({ type: "join_room", code, role, name, token: hostToken });

// One tab's connection and recovery owner. DOM and authoritative game state
// stay with the caller and server respectively. Storage is accessed lazily.
export function createRoomSession({ createSocket, storage, onChange = () => {},
  onMessage = () => {}, onError = () => {}, onSend = () => {} }) {
  let attempt = null;
  let current = Object.freeze({ state: "idle", acknowledged: false, code: null, role: null, name: null });
  const owns = (candidate) => attempt === candidate && !candidate.terminal;

  function publish(state) {
    current = Object.freeze({ state, acknowledged: attempt.acknowledged,
      code: attempt.identity.code, role: attempt.identity.role, name: attempt.identity.name });
    onChange(current);
  }

  function terminate(candidate, state, error) {
    if (!owns(candidate)) return;
    candidate.terminal = true;
    publish(state);
    if (state === "failed") {
      try { candidate.socket?.close(); } catch {}
    }
    if (error) onError(error);
  }

  function transmit(candidate, message) {
    try { candidate.socket.send(JSON.stringify(message)); }
    catch { terminate(candidate, "failed", "connection error"); return false; }
    onSend(message);
    return true;
  }

  function removeRecovery() { try { storage.removeItem(SESSION_KEY); } catch {} }

  function clearMatchingRecovery(identity) {
    try {
      const saved = capture(JSON.parse(storage.getItem(SESSION_KEY)));
      if (["code", "role", "name", "hostToken"].every((key) => saved[key] === identity[key])) removeRecovery();
    } catch {}
  }

  function start(credentials, restoring = false) {
    let identity;
    try { identity = capture(credentials); }
    catch (error) { onError(error.message); return false; }
    const previous = attempt;
    const candidate = { identity, restoring,
      socket: null, acknowledged: false, terminal: false };
    attempt = candidate;
    if (previous) {
      previous.terminal = true;
      try { previous.socket?.close(); } catch {}
    }
    publish("connecting");
    let socket;
    try { socket = candidate.socket = createSocket(); }
    catch { terminate(candidate, "failed", "connection error"); return false; }
    socket.onopen = () => {
      if (!owns(candidate) || current.state !== "connecting") return;
      publish("awaiting_joined");
      transmit(candidate, joinMessage(candidate.identity));
    };
    socket.onmessage = ({ data }) => {
      if (!owns(candidate) || current.state === "connecting") return;
      let message;
      try {
        message = JSON.parse(data);
        if (!message || typeof message !== "object" || Array.isArray(message) || typeof message.type !== "string")
          throw new Error();
      } catch { onError("invalid server message"); return; }
      if (message.type === "joined") {
        if (!["host", "player", "display"].includes(message.role) ||
          (message.role === "player" && (typeof message.name !== "string" || !message.name.trim()))) {
          onError("invalid join acknowledgement"); return;
        }
        if (candidate.acknowledged) {
          if (message.role !== candidate.identity.role || (message.name ?? null) !== candidate.identity.name)
            onError("conflicting join acknowledgement");
          return;
        }
        candidate.identity = { ...candidate.identity, role: message.role, name: message.name ?? null };
        if (message.role !== "host") candidate.identity.hostToken = null;
        candidate.acknowledged = true;
        try { storage.setItem(SESSION_KEY, JSON.stringify(candidate.identity)); } catch {}
        publish("joined");
      } else if (message.type === "error") {
        const silent = message.code === "unknown_room" && candidate.restoring && !candidate.acknowledged;
        if (message.code === "unknown_room") {
          clearMatchingRecovery(candidate.identity);
        }
        if (!candidate.acknowledged) terminate(candidate, "failed");
        if (!silent) onMessage(message);
      } else if (candidate.acknowledged) {
        onMessage(message);
      }
    };
    socket.onerror = () => terminate(candidate, "failed", "connection error");
    socket.onclose = () => terminate(candidate, "disconnected");
    return true;
  }

  function restore() {
    let record;
    try { record = storage.getItem(SESSION_KEY); } catch { return false; }
    if (record === null) return false;
    let identity;
    try { identity = capture(JSON.parse(record)); }
    catch { removeRecovery(); return false; }
    return start(identity, true);
  }

  function send(intent) {
    if (!attempt || !owns(attempt) || !attempt.acknowledged || attempt.socket.readyState !== 1) return false;
    let message = intent;
    if (intent.type === "join_room") {
      const join = joinMessage(attempt.identity);
      for (const field of ["code", "role", "name", "token"]) {
        if (Object.hasOwn(intent, field) && intent[field] !== join[field]) return false;
      }
      message = join;
    }
    return transmit(attempt, message);
  }

  return Object.freeze({ connect: (credentials) => start(credentials), restore, send,
    get current() { return current; } });
}
