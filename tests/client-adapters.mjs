// Controlled browser boundaries shared by the session and client-entry tests.
export class ControlledSocket {
  readyState = 0;
  sent = [];
  closed = false;
  open() { this.readyState = 1; this.onopen?.(); }
  message(message) { this.raw(JSON.stringify(message)); }
  raw(data) { this.onmessage?.({ data }); }
  error() { this.onerror?.(); }
  close() { this.closed = true; this.readyState = 3; this.onclose?.(); }
  send(data) { this.sent.push(JSON.parse(data)); }
}

export function memoryStorage(record = null) {
  const records = new Map(record === null ? [] : [["cz_session", record]]);
  const operations = [];
  return {
    operations,
    getItem(key) { operations.push(["read", key]); return records.get(key) ?? null; },
    setItem(key, value) { operations.push(["write", key]); records.set(key, value); },
    removeItem(key) { operations.push(["remove", key]); records.delete(key); },
    saved() { return JSON.parse(records.get("cz_session") ?? "null"); },
  };
}
