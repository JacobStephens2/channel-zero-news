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

export class ControlledElement {
  value = "";
  style = {};
  dataset = {};
  scrollHeight = 100;
  scrollWidth = 100;
  clientHeight = 800;
  clientWidth = 1000;
  listeners = new Map();
  constructor(classes = "") {
    const names = new Set(classes.split(/\s+/).filter(Boolean));
    this.classList = {
      contains: (name) => names.has(name),
      add: (...values) => values.forEach((name) => names.add(name)),
      remove: (...values) => values.forEach((name) => names.delete(name)),
      toggle(name, force = !names.has(name)) { if (force) names.add(name); else names.delete(name); },
    };
  }
  addEventListener(type, listener) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(listener);
  }
  async dispatch(type, event = {}) {
    for (const listener of this.listeners.get(type) ?? []) await listener(event);
  }
  set textContent(value) { this.text = value; this.html = ""; this.onHtml?.(""); }
  get textContent() { return this.text ?? ""; }
  set innerHTML(value) { this.html = value; this.text = ""; this.onHtml?.(value); }
  get innerHTML() { return this.html ?? ""; }
}

// Both interface and entry tests render the production form into this document.
export function controlledDocument(html) {
  const elements = new Map();
  for (const tag of html.matchAll(/<[^>]+\bid="([^"]+)"[^>]*>/g)) {
    elements.set(tag[1], new ControlledElement(tag[0].match(/class="([^"]*)"/)?.[1]));
  }
  const element = (id) => elements.get(id);
  let textareas = [];
  element("promptFields").onHtml = (value) => {
    textareas = [...value.matchAll(/<textarea data-i="(\d+)"/g)].map((match) => {
      const textarea = new ControlledElement(); textarea.dataset.i = match[1]; return textarea;
    });
    elements.delete("signoff");
    if (value.includes('id="signoff"')) elements.set("signoff", new ControlledElement());
  };
  const document = new ControlledElement();
  document.body = new ControlledElement();
  document.getElementById = (id) => element(id) ?? null;
  document.querySelectorAll = (selector) => {
    if (selector !== "#promptFields textarea[data-i]") throw new Error(`unexpected selector: ${selector}`);
    return textareas;
  };
  return { document, element, textareas: () => textareas,
    click: (id) => element(id).dispatch("click") };
}
