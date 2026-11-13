// Controlled browser boundaries shared by client interface and entry tests.
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

// Interface and entry tests use the production document's selectors and classes.
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

// Measurements depend on the font under test, unlike the default fixed DOM.
// Mutable bounds let resize tests change available space without timers.
export function measureSlide(slide, overrides = {}) {
  const bounds = { width: 1000, height: 800, widthPerPixel: 1, heightPerPixel: 2, ...overrides };
  const probes = [];
  Object.defineProperties(slide, {
    clientWidth: { get: () => bounds.width },
    clientHeight: { get: () => bounds.height },
    scrollWidth: { get: () => parseInt(slide.style.fontSize) * bounds.widthPerPixel },
    scrollHeight: { get: () => {
      const font = parseInt(slide.style.fontSize);
      probes.push(font);
      return font * bounds.heightPerPixel;
    } },
  });
  return { bounds, probes };
}
