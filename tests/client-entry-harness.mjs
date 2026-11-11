import { readFile } from "node:fs/promises";
import vm from "node:vm";
import { ControlledSocket, memoryStorage } from "./client-adapters.mjs";

class Element {
  value = "";
  textContent = "";
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
  set innerHTML(value) { this.html = value; this.onHtml?.(value); }
  get innerHTML() { return this.html ?? ""; }
}

// Executes the real inline ES module, linking the real session module. Only
// browser boundaries (DOM, transport, storage, REST and alerts) are controlled.
export async function loadClient({ storage = memoryStorage(), fetch = async () => ({
  ok: true, json: async () => ({ code: "HOST", host_token: "secret" }),
}) } = {}) {
  const html = await readFile(new URL("../static/index.html", import.meta.url), "utf8");
  const elements = new Map();
  for (const tag of html.matchAll(/<[^>]+\bid="([^"]+)"[^>]*>/g)) {
    elements.set(tag[1], new Element(tag[0].match(/class="([^"]*)"/)?.[1]));
  }
  const element = (id) => elements.get(id);
  element("role").value = "player";
  let textareas = [];
  element("promptFields").onHtml = (value) => {
    textareas = [...value.matchAll(/<textarea data-i="(\d+)"/g)].map((match) => {
      const textarea = new Element(); textarea.dataset.i = match[1]; return textarea;
    });
    if (value.includes('id="signoff"')) elements.set("signoff", new Element());
  };
  const document = new Element();
  document.body = new Element();
  document.getElementById = (id) => element(id) ?? null;
  document.querySelectorAll = () => textareas;
  const sockets = [], alerts = [];
  const context = vm.createContext({ document, window: new Element(),
    location: { protocol: "http:", host: "test.local" },
    WebSocket: class extends ControlledSocket { constructor(url) { super(); this.url = url; sockets.push(this); } },
    sessionStorage: storage, fetch, alert: (message) => alerts.push(message), console,
  });
  const script = html.match(/<script(?: type="module")?>([\s\S]*?)<\/script>/)[1];
  const entry = new vm.SourceTextModule(script, { context });
  await entry.link(async (specifier) => {
    assertSessionImport(specifier);
    return new vm.SourceTextModule(await readFile(new URL(`../static/${specifier}`, import.meta.url), "utf8"), { context });
  });
  await entry.evaluate();
  return { element, sockets, alerts, storage, document,
    click: (id) => element(id).dispatch("click"),
    editRole: async (role) => { element("role").value = role; await element("role").dispatch("change"); },
    textareas: () => textareas,
  };
}

function assertSessionImport(specifier) {
  if (specifier !== "./room-session.mjs") throw new Error(`unexpected client module: ${specifier}`);
}
