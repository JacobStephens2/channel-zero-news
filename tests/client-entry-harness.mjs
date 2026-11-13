import { readFile } from "node:fs/promises";
import vm from "node:vm";
import { ControlledElement, ControlledSocket, controlledDocument, memoryStorage } from "./client-adapters.mjs";

// Executes the real inline ES module and all three allowed native modules. Only
// browser boundaries (DOM, transport, storage, REST and alerts) are controlled.
export async function loadClient({ storage = memoryStorage(), fetch = async () => ({
  ok: true, json: async () => ({ code: "HOST", host_token: "secret" }),
}) } = {}) {
  const html = await readFile(new URL("../static/index.html", import.meta.url), "utf8");
  const dom = controlledDocument(html);
  const { document, element } = dom;
  element("role").value = "player";
  const sockets = [], alerts = [];
  const window = new ControlledElement();
  const context = vm.createContext({ document, window,
    location: { protocol: "http:", host: "test.local" },
    WebSocket: class extends ControlledSocket { constructor(url) { super(); this.url = url; sockets.push(this); } },
    sessionStorage: storage, fetch, alert: (message) => alerts.push(message), console,
  });
  const script = html.match(/<script(?: type="module")?>([\s\S]*?)<\/script>/)[1];
  const entry = new vm.SourceTextModule(script, { context });
  await entry.link(async (specifier) => {
    assertClientImport(specifier);
    return new vm.SourceTextModule(await readFile(new URL(`../static/${specifier}`, import.meta.url), "utf8"), { context });
  });
  await entry.evaluate();
  return { ...dom, window, sockets, alerts, storage,
    editRole: async (role) => { element("role").value = role; await element("role").dispatch("change"); },
  };
}

function assertClientImport(specifier) {
  if (!["./room-session.mjs", "./player-round-participation.mjs", "./performance-stage.mjs"].includes(specifier))
    throw new Error(`unexpected client module: ${specifier}`);
}
