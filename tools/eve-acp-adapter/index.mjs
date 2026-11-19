// ACP adapter: lets AionUI talk to the self-hosted Eve agent over stdin/stdout (JSON-RPC, one message per line).
// Env: EVE_URL (base URL), EVE_USERNAME, EVE_PASSWORD (httpBasic).
import readline from "node:readline";
import fs from "node:fs/promises";
import path from "node:path";

const EVE_URL = process.env.EVE_URL ?? "";
const AUTH = "Basic " + Buffer.from(`${process.env.EVE_USERNAME ?? ""}:${process.env.EVE_PASSWORD ?? ""}`).toString("base64");
const JSON_HEADERS = { "content-type": "application/json", authorization: AUTH };
let sessionCounter = 0;
const eveSessions = new Map(); // AionUI session id -> { sessionId, continuationToken, doneTurns }
const sessionFolders = new Map(); // AionUI session id -> folder where that chat's files are saved

const write = (obj) => process.stdout.write(JSON.stringify({ jsonrpc: "2.0", ...obj }) + "\n");
const respond = (id, result) => write({ id, result });
const fail = (id, code, message) => write({ id, error: { code, message } });
const notify = (method, params) => write({ method, params });

// Reads Eve's stream until the turn that this prompt started has settled. Older turns are skipped.
async function streamTurn(eveSessionId, acpSessionId, doneTurns) {
  const res = await fetch(`${EVE_URL}/eve/v1/session/${eveSessionId}/stream`, { headers: JSON_HEADERS });
  if (!res.ok || !res.body) throw new Error(`Eve stream returned ${res.status}`);
  const decoder = new TextDecoder();
  const textParts = new Set();
  let activeTurn = null;
  let buffer = "";
  const say = (text) =>
    notify("session/update", {
      sessionId: acpSessionId,
      update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text } },
    });
  for await (const chunk of res.body) {
    buffer += decoder.decode(chunk, { stream: true });
    let idx;
    while ((idx = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);
      if (!line) continue;
      const event = JSON.parse(line);
      // Newer format: one flat event per line. Older format: a list of facts per line.
      const items = [
        ...(event.type ? [{ type: event.type, data: event.data }] : []),
        ...(event.facts ?? []).map((f) => ({ type: f.type, data: f.data })),
      ];
      for (const { type, data } of items) {
        if (type === "turn.started" && !doneTurns.has(data.turnId)) activeTurn = data.turnId;
        if (!activeTurn) continue; // ignore replayed events from older turns
        if (type === "session.failed") throw new Error("Eve session failed");
        if (type === "message.appended" && data.messageDelta) say(data.messageDelta);
        if (type === "call.settled" && data?.output?.base64 && data?.output?.fileName) {
          const folder = sessionFolders.get(acpSessionId) ?? process.cwd();
          const name = path.basename(data.output.fileName);
          await fs.writeFile(path.join(folder, name), Buffer.from(data.output.base64, "base64"));
          say(`Saved file: ${name}`);
        }
        if ((type === "turn.completed" || type === "turn.settled") && data.turnId === activeTurn) {
          doneTurns.add(activeTurn);
          return;
        }
      }
      // Older format: text progress deltas, skipping reasoning parts.
      if (activeTurn && event.progress?.type === "content.delta") {
        const { delta, kind, partId } = event.progress.data ?? {};
        if (kind === "text") textParts.add(partId);
        if (delta && textParts.has(partId)) say(delta);
      }
    }
  }
}
// Sends the prompt to Eve: starts a session the first time, then continues it with the stored token.
async function askEve(acpSessionId, text) {
  const current = eveSessions.get(acpSessionId);
  const res = current
    ? await fetch(`${EVE_URL}/eve/v1/session/${current.sessionId}`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify({ continuationToken: current.continuationToken, message: text }),
      })
    : await fetch(`${EVE_URL}/eve/v1/session`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify({ message: text }),
      });
  if (!res.ok) throw new Error(`Eve returned ${res.status}`);
  const body = await res.json();
  const doneTurns = current?.doneTurns ?? new Set();
  eveSessions.set(acpSessionId, {
    sessionId: body.sessionId,
    continuationToken: body.continuationToken ?? current?.continuationToken,
    doneTurns,
  });
  await streamTurn(body.sessionId, acpSessionId, doneTurns);
}

async function handle(msg) {
  const { id, method, params } = msg;
  switch (method) {
    case "initialize":
      return respond(id, {
        protocolVersion: 1,
        serverCapabilities: { streaming: true, sessionManagement: true },
        serverInfo: { name: "eve-acp-adapter", version: "0.3.0" },
        auth_methods: [],
      });
    case "session/new": {
      sessionCounter++;
      const sessionId = `eve-session-${sessionCounter}`;
      sessionFolders.set(sessionId, params?.cwd ?? process.cwd());
      return respond(id, {
        sessionId,
        modes: [],
        configOptions: [],
        models: { currentModelId: "eve", availableModels: [{ id: "eve", name: "Eve" }] },
      });
    }
    case "session/prompt": {
      const sessionId = params?.sessionId ?? "unknown";
      const text = Array.isArray(params?.prompt) && params.prompt[0]?.text ? params.prompt[0].text : "";
      try {
        await askEve(sessionId, text);
        return respond(id, { stopReason: "end_turn" });
      } catch (err) {
        return fail(id, -32000, String(err?.message ?? err));
      }
    }
    case "session/cancel":
      return; // notification, nothing to return
    case "session/set_mode":
    case "session/set_model":
    case "session/set_config_option":
      return respond(id, {});
    default:
      if (id !== undefined) fail(id, -32601, `Method not found: ${method}`);
  }
}

readline.createInterface({ input: process.stdin }).on("line", (line) => {
  const trimmed = line.trim();
  if (!trimmed) return;
  let msg;
  try { msg = JSON.parse(trimmed); } catch { return; }
  handle(msg);
});
