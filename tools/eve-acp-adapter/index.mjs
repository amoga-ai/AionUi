// ACP adapter: lets AionUI talk to the self-hosted Eve agent over stdin/stdout (JSON-RPC, one message per line).
// Env: EVE_URL (base URL), EVE_USERNAME, EVE_PASSWORD (httpBasic), EVE_SEND_PATH (route that accepts a message; confirm on your Eve app).
import readline from "node:readline";

const EVE_URL = process.env.EVE_URL ?? "";
const EVE_SEND_PATH = process.env.EVE_SEND_PATH ?? "/eve/v1/sessions/{sessionId}/messages";
const AUTH = "Basic " + Buffer.from(`${process.env.EVE_USERNAME ?? ""}:${process.env.EVE_PASSWORD ?? ""}`).toString("base64");
let sessionCounter = 0;

const write = (obj) => process.stdout.write(JSON.stringify({ jsonrpc: "2.0", ...obj }) + "\n");
const respond = (id, result) => write({ id, result });
const fail = (id, code, message) => write({ id, error: { code, message } });
const notify = (method, params) => write({ method, params });

// Sends the prompt text to Eve and returns Eve's reply text.
async function askEve(sessionId, text) {
  const path = EVE_SEND_PATH.replace("{sessionId}", encodeURIComponent(sessionId));
  const res = await fetch(EVE_URL + path, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: AUTH },
    body: JSON.stringify({ message: text }),
  });
  if (!res.ok) throw new Error(`Eve returned ${res.status}`);
  const body = await res.json().catch(() => null);
  return typeof body?.text === "string" ? body.text : JSON.stringify(body ?? "");
}

async function handle(msg) {
  const { id, method, params } = msg;
  switch (method) {
    case "initialize":
      return respond(id, {
        protocolVersion: 1,
        serverCapabilities: { streaming: true, sessionManagement: true },
        serverInfo: { name: "eve-acp-adapter", version: "0.1.0" },
      });
    case "session/new": {
      sessionCounter++;
      return respond(id, {
        sessionId: `eve-session-${sessionCounter}`,
        modes: [],
        configOptions: [],
        models: { currentModelId: "eve", availableModels: [{ id: "eve", name: "Eve" }] },
      });
    }
    case "session/prompt": {
      const sessionId = params?.sessionId ?? "unknown";
      const text = Array.isArray(params?.prompt) && params.prompt[0]?.text ? params.prompt[0].text : "";
      try {
        const reply = await askEve(sessionId, text);
        notify("session/update", {
          sessionId,
          update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: reply } },
        });
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
