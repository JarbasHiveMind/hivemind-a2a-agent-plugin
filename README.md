# hivemind-a2a-agent-plugin

A [HiveMind](https://github.com/JarbasHiveMind/HiveMind-core) agent-protocol plugin
that bridges the hive to external
[A2A (Agent-to-Agent)](https://google.github.io/A2A/) agents.

Natural-language queries arriving from hive satellites are forwarded to a configured
A2A server via JSON-RPC 2.0 (`tasks/send` or `tasks/sendSubscribe`), and the
response is streamed back to the originating satellite.

---

## What is A2A?

[A2A](https://google.github.io/A2A/) is an open protocol for agent interoperability.
An A2A server:

1. Publishes an **agent card** at `GET /.well-known/agent.json` describing its
   capabilities, skills, and the URL that accepts tasks.
2. Accepts **task** requests as JSON-RPC 2.0 at its root URL — either a blocking
   `tasks/send` or a streaming `tasks/sendSubscribe` (SSE).

Any compliant A2A server (LangChain agents, Google ADK, CrewAI, custom FastAPI
services, …) works as a backend for this plugin.

---

## Installation

```bash
pip install hivemind-a2a-agent-plugin
```

The plugin registers itself under the `hivemind.agent.protocol` entry-point group
so HiveMind-core discovers it automatically when it is installed.

---

## Configuration

Add the following to your OVOS / HiveMind config (typically
`~/.config/hivemind/hivemind.conf`):

```json
{
  "hivemind": {
    "agent_protocol": "hivemind-a2a-agent-plugin",
    "a2a_agent": {
      "agent_url": "http://localhost:9999",
      "auth_header": "Bearer secret",
      "timeout": 60,
      "streaming": false
    }
  }
}
```

| Key          | Default | Description                                          |
|--------------|---------|------------------------------------------------------|
| `agent_url`  | —       | **Required.** Root URL of the A2A server.            |
| `auth_header`| —       | Optional `Authorization` header (e.g. `Bearer …`).  |
| `timeout`    | `60`    | HTTP timeout in seconds.                             |
| `streaming`  | `false` | Prefer `tasks/sendSubscribe` (SSE) when `true`.      |

---

## Hive wiring example

```
HiveMind master
  └── hivemind-a2a-agent-plugin  ←  loaded as agent_protocol
        └── A2A server  (http://localhost:9999)
              └── your LLM / agent / tool backend

Satellite (voice client, phone, …)
  → "what's the capital of France?"
  → HiveMind master receives utterance
  → plugin forwards to A2A server via tasks/send
  → A2A server responds: "Paris is the capital of France."
  → HiveMind master streams answer back to satellite
```

**Start a minimal FastAPI A2A server and the HiveMind master:**

```bash
# 1. run the example mock A2A server (also used by e2e tests)
uvicorn tests.e2e.mock_a2a_server:app --port 9999

# 2. configure hivemind-core to use this plugin (see above)
# 3. start hivemind-core
hivemind-core listen
```

---

## Session / context mapping

The plugin maps the HiveMind `session_id` directly to the A2A `sessionId` parameter
so multi-turn conversations are kept in context on the A2A server side.  No extra
state is stored in the plugin; the A2A server owns conversation history.

---

## Error handling

The plugin never silences errors.  If the A2A server is unreachable, returns an
empty response, or returns a JSON-RPC error object, a human-readable error string
is yielded to the satellite so the user always receives a reply.

---

## Development

```bash
git clone https://github.com/TigreGotico/hivemind-a2a-agent-plugin
cd hivemind-a2a-agent-plugin
pip install -e ".[dev]"
pytest tests/          # unit tests (no server needed)
pytest tests/e2e/      # e2e tests (spins up a FastAPI mock server in-process)
```
