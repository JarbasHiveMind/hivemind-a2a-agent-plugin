# hivemind-a2a-agent-plugin

An agent-protocol plugin for [HiveMind](https://github.com/JarbasHiveMind/HiveMind-core) that connects the hive to external [A2A (Agent-to-Agent)](https://google.github.io/A2A/) agents.

The plugin forwards natural-language queries from hive satellites to a configured A2A server over JSON-RPC 2.0 (`tasks/send` or `tasks/sendSubscribe`). It streams the response back to the satellite that sent the query.

---

## What is A2A?

[A2A](https://google.github.io/A2A/) is an open protocol for agent interoperability. An A2A server does two things:

1. It publishes an **agent card** at `GET /.well-known/agent.json`. The card describes the server's capabilities, skills, and the URL that accepts tasks.
2. It accepts **task** requests as JSON-RPC 2.0 at its root URL. A request is either a blocking `tasks/send` call or a streaming `tasks/sendSubscribe` call (SSE).

Any compliant A2A server works as a backend for this plugin, including LangChain agents, Google ADK, CrewAI, and custom FastAPI services.

---

## Installation

```bash
pip install hivemind-a2a-agent-plugin
```

The plugin registers itself under the `hivemind.agent.protocol` entry-point group. HiveMind-core discovers it automatically once it is installed.

---

## Configuration

Add the following to your OVOS / HiveMind config, typically at `~/.config/hivemind/hivemind.conf`:

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
| `agent_url`  | none    | **Required.** Root URL of the A2A server.            |
| `auth_header`| none    | Optional `Authorization` header (e.g. `Bearer …`).  |
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

**Run a minimal FastAPI A2A server and the HiveMind master:**

```bash
# 1. run the example mock A2A server (also used by e2e tests)
uvicorn tests.e2e.mock_a2a_server:app --port 9999

# 2. configure hivemind-core to use this plugin (see above)
# 3. start hivemind-core
hivemind-core listen
```

---

## Session and context mapping

The plugin maps the HiveMind `session_id` directly to the A2A `sessionId` parameter, so multi-turn conversations keep their context on the A2A server side. The plugin stores no extra state. The A2A server owns the conversation history.

---

## Error handling

The plugin never silences errors. The A2A server may be unreachable, return an empty response, or return a JSON-RPC error object. In each case, the plugin yields a human-readable error string to the satellite so the user always gets a reply.

---

## Development

```bash
git clone https://github.com/TigreGotico/hivemind-a2a-agent-plugin
cd hivemind-a2a-agent-plugin
pip install -e ".[dev]"
pytest tests/          # unit tests (no server needed)
pytest tests/e2e/      # e2e tests (spins up a FastAPI mock server in-process)
```

## Related projects

- [JarbasHiveMind/HiveMind-core](https://github.com/JarbasHiveMind/HiveMind-core): the HiveMind master this plugin connects to.

## Credits

Funded by [NGI0 Commons Fund](https://nlnet.nl/project/OpenVoiceOS) / [NLnet](https://nlnet.nl)
under grant agreement No [101135429](https://cordis.europa.eu/project/id/101135429),
through the European Commission's [Next Generation Internet](https://ngi.eu) programme.
</content>
