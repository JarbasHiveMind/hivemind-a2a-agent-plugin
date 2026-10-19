# hivemind-a2a-agent-plugin

An agent-protocol plugin for [hivemind-core](https://github.com/JarbasHiveMind/HiveMind-core). It answers the questions a hive sends to its master with an external agent that speaks the [Agent2Agent (A2A) protocol](https://a2a-protocol.org).

- [What you can do](#what-you-can-do)
- [User stories](#user-stories)
- [Reference](#reference)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

## What you can do

A2A is an open protocol that lets one agent call another, whatever framework each one uses inside. An A2A agent publishes an *agent card* at `/.well-known/agent-card.json`. The card names the agent, its skills, and the URL that accepts messages.

With this plugin as its agent protocol, hivemind-core answers each `QUERY` from a satellite or a child node. It sends the question to one A2A agent and returns the answer as `speak` chunks. The plugin uses the [a2a-sdk](https://pypi.org/project/a2a-sdk/) client. It reads the card, picks the transport the card offers, and speaks A2A 1.0 or 0.3, whichever the agent supports.

The plugin answers `QUERY` and `CASCADE` messages, through the `natural_language_query` seam of the HiveMind agent protocol. A plain `BUS` `recognizer_loop:utterance` gets no answer. That is the message that `hivemind-client terminal` and a voice satellite send. See [Troubleshooting](#troubleshooting).

## User stories

### My hive answers questions with an A2A agent

You run hivemind-core and have an A2A agent: a LangGraph or CrewAI service, a hosted assistant, or an OVOS box running [ovos-persona-server](https://github.com/OpenVoiceOS/ovos-persona-server) with `--a2a-base-url`. You want the hive to answer with that agent.

1. Install the plugin next to hivemind-core:

   ```bash
   uv pip install hivemind-a2a-agent-plugin
   ```

2. Select it in `~/.config/hivemind-core/server.json`. `agent_protocol.module` names the plugin, and the key with the plugin name holds its settings:

   ```json
   {
     "agent_protocol": {
       "module": "hivemind-a2a-agent-plugin",
       "hivemind-a2a-agent-plugin": {
         "agent_url": "http://127.0.0.1:8337/a2a",
         "timeout": 30
       }
     }
   }
   ```

3. Add a client, allow the message type it will ask with, and start the hub:

   ```bash
   hivemind-core add-client --name kitchen-sat --access-key kitchen-key \
       --password 'Correct-Horse-Battery-77' --allow recognizer_loop:utterance
   hivemind-core listen
   ```

On start, the log names the plugin:

```text
hivemind_core.service:run:441 - INFO - Agent protocol: A2AAgentProtocol
hivemind_a2a_agent_plugin:__post_init__:99 - INFO - A2AAgentProtocol: configured for A2A agent at http://127.0.0.1:8337/a2a
```

4. Ask a question with a `QUERY`. This client script uses hivemind-bus-client:

   ```python
   import threading
   import uuid

   from hivemind_bus_client.client import HiveMessageBusClient
   from hivemind_bus_client.identity import NodeIdentity
   from hivemind_bus_client.message import HiveMessage, HiveMessageType
   from ovos_bus_client.message import Message

   client = HiveMessageBusClient(key="kitchen-key", password="Correct-Horse-Battery-77",
                                 host="ws://127.0.0.1", port=5678,
                                 identity=NodeIdentity(app_name="a2a-query-example",
                                                       shared_fallback=False))
   done = threading.Event()


   def on_query(message: HiveMessage):
       payload = message.payload
       while isinstance(payload, HiveMessage):
           payload = payload.payload
       if payload.msg_type == "speak":
           print(payload.data["utterance"])
       elif payload.msg_type in ("hive.query.complete", "hive.query.timeout"):
           done.set()


   client.on(HiveMessageType.QUERY, on_query)
   client.connect()
   client.wait_for_handshake()
   question = Message("recognizer_loop:utterance",
                      {"utterances": ["what is the capital of portugal"], "lang": "en-US"})
   client.emit(HiveMessage(HiveMessageType.QUERY,
                           payload=HiveMessage(HiveMessageType.BUS, payload=question),
                           metadata={"query_id": str(uuid.uuid4())}))
   done.wait(30)
   client.close()
   ```

On success, the agent's answer is printed. With a test agent that echoes its input and its A2A `contextId`, the hive answered:

```text
echo[8a086dca-691d-499b-8def-a67bc59f3ddd]: what is the capital of portugal
```

`NodeIdentity(app_name="a2a-query-example", shared_fallback=False)` gives the script its own identity under `~/.config/hivemind/a2a-query-example/`. Without it, the client uses the shared `~/.config/hivemind/_identity.json`. That fails when the shared identity already pinned another hub at the same address, and it changes key files that other HiveMind clients of the same user read.

### The agent asks a question back

An agent can stop a task in `TASK_STATE_INPUT_REQUIRED` or `TASK_STATE_AUTH_REQUIRED` and ask for more. The satellite hears the question. With a test agent that asks for a city, the hive answered:

```text
which city?
```

The next `QUERY` starts a new A2A conversation, because hivemind-core passes no session id. The agent receives the answer without the question that came before it.

### The remote agent is down, slow or fails

The plugin never leaves a query without an answer. When the agent cannot answer, the satellite receives an error sentence, and then the end of the query.

| Situation | What the satellite receives |
|---|---|
| Agent unreachable | `Error validating A2A agent at http://127.0.0.1:1: could not fetch or parse its agent card (Network communication error fetching agent card from http://127.0.0.1:1/.well-known/agent-card.json: All connection attempts failed)` |
| Agent task failed | `Error contacting A2A agent: A2A task ended in TASK_STATE_FAILED: backend exploded` |
| Agent slower than `timeout` | `Error contacting A2A agent: Client Request timed out` |
| `agent_url` not set | `A2A agent not configured — no agent_url set.` |

A slow agent within `timeout` still answers. An agent that took 12 seconds, with `timeout: 30`, was answered after 13.8 seconds.

## Reference

### Settings

| Key | Default | Description |
|---|---|---|
| `agent_url` | none | **Required.** Base URL of the agent: the part before `/.well-known/agent-card.json`. |
| `auth_header` | none | Full `Authorization` header value, for example `Bearer <token>`. |
| `timeout` | `60` | Seconds to wait for each HTTP request. |
| `streaming` | `false` | Use `SendStreamingMessage`, so each chunk the agent sends becomes one `speak`. |

The plugin also reads the same keys from the OVOS configuration at `hivemind.a2a_agent`. Keys in `server.json` win.

### What is sent and received

| HiveMind | A2A |
|---|---|
| The `QUERY` utterance | `SendMessage` with one text part, role `ROLE_USER` |
| The query language | `{"lang": ...}` in the message metadata |
| Agent message, task artifacts, artifact updates | the `speak` text |
| Task status message, when the task has no artifact text | the `speak` text |
| Task state `FAILED`, `REJECTED` or `CANCELED` | an error sentence |

hivemind-core calls `natural_language_query(utterance, lang)` without a session id. The plugin then sends no `contextId`, the agent assigns a new one, and every query starts a new A2A conversation. A2A 1.0.1 section 3.4.1 says a client SHOULD NOT send a `contextId` it generated itself. A caller that passes `session_id` directly keeps the agent-assigned `contextId` of that session for its later queries.

## Deployment

The plugin runs inside hivemind-core and opens outbound HTTP connections only. A2A has no built-in encryption. Use an `https://` `agent_url` when the agent is on another machine, and `auth_header` when the agent asks for a token.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| A voice satellite or `hivemind-client terminal` gets no answer | They send `BUS` messages. The hub logs `Forwarding message 'recognizer_loop:utterance' to agent bus`, and this plugin does not answer the bus. Send a `QUERY`, as in the user story. |
| `Error validating A2A agent ... (HTTP 404)` | `agent_url` points at the wrong path, or the agent publishes only `/.well-known/agent.json`, the A2A 0.2 path. The plugin speaks A2A 0.3 and 1.0 only. |
| `ImportError: cannot import name 'BitStream' from 'bitstring'` | hivemind-bus-client 1.2.11a1 and older import `BitStream`, which bitstring 5 removed. With those versions, install `bitstring<5`. |
| `AttributeError: module 'httpx' has no attribute 'AsyncClient'` | A prerelease of httpx 1.0 is installed. Install `httpx<1`. |

## Development

```bash
git clone https://github.com/JarbasHiveMind/hivemind-a2a-agent-plugin
cd hivemind-a2a-agent-plugin
uv venv && uv pip install --prerelease=allow -e ".[test]"
.venv/bin/pytest tests/
```

The tests start a real a2a-sdk agent (`tests/a2a_agent.py`) in a subprocess. Run it alone to try the plugin by hand: `python tests/a2a_agent.py 9999`.

## Related projects

- [JarbasHiveMind/HiveMind-core](https://github.com/JarbasHiveMind/HiveMind-core): the HiveMind master this plugin connects to.
- [OpenVoiceOS/ovos-a2a-agent-plugin](https://github.com/OpenVoiceOS/ovos-a2a-agent-plugin): the same A2A client as an ovos-persona solver.

## Credits

Funded by [NGI0 Commons Fund](https://nlnet.nl/project/OpenVoiceOS) / [NLnet](https://nlnet.nl)
under grant agreement No [101135429](https://cordis.europa.eu/project/id/101135429),
through the European Commission's [Next Generation Internet](https://ngi.eu) programme.
