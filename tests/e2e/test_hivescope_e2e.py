"""hivescope e2e: a HiveMind satellite QUERY is answered by the A2A agent.

Full in-process HiveMind topology (hivescope ``TopologyBuilder``: real
``hivemind-core`` listener protocol, handshake, ACL, QUERY routing — no real
hive sockets) with ``A2AAgentProtocol`` as the master's agent, pointed at the
live mock A2A FastAPI server over a real localhost HTTP socket.

The proven round-trip:

    satellite ``QUERY`` (recognizer_loop:utterance)
      -> hivemind-core admits it and calls
         ``A2AAgentProtocol.natural_language_query``
      -> real JSON-RPC ``tasks/send`` HTTP call to the mock A2A server
      -> the echoed answer streams back as QUERY response chunks
      -> the originating satellite receives the answer
"""
from __future__ import annotations

import socket
import threading
import time

import pytest
import uvicorn

pytest.importorskip("hivescope")

from hivemind_bus_client.message import HiveMessage, HiveMessageType  # noqa: E402
from ovos_bus_client.message import Message  # noqa: E402
from hivescope.topology import TopologyBuilder  # noqa: E402

from tests.e2e.mock_a2a_server import app as mock_app  # noqa: E402
from hivemind_a2a_agent_plugin import A2AAgentProtocol  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def a2a_server_url():
    port = _free_port()
    config = uvicorn.Config(mock_app, host="127.0.0.1", port=port,
                            log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("Mock A2A server did not start in time")
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


def _query(utt: str, qid: str, peer: str) -> HiveMessage:
    inner = HiveMessage(
        HiveMessageType.BUS,
        payload=Message("recognizer_loop:utterance", {"utterances": [utt]}),
    )
    return HiveMessage(HiveMessageType.QUERY, payload=inner,
                       metadata={"query_id": qid, "originator_peer": peer})


def _answer_texts(records) -> list:
    """Extract the spoken answer text from recorded QUERY response chunks.

    A QUERY response is HiveMessage(QUERY, payload=HiveMessage(BUS,
    payload=Message("speak", ...))) — unwrap nested payloads until the
    ``speak`` Message is reached.
    """
    texts = []
    for rec in records:
        payload = rec.payload
        for _ in range(4):  # unwrap nested HiveMessage/dict payload envelopes
            if isinstance(payload, Message):
                if payload.msg_type == "speak":
                    texts.append(payload.data.get("utterance", ""))
                break
            if isinstance(payload, dict):
                if payload.get("type") == "speak":
                    texts.append(payload.get("data", {}).get("utterance", ""))
                    break
                payload = payload.get("payload")
            else:
                payload = getattr(payload, "payload", None)
            if payload is None:
                break
    return texts


def test_a2a_agent_answers_query(a2a_server_url):
    agent = A2AAgentProtocol(config={"agent_url": a2a_server_url})

    b = TopologyBuilder()
    m = b.add_master("M0", agent_protocol=agent)
    m.register_satellite("a2a-key", password="a2a-pw",
                         allowed_types=["recognizer_loop:utterance"])
    b.add_satellite("S0", upstream=m,
                    allowed_types=["recognizer_loop:utterance"])
    b.start_all()
    try:
        s = b.get_satellite("S0")
        s.send(_query("hello hive", "q1", s.peer))

        recv = s.recorder.wait_for(HiveMessageType.QUERY.value,
                                   direction="in", timeout=8.0)
        assert recv is not None, "A2A answer never routed back to the satellite"

        chunks = s.recorder.received(HiveMessageType.QUERY.value, direction="in")
        texts = _answer_texts(chunks)
        assert any("Echo: hello hive" in t for t in texts), \
            f"expected the mock A2A echo answer, got {texts!r}"
    finally:
        b.stop_all()


def test_a2a_unconfigured_agent_yields_error_answer():
    """Without an agent_url the protocol still answers (an error string), so a
    satellite QUERY gets a response rather than hanging until escalation."""
    agent = A2AAgentProtocol(config={})

    b = TopologyBuilder()
    m = b.add_master("M0", agent_protocol=agent)
    m.register_satellite("a2a-key", password="a2a-pw",
                         allowed_types=["recognizer_loop:utterance"])
    b.add_satellite("S0", upstream=m,
                    allowed_types=["recognizer_loop:utterance"])
    b.start_all()
    try:
        s = b.get_satellite("S0")
        s.send(_query("anyone there", "q2", s.peer))
        recv = s.recorder.wait_for(HiveMessageType.QUERY.value,
                                   direction="in", timeout=8.0)
        assert recv is not None, "unconfigured agent never answered the QUERY"
        texts = _answer_texts(
            s.recorder.received(HiveMessageType.QUERY.value, direction="in"))
        assert any("not configured" in t for t in texts), \
            f"expected the not-configured answer, got {texts!r}"
    finally:
        b.stop_all()
