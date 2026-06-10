"""End-to-end tests: A2AAgentProtocol against a live FastAPI mock A2A server.

The mock FastAPI server is started in a background thread on a free port using
uvicorn.  All HTTP calls go through a real socket so the sync httpx.Client works
without any monkey-patching.
"""
from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
import uvicorn

from tests.e2e.mock_a2a_server import app as mock_app
from hivemind_a2a_agent_plugin._client import A2AClient, AgentCard
from hivemind_a2a_agent_plugin import A2AAgentProtocol


# ---------------------------------------------------------------------------
# Local uvicorn fixture — picks a free port, starts in background thread
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server_url():
    port = _free_port()
    config = uvicorn.Config(mock_app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait until the server is accepting connections
    deadline = time.time() + 10
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("Mock A2A server did not start in time")
        time.sleep(0.05)

    yield f"http://127.0.0.1:{port}"

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(scope="module")
def a2a_client(server_url):
    client = A2AClient(base_url=server_url, streaming=False, timeout=10.0)
    yield client
    client.close()


# ---------------------------------------------------------------------------
# Agent card
# ---------------------------------------------------------------------------

class TestE2EAgentCard:
    def test_agent_card_discovery(self, a2a_client):
        card = a2a_client.fetch_agent_card()
        assert isinstance(card, AgentCard)
        assert card.name == "MockAgent"
        assert card.streaming is True
        assert len(card.skills) == 1
        assert card.skills[0].id == "echo"

    def test_agent_card_url(self, a2a_client, server_url):
        card = a2a_client.fetch_agent_card()
        assert card.url == "http://localhost:9999"   # value set inside the mock


# ---------------------------------------------------------------------------
# tasks/send (blocking)
# ---------------------------------------------------------------------------

class TestE2ESendTask:
    def test_echo_response(self, a2a_client):
        result = a2a_client.send_task("hello world", lang="en-us")
        assert result == "Echo: hello world"

    def test_session_id_accepted(self, a2a_client):
        result = a2a_client.send_task("ping", session_id="sess-e2e", lang="en-us")
        assert result == "Echo: ping"

    def test_unicode_text(self, a2a_client):
        result = a2a_client.send_task("olá mundo", lang="pt-pt")
        assert result == "Echo: olá mundo"


# ---------------------------------------------------------------------------
# tasks/sendSubscribe (SSE streaming)
# ---------------------------------------------------------------------------

class TestE2EStreamTask:
    def test_streaming_chunks(self, a2a_client):
        chunks = list(a2a_client.stream_task("hello stream", lang="en-us"))
        assert len(chunks) > 0
        full = "".join(chunks).strip()
        assert "hello" in full and "stream" in full

    def test_streaming_empty_message(self, a2a_client):
        chunks = list(a2a_client.stream_task("", lang="en-us"))
        assert isinstance(chunks, list)


# ---------------------------------------------------------------------------
# A2AAgentProtocol end-to-end via real socket
# ---------------------------------------------------------------------------

class TestE2EProtocol:
    def _make_protocol(self, server_url: str, streaming: bool = False) -> A2AAgentProtocol:
        proto = A2AAgentProtocol(config={
            "agent_url": server_url,
            "streaming": streaming,
            "timeout": 10,
        })
        return proto

    def test_protocol_blocking_query(self, server_url):
        proto = self._make_protocol(server_url, streaming=False)
        chunks = list(proto.natural_language_query("what time is it?", "en-us"))
        assert chunks[-1] is None
        answers = [c for c in chunks if c is not None]
        assert len(answers) >= 1
        assert "Echo" in answers[0]

    def test_protocol_streaming_query(self, server_url):
        proto = self._make_protocol(server_url, streaming=True)
        chunks = list(proto.natural_language_query("stream this please", "en-us"))
        assert chunks[-1] is None
        answers = [c for c in chunks if c is not None]
        assert len(answers) >= 1

    def test_protocol_session_mapping(self, server_url):
        proto = self._make_protocol(server_url, streaming=False)
        chunks1 = list(proto.natural_language_query("turn 1", "en-us",
                                                     session_id="e2e-session"))
        chunks2 = list(proto.natural_language_query("turn 2", "en-us",
                                                     session_id="e2e-session"))
        for chunks in [chunks1, chunks2]:
            assert chunks[-1] is None
            assert any(c for c in chunks if c is not None)

    def test_protocol_error_on_bad_server(self, server_url):
        proto = self._make_protocol(server_url, streaming=False)
        original = proto._client.send_task
        def _fail(*a, **kw):
            raise RuntimeError("A2A RPC error -32603: forced error")
        proto._client.send_task = _fail

        chunks = list(proto.natural_language_query("trigger error", "en-us"))
        assert chunks[-1] is None
        assert any("error" in (c or "").lower() for c in chunks)
