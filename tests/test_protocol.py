"""Unit tests for A2AAgentProtocol — mock A2AClient, no HTTP."""
from __future__ import annotations

import json
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from hivemind_a2a_agent_plugin import A2AAgentProtocol


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _protocol(agent_url: str = "http://a2a.test", streaming: bool = False,
              **extra) -> A2AAgentProtocol:
    cfg = {"agent_url": agent_url, "streaming": streaming, **extra}
    with patch("hivemind_a2a_agent_plugin._client.A2AClient") as MockClient:
        instance = MockClient.return_value
        instance.streaming = streaming
        proto = A2AAgentProtocol(config=cfg)
        proto._client = instance
    return proto


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProtocolInit:
    def test_no_url_yields_error(self):
        proto = A2AAgentProtocol(config={})
        chunks = list(proto.natural_language_query("hello", "en-us"))
        assert chunks[-1] is None
        assert "not configured" in chunks[0].lower()

    def test_url_creates_client(self):
        with patch("hivemind_a2a_agent_plugin.A2AClient") as MockClient:
            proto = A2AAgentProtocol(config={"agent_url": "http://a2a.test"})
        MockClient.assert_called_once()
        call_kwargs = MockClient.call_args
        assert call_kwargs.kwargs["base_url"] == "http://a2a.test"


class TestNaturalLanguageQueryBlocking:
    def test_basic_answer(self):
        proto = _protocol()
        proto._client.streaming = False
        proto._client.send_task.return_value = "Paris."

        chunks = list(proto.natural_language_query("capital of France?", "en-us"))
        assert chunks == ["Paris.", None]

    def test_empty_response_fallback(self):
        proto = _protocol()
        proto._client.streaming = False
        proto._client.send_task.return_value = ""

        chunks = list(proto.natural_language_query("hello", "en-us"))
        assert chunks[-1] is None
        assert "empty" in chunks[0].lower()

    def test_session_id_forwarded(self):
        proto = _protocol()
        proto._client.streaming = False
        proto._client.send_task.return_value = "ok"

        list(proto.natural_language_query("hi", "en-us", session_id="sid-42"))
        call_kwargs = proto._client.send_task.call_args
        assert call_kwargs.kwargs.get("session_id") == "sid-42"

    def test_lang_forwarded(self):
        proto = _protocol()
        proto._client.streaming = False
        proto._client.send_task.return_value = "ok"

        list(proto.natural_language_query("hi", "pt-pt", session_id="x"))
        call_kwargs = proto._client.send_task.call_args
        assert call_kwargs.kwargs.get("lang") == "pt-pt"

    def test_exception_yields_error_not_silence(self):
        proto = _protocol()
        proto._client.streaming = False
        proto._client.send_task.side_effect = RuntimeError("connection refused")

        chunks = list(proto.natural_language_query("hello", "en-us"))
        assert chunks[-1] is None
        assert len(chunks) == 2
        assert "connection refused" in chunks[0].lower() or "error" in chunks[0].lower()

    def test_rpc_error_yields_human_readable(self):
        proto = _protocol()
        proto._client.streaming = False
        proto._client.send_task.side_effect = RuntimeError("A2A RPC error -32603: internal")

        chunks = list(proto.natural_language_query("hello", "en-us"))
        assert any("error" in c.lower() for c in chunks if c is not None)


class TestNaturalLanguageQueryStreaming:
    def _stream_proto(self, chunks_to_yield: List[str]) -> A2AAgentProtocol:
        proto = _protocol(streaming=True)
        proto._client.streaming = True
        proto._client.stream_task.return_value = iter(chunks_to_yield)
        return proto

    def test_streaming_yields_chunks(self):
        proto = self._stream_proto(["Hello", " world"])
        chunks = list(proto.natural_language_query("hi", "en-us"))
        assert chunks == ["Hello", " world", None]

    def test_streaming_empty_fallback(self):
        proto = self._stream_proto([])
        chunks = list(proto.natural_language_query("hi", "en-us"))
        assert chunks[-1] is None
        assert "empty" in chunks[0].lower()

    def test_streaming_exception_yields_error(self):
        proto = _protocol(streaming=True)
        proto._client.streaming = True
        proto._client.stream_task.side_effect = Exception("SSE broken")
        chunks = list(proto.natural_language_query("hi", "en-us"))
        assert chunks[-1] is None
        assert any("error" in c.lower() or "sse" in c.lower()
                   for c in chunks if c)

    def test_sentinel_always_last(self):
        """None sentinel must be the last yielded value."""
        proto = self._stream_proto(["a", "b", "c"])
        chunks = list(proto.natural_language_query("q", "en-us"))
        assert chunks[-1] is None
        assert chunks[:-1] == ["a", "b", "c"]
