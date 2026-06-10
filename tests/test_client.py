"""Unit tests for the A2A client — mock HTTP, no real server."""
from __future__ import annotations

import json

import httpx
import pytest

from hivemind_a2a_agent_plugin._client import A2AClient, AgentCard, AgentSkill


# ---------------------------------------------------------------------------
# Helpers — build minimal JSON-RPC response bodies
# ---------------------------------------------------------------------------

def _send_response(text: str, rpc_id: str = "test-id") -> dict:
    return {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "result": {
            "artifacts": [
                {"parts": [{"type": "text", "text": text}]}
            ]
        },
    }


def _error_response(code: int, message: str, rpc_id: str = "test-id") -> dict:
    return {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "error": {"code": code, "message": message},
    }


AGENT_CARD_DICT = {
    "name": "TestAgent",
    "description": "A test agent",
    "url": "http://localhost:9999",
    "version": "1.0",
    "capabilities": {"streaming": True},
    "skills": [
        {
            "id": "qa",
            "name": "Q&A",
            "description": "Answers questions",
            "tags": ["qa"],
            "examples": ["What is 2+2?"],
        }
    ],
}


# ---------------------------------------------------------------------------
# AgentCard / AgentSkill parsing
# ---------------------------------------------------------------------------

class TestAgentCard:
    def test_from_dict_basic(self):
        card = AgentCard.from_dict(AGENT_CARD_DICT)
        assert card.name == "TestAgent"
        assert card.streaming is True
        assert len(card.skills) == 1
        assert card.skills[0].id == "qa"

    def test_from_dict_no_capabilities(self):
        d = {**AGENT_CARD_DICT, "capabilities": {}}
        card = AgentCard.from_dict(d)
        assert card.streaming is False

    def test_from_dict_no_skills(self):
        d = {**AGENT_CARD_DICT, "skills": []}
        card = AgentCard.from_dict(d)
        assert card.skills == []

    def test_skill_from_dict(self):
        skill = AgentSkill.from_dict(AGENT_CARD_DICT["skills"][0])
        assert skill.id == "qa"
        assert "qa" in skill.tags
        assert skill.examples == ["What is 2+2?"]


# ---------------------------------------------------------------------------
# A2AClient.fetch_agent_card
# ---------------------------------------------------------------------------

class TestFetchAgentCard:
    def test_fetch_success(self, httpx_mock):
        httpx_mock.add_response(
            method="GET",
            url="http://localhost:9999/.well-known/agent.json",
            json=AGENT_CARD_DICT,
        )
        client = A2AClient("http://localhost:9999")
        card = client.fetch_agent_card()
        assert card.name == "TestAgent"
        assert card.streaming is True

    def test_fetch_http_error(self, httpx_mock):
        httpx_mock.add_response(
            method="GET",
            url="http://localhost:9999/.well-known/agent.json",
            status_code=404,
        )
        client = A2AClient("http://localhost:9999")
        with pytest.raises(httpx.HTTPStatusError):
            client.fetch_agent_card()


# ---------------------------------------------------------------------------
# A2AClient.send_task
# ---------------------------------------------------------------------------

class TestSendTask:
    def test_send_basic(self, httpx_mock):
        httpx_mock.add_response(
            method="POST",
            url="http://localhost:9999",
            json=_send_response("Paris is the capital of France."),
        )
        client = A2AClient("http://localhost:9999")
        result = client.send_task("What is the capital of France?", lang="en-us")
        assert result == "Paris is the capital of France."

    def test_send_with_session(self, httpx_mock):
        def _capture(request: httpx.Request):
            body = json.loads(request.content)
            assert body["params"]["sessionId"] == "sess-123"
            return httpx.Response(200, json=_send_response("ok"))

        httpx_mock.add_callback(_capture, method="POST", url="http://localhost:9999")
        client = A2AClient("http://localhost:9999")
        result = client.send_task("hello", session_id="sess-123")
        assert result == "ok"

    def test_send_rpc_error(self, httpx_mock):
        httpx_mock.add_response(
            method="POST",
            url="http://localhost:9999",
            json=_error_response(-32603, "internal error"),
        )
        client = A2AClient("http://localhost:9999")
        with pytest.raises(RuntimeError, match="A2A RPC error"):
            client.send_task("hello")

    def test_send_empty_response(self, httpx_mock):
        httpx_mock.add_response(
            method="POST",
            url="http://localhost:9999",
            json={"jsonrpc": "2.0", "id": "x", "result": {}},
        )
        client = A2AClient("http://localhost:9999")
        result = client.send_task("hello")
        assert result == ""

    def test_send_message_fallback(self, httpx_mock):
        """result.message fallback path."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost:9999",
            json={
                "jsonrpc": "2.0",
                "id": "x",
                "result": {
                    "message": {"parts": [{"type": "text", "text": "fallback answer"}]}
                },
            },
        )
        client = A2AClient("http://localhost:9999")
        result = client.send_task("hello")
        assert result == "fallback answer"

    def test_send_auth_header_forwarded(self, httpx_mock):
        def _capture(request: httpx.Request):
            assert request.headers["authorization"] == "Bearer secret"
            return httpx.Response(200, json=_send_response("authed"))

        httpx_mock.add_callback(_capture, method="POST", url="http://localhost:9999")
        client = A2AClient("http://localhost:9999", auth_header="Bearer secret")
        result = client.send_task("hello")
        assert result == "authed"


# ---------------------------------------------------------------------------
# A2AClient.stream_task (SSE)
# ---------------------------------------------------------------------------

def _sse_body(*chunks: str, done: bool = True) -> bytes:
    lines = []
    for chunk in chunks:
        event = {
            "result": {
                "delta": {"parts": [{"type": "text", "text": chunk}]}
            }
        }
        lines.append(f"data: {json.dumps(event)}\n\n")
    if done:
        lines.append("data: [DONE]\n\n")
    return "".join(lines).encode()


class TestStreamTask:
    def test_stream_basic(self, httpx_mock):
        httpx_mock.add_response(
            method="POST",
            url="http://localhost:9999",
            content=_sse_body("Hello", " world"),
            headers={"Content-Type": "text/event-stream"},
        )
        client = A2AClient("http://localhost:9999", streaming=True)
        chunks = list(client.stream_task("hi"))
        assert chunks == ["Hello", " world"]

    def test_stream_skips_empty_lines(self, httpx_mock):
        body = b"data: \n\ndata: [DONE]\n\n"
        httpx_mock.add_response(
            method="POST",
            url="http://localhost:9999",
            content=body,
            headers={"Content-Type": "text/event-stream"},
        )
        client = A2AClient("http://localhost:9999", streaming=True)
        chunks = list(client.stream_task("hi"))
        assert chunks == []

    def test_stream_artifact_key(self, httpx_mock):
        event = {
            "result": {
                "artifact": {"parts": [{"type": "text", "text": "artifact chunk"}]}
            }
        }
        body = f"data: {json.dumps(event)}\n\ndata: [DONE]\n\n".encode()
        httpx_mock.add_response(
            method="POST",
            url="http://localhost:9999",
            content=body,
            headers={"Content-Type": "text/event-stream"},
        )
        client = A2AClient("http://localhost:9999", streaming=True)
        chunks = list(client.stream_task("hi"))
        assert chunks == ["artifact chunk"]
