"""Thin A2A JSON-RPC 2.0 client.

Vendored subset of ovos-a2a-solver-plugin's A2AClient so the HiveMind
plugin has zero extra dependencies beyond ``httpx``.  If ovos-a2a-solver-plugin
is importable its richer implementation is preferred at import time via the
``__init__`` module; this copy is the fallback.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urljoin

import httpx

from ovos_utils.log import LOG


@dataclass
class AgentSkill:
    """A capability advertised in an agent card."""
    id: str
    name: str
    description: str = ""
    tags: List[str] = field(default_factory=list)
    examples: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentSkill":
        return cls(
            id=d.get("id", ""),
            name=d.get("name", ""),
            description=d.get("description", ""),
            tags=d.get("tags", []),
            examples=d.get("examples", []),
        )


@dataclass
class AgentCard:
    """Parsed A2A agent card (``/.well-known/agent.json``)."""
    name: str
    description: str
    url: str
    version: str = "1.0"
    skills: List[AgentSkill] = field(default_factory=list)
    streaming: bool = False
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentCard":
        skills = [AgentSkill.from_dict(s) for s in d.get("skills", [])]
        caps = d.get("capabilities", {})
        return cls(
            name=d.get("name", ""),
            description=d.get("description", ""),
            url=d.get("url", ""),
            version=d.get("version", "1.0"),
            skills=skills,
            streaming=caps.get("streaming", False),
            raw=d,
        )


class A2AClient:
    """Minimal A2A JSON-RPC 2.0 client.

    Handles:
    - Agent-card discovery (``GET /.well-known/agent.json``)
    - Task submission via ``tasks/send`` (blocking)
    - Streaming via ``tasks/sendSubscribe`` (SSE)

    Args:
        base_url:    Root URL of the A2A server.
        auth_header: Optional ``Authorization`` header value.
        timeout:     HTTP timeout in seconds (default 60).
        streaming:   Prefer SSE streaming when True.
    """

    AGENT_CARD_PATH = "/.well-known/agent.json"
    JSONRPC_VERSION = "2.0"

    def __init__(
        self,
        base_url: str,
        auth_header: Optional[str] = None,
        timeout: float = 60.0,
        streaming: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.streaming = streaming
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if auth_header:
            headers["Authorization"] = auth_header
        self._http = httpx.Client(headers=headers, timeout=timeout)

    def fetch_agent_card(self) -> AgentCard:
        """Fetch and parse ``/.well-known/agent.json``."""
        url = self.base_url + self.AGENT_CARD_PATH
        LOG.debug(f"A2AClient: fetching agent card from {url}")
        resp = self._http.get(url)
        resp.raise_for_status()
        card = AgentCard.from_dict(resp.json())
        LOG.debug(f"A2AClient: discovered agent '{card.name}' at {card.url}")
        return card

    def send_task(
        self,
        message_text: str,
        session_id: Optional[str] = None,
        lang: Optional[str] = None,
        history: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        """Submit a task (blocking) and return the final text response."""
        rpc_id = str(uuid.uuid4())
        parts: List[Dict[str, Any]] = [{"type": "text", "text": message_text}]
        msg: Dict[str, Any] = {"role": "user", "parts": parts}
        if lang:
            msg["metadata"] = {"lang": lang}
        params: Dict[str, Any] = {"id": rpc_id, "message": msg}
        if session_id:
            params["sessionId"] = session_id
        if history:
            params["history"] = [
                {"role": t["role"],
                 "parts": [{"type": "text", "text": t["content"]}]}
                for t in history
            ]

        payload = {
            "jsonrpc": self.JSONRPC_VERSION,
            "id": rpc_id,
            "method": "tasks/send",
            "params": params,
        }
        resp = self._http.post(self.base_url, content=json.dumps(payload))
        resp.raise_for_status()
        return self._extract_text(resp.json(), rpc_id)

    def stream_task(
        self,
        message_text: str,
        session_id: Optional[str] = None,
        lang: Optional[str] = None,
        history: Optional[List[Dict[str, str]]] = None,
    ) -> Generator[str, None, None]:
        """Submit a task and yield text chunks via SSE (``tasks/sendSubscribe``)."""
        rpc_id = str(uuid.uuid4())
        parts: List[Dict[str, Any]] = [{"type": "text", "text": message_text}]
        msg: Dict[str, Any] = {"role": "user", "parts": parts}
        if lang:
            msg["metadata"] = {"lang": lang}
        params: Dict[str, Any] = {"id": rpc_id, "message": msg}
        if session_id:
            params["sessionId"] = session_id
        if history:
            params["history"] = [
                {"role": t["role"],
                 "parts": [{"type": "text", "text": t["content"]}]}
                for t in history
            ]

        payload = {
            "jsonrpc": self.JSONRPC_VERSION,
            "id": rpc_id,
            "method": "tasks/sendSubscribe",
            "params": params,
        }

        with self._http.stream(
            "POST", self.base_url,
            content=json.dumps(payload),
            headers={"Accept": "text/event-stream"},
        ) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                line = line.strip()
                if not line or not line.startswith("data:"):
                    continue
                data_str = line[len("data:"):].strip()
                if data_str == "[DONE]":
                    break
                try:
                    event = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                chunk = self._extract_stream_chunk(event)
                if chunk:
                    yield chunk

    @staticmethod
    def _extract_text(body: Dict[str, Any], rpc_id: str) -> str:
        if "error" in body:
            err = body["error"]
            raise RuntimeError(
                f"A2A RPC error {err.get('code')}: {err.get('message')}"
            )
        result = body.get("result", {})
        for artifact in result.get("artifacts", []):
            for part in artifact.get("parts", []):
                if part.get("type") == "text":
                    return part["text"]
        msg = result.get("message", {})
        for part in msg.get("parts", []):
            if part.get("type") == "text":
                return part["text"]
        LOG.warning(f"A2AClient: could not extract text from response: {body}")
        return ""

    @staticmethod
    def _extract_stream_chunk(event: Dict[str, Any]) -> str:
        result = event.get("result", {})
        for key in ("delta", "artifact"):
            artifact = result.get(key, {})
            for part in artifact.get("parts", []):
                if part.get("type") == "text":
                    return part["text"]
        return ""

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "A2AClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
