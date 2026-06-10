"""HiveMind A2A agent protocol plugin.

Bridges HiveMind natural-language queries to external A2A (Agent-to-Agent)
agents.  Any utterance arriving from hive satellites is forwarded to a
configured A2A server via JSON-RPC 2.0 (``tasks/send`` / ``tasks/sendSubscribe``),
and the response is streamed back through the HiveMind session.

The plugin is registered under the ``hivemind.agent.protocol`` entry-point
group so hivemind-core discovers it automatically.
"""
from __future__ import annotations

import dataclasses
from typing import Any, Dict, Iterator, Optional

from ovos_utils.log import LOG

from hivemind_plugin_manager.protocols import AgentProtocol

from hivemind_a2a_agent_plugin._client import A2AClient
from hivemind_a2a_agent_plugin.version import __version__

# Default configuration keys (all live under hivemind → a2a_agent in
# the OVOS config tree, but can also be supplied as plain kwargs).
_CFG_URL = "agent_url"
_CFG_AUTH = "auth_header"
_CFG_TIMEOUT = "timeout"
_CFG_STREAMING = "streaming"

_DEFAULT_TIMEOUT = 60.0


@dataclasses.dataclass()
class A2AAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that delegates NL queries to an A2A agent.

    Configuration (passed via ``config`` dict or the OVOS ``Configuration``
    key ``"hivemind" → "a2a_agent"``):

    .. code-block:: yaml

       hivemind:
         a2a_agent:
           agent_url: "http://localhost:9999"
           auth_header: "Bearer secret"   # optional
           timeout: 60                    # seconds, optional
           streaming: false               # prefer SSE when true, optional

    The plugin is stateless with respect to the HiveMind client transport —
    it only owns the outbound HTTP connection to the A2A server and maps
    HiveMind session IDs to A2A ``sessionId`` values so conversation context
    is preserved across multi-turn exchanges.
    """

    config: Dict[str, Any] = dataclasses.field(default_factory=dict)

    # Internal state — not part of the public interface.
    _client: Optional[A2AClient] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        # Merge OVOS global config if available, but explicit kwargs win.
        try:
            from ovos_config import Configuration
            cfg_root = Configuration()
            ovos_a2a = cfg_root.get("hivemind", {}).get("a2a_agent", {})
        except Exception:  # pragma: no cover
            ovos_a2a = {}

        merged: Dict[str, Any] = {**ovos_a2a, **self.config}

        url: Optional[str] = merged.get(_CFG_URL)
        if not url:
            LOG.warning(
                "A2AAgentProtocol: no agent_url configured — "
                "natural_language_query will return error answers"
            )
        else:
            auth = merged.get(_CFG_AUTH)
            timeout = float(merged.get(_CFG_TIMEOUT, _DEFAULT_TIMEOUT))
            streaming = bool(merged.get(_CFG_STREAMING, False))
            self._client = A2AClient(
                base_url=url,
                auth_header=auth,
                timeout=timeout,
                streaming=streaming,
            )
            LOG.info(f"A2AAgentProtocol: connected to A2A agent at {url}")

    # ------------------------------------------------------------------
    # AgentProtocol implementation
    # ------------------------------------------------------------------

    def natural_language_query(
        self,
        utterance: str,
        lang: str,
        session_id: Optional[str] = None,
    ) -> "Iterator[Optional[str]]":
        """Forward *utterance* to the A2A agent and yield response chunks.

        Args:
            utterance: The user's text, as received from a hive satellite.
            lang:      BCP-47 language tag (e.g. ``"en-us"``).  Forwarded
                       as context metadata so A2A agents can apply
                       language-specific processing.
            session_id: HiveMind session identifier; mapped 1-to-1 to the
                        A2A ``sessionId`` so multi-turn context is preserved.

        Yields:
            Non-empty text chunks from the agent response, terminated by
            ``None`` (the AgentProtocol sentinel).  On any error a
            human-readable error string is yielded before the sentinel so
            callers always get at least one answer.
        """
        if self._client is None:
            yield "A2A agent not configured — no agent_url set."
            yield None
            return

        LOG.debug(
            f"A2AAgentProtocol: query lang={lang!r} session={session_id!r} "
            f"utterance={utterance!r}"
        )

        try:
            if self._client.streaming:
                yielded_any = False
                for chunk in self._client.stream_task(
                    message_text=utterance,
                    session_id=session_id,
                    lang=lang,
                ):
                    if chunk:
                        yielded_any = True
                        yield chunk
                if not yielded_any:
                    yield "The A2A agent returned an empty streaming response."
            else:
                text = self._client.send_task(
                    message_text=utterance,
                    session_id=session_id,
                    lang=lang,
                )
                if text:
                    yield text
                else:
                    yield "The A2A agent returned an empty response."
        except Exception as exc:
            LOG.error(f"A2AAgentProtocol: error querying A2A agent: {exc}", exc_info=True)
            yield f"Error contacting A2A agent: {exc}"

        yield None


__all__ = ["A2AAgentProtocol", "__version__"]
