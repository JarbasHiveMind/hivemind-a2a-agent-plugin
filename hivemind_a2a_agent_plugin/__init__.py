"""HiveMind A2A agent protocol plugin.

Bridges HiveMind natural-language queries to external A2A (Agent-to-Agent)
agents.  Any utterance arriving from hive satellites, as a ``QUERY`` or as a
``BUS`` ``recognizer_loop:utterance`` or ``ovos.utterance.handle``, is sent to a configured A2A agent with
``SendMessage`` (or ``SendStreamingMessage``), and the answer is streamed back
as ``speak`` messages.

The plugin is registered under the ``hivemind.agent.protocol`` entry-point
group so hivemind-core discovers it automatically.
"""
from __future__ import annotations

import dataclasses
import threading
import queue
from collections import deque
from typing import Any, Dict, Iterator, Optional

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message
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
_MAX_WORKERS = 16


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
    it only owns the outbound HTTP connection to the A2A agent. hivemind-core
    calls :meth:`natural_language_query` with no session id, so each query is
    sent without a ``contextId`` and the agent starts a new conversation.

    A ``BUS`` ``recognizer_loop:utterance`` or ``ovos.utterance.handle`` that
    hivemind-core places on the agent bus is answered too. The Layer-1 session id keys the conversation,
    so follow-ups keep their ``contextId``. Each answer chunk is emitted as a
    ``speak`` reply and the last one is followed by ``ovos.utterance.handled``.
    hivemind-core does not listen on the agent bus, so the plugin sends each
    reply whose ``destination`` names a connected peer to that peer.
    """

    config: Dict[str, Any] = dataclasses.field(default_factory=dict)

    # Internal state — not part of the public interface.
    _client: Optional[A2AClient] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    _agent_url: Optional[str] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    _agent_card: Optional[Any] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    _agent_card_fetched: bool = dataclasses.field(
        default=False, init=False, repr=False, compare=False
    )
    _work: queue.SimpleQueue = dataclasses.field(
        default_factory=queue.SimpleQueue, init=False, repr=False, compare=False
    )
    # session id -> utterances that wait for the running call of that session.
    # A session is a key only while one of its calls is in flight.
    _pending: dict[str, deque] = dataclasses.field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    _pending_guard: threading.Lock = dataclasses.field(
        default_factory=threading.Lock, init=False, repr=False, compare=False
    )
    _workers: int = dataclasses.field(default=0, init=False, repr=False, compare=False)
    _idle: int = dataclasses.field(default=0, init=False, repr=False, compare=False)

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
            self._agent_url = url
            self._client = A2AClient(
                base_url=url,
                auth_header=auth,
                timeout=timeout,
                streaming=streaming,
            )
            LOG.info(f"A2AAgentProtocol: configured for A2A agent at {url}")

        self.bus.on("recognizer_loop:utterance", self.handle_utterance)
        self.bus.on("ovos.utterance.handle", self.handle_utterance)
        self.bus.on("message", self.handle_internal_message)

    # ------------------------------------------------------------------
    # Layer-1 bus mode (HIVEMIND-AGENT-1 section 3)
    # ------------------------------------------------------------------

    def handle_utterance(self, message: Message) -> None:
        """Answer a ``recognizer_loop:utterance`` or ``ovos.utterance.handle``.

        The A2A call can take as long as the configured timeout, so it runs on
        a daemon worker thread and does not hold up the connection that delivered the
        message. A blank utterance is not sent to the agent.
        """
        utterances = message.data.get("utterances") or []
        if not utterances or not str(utterances[0]).strip():
            self.bus.emit(message.reply("ovos.utterance.handled", {}))
            return
        session = message.context.get("session") or {}
        lang = (message.data.get("lang") or message.context.get("lang")
                or session.get("lang") or "en-US")
        job = (message, str(utterances[0]), lang, session.get("session_id"))
        session_id = job[3]
        with self._pending_guard:
            if session_id in self._pending:
                self._pending[session_id].append(job)
                return
            if self._work.qsize() >= self._idle and self._workers < _MAX_WORKERS:
                threading.Thread(target=self._worker, daemon=True,
                                 name=f"a2a-utterance-{self._workers + 1}").start()
                self._workers += 1
            if session_id:
                self._pending[session_id] = deque()
            self._work.put(job)

    def _worker(self) -> None:
        while True:
            with self._pending_guard:
                self._idle += 1
            job = self._work.get()
            with self._pending_guard:
                self._idle -= 1
            self._drain(job)

    def _drain(self, job) -> None:
        """Answer *job*, then the utterances that queued behind it in its session.

        One session holds one worker at a time and its calls run in arrival
        order, so two first utterances cannot start two A2A conversations. A
        call that waits for its session holds no worker.
        """
        while job:
            peer = job[0].context.get("source")
            if self.hm_protocol and peer not in self.clients:
                LOG.warning(f"A2AAgentProtocol: peer {peer} is gone; utterance "
                            f"dropped (session {job[3]})")
            else:
                try:
                    self._answer_utterance(*job)
                except Exception as exc:  # noqa: BLE001
                    LOG.error(f"A2AAgentProtocol: could not finish an utterance: "
                              f"{exc}", exc_info=True)
            session_id = job[3]
            if not session_id:
                return
            with self._pending_guard:
                waiting = self._pending[session_id]
                job = waiting.popleft() if waiting else None
                if job is None:
                    del self._pending[session_id]

    def _answer_utterance(self, message: Message, utterance: str, lang: str,
                          session_id: str | None) -> None:
        try:
            for chunk in self.natural_language_query(utterance, lang, session_id):
                if chunk is None:
                    break
                self.bus.emit(message.reply(
                    "speak", {"utterance": chunk, "lang": lang}))
        except Exception as exc:  # noqa: BLE001
            LOG.error(f"A2AAgentProtocol: error answering utterance: {exc}",
                      exc_info=True)
            self.bus.emit(message.reply(
                "speak", {"utterance": f"Error contacting A2A agent: {exc}",
                          "lang": lang}))
        finally:
            self.bus.emit(message.reply("ovos.utterance.handled", {}))

    def handle_internal_message(self, raw: str) -> None:
        """Send a Layer-1 reply to the connected peers its destination names.

        A request from a peer has that peer as its ``source``. It is not a
        reply, so it is never sent back.
        """
        message = Message.deserialize(raw)
        destination = message.context.get("destination") or []
        if not isinstance(destination, list):
            destination = [destination]
        clients = dict(self.clients)
        if message.context.get("source") in clients:
            return
        origin = message.context.get("peer")
        is_response = isinstance(origin, str) and message.context.get("source") != origin
        for peer in destination:
            client = clients.get(peer)
            if client is None:
                if is_response and peer == origin:
                    session = message.context.get("session") or {}
                    LOG.warning(
                        f"A2AAgentProtocol: {message.msg_type} - destination "
                        f"{destination} names no connected peer; response dropped "
                        f"(session {session.get('session_id')})")
                continue
            try:
                client.send(HiveMessage(HiveMessageType.BUS, payload=message,
                                        source_peer=peer, target_peers=[peer]))
            except Exception as exc:  # noqa: BLE001
                LOG.warning(f"A2AAgentProtocol: could not send {message.msg_type} "
                            f"to {peer}: {exc}")

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
            session_id: Optional local conversation id. hivemind-core passes
                        none, so an answer to a question from the agent
                        starts a new task. A caller that passes one keeps the
                        agent-assigned ``contextId`` and continues a task that
                        waits for input.

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

        if not self._agent_card_fetched:
            self._agent_card_fetched = True
            try:
                card = self._client.fetch_agent_card()
                if not card or not getattr(card, "name", None):
                    raise ValueError("agent card is missing or invalid")
                self._agent_card = card
            except Exception as exc:
                LOG.error(
                    f"A2AAgentProtocol: failed to validate agent card at "
                    f"{self._agent_url}: {exc}", exc_info=True
                )
                yield (
                    f"Error validating A2A agent at {self._agent_url}: "
                    f"could not fetch or parse its agent card ({exc})"
                )
                yield None
                return

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
