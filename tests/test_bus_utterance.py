"""BUS ``recognizer_loop:utterance`` is answered on the agent bus."""
from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from ovos_bus_client.message import Message

from hivemind_a2a_agent_plugin import A2AAgentProtocol

PEER = "sat::session-1"
SESSION = {"session_id": "ns:sat-session"}


def _protocol(streaming: bool = False) -> A2AAgentProtocol:
    with patch("hivemind_a2a_agent_plugin.A2AClient") as MockClient:
        proto = A2AAgentProtocol(config={"agent_url": "http://a2a.test"})
    proto._agent_card_fetched = True
    proto._client = MockClient.return_value
    proto._client.streaming = streaming
    return proto


def _utterance(text: str = "what is the capital of portugal") -> Message:
    """The message hivemind-core places on the agent bus for a BUS utterance."""
    return Message(
        "recognizer_loop:utterance",
        {"utterances": [text], "lang": "pt-PT"},
        {"source": PEER, "peer": PEER, "destination": "skills",
         "session": dict(SESSION)},
    )


def _run(proto: A2AAgentProtocol, message: Message, timeout: float = 5.0):
    """Emit *message* and return every message the bus saw, once handled."""
    seen = []
    done = threading.Event()
    proto.bus.on("message", lambda raw: seen.append(Message.deserialize(raw)))
    proto.bus.on("ovos.utterance.handled", lambda _: done.set())
    proto.bus.emit(message)
    assert done.wait(timeout), "ovos.utterance.handled never emitted"
    return seen


def test_utterance_is_answered_with_speak_and_handled():
    proto = _protocol()
    proto._client.send_task.return_value = "Lisbon."

    seen = _run(proto, _utterance())

    replies = [m for m in seen if m.msg_type != "recognizer_loop:utterance"]
    assert [m.msg_type for m in replies] == ["speak", "ovos.utterance.handled"]
    speak = replies[0]
    assert speak.data == {"utterance": "Lisbon.", "lang": "pt-PT"}
    assert speak.context["destination"] == PEER
    assert replies[1].context["destination"] == PEER
    proto._client.send_task.assert_called_once_with(
        message_text="what is the capital of portugal",
        session_id=SESSION["session_id"], lang="pt-PT")


def test_streamed_chunks_each_become_a_speak():
    proto = _protocol(streaming=True)
    proto._client.stream_task.return_value = iter(["One.", "Two."])

    seen = _run(proto, _utterance())

    assert [m.data["utterance"] for m in seen if m.msg_type == "speak"] == ["One.", "Two."]


def test_agent_failure_is_spoken_and_handled():
    proto = _protocol()
    proto._client.send_task.side_effect = RuntimeError("agent down")

    seen = _run(proto, _utterance())

    spoken = [m.data["utterance"] for m in seen if m.msg_type == "speak"]
    assert spoken == ["Error contacting A2A agent: agent down"]
    assert seen[-1].msg_type == "ovos.utterance.handled"


def test_follow_up_reuses_the_session_id():
    proto = _protocol()
    proto._client.send_task.return_value = "ok"

    _run(proto, _utterance("first"))
    _run(proto, _utterance("and its population?"))

    sessions = [c.kwargs["session_id"] for c in proto._client.send_task.call_args_list]
    assert sessions == [SESSION["session_id"]] * 2


def test_replies_are_sent_to_the_connected_peer_only():
    proto = _protocol()
    proto._client.send_task.return_value = "Lisbon."
    satellite, other = MagicMock(), MagicMock()
    proto.hm_protocol = SimpleNamespace(clients={PEER: satellite, "other::s": other})

    _run(proto, _utterance())

    sent = [c.args[0].payload for c in satellite.send.call_args_list]
    assert [m.msg_type for m in sent] == ["speak", "ovos.utterance.handled"]
    assert sent[0].data["utterance"] == "Lisbon."
    other.send.assert_not_called()


def test_the_request_is_not_sent_back_to_its_sender():
    proto = _protocol()
    proto._client.send_task.return_value = "x"
    satellite = MagicMock()
    proto.hm_protocol = SimpleNamespace(clients={PEER: satellite})
    request = _utterance()
    request.context["destination"] = PEER

    _run(proto, request)

    assert all(c.args[0].payload.msg_type != "recognizer_loop:utterance"
               for c in satellite.send.call_args_list)


@pytest.mark.parametrize("emit_legacy", ["true", "false"])
@pytest.mark.parametrize("topic", ["recognizer_loop:utterance", "ovos.utterance.handle"])
@pytest.mark.parametrize("session", [SESSION, {"session_id": "default"}, None],
                         ids=["namespaced", "default", "no-session"])
def test_each_utterance_topic_is_answered_exactly_once(
        monkeypatch, topic, emit_legacy, session):
    monkeypatch.setenv("OVOS_BUS_EMIT_LEGACY", emit_legacy)
    proto = _protocol()
    proto._client.send_task.return_value = "Lisbon."
    message = _utterance()
    message.msg_type = topic
    if session is None:
        del message.context["session"]
    else:
        message.context["session"] = dict(session)

    seen = _run(proto, message)
    threading.Event().wait(0.3)  # a duplicate answer would land here

    assert [m.data["utterance"] for m in seen if m.msg_type == "speak"] == ["Lisbon."]
    assert [m.msg_type for m in seen].count("ovos.utterance.handled") == 1
    proto._client.send_task.assert_called_once()


def test_a_failed_thread_start_leaves_the_session_usable(monkeypatch):
    proto = _protocol()
    proto._client.send_task.return_value = "ok"
    real_start = threading.Thread.start
    calls = []

    def start_once_broken(thread):
        calls.append(thread.name)
        if len(calls) == 1:
            raise RuntimeError("can't start new thread")
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start_once_broken)
    with pytest.raises(RuntimeError):
        proto.handle_utterance(_utterance("one"))
    assert proto._pending == {}
    assert proto._workers == 0

    seen = _run(proto, _utterance("two"))

    assert [m.data["utterance"] for m in seen if m.msg_type == "speak"] == ["ok"]
    proto._client.send_task.assert_called_once_with(
        message_text="two", session_id=SESSION["session_id"], lang="pt-PT")


def test_queued_utterances_of_a_gone_peer_are_not_sent_to_the_agent(caplog):
    proto = _protocol()
    clients = {PEER: MagicMock()}
    proto.hm_protocol = SimpleNamespace(clients=clients)
    first_running, release = threading.Event(), threading.Event()
    sent = []

    def send_task(message_text, **_):
        sent.append(message_text)
        first_running.set()
        assert release.wait(5)
        return "ok"

    proto._client.send_task.side_effect = send_task
    handled = []
    proto.bus.on("ovos.utterance.handled", lambda m: handled.append(m))
    proto.handle_utterance(_utterance("one"))
    assert first_running.wait(5)
    proto.handle_utterance(_utterance("two"))
    proto.handle_utterance(_utterance("three"))
    del clients[PEER]
    with patch("hivemind_a2a_agent_plugin.LOG") as log:
        release.set()
        deadline = time.time() + 5
        while proto._pending and time.time() < deadline:
            time.sleep(0.01)

    assert sent == ["one"]
    warnings = [c.args[0] for c in log.warning.call_args_list
                if "is gone" in c.args[0]]
    assert len(warnings) == 2
    assert all(PEER in w and SESSION["session_id"] in w for w in warnings)


def test_blank_utterances_are_not_sent_to_the_agent():
    for utterances in ([""], ["  "], []):
        proto = _protocol()
        message = _utterance()
        message.data["utterances"] = utterances

        seen = _run(proto, message)

        assert [m.msg_type for m in seen] == [
            "recognizer_loop:utterance", "ovos.utterance.handled"], utterances
        proto._client.send_task.assert_not_called()


def test_calls_of_one_session_never_overlap_and_other_sessions_do():
    proto = _protocol()
    lock, state = threading.Lock(), {"now": 0, "max": 0}
    release = threading.Event()

    def send_task(message_text, session_id, lang):
        with lock:
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
        release.wait(0.3)
        with lock:
            state["now"] -= 1
        return "ok"

    proto._client.send_task.side_effect = send_task
    same = [_utterance(f"u{i}") for i in range(4)]
    handled = threading.Semaphore(0)
    proto.bus.on("ovos.utterance.handled", lambda _: handled.release())
    for m in same:
        proto.bus.emit(m)
    for _ in same:
        assert handled.acquire(timeout=10)
    assert state["max"] == 1

    state["max"] = 0
    others = []
    for i in range(3):
        m = _utterance(f"o{i}")
        m.context["session"] = {"session_id": f"other-{i}"}
        others.append(m)
    for m in others:
        proto.bus.emit(m)
    for _ in others:
        assert handled.acquire(timeout=10)
    assert state["max"] > 1


def test_worker_threads_are_bounded():
    proto = _protocol()
    release = threading.Event()
    proto._client.send_task.side_effect = lambda **kw: release.wait(5) and "ok"
    before = threading.active_count()
    for i in range(60):
        m = _utterance(f"u{i}")
        m.context["session"] = {"session_id": f"s{i}"}
        proto.bus.emit(m)
    grown = threading.active_count() - before
    release.set()
    assert grown <= 16


def test_reply_to_a_disconnected_peer_is_logged():
    proto = _protocol()
    clients = {PEER: MagicMock()}
    proto.hm_protocol = SimpleNamespace(clients=clients)

    def send_task(**_):
        clients.clear()  # the peer leaves while the agent works
        return "Lisbon."

    proto._client.send_task.side_effect = send_task

    with patch("hivemind_a2a_agent_plugin.LOG") as log:
        _run(proto, _utterance())

    texts = [c.args[0] for c in log.warning.call_args_list]
    assert any(PEER in t and SESSION["session_id"] in t and "speak" in t for t in texts), texts
    assert not any("recognizer_loop:utterance" in t for t in texts)


def _session_utterance(text: str, session_id: str) -> Message:
    m = _utterance(text)
    m.context["session"] = {"session_id": session_id}
    return m


def test_a_queued_session_does_not_hold_workers_from_other_sessions():
    proto = _protocol()
    gate = threading.Event()

    def send_task(message_text, session_id, lang):
        if session_id == "busy":
            gate.wait(10)
        return "ok"

    proto._client.send_task.side_effect = send_task
    victim_done = threading.Event()
    proto.bus.on("ovos.utterance.handled", lambda m: victim_done.set()
                 if m.context["session"]["session_id"] == "victim" else None)
    for i in range(40):
        proto.bus.emit(_session_utterance(f"slow{i}", "busy"))
    proto.bus.emit(_session_utterance("hello", "victim"))

    answered = victim_done.wait(5)
    gate.set()
    assert answered, "another session's answer waited behind the queued session"


class _LifoLock:
    """A correct mutex that wakes its newest waiter first.

    threading.Lock promises no order among waiters, so a design that makes
    calls wait on one may run them out of arrival order.
    """

    def __init__(self):
        self._cond = threading.Condition(threading.RLock())
        self._held = False
        self._waiters = []

    def acquire(self, blocking=True, timeout=-1):
        with self._cond:
            if not self._held and not self._waiters:
                self._held = True
                return True
            ticket = object()
            self._waiters.append(ticket)
            while self._held or self._waiters[-1] is not ticket:
                self._cond.wait(0.05)
            self._waiters.pop()
            self._held = True
            return True

    def release(self):
        with self._cond:
            self._held = False
            self._cond.notify_all()

    def locked(self):
        return self._held

    __enter__ = acquire

    def __exit__(self, *exc):
        self.release()


def test_calls_of_one_session_run_in_arrival_order(monkeypatch):
    proto = _protocol()
    monkeypatch.setattr("hivemind_a2a_agent_plugin.threading", SimpleNamespace(
        Lock=_LifoLock, Thread=threading.Thread))
    order, gate = [], threading.Event()
    done = threading.Semaphore(0)

    def send_task(message_text, session_id, lang):
        if not order:
            gate.wait(5)
        order.append(message_text)
        return "ok"

    proto._client.send_task.side_effect = send_task
    proto.bus.on("ovos.utterance.handled", lambda _: done.release())
    for i in range(6):
        proto.bus.emit(_session_utterance(f"u{i}", "s"))
        time.sleep(0.1)  # let each call reach its wait before the next arrives
    gate.set()
    for _ in range(6):
        assert done.acquire(timeout=10)
    assert order == [f"u{i}" for i in range(6)]


def test_worker_threads_are_daemons():
    proto = _protocol()
    proto._client.send_task.return_value = "ok"
    _run(proto, _utterance())
    workers = [t for t in threading.enumerate() if t.name.startswith("a2a-utterance")]
    assert workers
    assert all(t.daemon for t in workers)
