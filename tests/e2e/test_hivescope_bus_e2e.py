"""hivescope e2e: a satellite BUS utterance is answered by the A2A agent.

A real ``hivemind-core`` listener protocol with ``A2AAgentProtocol`` as the
master's agent, pointed at the real a2a-sdk agent (tests/a2a_agent.py). Two
satellites send ``BUS`` messages that carry an utterance. Each hears only the
answers to its own.
"""
from __future__ import annotations

import time

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivescope.topology import TopologyBuilder
from ovos_bus_client.message import Message

from hivemind_a2a_agent_plugin import A2AAgentProtocol

TYPES = ["recognizer_loop:utterance", "ovos.utterance.handle"]


def _bus(topic: str, utt: str) -> HiveMessage:
    return HiveMessage(HiveMessageType.BUS, payload=Message(
        topic, {"utterances": [utt], "lang": "en-US"}))


def _l1(records) -> list:
    """The Layer-1 messages of recorded BUS frames; hivescope records them as dicts."""
    return [r.payload if isinstance(r.payload, Message)
            else Message(r.payload["type"], r.payload["data"], r.payload["context"])
            for r in records]


def _turn(sat, other, topic: str, utt: str) -> list:
    """Send one utterance and return the Layer-1 messages up to the handled one.

    The other satellite must receive no BUS frame meanwhile."""
    sat.recorder.clear()
    other.recorder.clear()
    sat.send(_bus(topic, utt))
    assert sat.recorder.wait_for(HiveMessageType.BUS.value, direction="in",
                                 timeout=10.0), f"no answer to {utt!r}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        got = _l1(sat.recorder.received(HiveMessageType.BUS.value, direction="in"))
        if any(m.msg_type == "ovos.utterance.handled" for m in got):
            time.sleep(0.3)  # a reply sent to the wrong peer lands in this window
            assert other.recorder.received(HiveMessageType.BUS.value, direction="in") == []
            return got
        time.sleep(0.05)
    raise AssertionError(f"no ovos.utterance.handled for {utt!r}; got {[m.msg_type for m in got]}")


def test_bus_utterance_round_trip_with_two_satellites(a2a_agent_url):
    agent = A2AAgentProtocol(config={"agent_url": a2a_agent_url})
    b = TopologyBuilder()
    m = b.add_master("M0", agent_protocol=agent)
    m.register_satellite("a2a-key", password="a2a-pw", allowed_types=TYPES)
    b.add_satellite("S0", upstream=m, allowed_types=TYPES)
    b.add_satellite("S1", upstream=m, allowed_types=TYPES)
    b.start_all()
    try:
        s0, s1 = b.get_satellite("S0"), b.get_satellite("S1")

        got = _turn(s0, s1, TYPES[0], "remember red")
        assert [(x.msg_type, x.data.get("utterance")) for x in got] == [
            ("speak", "stored"), ("ovos.utterance.handled", None)]
        got = _turn(s1, s0, TYPES[1], "remember green")
        assert [x.data.get("utterance") for x in got if x.msg_type == "speak"] == ["stored"]

        # follow-ups stay in each satellite's own A2A conversation
        assert [x.data["utterance"] for x in _turn(s0, s1, TYPES[0], "recall")
                if x.msg_type == "speak"] == ["red"]
        assert [x.data["utterance"] for x in _turn(s1, s0, TYPES[1], "recall")
                if x.msg_type == "speak"] == ["green"]
    finally:
        b.stop_all()
