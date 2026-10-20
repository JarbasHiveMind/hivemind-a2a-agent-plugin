"""A2AAgentProtocol against a real a2a-sdk agent (see tests/a2a_agent.py).

Every query goes through ``natural_language_query(utterance, lang)``, the
call hivemind-core makes. The hub passes no session id.
"""
from __future__ import annotations

import re

from hivemind_a2a_agent_plugin import A2AAgentProtocol

ECHO = re.compile(r"echo\[(?P<context>[^\]]+)\]: (?P<text>.*)", re.DOTALL)


def query(url: str, utterance: str, streaming: bool = False, lang="en-us", timeout=10) -> list:
    proto = A2AAgentProtocol(config={"agent_url": url, "streaming": streaming, "timeout": timeout})
    return list(proto.natural_language_query(utterance, lang))


def test_blocking_query_yields_the_answer_then_the_sentinel(a2a_agent_url):
    answer, sentinel = query(a2a_agent_url, "what time is it?")
    assert ECHO.fullmatch(answer)["text"] == "what time is it?"
    assert sentinel is None


def test_streaming_query_yields_each_chunk(a2a_agent_url):
    assert query(a2a_agent_url, "chunks stream this", streaming=True) == ["stream ", "this ", None]


def test_each_hub_query_starts_a_new_agent_conversation(a2a_agent_url):
    proto = A2AAgentProtocol(config={"agent_url": a2a_agent_url})
    contexts = [ECHO.fullmatch(next(proto.natural_language_query(turn, "en-us")))["context"]
                for turn in ("turn 1", "turn 2")]
    assert contexts[0] != contexts[1]


def test_satellite_lang_reaches_the_agent(a2a_agent_url):
    answer, _ = query(a2a_agent_url, "lang", lang="pt-PT")
    assert ECHO.fullmatch(answer)["text"] == "lang=pt-PT"


def test_a_question_from_the_agent_reaches_the_satellite(a2a_agent_url):
    assert query(a2a_agent_url, "ask") == ["which city?", None]


def test_failed_remote_task_is_spoken_as_an_error(a2a_agent_url):
    chunks = query(a2a_agent_url, "fail")
    assert chunks[-1] is None
    assert chunks[0] == "Error contacting A2A agent: A2A task ended in TASK_STATE_FAILED: backend exploded"


def test_slow_agent_answers_with_a_timeout_error(a2a_agent_url):
    chunks = query(a2a_agent_url, "slow 5 x", timeout=1)
    assert chunks[-1] is None
    assert chunks[0].startswith("Error contacting A2A agent: ")


def test_unreachable_agent_answers_with_a_card_error():
    chunks = query("http://127.0.0.1:1", "hello", timeout=3)
    assert chunks[-1] is None
    assert chunks[0].startswith("Error validating A2A agent at http://127.0.0.1:1")


def test_agent_killed_mid_query_answers_with_an_error(a2a_agent_process):
    proc, url = a2a_agent_process
    proto = A2AAgentProtocol(config={"agent_url": url, "timeout": 10})
    answer, _ = proto.natural_language_query("warm up", "en-us")
    assert ECHO.fullmatch(answer)["text"] == "warm up"
    killer = __import__("threading").Timer(1.0, proc.kill)
    killer.start()
    chunks = list(proto.natural_language_query("slow 5 x", "en-us"))
    killer.join()
    assert chunks[-1] is None
    assert chunks[0].startswith("Error contacting A2A agent: ")


def test_the_answer_to_a_question_cannot_continue_the_task_through_the_hub(a2a_agent_url):
    """The hub passes no session id, so the plugin cannot link the answer to the question."""
    proto = A2AAgentProtocol(config={"agent_url": a2a_agent_url})
    assert list(proto.natural_language_query("ask", "en-us")) == ["which city?", None]
    answer, _ = proto.natural_language_query("lisbon", "en-us")
    assert ECHO.fullmatch(answer)["text"] == "lisbon"


def test_a_caller_with_a_session_id_continues_the_task(a2a_agent_url):
    proto = A2AAgentProtocol(config={"agent_url": a2a_agent_url})
    assert list(proto.natural_language_query("ask", "en-us", session_id="sat-1")) == ["which city?", None]
    answer, _ = proto.natural_language_query("lisbon", "en-us", session_id="sat-1")
    assert re.fullmatch(r"continued\[[^\]]+\]: lisbon", answer)
