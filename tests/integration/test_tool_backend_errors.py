"""A live backend failing mid-diagnosis costs the model one signal, not the
whole session. Runs run_diagnosis end to end with a scripted chat model (no
API key, no network) and a Kafka gateway whose partition_throughput raises
the same error a dropped metrics-aggregator scrape produced in practice.
"""

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.orchestrator import session
from dp_ops_agent.orchestrator.session import run_diagnosis
from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

# Module-level, not a _ScriptedModel attribute: BaseChatModel is a pydantic
# model, so a class attribute would become a per-instance field.
_SEEN: list[list[BaseMessage]] = []

# Attached to every scripted reply, so session totals are predictable.
_USAGE = {
    "input_tokens": 100,
    "output_tokens": 10,
    "total_tokens": 110,
    "input_token_details": {"cache_read": 60, "cache_creation": 20},
}


class _ScriptedModel(BaseChatModel):
    """Calls hot_partition_skew, then under_replicated_partitions, then
    submits a diagnosis citing the latter's signal_id (read back from its
    ToolMessage, since signal ids are generated at runtime), then ends the
    run with a plain reply. Records every
    message list it's called with so the test can inspect what it saw."""

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools: Any, **kwargs: Any) -> "_ScriptedModel":
        return self

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs):
        _SEEN.append(list(messages))
        turn = sum(isinstance(m, AIMessage) for m in messages)
        if turn == 0:
            call = ("hot_partition_skew", {"topic": "orders", "window_minutes": 5})
        elif turn == 1:
            call = ("under_replicated_partitions", {"topics": ["orders"]})
        elif turn == 2:
            signal_id = json.loads(messages[-1].content)["signal_id"]
            call = (
                "submit_diagnosis",
                {
                    "root_cause_hypothesis": "Under-replicated partitions on orders.",
                    "root_cause_signal_id": signal_id,
                    "confidence": "low",
                    "evidence_chain": [
                        {"step": 1, "signal_id": signal_id, "interpretation": "URP present."}
                    ],
                },
            )
        else:
            return ChatResult(
                generations=[
                    ChatGeneration(message=AIMessage(content="Done.", usage_metadata=_USAGE))
                ]
            )
        name, args = call
        message = AIMessage(
            content="",
            tool_calls=[{"name": name, "args": args, "id": f"call-{turn}"}],
            usage_metadata=_USAGE,
        )
        return ChatResult(generations=[ChatGeneration(message=message)])


class _FailingKafkaGateway(FixtureKafkaGateway):
    def __init__(self, fixture: Path, exc: Exception) -> None:
        super().__init__(fixture)
        self._exc = exc

    async def partition_throughput(self, topic: str, window_minutes: int) -> dict[int, float]:
        raise self._exc


async def _run(tmp_path, monkeypatch, exc: Exception, fail: bool = True):
    _SEEN.clear()
    monkeypatch.setattr(session, "ChatAnthropic", lambda model: _ScriptedModel())
    audit = JsonlAuditSink(tmp_path, "s1")
    kafka_fixture = FIXTURES / "kafka" / "urp_lag_spike_incident.json"
    result = await run_diagnosis(
        session_id="s1",
        alert_text="consumer lag alert on orders",
        kafka_gateway=(
            _FailingKafkaGateway(kafka_fixture, exc) if fail else FixtureKafkaGateway(kafka_fixture)
        ),
        flink_gateway=FixtureFlinkGateway(FIXTURES / "flink" / "healthy_baseline.json"),
        lineage_gateway=FixtureLineageGateway(FIXTURES / "lineage" / "empty.json"),
        dbt_gateway=FixtureDbtGateway(FIXTURES / "dbt" / "healthy_baseline.json"),
        audit=audit,
        model="scripted",
    )
    return result, audit


@pytest.mark.asyncio
async def test_backend_error_is_reported_to_model_and_diagnosis_completes(tmp_path, monkeypatch):
    exc = httpx.RemoteProtocolError(
        "Server disconnected without sending a response.",
        request=httpx.Request("GET", "http://metrics-aggregator:5559/metrics"),
    )

    result, audit = await _run(tmp_path, monkeypatch, exc)

    assert result.diagnosis.confidence == "low"
    skew_results = [
        m for m in _SEEN[-1] if isinstance(m, ToolMessage) and m.name == "hot_partition_skew"
    ]
    assert len(skew_results) == 1
    assert skew_results[0].status == "error"
    assert "RemoteProtocolError" in skew_results[0].content
    [event] = audit.query(event_type="tool_error")
    assert event.payload["tool"] == "hot_partition_skew"


@pytest.mark.asyncio
async def test_non_backend_error_still_aborts_the_session(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="real bug"):
        await _run(tmp_path, monkeypatch, ValueError("real bug"))


@pytest.mark.asyncio
async def test_session_token_usage_is_totalled_and_audited(tmp_path, monkeypatch):
    exc = httpx.ConnectError("refused", request=httpx.Request("GET", "http://x"))

    result, audit = await _run(tmp_path, monkeypatch, exc)

    # Four model replies (three tool calls, then "Done."), each with _USAGE.
    expected = {
        "input_tokens": 400,
        "output_tokens": 40,
        "cache_read_tokens": 240,
        "cache_creation_tokens": 80,
        "model_calls": 4,
    }
    assert result.usage.model_dump() == expected
    [event] = audit.query(event_type="session_usage")
    assert event.payload == expected


@pytest.mark.asyncio
async def test_agent_is_built_with_prompt_caching(tmp_path, monkeypatch):
    from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

    built = {}
    real_create_agent = session.create_agent

    def capture(**kwargs):
        built.update(kwargs)
        return real_create_agent(**kwargs)

    monkeypatch.setattr(session, "create_agent", capture)
    await _run(tmp_path, monkeypatch, ValueError("unused"), fail=False)

    assert any(isinstance(m, AnthropicPromptCachingMiddleware) for m in built["middleware"])


def test_cache_writes_reported_by_ttl_are_counted():
    # langchain-anthropic (chat_models.py) reports cache_creation=0 and puts
    # the writes under per-TTL keys when Anthropic breaks them down that way,
    # which is what a real Sonnet run returned.
    from dp_ops_agent.orchestrator.session import _total_usage

    reply = AIMessage(
        content="",
        usage_metadata={
            "input_tokens": 5000,
            "output_tokens": 50,
            "total_tokens": 5050,
            "input_token_details": {
                "cache_read": 0,
                "cache_creation": 0,
                "ephemeral_5m_input_tokens": 4200,
                "ephemeral_1h_input_tokens": 0,
            },
        },
    )

    assert _total_usage([reply]).cache_creation_tokens == 4200
