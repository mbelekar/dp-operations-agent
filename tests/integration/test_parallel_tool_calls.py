"""A turn's tool calls run concurrently end to end: LangGraph's tool node
gathers them, and the async gateways don't hold the event loop, so calls to
different systems overlap instead of queuing. Runs run_diagnosis with a
scripted chat model (no API key, no network) that issues three tool calls,
one per system, in a single message.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import httpx
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.orchestrator import session
from dp_ops_agent.orchestrator.session import run_diagnosis
from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.flink.gateway import CheckpointHistoryView
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.kafka.gateway import ClusterMetadataView
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from dp_ops_agent.tools.lineage.gateway import LineageGraphView

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
DELAY = 0.3  # per backend call; 0.9s for the three if they queued

# Module-level, not a _ScriptedModel attribute: BaseChatModel is a pydantic
# model, so a class attribute would become a per-instance field.
_SEEN: list[list[BaseMessage]] = []

_PARALLEL_CALLS = [
    ("under_replicated_partitions", {"topics": ["orders"]}),
    ("checkpoint_failure", {"job_id": "orders-processing-job"}),
    ("walk_lineage_upstream", {"node_id": "job:flink:orders-processing-job"}),
]


class _ScriptedModel(BaseChatModel):
    """Turn 0: three tool calls in one message, one per system. Turn 1:
    submit a diagnosis citing the under_replicated_partitions signal. Then
    end the run."""

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools: Any, **kwargs: Any) -> "_ScriptedModel":
        return self

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs):
        _SEEN.append(list(messages))
        turn = sum(isinstance(m, AIMessage) for m in messages)
        if turn == 0:
            calls = [
                {"name": name, "args": args, "id": f"call-{i}"}
                for i, (name, args) in enumerate(_PARALLEL_CALLS)
            ]
        elif turn == 1:
            [urp] = [
                m
                for m in messages
                if isinstance(m, ToolMessage) and m.name == "under_replicated_partitions"
            ]
            signal_id = json.loads(urp.content)["signal_id"]
            calls = [
                {
                    "name": "submit_diagnosis",
                    "args": {
                        "root_cause_hypothesis": "Partition 7 of orders is under-replicated.",
                        "root_cause_signal_id": signal_id,
                        "confidence": "medium",
                        "evidence_chain": [
                            {"step": 1, "signal_id": signal_id, "interpretation": "URP."}
                        ],
                    },
                    "id": "call-submit",
                }
            ]
        else:
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="Done."))])
        message = AIMessage(content="", tool_calls=calls)
        return ChatResult(generations=[ChatGeneration(message=message)])


# (name, start, end) of every slow backend call, in perf_counter seconds.
_CALLS: list[tuple[str, float, float]] = []


async def _slow(name: str, call):
    start = time.perf_counter()
    await asyncio.sleep(DELAY)
    result = await call
    _CALLS.append((name, start, time.perf_counter()))
    return result


class _SlowKafka(FixtureKafkaGateway):
    async def cluster_metadata(self, topics: list[str]) -> ClusterMetadataView:
        return await _slow("kafka", super().cluster_metadata(topics))


class _SlowFlink(FixtureFlinkGateway):
    async def checkpoint_history(self, job_id: str) -> CheckpointHistoryView:
        return await _slow("flink", super().checkpoint_history(job_id))


class _SlowLineage(FixtureLineageGateway):
    def __init__(self, fixture: Path, error: Exception | None = None) -> None:
        super().__init__(fixture)
        self._error = error

    async def upstream_lineage(self, node_id: str, depth: int = 5) -> LineageGraphView:
        if self._error is not None:
            await asyncio.sleep(DELAY)
            raise self._error
        return await _slow("lineage", super().upstream_lineage(node_id, depth))


async def _run(tmp_path, monkeypatch, lineage_error: Exception | None = None):
    _SEEN.clear()
    _CALLS.clear()
    monkeypatch.setattr(session, "ChatAnthropic", lambda model: _ScriptedModel())
    audit = JsonlAuditSink(tmp_path, "s1")
    result = await run_diagnosis(
        session_id="s1",
        alert_text="checkpoint and lag alert on orders-processing-job",
        kafka_gateway=_SlowKafka(FIXTURES / "kafka" / "urp_lag_spike_incident.json"),
        flink_gateway=_SlowFlink(FIXTURES / "flink" / "checkpoint_failure_incident.json"),
        lineage_gateway=_SlowLineage(
            FIXTURES / "lineage" / "flink_job_to_kafka_topic.json", lineage_error
        ),
        dbt_gateway=FixtureDbtGateway(FIXTURES / "dbt" / "healthy_baseline.json"),
        audit=audit,
        model="scripted",
    )
    return result, audit


def _tool_results() -> dict[str, ToolMessage]:
    return {m.name: m for m in _SEEN[-1] if isinstance(m, ToolMessage)}


async def test_a_turns_tool_calls_to_different_systems_overlap(tmp_path, monkeypatch):
    result, audit = await _run(tmp_path, monkeypatch)

    assert sorted(name for name, _, _ in _CALLS) == ["flink", "kafka", "lineage"]
    # Every call started before any of them finished: they overlapped.
    assert max(start for _, start, _ in _CALLS) < min(end for _, _, end in _CALLS)
    span = max(end for _, _, end in _CALLS) - min(start for _, start, _ in _CALLS)
    assert span < 2 * DELAY  # 3 * DELAY if they queued

    assert result.diagnosis.system == "kafka"
    assert {m.status for m in _tool_results().values()} == {"success"}
    collected = [e.payload["tool"] for e in audit.query(event_type="signal_collected")]
    assert sorted(collected) == [
        "flink.checkpoint_failure",
        "kafka.under_replicated_partitions",
        "lineage.walk_lineage_upstream",
    ]


async def test_a_backend_error_in_one_concurrent_call_leaves_the_others(tmp_path, monkeypatch):
    # An HTTP error status: a backend error, not retried (a transport error
    # would be retried once after a delay first).
    request = httpx.Request("GET", "http://marquez:5000/api/v1/lineage")
    error = httpx.HTTPStatusError(
        "503 Service Unavailable", request=request, response=httpx.Response(503, request=request)
    )

    result, audit = await _run(tmp_path, monkeypatch, lineage_error=error)

    results = _tool_results()
    assert results["walk_lineage_upstream"].status == "error"
    assert "HTTPStatusError" in results["walk_lineage_upstream"].content
    assert results["under_replicated_partitions"].status == "success"
    assert results["checkpoint_failure"].status == "success"
    assert result.diagnosis.system == "kafka"
    [event] = audit.query(event_type="tool_error")
    assert event.payload["tool"] == "walk_lineage_upstream"
