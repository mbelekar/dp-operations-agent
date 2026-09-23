from pathlib import Path

import pytest

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.evidence.schema import Signal
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from dp_ops_agent.tools.lineage.tools import build_lineage_tools

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "lineage"


def _build_tools(tmp_path, fixture_name: str):
    gateway = FixtureLineageGateway(FIXTURE_DIR / fixture_name)
    audit = JsonlAuditSink(tmp_path, "test-session")
    collected: list[Signal] = []
    tools = build_lineage_tools(gateway, audit, "test-session", collected)
    return {t.name: t for t in tools}, collected, audit


def _signal_from_result(result: str) -> Signal:
    return Signal.model_validate_json(result)


@pytest.mark.asyncio
async def test_walk_lineage_upstream_finds_kafka_topic(tmp_path):
    tools, collected, _ = _build_tools(tmp_path, "flink_job_to_kafka_topic.json")
    result = await tools["walk_lineage_upstream"].ainvoke(
        {"node_id": "job:flink:orders-processing-job"}
    )
    signal = _signal_from_result(result)

    assert signal.severity == "ok"
    assert signal.signal_type == "lineage_upstream"
    assert signal.observed["upstream_nodes"] == [
        {"id": "dataset:kafka:orders", "type": "DATASET"}
    ]
    assert collected == [signal]


@pytest.mark.asyncio
async def test_walk_lineage_upstream_unknown_node_is_empty(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "empty.json")
    result = await tools["walk_lineage_upstream"].ainvoke({"node_id": "job:flink:no-such-job"})
    signal = _signal_from_result(result)

    assert signal.severity == "ok"
    assert signal.observed["upstream_nodes"] == []


@pytest.mark.asyncio
async def test_walk_lineage_upstream_is_audit_logged(tmp_path):
    tools, collected, audit = _build_tools(tmp_path, "flink_job_to_kafka_topic.json")
    await tools["walk_lineage_upstream"].ainvoke({"node_id": "job:flink:orders-processing-job"})

    events = audit.query(event_type="signal_collected")
    assert len(events) == 1
    assert len(collected) == 1
