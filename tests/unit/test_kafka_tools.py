from pathlib import Path

import pytest

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.evidence.schema import Signal
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.kafka.tools import build_kafka_tools

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "kafka"


def _build_tools(tmp_path, fixture_name: str):
    gateway = FixtureKafkaGateway(FIXTURE_DIR / fixture_name)
    audit = JsonlAuditSink(tmp_path, "test-session")
    collected: list[Signal] = []
    tools = build_kafka_tools(gateway, audit, "test-session", collected)
    return {t.name: t for t in tools}, collected, audit


def _signal_from_result(result: str) -> Signal:
    return Signal.model_validate_json(result)


@pytest.mark.asyncio
async def test_under_replicated_partitions_detects_urp(tmp_path):
    tools, collected, _ = _build_tools(tmp_path, "urp_lag_spike_incident.json")
    result = await tools["under_replicated_partitions"].ainvoke({"topics": ["orders"]})
    signal = _signal_from_result(result)

    assert signal.severity == "critical"
    partitions = signal.observed["partitions"]
    p7 = next(p for p in partitions if p["partition"] == 7)
    assert p7["under_replicated"] is True
    assert collected == [signal]


@pytest.mark.asyncio
async def test_under_replicated_partitions_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["under_replicated_partitions"].ainvoke({"topics": ["orders"]})
    signal = _signal_from_result(result)
    assert signal.severity == "ok"


@pytest.mark.asyncio
async def test_isr_churn_flags_high_shrink_rate(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "urp_lag_spike_incident.json")
    result = await tools["isr_churn"].ainvoke({"broker_id": 1, "window_minutes": 10})
    signal = _signal_from_result(result)
    assert signal.severity == "critical"
    assert signal.observed["max_shrink_rate"] == 2.4


@pytest.mark.asyncio
async def test_consumer_lag_trend_flags_large_lag(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "urp_lag_spike_incident.json")
    result = await tools["consumer_lag_trend"].ainvoke({"group": "billing-svc", "topic": "orders"})
    signal = _signal_from_result(result)
    assert signal.severity == "critical"
    assert signal.observed["lag_by_partition"]["7"] == 12500


@pytest.mark.asyncio
async def test_consumer_lag_trend_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["consumer_lag_trend"].ainvoke({"group": "billing-svc", "topic": "orders"})
    signal = _signal_from_result(result)
    assert signal.severity == "ok"


@pytest.mark.asyncio
async def test_rebalance_frequency_flags_storm(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "rebalance_storm_incident.json")
    result = await tools["rebalance_frequency"].ainvoke({"group": "billing-svc", "window_minutes": 10})
    signal = _signal_from_result(result)
    assert signal.severity == "critical"
    assert signal.observed["rebalance_count"] == 6


@pytest.mark.asyncio
async def test_rebalance_frequency_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["rebalance_frequency"].ainvoke({"group": "billing-svc", "window_minutes": 10})
    signal = _signal_from_result(result)
    assert signal.severity == "ok"
    assert signal.observed["rebalance_count"] == 0


@pytest.mark.asyncio
async def test_hot_partition_skew_detects_skew(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "hot_partition_skew_incident.json")
    result = await tools["hot_partition_skew"].ainvoke({"topic": "orders", "window_minutes": 10})
    signal = _signal_from_result(result)
    assert signal.severity == "critical"
    assert signal.observed["skew_ratio"] > 5


@pytest.mark.asyncio
async def test_hot_partition_skew_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["hot_partition_skew"].ainvoke({"topic": "orders", "window_minutes": 10})
    signal = _signal_from_result(result)
    assert signal.severity == "ok"


@pytest.mark.asyncio
async def test_schema_registry_compat_flags_incompatibility(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "schema_incompatibility_incident.json")
    result = await tools["schema_registry_compat"].ainvoke({"subject": "orders-value"})
    signal = _signal_from_result(result)
    assert signal.severity == "critical"
    assert signal.observed["is_compatible"] is False


@pytest.mark.asyncio
async def test_schema_registry_compat_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["schema_registry_compat"].ainvoke({"subject": "orders-value"})
    signal = _signal_from_result(result)
    assert signal.severity == "ok"
    assert signal.observed["is_compatible"] is True


@pytest.mark.asyncio
async def test_every_tool_call_is_audit_logged(tmp_path):
    tools, collected, audit = _build_tools(tmp_path, "urp_lag_spike_incident.json")
    await tools["under_replicated_partitions"].ainvoke({"topics": ["orders"]})
    await tools["isr_churn"].ainvoke({"broker_id": 1, "window_minutes": 10})

    events = audit.query(event_type="signal_collected")
    assert len(events) == 2
    assert len(collected) == 2
