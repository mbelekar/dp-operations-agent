import json
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "args"),
    [
        ("under_replicated_partitions", {"topics": ["no-such-topic"]}),
        ("isr_churn", {"broker_id": 99, "window_minutes": 10}),
        ("consumer_lag_trend", {"group": "billing-svc", "topic": "no-such-topic"}),
        # Used to be critical: no committed offsets was read as offset 0, so
        # the "lag" was the whole topic.
        ("consumer_lag_trend", {"group": "no-such-group", "topic": "orders"}),
        ("rebalance_frequency", {"group": "no-such-group", "window_minutes": 10}),
        ("hot_partition_skew", {"topic": "no-such-topic", "window_minutes": 10}),
        # Used to be ok: an empty registry response was read as is_compatible.
        ("schema_registry_compat", {"subject": "no-such-subject"}),
    ],
    ids=["urp", "isr", "lag-unknown-topic", "lag-unknown-group", "rebalance", "skew", "schema"],
)
async def test_no_data_for_the_identifiers_is_unknown(tmp_path, tool, args):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    signal = _signal_from_result(await tools[tool].ainvoke(args))

    assert signal.severity == "unknown"
    assert signal.observed["no_data_reason"]
    # The unknown result names what does exist, so one retry can land.
    if tool == "consumer_lag_trend":
        unknown_topic = args["topic"] == "no-such-topic"
        field, names = ("known_topics", ["orders"]) if unknown_topic else ("known_groups", ["billing-svc"])
    else:
        field, names = {
            "under_replicated_partitions": ("known_topics", ["orders"]),
            "isr_churn": ("known_brokers", [1]),
            "rebalance_frequency": ("known_groups", ["billing-svc"]),
            "hot_partition_skew": ("known_topics", ["orders"]),
            "schema_registry_compat": ("known_subjects", ["orders-value"]),
        }[tool]
    assert signal.observed[field] == names
    for name in names:
        assert str(name) in signal.observed["no_data_reason"]


def _tools_for(tmp_path, snapshot: dict):
    fixture = tmp_path / "snapshot.json"
    fixture.write_text(json.dumps(snapshot))
    return {
        t.name: t
        for t in build_kafka_tools(
            FixtureKafkaGateway(fixture), JsonlAuditSink(tmp_path, "s"), "s", []
        )
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshot", "tool", "args"),
    [
        # Live mode's permanent gap: the topic is real, per-partition
        # throughput is never reported.
        (
            {"cluster_metadata": {"orders": [{"topic": "orders", "id": 0, "replicas": [1],
                                              "isr": [1], "leader": 1}]}},
            "hot_partition_skew",
            {"topic": "orders", "window_minutes": 10},
        ),
        # Live mode's other one: the subject is real, no verdict comes back.
        (
            {"schema_registry_subject": {"orders-value": {}}},
            "schema_registry_compat",
            {"subject": "orders-value"},
        ),
        (
            {"consumer_group_state_history": {"billing-svc": [{"ts": "t", "state": "Stable"}]},
             "topic_high_watermarks": {"orders": {"0": 10}}},
            "consumer_lag_trend",
            {"group": "billing-svc", "topic": "orders"},
        ),
    ],
    ids=["skew-real-topic", "schema-real-subject", "lag-real-group-and-topic"],
)
async def test_identifier_that_exists_without_data_says_not_to_retry(tmp_path, snapshot, tool, args):
    signal = _signal_from_result(await _tools_for(tmp_path, snapshot)[tool].ainvoke(args))

    assert signal.severity == "unknown"
    assert "exists" in signal.observed["no_data_reason"]
    assert "don't retry" in signal.observed["no_data_reason"]


@pytest.mark.asyncio
async def test_consumer_lag_ignores_partitions_without_a_committed_offset(tmp_path):
    fixture = tmp_path / "partial_offsets.json"
    fixture.write_text(
        json.dumps(
            {
                "consumer_group_offsets": {"billing-svc": {"0": 1000}},
                "topic_high_watermarks": {"orders": {"0": 1050, "1": 900000}},
            }
        )
    )
    tools = {
        t.name: t
        for t in build_kafka_tools(
            FixtureKafkaGateway(fixture), JsonlAuditSink(tmp_path, "s"), "s", []
        )
    }

    signal = _signal_from_result(
        await tools["consumer_lag_trend"].ainvoke({"group": "billing-svc", "topic": "orders"})
    )

    assert signal.severity == "ok"
    assert signal.observed["lag_by_partition"] == {"0": 50}
    assert signal.observed["partitions_without_committed_offset"] == ["1"]
