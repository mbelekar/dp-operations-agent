"""Validates the tool wiring end to end without spending API calls: builds
the tool list exactly as orchestrator/session.py does, and invokes tools
through the same LangChain BaseTool interface create_agent's ToolNode uses.
"""

import json
from pathlib import Path

import pytest

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.evidence.schema import Diagnosis
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from dp_ops_agent.tools.registry import TOOL_NAMES, build_tools

KAFKA_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "kafka" / "urp_lag_spike_incident.json"
)
FLINK_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "flink" / "healthy_baseline.json"
)
LINEAGE_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "lineage" / "empty.json"
)

FLAGSHIP_KAFKA_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "kafka"
    / "isr_churn_upstream_incident.json"
)
FLAGSHIP_FLINK_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "flink"
    / "watermark_lag_cross_system_incident.json"
)
FLAGSHIP_LINEAGE_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "lineage"
    / "flink_job_to_kafka_topic.json"
)


def _build_tools(
    tmp_path,
    session_id: str,
    kafka_fixture: Path = KAFKA_FIXTURE,
    flink_fixture: Path = FLINK_FIXTURE,
    lineage_fixture: Path = LINEAGE_FIXTURE,
):
    kafka_gateway = FixtureKafkaGateway(kafka_fixture)
    flink_gateway = FixtureFlinkGateway(flink_fixture)
    lineage_gateway = FixtureLineageGateway(lineage_fixture)
    audit = JsonlAuditSink(tmp_path, session_id)
    result_holder: dict[str, Diagnosis] = {}
    tools = {
        t.name: t
        for t in build_tools(
            kafka_gateway,
            flink_gateway,
            lineage_gateway,
            audit,
            session_id,
            "claude-sonnet-5",
            result_holder,
        )
    }
    return tools, result_holder


def test_build_tools_registers_every_tool(tmp_path):
    tools, _ = _build_tools(tmp_path, "wiring-test")
    assert set(tools.keys()) == set(TOOL_NAMES)


@pytest.mark.asyncio
async def test_signal_from_kafka_tool_is_citable_in_submit_diagnosis(tmp_path):
    tools, result_holder = _build_tools(tmp_path, "wiring-test-2")

    urp_result = await tools["under_replicated_partitions"].ainvoke({"topics": ["orders"]})
    signal_id = json.loads(urp_result)["signal_id"]

    submit_result = await tools["submit_diagnosis"].ainvoke(
        {
            "root_cause_hypothesis": "Partition 7 is under-replicated",
            "root_cause_signal_id": signal_id,
            "confidence": "high",
            "evidence_chain": [
                {"step": 1, "signal_id": signal_id, "interpretation": "URP on partition 7"}
            ],
        }
    )

    assert "rejected" not in submit_result
    assert "diagnosis" in result_holder
    assert result_holder["diagnosis"].evidence_chain[0].signal_id == signal_id
    assert result_holder["diagnosis"].root_cause_signal_id == signal_id


@pytest.mark.asyncio
async def test_signal_from_flink_tool_is_citable_in_submit_diagnosis(tmp_path):
    tools, result_holder = _build_tools(tmp_path, "wiring-test-flink")

    cp_result = await tools["checkpoint_failure"].ainvoke(
        {"job_id": "orders-processing-job"}
    )
    signal_id = json.loads(cp_result)["signal_id"]

    submit_result = await tools["submit_diagnosis"].ainvoke(
        {
            "root_cause_hypothesis": "Checkpointing is healthy",
            "root_cause_signal_id": signal_id,
            "confidence": "high",
            "evidence_chain": [
                {"step": 1, "signal_id": signal_id, "interpretation": "checkpoint history is clean"}
            ],
        }
    )

    assert "rejected" not in submit_result
    assert "diagnosis" in result_holder
    assert result_holder["diagnosis"].system == "flink"


@pytest.mark.asyncio
async def test_submit_diagnosis_rejects_ungrounded_signal_id(tmp_path):
    tools, result_holder = _build_tools(tmp_path, "wiring-test-3")

    submit_result = await tools["submit_diagnosis"].ainvoke(
        {
            "root_cause_hypothesis": "made up",
            "root_cause_signal_id": "never-collected",
            "confidence": "low",
            "evidence_chain": [
                {"step": 1, "signal_id": "never-collected", "interpretation": "x"}
            ],
        }
    )

    assert "rejected" in submit_result
    assert "diagnosis" not in result_holder


@pytest.mark.asyncio
async def test_submit_diagnosis_rejects_empty_evidence_chain(tmp_path):
    tools, result_holder = _build_tools(tmp_path, "wiring-test-4")

    submit_result = await tools["submit_diagnosis"].ainvoke(
        {
            "root_cause_hypothesis": "no evidence gathered",
            "root_cause_signal_id": "doesnt-matter",
            "confidence": "low",
            "evidence_chain": [],
        }
    )

    assert "rejected" in submit_result
    assert "diagnosis" not in result_holder


@pytest.mark.asyncio
async def test_flagship_cross_system_fixture_evidence_is_retrievable_and_gradeable(tmp_path):
    """Not a live-model assertion (that's what ./auto/eval is for) — just
    that the flagship fixture set (Flink watermark lag -> lineage -> Kafka
    isr_churn) actually wires together: each tool returns the expected
    severity, and a diagnosis citing the Kafka signal as root cause (despite
    a Flink-worded investigation) is accepted and correctly derives
    system == "kafka"."""
    tools, result_holder = _build_tools(
        tmp_path,
        "wiring-flagship",
        kafka_fixture=FLAGSHIP_KAFKA_FIXTURE,
        flink_fixture=FLAGSHIP_FLINK_FIXTURE,
        lineage_fixture=FLAGSHIP_LINEAGE_FIXTURE,
    )

    wl_result = json.loads(
        await tools["watermark_lag"].ainvoke(
            {"job_id": "orders-processing-job", "vertex_id": "source"}
        )
    )
    assert wl_result["severity"] == "critical"
    wl_signal_id = wl_result["signal_id"]

    lineage_result = json.loads(
        await tools["walk_lineage_upstream"].ainvoke(
            {"node_id": "job:flink:orders-processing-job"}
        )
    )
    assert lineage_result["observed"]["upstream_nodes"] == [
        {"id": "dataset:kafka:orders", "type": "DATASET"}
    ]

    isr_result = json.loads(
        await tools["isr_churn"].ainvoke({"broker_id": 1, "window_minutes": 10})
    )
    assert isr_result["severity"] == "critical"
    isr_signal_id = isr_result["signal_id"]

    submit_result = await tools["submit_diagnosis"].ainvoke(
        {
            "root_cause_hypothesis": (
                "Broker 1's rising ISR-shrink rate is causing upstream Kafka instability "
                "that starves the Flink source of timely data, manifesting as watermark lag"
            ),
            "root_cause_signal_id": isr_signal_id,
            "confidence": "high",
            "evidence_chain": [
                {"step": 1, "signal_id": wl_signal_id, "interpretation": "Watermark lag critical"},
                {
                    "step": 2,
                    "signal_id": lineage_result["signal_id"],
                    "interpretation": "Lineage traces the job upstream to the orders topic",
                },
                {"step": 3, "signal_id": isr_signal_id, "interpretation": "ISR churn critical"},
            ],
        }
    )

    assert "rejected" not in submit_result
    assert "diagnosis" in result_holder
    assert result_holder["diagnosis"].system == "kafka"
    assert result_holder["diagnosis"].root_cause_signal_id == isr_signal_id


@pytest.mark.asyncio
async def test_submit_diagnosis_rejects_root_cause_signal_id_not_cited(tmp_path):
    tools, result_holder = _build_tools(tmp_path, "wiring-test-5")

    urp_result = await tools["under_replicated_partitions"].ainvoke({"topics": ["orders"]})
    cited_signal_id = json.loads(urp_result)["signal_id"]

    lag_result = await tools["consumer_lag_trend"].ainvoke(
        {"group": "billing-svc", "topic": "orders"}
    )
    uncited_signal_id = json.loads(lag_result)["signal_id"]

    submit_result = await tools["submit_diagnosis"].ainvoke(
        {
            "root_cause_hypothesis": "made up",
            "root_cause_signal_id": uncited_signal_id,
            "confidence": "low",
            "evidence_chain": [
                {"step": 1, "signal_id": cited_signal_id, "interpretation": "URP on partition 7"}
            ],
        }
    )

    assert "rejected" in submit_result
    assert "diagnosis" not in result_holder
