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
from dp_ops_agent.tools.registry import TOOL_NAMES, build_tools

KAFKA_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "kafka" / "urp_lag_spike_incident.json"
)
FLINK_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "flink" / "healthy_baseline.json"
)


def _build_tools(tmp_path, session_id: str):
    kafka_gateway = FixtureKafkaGateway(KAFKA_FIXTURE)
    flink_gateway = FixtureFlinkGateway(FLINK_FIXTURE)
    audit = JsonlAuditSink(tmp_path, session_id)
    result_holder: dict[str, Diagnosis] = {}
    tools = {
        t.name: t
        for t in build_tools(
            kafka_gateway, flink_gateway, audit, session_id, "claude-sonnet-5", result_holder
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
            "confidence": "high",
            "evidence_chain": [
                {"step": 1, "signal_id": signal_id, "interpretation": "URP on partition 7"}
            ],
        }
    )

    assert "rejected" not in submit_result
    assert "diagnosis" in result_holder
    assert result_holder["diagnosis"].evidence_chain[0].signal_id == signal_id


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
        {"root_cause_hypothesis": "no evidence gathered", "confidence": "low", "evidence_chain": []}
    )

    assert "rejected" in submit_result
    assert "diagnosis" not in result_holder
