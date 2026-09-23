"""Full loop against a live model. Opt-in only (spends API calls):

    ANTHROPIC_API_KEY=... pytest -m llm tests/integration/test_diagnose_loop.py

Excluded from the default test run via pyproject.toml's addopts-free default
(select explicitly with -m llm); asserts mechanically, not by judging prose.
"""

import os
from pathlib import Path

import pytest

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.orchestrator.session import run_diagnosis
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway

KAFKA_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "kafka" / "urp_lag_spike_incident.json"
FLINK_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "flink" / "healthy_baseline.json"
LINEAGE_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "lineage" / "empty.json"

pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(
        not os.environ.get("ANTHROPIC_API_KEY"), reason="requires ANTHROPIC_API_KEY"
    ),
]


@pytest.mark.asyncio
async def test_diagnose_loop_produces_grounded_diagnosis(tmp_path):
    kafka_gateway = FixtureKafkaGateway(KAFKA_FIXTURE)
    flink_gateway = FixtureFlinkGateway(FLINK_FIXTURE)
    lineage_gateway = FixtureLineageGateway(LINEAGE_FIXTURE)
    session_id = "llm-full-loop-test"
    audit = JsonlAuditSink(tmp_path, session_id)

    result = await run_diagnosis(
        session_id=session_id,
        alert_text="PagerDuty: consumer lag alert on billing-svc/orders",
        kafka_gateway=kafka_gateway,
        flink_gateway=flink_gateway,
        lineage_gateway=lineage_gateway,
        audit=audit,
        model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-5"),
    )

    diagnosis = result.diagnosis
    assert diagnosis.evidence_chain, "expected at least one evidence_chain entry"

    signal_collected_events = audit.query(event_type="signal_collected", session_id=session_id)
    assert signal_collected_events, "expected at least one diagnostic tool to have been called"

    collected_signal_ids = {e.payload["signal_id"] for e in signal_collected_events}
    for entry in diagnosis.evidence_chain:
        assert entry.signal_id in collected_signal_ids
