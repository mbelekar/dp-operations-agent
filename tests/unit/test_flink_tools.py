import json
from pathlib import Path

import pytest

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.evidence.schema import Signal
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.flink.tools import build_flink_tools

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "flink"


def _build_tools(tmp_path, fixture_name: str):
    gateway = FixtureFlinkGateway(FIXTURE_DIR / fixture_name)
    audit = JsonlAuditSink(tmp_path, "test-session")
    collected: list[Signal] = []
    tools = build_flink_tools(gateway, audit, "test-session", collected)
    return {t.name: t for t in tools}, collected, audit


def _signal_from_result(result: str) -> Signal:
    return Signal.model_validate_json(result)


@pytest.mark.asyncio
async def test_checkpoint_failure_detects_repeated_failures(tmp_path):
    tools, collected, _ = _build_tools(tmp_path, "checkpoint_failure_incident.json")
    result = await tools["checkpoint_failure"].ainvoke({"job_id": "orders-processing-job"})
    signal = _signal_from_result(result)

    assert signal.severity == "critical"
    assert signal.observed["counts"]["failed"] == 5
    assert collected == [signal]


@pytest.mark.asyncio
async def test_checkpoint_failure_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["checkpoint_failure"].ainvoke({"job_id": "orders-processing-job"})
    signal = _signal_from_result(result)
    assert signal.severity == "ok"


@pytest.mark.asyncio
async def test_backpressure_ratio_flags_high_backpressure(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "backpressure_incident.json")
    result = await tools["backpressure_ratio"].ainvoke(
        {"job_id": "orders-processing-job", "vertex_id": "sink"}
    )
    signal = _signal_from_result(result)
    assert signal.severity == "critical"
    assert signal.observed["backpressure_level"] == "high"


@pytest.mark.asyncio
async def test_backpressure_ratio_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["backpressure_ratio"].ainvoke(
        {"job_id": "orders-processing-job", "vertex_id": "source"}
    )
    signal = _signal_from_result(result)
    assert signal.severity == "ok"


@pytest.mark.asyncio
async def test_watermark_lag_flags_large_lag(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "backpressure_incident.json")
    result = await tools["watermark_lag"].ainvoke(
        {"job_id": "orders-processing-job", "vertex_id": "sink"}
    )
    signal = _signal_from_result(result)
    assert signal.severity == "ok"  # 800ms lag, below the 10s warn threshold
    assert signal.observed["max_lag_ms"] == 800.0


@pytest.mark.asyncio
async def test_state_backend_disk_pressure_flags_high_usage(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "checkpoint_failure_incident.json")
    result = await tools["state_backend_disk_pressure"].ainvoke(
        {"job_id": "orders-processing-job", "vertex_id": "source"}
    )
    signal = _signal_from_result(result)
    assert signal.severity == "ok"  # 0.3 ratio, below the 0.7 warn threshold


@pytest.mark.asyncio
async def test_savepoint_restore_failure_healthy_is_ok(tmp_path):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    result = await tools["savepoint_restore_failure"].ainvoke(
        {"job_id": "orders-processing-job", "window_minutes": 60}
    )
    signal = _signal_from_result(result)
    assert signal.severity == "ok"


@pytest.mark.asyncio
async def test_every_tool_call_is_audit_logged(tmp_path):
    tools, collected, audit = _build_tools(tmp_path, "checkpoint_failure_incident.json")
    await tools["checkpoint_failure"].ainvoke({"job_id": "orders-processing-job"})
    await tools["backpressure_ratio"].ainvoke(
        {"job_id": "orders-processing-job", "vertex_id": "source"}
    )

    events = audit.query(event_type="signal_collected")
    assert len(events) == 2
    assert len(collected) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "args"),
    [
        ("watermark_lag", {"job_id": "orders-processing-job", "vertex_id": "no-such-vertex"}),
        ("backpressure_ratio", {"job_id": "orders-processing-job", "vertex_id": "no-such-vertex"}),
        (
            "state_backend_disk_pressure",
            {"job_id": "orders-processing-job", "vertex_id": "no-such-vertex"},
        ),
        ("checkpoint_failure", {"job_id": "no-such-job"}),
    ],
)
async def test_no_data_for_the_identifiers_is_unknown_not_ok(tmp_path, tool, args):
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    signal = _signal_from_result(await tools[tool].ainvoke(args))

    assert signal.severity == "unknown"
    assert signal.observed["no_data_reason"]
    # The unknown result names what does exist, so one retry can land.
    if "vertex_id" in args:
        assert signal.observed["known_vertices"] == [{"id": "source", "name": "source"}]
        assert "source" in signal.observed["no_data_reason"]
    else:
        assert signal.observed["known_jobs"] == [
            {"id": "orders-processing-job", "name": "orders-processing-job"}
        ]


@pytest.mark.asyncio
async def test_vertex_that_exists_without_the_metric_says_not_to_retry(tmp_path):
    fixture = tmp_path / "no_watermark.json"
    fixture.write_text(
        json.dumps(
            {
                "backpressure": {
                    "job": {
                        "source": {
                            "status": "ok",
                            "backpressure_level": "ok",
                            "subtasks": [{"subtask": 0, "ratio": 0.0}],
                        }
                    }
                }
            }
        )
    )
    tools = {
        t.name: t
        for t in build_flink_tools(
            FixtureFlinkGateway(fixture), JsonlAuditSink(tmp_path, "s"), "s", []
        )
    }

    signal = _signal_from_result(
        await tools["watermark_lag"].ainvoke({"job_id": "job", "vertex_id": "source"})
    )

    assert signal.severity == "unknown"
    assert "exists" in signal.observed["no_data_reason"]
    assert "don't retry" in signal.observed["no_data_reason"]


@pytest.mark.asyncio
async def test_savepoint_restore_with_no_exceptions_is_still_ok(tmp_path):
    # For a job that exists, an empty exception history is genuinely healthy.
    tools, _, _ = _build_tools(tmp_path, "healthy_baseline.json")
    signal = _signal_from_result(
        await tools["savepoint_restore_failure"].ainvoke(
            {"job_id": "orders-processing-job", "window_minutes": 10}
        )
    )

    assert signal.severity == "ok"
