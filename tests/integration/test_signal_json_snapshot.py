"""Pins the exact JSON every diagnostic tool returns, branch by branch.

The model reads this JSON, the audit log stores it, and the system prompt
names fields in it (scope.lineage_node_id, no_data_reason, known_*). So any
change to it, including key order, a key appearing or disappearing, or 3
becoming 3.0, is a change to what the agent sees. Refactors of how signals
are built (e.g. typed payload models) must leave this test passing
unchanged.

Only values that differ on every run are normalized: signal_id and the
three timestamps. Everything else is compared as the raw JSON text.

Each case also asserts the severity and no_data_reason it expects, so a
case that silently stops reaching its branch fails here instead of
snapshotting the wrong branch.

To regenerate after an intended wire-format change (never in CI):
    UPDATE_SIGNAL_SNAPSHOTS=1 ./auto/test tests/integration/test_signal_json_snapshot.py
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.evidence.schema import Diagnosis, Signal
from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.dbt.tools import build_dbt_tools
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.flink.tools import build_flink_tools
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.kafka.tools import build_kafka_tools
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from dp_ops_agent.tools.lineage.tools import build_lineage_tools
from dp_ops_agent.tools.registry import build_tools
from tests.unit.test_dbt_tools import (
    FCT,
    GENERATED_AT,
    NOT_NULL,
    STG,
    UNIQUE,
    _catalog,
    _drift_runs,
    _manifest,
    _rows,
    _rr,
    _sources,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
EDGE = FIXTURES / "snapshots" / "inputs"
SNAPSHOT = FIXTURES / "snapshots" / "signals.json"
UPDATE = os.environ.get("UPDATE_SIGNAL_SNAPSHOTS") == "1"

_VOLATILE = re.compile(r'"(signal_id|collected_at|window_start|window_end)":"[^"]*"')

# Sentinel for "no_data_reason must be absent from observed".
ABSENT = None


@dataclass(frozen=True)
class Case:
    id: str
    system: str  # kafka | flink | lineage | dbt
    tool: str
    args: dict[str, Any]
    severity: str
    reason: str | None  # substring of observed.no_data_reason, or ABSENT
    fixture: Path | None = None  # kafka, flink, lineage
    current: dict[str, Any] = field(default_factory=dict)  # dbt
    state: dict[str, Any] | None = None  # dbt


def _k(name: str) -> Path:
    return FIXTURES / "kafka" / f"{name}.json"


def _f(name: str) -> Path:
    return FIXTURES / "flink" / f"{name}.json"


KAFKA_EDGE = EDGE / "kafka_edge_cases.json"
FLINK_EDGE = EDGE / "flink_edge_cases.json"
DBT_MANIFEST_ONLY = {"manifest": _manifest(), "run_results": _rr({})}


def _kafka(id, tool, args, fixture, severity, reason=ABSENT):
    return Case(id, "kafka", tool, args, severity, reason, fixture=fixture)


def _flink(id, tool, args, fixture, severity, reason=ABSENT):
    return Case(id, "flink", tool, args, severity, reason, fixture=fixture)


def _dbt(id, tool, args, current, severity, reason=ABSENT, state=None):
    return Case(id, "dbt", tool, args, severity, reason, current=current, state=state)


def _vertex_cases(tool: str, normal_fixture: Path, normal_vertex: str, normal_severity: str):
    return [
        _flink(
            f"{tool}/normal",
            tool,
            {"job_id": "orders-processing-job", "vertex_id": normal_vertex},
            normal_fixture,
            normal_severity,
        ),
        _flink(
            f"{tool}/vertex-exists-no-data",
            tool,
            {"job_id": "quiet-job", "vertex_id": "idle-vertex"},
            FLINK_EDGE,
            "unknown",
            "vertex 'idle-vertex' of job 'quiet-job' exists but reports no",
        ),
        _flink(
            f"{tool}/unknown-vertex",
            tool,
            {"job_id": "quiet-job", "vertex_id": "nope"},
            FLINK_EDGE,
            "unknown",
            "no vertex 'nope' in job 'quiet-job'",
        ),
        _flink(
            f"{tool}/job-without-vertices",
            tool,
            {"job_id": "nope-job", "vertex_id": "nope"},
            FLINK_EDGE,
            "unknown",
            "no vertices found for job 'nope-job'",
        ),
    ]


def _model_not_found(tool: str) -> Case:
    return _dbt(
        f"{tool}/model-not-found",
        tool,
        {"model": "no_such_model"},
        DBT_MANIFEST_ONLY,
        "unknown",
        "no dbt model named 'no_such_model'",
    )


CASES: list[Case] = [
    # --- kafka ----------------------------------------------------------------
    _kafka(
        "under_replicated_partitions/normal",
        "under_replicated_partitions",
        {"topics": ["orders"]},
        _k("urp_lag_spike_incident"),
        "critical",
    ),
    _kafka(
        "under_replicated_partitions/unknown-topic",
        "under_replicated_partitions",
        {"topics": ["nope"]},
        _k("healthy_baseline"),
        "unknown",
        "no topic 'nope'",
    ),
    _kafka(
        "under_replicated_partitions/topic-exists-no-data",
        "under_replicated_partitions",
        {"topics": ["orders"]},
        KAFKA_EDGE,
        "unknown",
        "topic 'orders' exists but reports no partition metadata",
    ),
    _kafka(
        "isr_churn/normal",
        "isr_churn",
        {"broker_id": 1, "window_minutes": 30},
        _k("isr_churn_upstream_incident"),
        "critical",
    ),
    _kafka(
        "isr_churn/unknown-broker",
        "isr_churn",
        {"broker_id": 99, "window_minutes": 30},
        _k("healthy_baseline"),
        "unknown",
        "no broker 99;",
    ),
    _kafka(
        "isr_churn/broker-exists-no-data",
        "isr_churn",
        {"broker_id": 2, "window_minutes": 30},
        KAFKA_EDGE,
        "unknown",
        "broker 2 exists but reports no ISR",
    ),
    _kafka(
        "consumer_lag_trend/normal",
        "consumer_lag_trend",
        {"group": "billing-svc", "topic": "orders"},
        _k("urp_lag_spike_incident"),
        "critical",
    ),
    _kafka(
        "consumer_lag_trend/partition-without-offset",
        "consumer_lag_trend",
        {"group": "no-history-group", "topic": "orders"},
        KAFKA_EDGE,
        "ok",
    ),
    _kafka(
        "consumer_lag_trend/unknown-topic",
        "consumer_lag_trend",
        {"group": "billing-svc", "topic": "nope"},
        _k("healthy_baseline"),
        "unknown",
        "no topic 'nope'",
    ),
    _kafka(
        "consumer_lag_trend/unknown-group",
        "consumer_lag_trend",
        {"group": "nope", "topic": "orders"},
        _k("healthy_baseline"),
        "unknown",
        "no consumer group 'nope'",
    ),
    _kafka(
        "consumer_lag_trend/group-exists-no-offsets",
        "consumer_lag_trend",
        {"group": "idle-group", "topic": "orders"},
        KAFKA_EDGE,
        "unknown",
        "consumer group 'idle-group' exists but has no committed offsets",
    ),
    _kafka(
        "rebalance_frequency/normal",
        "rebalance_frequency",
        {"group": "billing-svc", "window_minutes": 30},
        _k("rebalance_storm_incident"),
        "critical",
    ),
    _kafka(
        "rebalance_frequency/unknown-group",
        "rebalance_frequency",
        {"group": "nope", "window_minutes": 30},
        _k("healthy_baseline"),
        "unknown",
        "no consumer group 'nope'",
    ),
    _kafka(
        "rebalance_frequency/group-exists-no-history",
        "rebalance_frequency",
        {"group": "no-history-group", "window_minutes": 30},
        KAFKA_EDGE,
        "unknown",
        "consumer group 'no-history-group' exists but has no state history",
    ),
    _kafka(
        "hot_partition_skew/normal",
        "hot_partition_skew",
        {"topic": "orders", "window_minutes": 30},
        _k("hot_partition_skew_incident"),
        "critical",
    ),
    _kafka(
        "hot_partition_skew/unknown-topic",
        "hot_partition_skew",
        {"topic": "nope", "window_minutes": 30},
        _k("healthy_baseline"),
        "unknown",
        "no topic 'nope'",
    ),
    _kafka(
        "hot_partition_skew/topic-exists-no-data",
        "hot_partition_skew",
        {"topic": "metadata-only", "window_minutes": 30},
        KAFKA_EDGE,
        "unknown",
        "topic 'metadata-only' exists but reports no per-partition throughput",
    ),
    _kafka(
        "schema_registry_compat/compatible",
        "schema_registry_compat",
        {"subject": "orders-value"},
        _k("healthy_baseline"),
        "ok",
    ),
    _kafka(
        "schema_registry_compat/incompatible",
        "schema_registry_compat",
        {"subject": "orders-value"},
        _k("schema_incompatibility_incident"),
        "critical",
    ),
    _kafka(
        "schema_registry_compat/unknown-subject",
        "schema_registry_compat",
        {"subject": "nope"},
        _k("healthy_baseline"),
        "unknown",
        "no schema subject 'nope'",
    ),
    _kafka(
        "schema_registry_compat/subject-exists-no-verdict",
        "schema_registry_compat",
        {"subject": "no-verdict-value"},
        KAFKA_EDGE,
        "unknown",
        "schema subject 'no-verdict-value' exists but has no compatibility verdict",
    ),
    # --- flink ----------------------------------------------------------------
    _flink(
        "checkpoint_failure/failing",
        "checkpoint_failure",
        {"job_id": "orders-processing-job"},
        _f("checkpoint_failure_incident"),
        "critical",
    ),
    _flink(
        "checkpoint_failure/healthy",
        "checkpoint_failure",
        {"job_id": "orders-processing-job"},
        _f("healthy_baseline"),
        "ok",
    ),
    _flink(
        "checkpoint_failure/job-exists-no-checkpoints",
        "checkpoint_failure",
        {"job_id": "no-checkpoints-job"},
        FLINK_EDGE,
        "unknown",
        "job 'no-checkpoints-job' exists but has no checkpoints recorded",
    ),
    _flink(
        "checkpoint_failure/unknown-job",
        "checkpoint_failure",
        {"job_id": "nope"},
        FLINK_EDGE,
        "unknown",
        "no job 'nope'",
    ),
    *_vertex_cases("backpressure_ratio", _f("backpressure_incident"), "sink", "critical"),
    *_vertex_cases(
        "watermark_lag", _f("watermark_lag_cross_system_incident"), "source", "critical"
    ),
    *_vertex_cases("state_backend_disk_pressure", _f("healthy_baseline"), "source", "ok"),
    _flink(
        "savepoint_restore_failure/no-match",
        "savepoint_restore_failure",
        {"job_id": "orders-processing-job", "window_minutes": 60},
        _f("healthy_baseline"),
        "ok",
    ),
    _flink(
        "savepoint_restore_failure/match",
        "savepoint_restore_failure",
        {"job_id": "restore-job", "window_minutes": 60},
        FLINK_EDGE,
        "critical",
    ),
    # --- lineage --------------------------------------------------------------
    Case(
        "walk_lineage_upstream/found",
        "lineage",
        "walk_lineage_upstream",
        {"node_id": "job:flink:orders-processing-job"},
        "ok",
        ABSENT,
        fixture=FIXTURES / "lineage" / "flink_job_to_kafka_topic.json",
    ),
    Case(
        "walk_lineage_upstream/not-found",
        "lineage",
        "walk_lineage_upstream",
        {"node_id": "dataset:kafka:nope"},
        "unknown",
        "lineage has no node 'dataset:kafka:nope'",
        fixture=FIXTURES / "lineage" / "empty.json",
    ),
    # --- dbt: test_failure ----------------------------------------------------
    _dbt(
        "test_failure/failing-previously-passed",
        "test_failure",
        {"model": "stg_orders"},
        {
            "manifest": _manifest(stg_checksum="aaa"),
            "run_results": _rr(
                {
                    NOT_NULL: {"status": "fail", "failures": 10, "message": "Got 10 results"},
                    UNIQUE: "pass",
                }
            ),
        },
        "critical",
        state={
            "manifest": _manifest(stg_checksum="aaa"),
            "run_results": _rr({NOT_NULL: "pass", UNIQUE: "pass"}),
        },
    ),
    _dbt(
        "test_failure/no-previous-run",
        "test_failure",
        {"model": "stg_orders"},
        {"manifest": _manifest(), "run_results": _rr({NOT_NULL: "fail"})},
        "critical",
    ),
    _dbt(
        "test_failure/warning",
        "test_failure",
        {"model": "stg_orders"},
        {"manifest": _manifest(), "run_results": _rr({NOT_NULL: "warn", UNIQUE: "pass"})},
        "warn",
    ),
    _dbt(
        "test_failure/skipped",
        "test_failure",
        {"model": "stg_orders"},
        {"manifest": _manifest(), "run_results": _rr({NOT_NULL: "skipped", UNIQUE: "pass"})},
        "ok",
    ),
    _dbt(
        "test_failure/no-tests-ran",
        "test_failure",
        {"model": "stg_orders"},
        DBT_MANIFEST_ONLY,
        "unknown",
        "no test on 'stg_orders' ran in the latest run",
    ),
    _model_not_found("test_failure"),
    # --- dbt: model_run_failure -----------------------------------------------
    _dbt(
        "model_run_failure/error",
        "model_run_failure",
        {"model": "fct_orders"},
        {
            "manifest": _manifest(),
            "run_results": _rr(
                {FCT: {"status": "error", "message": "Runtime Error in model fct_orders"}}
            ),
        },
        "critical",
        state={"manifest": _manifest(), "run_results": _rr({FCT: "success"})},
    ),
    _dbt(
        "model_run_failure/not-run",
        "model_run_failure",
        {"model": "fct_orders"},
        DBT_MANIFEST_ONLY,
        "unknown",
        "'fct_orders' is not in the latest run's results",
    ),
    _model_not_found("model_run_failure"),
    # --- dbt: freshness_check_failure -----------------------------------------
    _dbt(
        "freshness_check_failure/stale",
        "freshness_check_failure",
        {"source": "raw.orders_sink"},
        {"manifest": _manifest(), "sources": _sources("error", age_seconds=18026.8)},
        "critical",
    ),
    _dbt(
        "freshness_check_failure/not-checked",
        "freshness_check_failure",
        {"source": "raw.orders_sink"},
        {"manifest": _manifest(), "sources": {"generated_at": GENERATED_AT, "results": {}}},
        "unknown",
        "`dbt source freshness` has no result for 'raw.orders_sink'",
    ),
    _dbt(
        "freshness_check_failure/source-not-found",
        "freshness_check_failure",
        {"source": "raw.nope"},
        {"manifest": _manifest(), "sources": _sources()},
        "unknown",
        "raw.nope",
    ),
    # --- dbt: incremental_model_drift -----------------------------------------
    _dbt(
        "incremental_model_drift/sharp-drop",
        "incremental_model_drift",
        {"model": "fct_orders"},
        _drift_runs(_rows(5), _rows(1000))[0],
        "critical",
        state=_drift_runs(_rows(5), _rows(1000))[1],
    ),
    _dbt(
        "incremental_model_drift/not-comparable",
        "incremental_model_drift",
        {"model": "fct_orders"},
        _drift_runs(_rows(10, "INSERT"), _rows(1000, "SELECT"))[0],
        "unknown",
        "runs aren't comparable",
        state=_drift_runs(_rows(10, "INSERT"), _rows(1000, "SELECT"))[1],
    ),
    _dbt(
        "incremental_model_drift/no-rows-affected",
        "incremental_model_drift",
        {"model": "fct_orders"},
        _drift_runs({"status": "success"}, {"status": "success"})[0],
        "unknown",
        "rows_affected missing",
        state=_drift_runs({"status": "success"}, {"status": "success"})[1],
    ),
    _dbt(
        "incremental_model_drift/zero-previous-rows",
        "incremental_model_drift",
        {"model": "fct_orders"},
        _drift_runs(_rows(10), _rows(0))[0],
        "unknown",
        "previous run affected 0 rows",
        state=_drift_runs(_rows(10), _rows(0))[1],
    ),
    _dbt(
        "incremental_model_drift/not-incremental",
        "incremental_model_drift",
        {"model": "fct_orders"},
        _drift_runs(_rows(5), _rows(1000), materialized="table")[0],
        "ok",
        state=_drift_runs(_rows(5), _rows(1000), materialized="table")[1],
    ),
    _model_not_found("incremental_model_drift"),
    # --- dbt: dependency_graph_compile_error ----------------------------------
    _dbt(
        "dependency_graph_compile_error/parent-changed",
        "dependency_graph_compile_error",
        {"model": "stg_orders"},
        {
            "manifest": _manifest(),
            "run_results": _rr({STG: "error"}),
            "catalog": _catalog(order_id="BIGINT", amount="VARCHAR", currency="VARCHAR"),
        },
        "critical",
        state={
            "manifest": _manifest(),
            "run_results": _rr({STG: "success"}),
            "catalog": _catalog(order_id="BIGINT", amount="DECIMAL(21,1)", loaded_at="TIMESTAMP"),
        },
    ),
    _dbt(
        "dependency_graph_compile_error/catalog-missing",
        "dependency_graph_compile_error",
        {"model": "stg_orders"},
        {"manifest": _manifest(), "run_results": _rr({STG: "error"})},
        "unknown",
        "catalog.json missing",
        state={"manifest": _manifest(), "run_results": _rr({STG: "success"})},
    ),
    _dbt(
        "dependency_graph_compile_error/parents-not-in-catalog",
        "dependency_graph_compile_error",
        {"model": "stg_orders"},
        {
            "manifest": _manifest(),
            "run_results": _rr({STG: "error"}),
            "catalog": {"generated_at": GENERATED_AT, "columns": {}},
        },
        "unknown",
        "none of the model's parents are in both runs' catalogs",
        state={
            "manifest": _manifest(),
            "run_results": _rr({STG: "success"}),
            "catalog": {"generated_at": GENERATED_AT, "columns": {}},
        },
    ),
    _model_not_found("dependency_graph_compile_error"),
]


def _tools(case: Case, tmp_path: Path):
    audit = JsonlAuditSink(tmp_path, "snapshot")
    collected: list[Signal] = []
    if case.system == "kafka":
        tools = build_kafka_tools(FixtureKafkaGateway(case.fixture), audit, "snapshot", collected)
    elif case.system == "flink":
        tools = build_flink_tools(FixtureFlinkGateway(case.fixture), audit, "snapshot", collected)
    elif case.system == "lineage":
        tools = build_lineage_tools(
            FixtureLineageGateway(case.fixture), audit, "snapshot", collected
        )
    else:
        path = tmp_path / "dbt_snapshot.json"
        path.write_text(json.dumps({"current": case.current, "state": case.state or {}}))
        tools = build_dbt_tools(FixtureDbtGateway(path), audit, "snapshot", collected)
    return {t.name: t for t in tools}, audit


def _normalize(raw: str) -> str:
    return _VOLATILE.sub(lambda m: f'"{m.group(1)}":"<volatile>"', raw)


def _audit_events(audit: JsonlAuditSink, event_type: str) -> list[dict[str, Any]]:
    lines = audit.path.read_text().splitlines()
    return [e for e in map(json.loads, lines) if e["event_type"] == event_type]


def _load_snapshot() -> dict[str, str]:
    return json.loads(SNAPSHOT.read_text()) if SNAPSHOT.exists() else {}


def test_case_ids_are_unique_and_snapshot_has_no_stale_entries():
    ids = [c.id for c in CASES]
    assert len(ids) == len(set(ids))
    if not UPDATE:
        assert set(_load_snapshot()) == set(ids)


def test_every_signal_tool_has_a_case():
    tools_with_cases = {c.tool for c in CASES}
    assert len(tools_with_cases) == 17


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
async def test_tool_json_matches_snapshot(case: Case, tmp_path):
    tools, audit = _tools(case, tmp_path)
    raw = await tools[case.tool].ainvoke(case.args)
    result = json.loads(raw)

    # The case reaches the branch it names.
    assert result["severity"] == case.severity
    if case.reason is ABSENT:
        assert "no_data_reason" not in result["observed"]
    else:
        assert case.reason in result["observed"]["no_data_reason"]

    # The audit log stores exactly what the model was shown.
    [event] = _audit_events(audit, "signal_collected")
    assert json.dumps(event["payload"]) == json.dumps(result)

    normalized = _normalize(raw)
    if UPDATE:
        snapshot = _load_snapshot()
        snapshot[case.id] = normalized
        SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT.write_text(json.dumps(dict(sorted(snapshot.items())), indent=2) + "\n")
        return
    assert normalized == _load_snapshot()[case.id]


@pytest.mark.asyncio
async def test_diagnosis_payload_serializes_signals_exactly_as_the_tools_did(tmp_path):
    """Diagnosis.signals is typed list[Signal]; serializing through it must
    give the same JSON as the tool returned, for every system."""
    audit = JsonlAuditSink(tmp_path, "snapshot-diagnosis")
    holder: dict[str, Diagnosis] = {}
    tools = {
        t.name: t
        for t in build_tools(
            FixtureKafkaGateway(_k("urp_lag_spike_incident")),
            FixtureFlinkGateway(_f("backpressure_incident")),
            FixtureLineageGateway(FIXTURES / "lineage" / "flink_job_to_kafka_topic.json"),
            FixtureDbtGateway(FIXTURES / "dbt" / "healthy_baseline.json"),
            audit,
            "snapshot-diagnosis",
            "claude-sonnet-5",
            holder,
        )
    }
    returned = [
        await tools["under_replicated_partitions"].ainvoke({"topics": ["orders"]}),
        await tools["consumer_lag_trend"].ainvoke({"group": "billing-svc", "topic": "nope"}),
        await tools["backpressure_ratio"].ainvoke(
            {"job_id": "orders-processing-job", "vertex_id": "sink"}
        ),
        await tools["walk_lineage_upstream"].ainvoke(
            {"node_id": "job:flink:orders-processing-job"}
        ),
        await tools["test_failure"].ainvoke({"model": "stg_orders"}),
    ]
    root = json.loads(returned[0])
    submit = await tools["submit_diagnosis"].ainvoke(
        {
            "root_cause_hypothesis": "partition under-replicated",
            "root_cause_signal_id": root["signal_id"],
            "confidence": "medium",
            "evidence_chain": [
                {"step": i, "signal_id": json.loads(r)["signal_id"], "interpretation": "x"}
                for i, r in enumerate(returned, start=1)
            ],
        }
    )
    assert "rejected" not in submit

    [event] = _audit_events(audit, "diagnosis_completed")
    in_diagnosis = [json.dumps(s) for s in event["payload"]["signals"]]
    from_tools = [json.dumps(json.loads(r)) for r in returned]
    assert in_diagnosis == from_tools
