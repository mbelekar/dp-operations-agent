"""Each test builds the current and previous (state) dbt runs it's
comparing as a FixtureDbtGateway snapshot, so the run pair a verdict rests
on is visible in the test itself."""

import json

import pytest

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.evidence.schema import Signal
from dp_ops_agent.evidence.signals.dbt import (
    DbtModelNotFound,
    DbtModelScope,
    DbtSourceNotFound,
    DbtSourceScope,
    DependencyGraphCompileErrorObserved,
    DependencyGraphCompileErrorSignal,
    FreshnessCheckFailureObserved,
    FreshnessCheckFailureSignal,
    IncrementalModelDriftObserved,
    IncrementalModelDriftSignal,
    ModelRunFailureObserved,
    ModelRunFailureSignal,
    TestFailureObserved,
    TestFailureSignal,
)
from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.dbt.gateway import DbtArtifactsUnavailable
from dp_ops_agent.tools.dbt.tools import build_dbt_tools

GENERATED_AT = "2026-09-24T00:08:43Z"
STG = "model.shop.stg_orders"
FCT = "model.shop.fct_orders"
SRC = "source.shop.raw.orders_sink"
NOT_NULL = "test.shop.not_null_stg_orders_order_id.81cfe2fe64"
UNIQUE = "test.shop.unique_stg_orders_order_id.e3b841c71a"


def _manifest(stg_checksum="aaa", fct_materialized="incremental"):
    def model(uid, name, checksum, depends_on, materialized):
        return {
            "unique_id": uid,
            "resource_type": "model",
            "name": name,
            "schema_name": "analytics",
            "table_name": name,
            "checksum": checksum,
            "depends_on": depends_on,
            "materialized": materialized,
        }

    def test(uid, name):
        return {
            "unique_id": uid,
            "resource_type": "test",
            "name": name,
            "depends_on": [STG],
            "attached_node": STG,
        }

    return {
        "generated_at": GENERATED_AT,
        "nodes": {
            STG: model(STG, "stg_orders", stg_checksum, [SRC], "view"),
            FCT: model(FCT, "fct_orders", "fff", [STG], fct_materialized),
            NOT_NULL: test(NOT_NULL, "not_null_stg_orders_order_id"),
            UNIQUE: test(UNIQUE, "unique_stg_orders_order_id"),
            SRC: {
                "unique_id": SRC,
                "resource_type": "source",
                "name": "orders_sink",
                "source_name": "raw",
                "schema_name": "raw",
                "table_name": "orders_sink",
            },
        },
    }


def _run_results(**statuses):
    """statuses: unique_id -> status, or -> dict of NodeRunResult fields."""
    results = {}
    for uid, value in statuses.items():
        fields = value if isinstance(value, dict) else {"status": value}
        results[uid] = {"unique_id": uid, **fields}
    return {"generated_at": GENERATED_AT, "invocation": "build", "results": results}


# Keyword arguments can't contain dots, so tests pass unique ids via **{...}.
def _rr(statuses: dict):
    return _run_results(**statuses)


def _sources(status="pass", age_seconds=600.0):
    return {
        "generated_at": GENERATED_AT,
        "results": {
            SRC: {
                "unique_id": SRC,
                "status": status,
                "max_loaded_at": "2026-09-23T19:08:09Z",
                "age_seconds": age_seconds,
                "criteria": {"error_after": {"count": 2, "period": "hour"}},
            }
        },
    }


def _build_tools(tmp_path, current: dict, state: dict | None = None):
    path = tmp_path / "dbt_snapshot.json"
    path.write_text(json.dumps({"current": current, "state": state or {}}))
    audit = JsonlAuditSink(tmp_path, "test-session")
    collected: list[Signal] = []
    tools = build_dbt_tools(FixtureDbtGateway(path), audit, "test-session", collected)
    return {t.name: t for t in tools}, collected


async def _signal(tools, name: str, args: dict) -> Signal:
    return Signal.model_validate_json(await tools[name].ainvoke(args))


# --- test_failure -----------------------------------------------------------


@pytest.mark.asyncio
async def test_failure_that_passed_last_run_with_unchanged_code_points_upstream(tmp_path):
    tools, collected = _build_tools(
        tmp_path,
        current={
            "manifest": _manifest(stg_checksum="aaa"),
            "run_results": _rr(
                {
                    NOT_NULL: {"status": "fail", "failures": 10, "message": "Got 10 results"},
                    UNIQUE: "pass",
                }
            ),
        },
        state={
            "manifest": _manifest(stg_checksum="aaa"),
            "run_results": _rr({NOT_NULL: "pass", UNIQUE: "pass"}),
        },
    )

    result = await tools["test_failure"].ainvoke({"model": "stg_orders"})
    signal = Signal.model_validate_json(result)

    assert signal.tool == "dbt.test_failure"
    assert signal.severity == "critical"
    assert signal.scope == {"model": "stg_orders", "lineage_node_id": "job:dbt:stg_orders"}
    assert signal.observed["model_code_changed"] is False
    assert signal.observed["all_failing_previously_passed"] is True
    [failing] = signal.observed["failing_tests"]
    assert failing == {
        "test": "not_null_stg_orders_order_id",
        "status": "fail",
        "failures": 10,
        "message": "Got 10 results",
        "previously_passed": True,
    }
    assert signal.observed["artifacts_generated_at"]["run_results"] == "2026-09-24T00:08:43Z"
    # What was collected is exactly what the model was shown.
    assert [s.model_dump_json() for s in collected] == [result]


@pytest.mark.asyncio
async def test_failure_after_a_code_change_is_flagged_as_changed(tmp_path):
    tools, _ = _build_tools(
        tmp_path,
        current={"manifest": _manifest(stg_checksum="bbb"), "run_results": _rr({NOT_NULL: "fail"})},
        state={"manifest": _manifest(stg_checksum="aaa"), "run_results": _rr({NOT_NULL: "pass"})},
    )

    signal = await _signal(tools, "test_failure", {"model": "stg_orders"})

    assert signal.severity == "critical"
    assert signal.observed["model_code_changed"] is True


@pytest.mark.asyncio
async def test_failure_with_no_previous_run_reports_unknown_not_false(tmp_path):
    tools, _ = _build_tools(
        tmp_path, current={"manifest": _manifest(), "run_results": _rr({NOT_NULL: "fail"})}
    )

    signal = await _signal(tools, "test_failure", {"model": "stg_orders"})

    assert signal.observed["model_code_changed"] is None
    assert signal.observed["failing_tests"][0]["previously_passed"] is None
    assert signal.observed["all_failing_previously_passed"] is None


@pytest.mark.asyncio
async def test_skipped_tests_are_listed_separately_and_are_not_failures(tmp_path):
    tools, _ = _build_tools(
        tmp_path,
        current={
            "manifest": _manifest(),
            "run_results": _rr({NOT_NULL: "skipped", UNIQUE: "pass"}),
        },
    )

    signal = await _signal(tools, "test_failure", {"model": "stg_orders"})

    assert signal.severity == "ok"
    assert signal.observed["failing_tests"] == []
    assert signal.observed["skipped_tests"] == ["not_null_stg_orders_order_id"]


@pytest.mark.asyncio
async def test_warning_test_is_warn(tmp_path):
    tools, _ = _build_tools(
        tmp_path,
        current={"manifest": _manifest(), "run_results": _rr({NOT_NULL: "warn", UNIQUE: "pass"})},
    )

    signal = await _signal(tools, "test_failure", {"model": "stg_orders"})

    assert signal.severity == "warn"
    assert signal.observed["warning_tests"][0]["test"] == "not_null_stg_orders_order_id"


@pytest.mark.asyncio
async def test_unknown_model_is_reported_not_found(tmp_path):
    tools, _ = _build_tools(tmp_path, current={"manifest": _manifest(), "run_results": _rr({})})

    signal = await _signal(tools, "test_failure", {"model": "no_such_model"})

    assert signal.severity == "unknown"
    assert signal.observed["model_found"] is False
    assert signal.observed["known_models"] == ["fct_orders", "stg_orders"]
    assert "stg_orders" in signal.observed["no_data_reason"]


@pytest.mark.asyncio
async def test_model_with_no_test_results_is_unknown(tmp_path):
    tools, _ = _build_tools(tmp_path, current={"manifest": _manifest(), "run_results": _rr({})})

    signal = await _signal(tools, "test_failure", {"model": "stg_orders"})

    assert signal.severity == "unknown"
    assert signal.observed["not_run_tests"] == [
        "not_null_stg_orders_order_id",
        "unique_stg_orders_order_id",
    ]


# --- model_run_failure ------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "severity"), [("error", "critical"), ("skipped", "warn"), ("success", "ok")]
)
async def test_model_run_failure_severity(tmp_path, status, severity):
    tools, _ = _build_tools(
        tmp_path,
        current={
            "manifest": _manifest(),
            "run_results": _rr(
                {FCT: {"status": status, "message": "Runtime Error in model fct_orders"}}
            ),
        },
        state={"manifest": _manifest(), "run_results": _rr({FCT: "success"})},
    )

    signal = await _signal(tools, "model_run_failure", {"model": "fct_orders"})

    assert signal.severity == severity
    assert signal.observed["status"] == status
    assert signal.observed["previous_status"] == "success"
    assert signal.observed["model_code_changed"] is False
    assert signal.scope["lineage_node_id"] == "job:dbt:fct_orders"


@pytest.mark.asyncio
async def test_model_that_did_not_run_is_unknown(tmp_path):
    tools, _ = _build_tools(tmp_path, current={"manifest": _manifest(), "run_results": _rr({})})

    signal = await _signal(tools, "model_run_failure", {"model": "fct_orders"})

    assert signal.severity == "unknown"
    assert signal.observed["status"] == "not_run"


# --- freshness_check_failure ------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "severity"),
    [("error", "critical"), ("runtime error", "critical"), ("warn", "warn"), ("pass", "ok")],
)
async def test_freshness_severity_and_lineage_node(tmp_path, status, severity):
    tools, _ = _build_tools(
        tmp_path,
        current={"manifest": _manifest(), "sources": _sources(status, age_seconds=18026.8)},
    )

    signal = await _signal(tools, "freshness_check_failure", {"source": "raw.orders_sink"})

    assert signal.tool == "dbt.freshness_check_failure"
    assert signal.severity == severity
    assert signal.scope == {
        "source": "raw.orders_sink",
        "lineage_node_id": "dataset:warehouse:raw.orders_sink",
    }
    assert signal.observed["age_seconds"] == 18026.8


@pytest.mark.asyncio
async def test_freshness_unknown_source_is_reported_not_found(tmp_path):
    tools, _ = _build_tools(tmp_path, current={"manifest": _manifest(), "sources": _sources()})

    signal = await _signal(tools, "freshness_check_failure", {"source": "raw.nope"})

    assert signal.severity == "unknown"
    assert signal.observed["source_found"] is False
    assert signal.observed["known_sources"] == ["raw.orders_sink"]
    assert "raw.orders_sink" in signal.observed["no_data_reason"]


@pytest.mark.asyncio
async def test_freshness_not_checked_is_unknown(tmp_path):
    sources = {"generated_at": GENERATED_AT, "results": {}}
    tools, _ = _build_tools(tmp_path, current={"manifest": _manifest(), "sources": sources})

    signal = await _signal(tools, "freshness_check_failure", {"source": "raw.orders_sink"})

    assert signal.severity == "unknown"
    assert signal.observed["status"] == "not_checked"


# --- incremental_model_drift ------------------------------------------------


def _drift_runs(current: dict, previous: dict, materialized="incremental"):
    return (
        {"manifest": _manifest(fct_materialized=materialized), "run_results": _rr({FCT: current})},
        {"manifest": _manifest(fct_materialized=materialized), "run_results": _rr({FCT: previous})},
    )


def _rows(rows, code="INSERT"):
    return {"status": "success", "adapter_code": code, "rows_affected": rows}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_rows", "severity"), [(5, "critical"), (300, "warn"), (900, "ok")]
)
async def test_incremental_drift_thresholds(tmp_path, current_rows, severity):
    current, state = _drift_runs(_rows(current_rows), _rows(1000))
    tools, _ = _build_tools(tmp_path, current=current, state=state)

    signal = await _signal(tools, "incremental_model_drift", {"model": "fct_orders"})

    assert signal.severity == severity
    assert signal.observed["comparable"] is True
    assert signal.observed["ratio"] == pytest.approx(current_rows / 1000)


@pytest.mark.asyncio
async def test_incremental_drift_after_initial_full_build_is_not_comparable(tmp_path):
    # Verbatim dbt-postgres codes: the first build is CREATE TABLE AS (SELECT
    # 1000), later incremental runs INSERT only the new rows.
    current, state = _drift_runs(_rows(10, "INSERT"), _rows(1000, "SELECT"))
    tools, _ = _build_tools(tmp_path, current=current, state=state)

    signal = await _signal(tools, "incremental_model_drift", {"model": "fct_orders"})

    assert signal.severity == "unknown"
    assert signal.observed["comparable"] is False
    assert signal.observed["previous_adapter_code"] == "SELECT"


@pytest.mark.asyncio
async def test_incremental_drift_without_rows_affected_is_unavailable(tmp_path):
    # dbt-duckdb doesn't report rows_affected.
    current, state = _drift_runs({"status": "success"}, {"status": "success"})
    tools, _ = _build_tools(tmp_path, current=current, state=state)

    signal = await _signal(tools, "incremental_model_drift", {"model": "fct_orders"})

    assert signal.severity == "unknown"
    assert signal.observed["rows_affected_available"] is False


@pytest.mark.asyncio
async def test_incremental_drift_on_non_incremental_model_is_ok(tmp_path):
    current, state = _drift_runs(_rows(5), _rows(1000), materialized="table")
    tools, _ = _build_tools(tmp_path, current=current, state=state)

    signal = await _signal(tools, "incremental_model_drift", {"model": "fct_orders"})

    assert signal.severity == "ok"
    assert signal.observed["materialized"] == "table"


# --- dependency_graph_compile_error -----------------------------------------


def _catalog(**source_columns):
    return {"generated_at": GENERATED_AT, "columns": {SRC: source_columns}}


@pytest.mark.asyncio
@pytest.mark.parametrize(("stg_status", "severity"), [("error", "critical"), ("success", "warn")])
async def test_parent_schema_change_is_reported(tmp_path, stg_status, severity):
    tools, _ = _build_tools(
        tmp_path,
        current={
            "manifest": _manifest(),
            "run_results": _rr({STG: stg_status}),
            "catalog": _catalog(order_id="BIGINT", amount="VARCHAR", currency="VARCHAR"),
        },
        state={
            "manifest": _manifest(),
            "run_results": _rr({STG: "success"}),
            "catalog": _catalog(order_id="BIGINT", amount="DECIMAL(21,1)", loaded_at="TIMESTAMP"),
        },
    )

    signal = await _signal(tools, "dependency_graph_compile_error", {"model": "stg_orders"})

    assert signal.severity == severity
    assert signal.observed["model_code_changed"] is False
    [parent] = signal.observed["changed_parents"]
    assert parent == {
        "parent": "raw.orders_sink",
        "lineage_node_id": "dataset:warehouse:raw.orders_sink",
        "added": ["currency"],
        "removed": ["loaded_at"],
        "retyped": {"amount": ["DECIMAL(21,1)", "VARCHAR"]},
    }


@pytest.mark.asyncio
async def test_unchanged_parents_are_ok(tmp_path):
    catalog = _catalog(order_id="BIGINT")
    tools, _ = _build_tools(
        tmp_path,
        current={"manifest": _manifest(), "run_results": _rr({STG: "error"}), "catalog": catalog},
        state={"manifest": _manifest(), "run_results": _rr({STG: "success"}), "catalog": catalog},
    )

    signal = await _signal(tools, "dependency_graph_compile_error", {"model": "stg_orders"})

    assert signal.severity == "ok"
    assert signal.observed["changed_parents"] == []


@pytest.mark.asyncio
async def test_missing_catalog_is_reported_not_guessed(tmp_path):
    tools, _ = _build_tools(
        tmp_path,
        current={"manifest": _manifest(), "run_results": _rr({STG: "error"})},
        state={"manifest": _manifest(), "run_results": _rr({STG: "success"})},
    )

    signal = await _signal(tools, "dependency_graph_compile_error", {"model": "stg_orders"})

    assert signal.severity == "unknown"
    assert signal.observed["catalog_available"] is False


@pytest.mark.asyncio
async def test_parents_missing_from_catalog_are_unknown(tmp_path):
    catalog = {"generated_at": GENERATED_AT, "columns": {}}
    tools, _ = _build_tools(
        tmp_path,
        current={"manifest": _manifest(), "run_results": _rr({STG: "error"}), "catalog": catalog},
        state={"manifest": _manifest(), "run_results": _rr({STG: "success"}), "catalog": catalog},
    )

    signal = await _signal(tools, "dependency_graph_compile_error", {"model": "stg_orders"})

    assert signal.severity == "unknown"
    assert signal.observed["parents_not_in_catalog"] == [SRC]


# --- unavailable artifacts ----------------------------------------------------


@pytest.mark.asyncio
async def test_missing_current_artifacts_raise_for_the_error_middleware(tmp_path):
    tools, collected = _build_tools(tmp_path, current={})

    with pytest.raises(DbtArtifactsUnavailable):
        await tools["test_failure"].ainvoke({"model": "stg_orders"})
    assert collected == []


_FULL_RUN = {
    "manifest": _manifest(),
    "run_results": _rr({NOT_NULL: "pass", FCT: "success"}),
    "sources": _sources(),
    "catalog": _catalog(order_id="BIGINT"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "args", "signal_class", "scope_class", "observed_class"),
    [
        (
            "test_failure",
            {"model": "stg_orders"},
            TestFailureSignal,
            DbtModelScope,
            TestFailureObserved,
        ),
        (
            "model_run_failure",
            {"model": "fct_orders"},
            ModelRunFailureSignal,
            DbtModelScope,
            ModelRunFailureObserved,
        ),
        (
            "freshness_check_failure",
            {"source": "raw.orders_sink"},
            FreshnessCheckFailureSignal,
            DbtSourceScope,
            FreshnessCheckFailureObserved,
        ),
        (
            "incremental_model_drift",
            {"model": "fct_orders"},
            IncrementalModelDriftSignal,
            DbtModelScope,
            IncrementalModelDriftObserved,
        ),
        (
            "dependency_graph_compile_error",
            {"model": "stg_orders"},
            DependencyGraphCompileErrorSignal,
            DbtModelScope,
            DependencyGraphCompileErrorObserved,
        ),
        # Not found: same signal class, the not-found payload.
        (
            "test_failure",
            {"model": "no_such_model"},
            TestFailureSignal,
            DbtModelScope,
            DbtModelNotFound,
        ),
        (
            "freshness_check_failure",
            {"source": "raw.nope"},
            FreshnessCheckFailureSignal,
            DbtSourceScope,
            DbtSourceNotFound,
        ),
    ],
    ids=[
        "test_failure",
        "model_run_failure",
        "freshness",
        "drift",
        "dependency_graph",
        "model-not-found",
        "source-not-found",
    ],
)
async def test_tools_collect_typed_signals(
    tmp_path, tool, args, signal_class, scope_class, observed_class
):
    tools, collected = _build_tools(tmp_path, current=_FULL_RUN, state=_FULL_RUN)
    await tools[tool].ainvoke(args)

    [signal] = collected
    assert type(signal) is signal_class
    assert type(signal.scope) is scope_class
    assert type(signal.observed) is observed_class
