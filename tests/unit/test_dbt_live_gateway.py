import json
from datetime import UTC, datetime

import pytest

from dp_ops_agent.tools.dbt.gateway import DbtArtifactsUnavailable
from dp_ops_agent.tools.dbt.live_gateway import LiveDbtGateway

# Artifact shapes trimmed from real dbt-core 1.12.5 output (dbt-duckdb and
# dbt-postgres): run-results/v6, sources/v3, manifest/v12, catalog/v1. Each
# test writes only the files the run it simulates would have produced.

GENERATED_AT = "2026-09-24T00:08:43.181038Z"


def _run_results(results, which="build"):
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "generated_at": GENERATED_AT,
        },
        "args": {"which": which},
        "results": results,
    }


RUN_RESULTS = _run_results(
    [
        {
            "unique_id": "model.shop.stg_orders",
            "status": "success",
            "message": "OK",
            "failures": None,
            "adapter_response": {"_message": "OK"},
        },
        {
            "unique_id": "test.shop.not_null_stg_orders_order_id.81cfe2fe64",
            "status": "fail",
            "message": "Got 10 results, configured to fail if != 0",
            "failures": 10,
            "adapter_response": {"_message": "OK"},
        },
        {
            "unique_id": "model.shop.fct_orders",
            "status": "skipped",
            "message": "",
            "failures": None,
            "adapter_response": {},
        },
    ]
)

SOURCES = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/sources/v3.json",
        "generated_at": "2026-09-24T00:08:36.756526Z",
    },
    "results": [
        {
            "unique_id": "source.shop.raw.orders_sink",
            "max_loaded_at": "2026-09-23T19:08:09.905258+00:00",
            "snapshotted_at": "2026-09-24T00:08:36.748494+00:00",
            "max_loaded_at_time_ago_in_s": 18026.843236,
            "status": "error",
            "criteria": {
                "warn_after": {"count": 1, "period": "hour"},
                "error_after": {"count": 2, "period": "hour"},
                "filter": None,
            },
        }
    ],
}

MANIFEST = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "generated_at": "2026-09-24T00:08:42.818066Z",
    },
    "nodes": {
        "model.shop.stg_orders": {
            "resource_type": "model",
            "name": "stg_orders",
            "schema": "analytics",
            "alias": "stg_orders",
            "checksum": {"name": "sha256", "checksum": "de32051fcf"},
            "depends_on": {"nodes": ["source.shop.raw.orders_sink"]},
            "config": {"materialized": "view"},
        },
        "model.shop.fct_orders": {
            "resource_type": "model",
            "name": "fct_orders",
            "schema": "analytics",
            "alias": "fct_orders",
            "checksum": {"name": "sha256", "checksum": "2e54994b92"},
            "depends_on": {"nodes": ["model.shop.stg_orders"]},
            "config": {"materialized": "incremental"},
        },
        "test.shop.not_null_stg_orders_order_id.81cfe2fe64": {
            "resource_type": "test",
            "name": "not_null_stg_orders_order_id",
            "schema": "analytics_dbt_test__audit",
            "alias": "not_null_stg_orders_order_id",
            "checksum": {"name": "none", "checksum": ""},
            "depends_on": {"nodes": ["model.shop.stg_orders"]},
            "config": {"materialized": "test"},
            "attached_node": "model.shop.stg_orders",
        },
    },
    "sources": {
        "source.shop.raw.orders_sink": {
            "resource_type": "source",
            "source_name": "raw",
            "name": "orders_sink",
            "schema": "raw",
            "identifier": "orders_sink",
        }
    },
}

CATALOG = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
        "generated_at": "2026-09-24T00:08:39.941583Z",
    },
    "nodes": {
        "model.shop.stg_orders": {
            "columns": {
                "order_id": {"name": "order_id", "type": "BIGINT", "index": 1},
                "amount": {"name": "amount", "type": "DECIMAL(21,1)", "index": 2},
            }
        }
    },
    "sources": {
        "source.shop.raw.orders_sink": {
            "columns": {
                "order_id": {"name": "order_id", "type": "BIGINT", "index": 1},
                "currency": {"name": "currency", "type": "VARCHAR", "index": 2},
            }
        }
    },
}


def _write(directory, **artifacts):
    directory.mkdir(parents=True, exist_ok=True)
    for name, body in artifacts.items():
        (directory / f"{name}.json").write_text(json.dumps(body))
    return directory


def _gateway(tmp_path, state: dict | None = None, **target):
    target_dir = _write(tmp_path / "target", **target)
    state_dir = _write(tmp_path / "state", **state) if state is not None else None
    return LiveDbtGateway(target_dir, state_dir)


def test_run_results_keep_status_failures_and_skips(tmp_path):
    view = _gateway(tmp_path, run_results=RUN_RESULTS).run_results("current")

    assert view.invocation == "build"
    assert view.generated_at == datetime(2026, 9, 24, 0, 8, 43, 181038, tzinfo=UTC)
    test = view.results["test.shop.not_null_stg_orders_order_id.81cfe2fe64"]
    assert (test.status, test.failures) == ("fail", 10)
    assert test.message == "Got 10 results, configured to fail if != 0"
    assert view.results["model.shop.fct_orders"].status == "skipped"


def test_rows_affected_is_read_when_the_adapter_reports_it(tmp_path):
    # Verbatim adapter_response from dbt-postgres for an incremental insert.
    run_results = _run_results(
        [
            {
                "unique_id": "model.shop.fct_orders",
                "status": "success",
                "message": "INSERT 0 10",
                "failures": None,
                "adapter_response": {
                    "_message": "INSERT 0 10",
                    "code": "INSERT",
                    "rows_affected": 10,
                },
            }
        ],
        which="run",
    )

    result = (
        _gateway(tmp_path, run_results=run_results)
        .run_results("current")
        .results["model.shop.fct_orders"]
    )

    assert (result.adapter_code, result.rows_affected) == ("INSERT", 10)


def test_rows_affected_is_none_when_the_adapter_omits_it(tmp_path):
    # dbt-duckdb reports only {"_message": "OK"}.
    result = (
        _gateway(tmp_path, run_results=RUN_RESULTS)
        .run_results("current")
        .results["model.shop.stg_orders"]
    )

    assert (result.adapter_code, result.rows_affected) == (None, None)


@pytest.mark.parametrize("which", ["generate", "source"])
def test_run_results_clobbered_by_a_non_build_command_is_unavailable(tmp_path, which):
    gateway = _gateway(tmp_path, run_results=_run_results([], which=which))

    with pytest.raises(DbtArtifactsUnavailable, match=f"dbt '{which}' invocation"):
        gateway.run_results("current")


def test_clobbered_state_run_results_is_also_unavailable(tmp_path):
    gateway = _gateway(
        tmp_path, state={"run_results": _run_results([], which="generate")}, run_results=RUN_RESULTS
    )

    with pytest.raises(DbtArtifactsUnavailable, match="dbt 'generate' invocation"):
        gateway.run_results("state")


def test_manifest_merges_models_tests_and_sources(tmp_path):
    view = _gateway(tmp_path, manifest=MANIFEST).manifest("current")

    fct = view.nodes["model.shop.fct_orders"]
    assert (fct.name, fct.schema_name, fct.table_name) == ("fct_orders", "analytics", "fct_orders")
    assert fct.checksum == "2e54994b92"
    assert fct.materialized == "incremental"
    assert fct.depends_on == ["model.shop.stg_orders"]
    test = view.nodes["test.shop.not_null_stg_orders_order_id.81cfe2fe64"]
    assert test.attached_node == "model.shop.stg_orders"
    source = view.nodes["source.shop.raw.orders_sink"]
    assert (source.resource_type, source.name, source.source_name) == (
        "source",
        "orders_sink",
        "raw",
    )
    assert (source.schema_name, source.table_name) == ("raw", "orders_sink")


def test_source_freshness_results(tmp_path):
    view = _gateway(tmp_path, sources=SOURCES).source_freshness("current")

    result = view.results["source.shop.raw.orders_sink"]
    assert result.status == "error"
    assert result.age_seconds == pytest.approx(18026.843236)
    assert result.max_loaded_at == datetime(2026, 9, 23, 19, 8, 9, 905258, tzinfo=UTC)
    assert result.criteria["error_after"] == {"count": 2, "period": "hour"}


def test_catalog_columns_for_nodes_and_sources(tmp_path):
    view = _gateway(tmp_path, catalog=CATALOG).catalog("current")

    assert view.columns["model.shop.stg_orders"] == {
        "order_id": "BIGINT",
        "amount": "DECIMAL(21,1)",
    }
    assert view.columns["source.shop.raw.orders_sink"]["currency"] == "VARCHAR"


@pytest.mark.parametrize(
    ("method", "filename"),
    [
        ("run_results", "run_results.json"),
        ("manifest", "manifest.json"),
        ("source_freshness", "sources.json"),
    ],
)
def test_missing_current_artifact_is_unavailable(tmp_path, method, filename):
    gateway = _gateway(tmp_path)

    with pytest.raises(DbtArtifactsUnavailable, match=filename):
        getattr(gateway, method)("current")


def test_missing_catalog_is_none_not_an_error(tmp_path):
    assert _gateway(tmp_path).catalog("current") is None


@pytest.mark.parametrize("method", ["run_results", "manifest", "source_freshness", "catalog"])
def test_no_state_dir_means_no_previous_run(tmp_path, method):
    assert getattr(_gateway(tmp_path, run_results=RUN_RESULTS), method)("state") is None


def test_empty_state_dir_means_no_previous_run(tmp_path):
    gateway = _gateway(tmp_path, state={}, run_results=RUN_RESULTS)

    assert gateway.run_results("state") is None
    assert gateway.manifest("state") is None


def test_state_run_is_read_from_state_dir(tmp_path):
    gateway = _gateway(tmp_path, state={"manifest": MANIFEST}, manifest={**MANIFEST, "nodes": {}})

    assert "model.shop.fct_orders" in gateway.manifest("state").nodes
    assert "model.shop.fct_orders" not in gateway.manifest("current").nodes
