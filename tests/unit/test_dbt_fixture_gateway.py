import json

import pytest

from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.dbt.gateway import DbtArtifactsUnavailable

SNAPSHOT = {
    "current": {
        "run_results": {
            "invocation": "build",
            "results": {"model.shop.stg_orders": {"unique_id": "model.shop.stg_orders", "status": "success"}},
        },
        "manifest": {
            "nodes": {
                "model.shop.stg_orders": {
                    "unique_id": "model.shop.stg_orders",
                    "resource_type": "model",
                    "name": "stg_orders",
                    "checksum": "abc",
                }
            }
        },
    },
    "state": {"run_results": {"invocation": "build", "results": {}}},
}


def _gateway(tmp_path, snapshot=SNAPSHOT) -> FixtureDbtGateway:
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snapshot))
    return FixtureDbtGateway(path)


def test_loads_views_for_each_run(tmp_path):
    gateway = _gateway(tmp_path)

    assert gateway.run_results("current").results["model.shop.stg_orders"].status == "success"
    assert gateway.manifest("current").nodes["model.shop.stg_orders"].checksum == "abc"
    assert gateway.run_results("state").results == {}


def test_missing_current_artifact_is_unavailable(tmp_path):
    with pytest.raises(DbtArtifactsUnavailable, match="sources"):
        _gateway(tmp_path).source_freshness("current")


def test_missing_state_artifact_and_catalog_are_none(tmp_path):
    gateway = _gateway(tmp_path)

    assert gateway.manifest("state") is None
    assert gateway.catalog("current") is None
