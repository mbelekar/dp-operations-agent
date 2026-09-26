"""Deterministic DbtArtifactsGateway backed by a JSON snapshot file.

Unlike LiveDbtGateway, the snapshot holds the gateway's own view shapes
(tools/dbt/gateway.py), not raw dbt artifacts, the same as every other
fixture gateway. Missing keys follow the Protocol's missing-artifact
semantics, so a fixture can also simulate an unavailable or first-ever run.

{
  "current": {
    "run_results": {"generated_at": ..., "invocation": "build", "results": {"<unique_id>": {...NodeRunResult}}},
    "sources": {"generated_at": ..., "results": {"<unique_id>": {...SourceFreshnessResult}}},
    "manifest": {"generated_at": ..., "nodes": {"<unique_id>": {...ManifestNode}}},
    "catalog": {"generated_at": ..., "columns": {"<unique_id>": {"<column>": "<type>"}}}
  },
  "state": {... same keys, for the previous run ...}
}
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

from dp_ops_agent.tools.dbt.gateway import (
    ArtifactRun,
    CatalogView,
    DbtArtifactsUnavailable,
    ManifestView,
    RunResultsView,
    SourceFreshnessView,
)

V = TypeVar("V", bound=BaseModel)


class FixtureDbtGateway:
    def __init__(self, snapshot_path: str | Path) -> None:
        self._data: dict[str, Any] = json.loads(Path(snapshot_path).read_text())

    def _view(self, run: ArtifactRun, key: str, view: type[V], required: bool = True) -> V | None:
        raw = self._data.get(run, {}).get(key)
        if raw is None:
            if required and run == "current":
                raise DbtArtifactsUnavailable(f"fixture has no current {key}")
            return None
        return view(**raw)

    def run_results(self, run: ArtifactRun) -> RunResultsView | None:
        return self._view(run, "run_results", RunResultsView)

    def source_freshness(self, run: ArtifactRun) -> SourceFreshnessView | None:
        return self._view(run, "sources", SourceFreshnessView)

    def manifest(self, run: ArtifactRun) -> ManifestView | None:
        return self._view(run, "manifest", ManifestView)

    def catalog(self, run: ArtifactRun) -> CatalogView | None:
        return self._view(run, "catalog", CatalogView, required=False)
