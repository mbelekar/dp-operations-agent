"""DbtArtifactsGateway over a dbt-core target/ directory (and, optionally,
a --state directory holding the previous run's artifacts).

dbt overwrites run_results.json on every command, including `dbt docs
generate` and each of `dbt run` / `dbt test` separately, so the run order
that leaves all four artifacts usable is:

    dbt source freshness && dbt docs generate && dbt build

(catalog.json then describes relations as of the previous build, which is
enough for spotting an upstream schema change). Copy target/ to the state
directory before the next run to keep it as "the previous run".
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from dp_ops_agent.tools.dbt.gateway import (
    ArtifactRun,
    CatalogView,
    DbtArtifactsUnavailable,
    ManifestNode,
    ManifestView,
    NodeRunResult,
    RunResultsView,
    SourceFreshnessResult,
    SourceFreshnessView,
)

_RUN_RESULTS_INVOCATIONS = ("build", "run", "test")


class LiveDbtGateway:
    def __init__(self, target_dir: str | Path, state_dir: str | Path | None = None) -> None:
        self._dirs: dict[ArtifactRun, Path | None] = {
            "current": Path(target_dir),
            "state": Path(state_dir) if state_dir is not None else None,
        }

    async def _load(self, run: ArtifactRun, filename: str, required: bool) -> dict[str, Any] | None:
        directory = self._dirs[run]
        path = directory / filename if directory is not None else None
        if path is None or not path.is_file():
            if required and run == "current":
                raise DbtArtifactsUnavailable(f"{filename} not found in dbt target dir {directory}")
            return None
        # Off the event loop: manifest.json can be several MB to read and parse,
        # and the tool node runs other tool calls concurrently.
        return await asyncio.to_thread(lambda: json.loads(path.read_text()))

    async def run_results(self, run: ArtifactRun) -> RunResultsView | None:
        raw = await self._load(run, "run_results.json", required=True)
        if raw is None:
            return None
        which = raw.get("args", {}).get("which")
        if which not in _RUN_RESULTS_INVOCATIONS:
            raise DbtArtifactsUnavailable(
                f"{run} run_results.json was written by a dbt {which!r} invocation "
                "(args.which), not build/run/test; "
                "it was overwritten after the build (run `dbt docs generate` before "
                "`dbt build`, not after)"
            )
        results = {}
        for r in raw["results"]:
            adapter_response = r.get("adapter_response") or {}
            results[r["unique_id"]] = NodeRunResult(
                unique_id=r["unique_id"],
                status=r["status"],
                message=r.get("message"),
                failures=r.get("failures"),
                adapter_code=adapter_response.get("code"),
                rows_affected=adapter_response.get("rows_affected"),
            )
        return RunResultsView(
            generated_at=raw["metadata"].get("generated_at"), invocation=which, results=results
        )

    async def source_freshness(self, run: ArtifactRun) -> SourceFreshnessView | None:
        raw = await self._load(run, "sources.json", required=True)
        if raw is None:
            return None
        return SourceFreshnessView(
            generated_at=raw["metadata"].get("generated_at"),
            results={
                r["unique_id"]: SourceFreshnessResult(
                    unique_id=r["unique_id"],
                    status=r["status"],
                    max_loaded_at=r.get("max_loaded_at"),
                    age_seconds=r.get("max_loaded_at_time_ago_in_s"),
                    criteria=r.get("criteria") or {},
                )
                for r in raw["results"]
            },
        )

    async def manifest(self, run: ArtifactRun) -> ManifestView | None:
        raw = await self._load(run, "manifest.json", required=True)
        if raw is None:
            return None
        nodes = {}
        for unique_id, n in raw.get("nodes", {}).items():
            nodes[unique_id] = ManifestNode(
                unique_id=unique_id,
                resource_type=n["resource_type"],
                name=n["name"],
                schema_name=n.get("schema"),
                table_name=n.get("alias") or n["name"],
                checksum=(n.get("checksum") or {}).get("checksum"),
                depends_on=(n.get("depends_on") or {}).get("nodes", []),
                materialized=(n.get("config") or {}).get("materialized"),
                attached_node=n.get("attached_node"),
            )
        for unique_id, s in raw.get("sources", {}).items():
            nodes[unique_id] = ManifestNode(
                unique_id=unique_id,
                resource_type="source",
                name=s["name"],
                schema_name=s.get("schema"),
                table_name=s.get("identifier") or s["name"],
                source_name=s.get("source_name"),
            )
        return ManifestView(generated_at=raw["metadata"].get("generated_at"), nodes=nodes)

    async def catalog(self, run: ArtifactRun) -> CatalogView | None:
        raw = await self._load(run, "catalog.json", required=False)
        if raw is None:
            return None
        columns = {
            unique_id: {c["name"]: c["type"] for c in entry["columns"].values()}
            for section in ("nodes", "sources")
            for unique_id, entry in raw.get(section, {}).items()
        }
        return CatalogView(generated_at=raw["metadata"].get("generated_at"), columns=columns)
