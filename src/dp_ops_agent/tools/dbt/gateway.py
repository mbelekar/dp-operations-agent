"""Gateway abstraction over dbt run artifacts.

Two implementations share this Protocol: LiveDbtGateway (a dbt-core target/
directory on disk) and FixtureDbtGateway (canned JSON snapshots for
tests/demos), mirroring tools/flink/gateway.py's split.

Verified against real dbt-core 1.12.5 output (dbt-duckdb and dbt-postgres)
before writing this: run-results/v6, sources/v3, manifest/v12, catalog/v1.

Every method takes a run: "current" is the latest run (target_dir), "state"
is the previous run, in the directory dbt's own --state flag would point at.
Comparing the two is how the dbt tools tell "the model's code changed" apart
from "the model is unchanged but its inputs went bad" (see tools/dbt/tools.py).

Missing-artifact semantics, shared by every implementation:
- The current run's run_results, manifest, or sources missing raises
  DbtArtifactsUnavailable, which the tool-error middleware reports to the
  model as a tool error (orchestrator/tool_errors.py).
- Anything missing from the state run returns None: there simply is no
  previous run to compare against, which the tools report as unknown.
- A missing catalog returns None in either run; it only exists if
  `dbt docs generate` ran.
- A run_results.json written by anything other than build/run/test (e.g.
  `dbt docs generate`, which overwrites it) raises in either run: that's a
  setup mistake, not "no previous run", and treating it as data would report
  every node as absent.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field

ArtifactRun = Literal["current", "state"]


class DbtArtifactsUnavailable(Exception):
    """A dbt artifact the tool needs is missing or unusable."""


class NodeRunResult(BaseModel):
    unique_id: str
    status: str
    message: str | None = None
    failures: int | None = None
    # adapter_response is adapter-specific: dbt-postgres reports code and
    # rows_affected, dbt-duckdb reports neither.
    adapter_code: str | None = None
    rows_affected: int | None = None


class RunResultsView(BaseModel):
    generated_at: datetime | None = None
    invocation: str  # args.which: "build", "run", or "test"
    results: dict[str, NodeRunResult]


class SourceFreshnessResult(BaseModel):
    unique_id: str
    status: str  # "pass", "warn", "error", or "runtime error"
    max_loaded_at: datetime | None = None
    age_seconds: float | None = None
    criteria: dict[str, Any] = Field(default_factory=dict)


class SourceFreshnessView(BaseModel):
    generated_at: datetime | None = None
    results: dict[str, SourceFreshnessResult]


class ManifestNode(BaseModel):
    unique_id: str
    resource_type: str  # "model", "test", "source", ...
    name: str
    schema_name: str | None = None
    table_name: str | None = None  # alias for models, identifier for sources
    source_name: str | None = None  # sources only
    checksum: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    materialized: str | None = None
    attached_node: str | None = None  # tests only: the model the test is on


class ManifestView(BaseModel):
    generated_at: datetime | None = None
    nodes: dict[str, ManifestNode]  # models, tests, and sources, by unique_id


class CatalogView(BaseModel):
    generated_at: datetime | None = None
    columns: dict[str, dict[str, str]]  # unique_id -> column name -> type


class DbtArtifactsGateway(Protocol):
    async def run_results(self, run: ArtifactRun) -> RunResultsView | None: ...

    async def source_freshness(self, run: ArtifactRun) -> SourceFreshnessView | None: ...

    async def manifest(self, run: ArtifactRun) -> ManifestView | None: ...

    async def catalog(self, run: ArtifactRun) -> CatalogView | None: ...
