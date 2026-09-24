"""Gateway abstraction over Flink job metadata/metrics sources.

Two implementations share this Protocol: LiveFlinkGateway (Flink REST API
over httpx) and FixtureFlinkGateway (canned JSON snapshots for tests/demos),
mirroring tools/kafka/gateway.py's split.

Verified against the real Flink REST API before writing this: GET
/jobs/:jobid/checkpoints, GET /jobs/:jobid/vertices/:vertexid/backpressure,
GET /jobs/:jobid/vertices/:vertexid/metrics, GET /jobs/:jobid/exceptions.
There is no dedicated REST field for "savepoint restore failure" — it has to
be inferred from the exceptions endpoint (see tools/flink/tools.py), so
job_exceptions() is the one method here without a matching clean status
field on the Flink side.
"""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import BaseModel


class CheckpointCounts(BaseModel):
    completed: int
    failed: int
    in_progress: int
    restored: int
    total: int


class CheckpointHistoryEntry(BaseModel):
    id: int
    status: str
    trigger_timestamp: int  # epoch ms
    end_to_end_duration: int | None = None  # ms


class CheckpointHistoryView(BaseModel):
    counts: CheckpointCounts
    history: list[CheckpointHistoryEntry]


class SubtaskBackpressure(BaseModel):
    subtask: int
    ratio: float


class BackpressureView(BaseModel):
    status: str
    backpressure_level: str
    subtasks: list[SubtaskBackpressure]


class NamedId(BaseModel):
    """A job or vertex as the REST API identifies it: tools take the id, the
    name is what a human (or the model) recognizes it by. Live ids are hex."""

    id: str
    name: str


class FlinkMetricsGateway(Protocol):
    # Only called when a lookup found nothing, to tell the model which
    # identifiers do exist (see ADR-0009).
    def list_jobs(self) -> list[NamedId]: ...

    def list_vertices(self, job_id: str) -> list[NamedId]: ...

    def checkpoint_history(self, job_id: str) -> CheckpointHistoryView: ...

    def backpressure(self, job_id: str, vertex_id: str) -> BackpressureView: ...

    def watermark_lag(self, job_id: str, vertex_id: str) -> dict[int, float]: ...

    def task_manager_disk_metrics(
        self, job_id: str, vertex_id: str
    ) -> dict[str, float]: ...

    def job_exceptions(
        self, job_id: str, window_minutes: int
    ) -> list[dict[str, Any]]: ...
