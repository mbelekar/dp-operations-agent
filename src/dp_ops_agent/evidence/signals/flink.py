"""Scope and observed payloads for the Flink signals. Field order is key
order in the JSON; see base.py."""

from __future__ import annotations

from dp_ops_agent.evidence.signals.base import Absent, PassThrough, Payload, Scope, SignalTargets
from dp_ops_agent.tools.flink.gateway import CheckpointCounts, SubtaskBackpressure


class JobScope(Scope):
    job_id: str

    def targets(self) -> SignalTargets:
        return SignalTargets(job_ids=frozenset({self.job_id}))


class JobVertexScope(Scope):
    job_id: str
    vertex_id: str

    def targets(self) -> SignalTargets:
        return SignalTargets(job_ids=frozenset({self.job_id}))


class NamedRef(Payload):
    """A job or vertex the model can pass back: the id, and its display name."""

    id: str
    name: str


class CheckpointFailureObserved(Payload):
    counts: CheckpointCounts
    most_recent_status: str | None
    # The checkpoint tool emits known_jobs before no_data_reason.
    known_jobs: Absent[list[NamedRef]] = None
    no_data_reason: Absent[str] = None


class BackpressureRatioObserved(Payload):
    status: str
    backpressure_level: str
    subtasks: list[SubtaskBackpressure]
    no_data_reason: Absent[str] = None
    known_vertices: Absent[list[NamedRef]] = None
    known_jobs: Absent[list[NamedRef]] = None


class WatermarkLagObserved(Payload):
    # From a gateway dict, not a pydantic model: keep the gateway's number type.
    lag_ms_by_subtask: dict[str, int | float]
    max_lag_ms: int | float
    no_data_reason: Absent[str] = None
    known_vertices: Absent[list[NamedRef]] = None
    known_jobs: Absent[list[NamedRef]] = None


class StateBackendDiskPressureObserved(Payload):
    metrics: dict[str, int | float]
    no_data_reason: Absent[str] = None
    known_vertices: Absent[list[NamedRef]] = None
    known_jobs: Absent[list[NamedRef]] = None


class SavepointRestoreFailureObserved(Payload):
    matching_exceptions: list[PassThrough]
    total_exceptions: int
