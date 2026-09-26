"""Scope and observed payloads for the Flink signals. Field order is key
order in the JSON; see base.py."""

from __future__ import annotations

from typing import ClassVar, Literal

from dp_ops_agent.evidence.signals.base import (
    Absent,
    DiagnosedSystem,
    PassThrough,
    Payload,
    Scope,
    SignalBase,
    SignalTargets,
)


class JobScope(Scope):
    job_id: str

    def targets(self) -> SignalTargets:
        return SignalTargets(job_ids=frozenset({self.job_id}))


class JobVertexScope(Scope):
    job_id: str
    vertex_id: str

    def targets(self) -> SignalTargets:
        return SignalTargets(job_ids=frozenset({self.job_id}))


class CheckpointCounts(Payload):
    """Wire format, owned here rather than borrowed from the gateway's model
    of the same shape, so a gateway-side field can't leak into the JSON."""

    completed: int
    failed: int
    in_progress: int
    restored: int
    total: int


class SubtaskBackpressure(Payload):
    subtask: int
    ratio: float


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


# --- signals ------------------------------------------------------------------
# tool and signal_type are fixed per class; they keep their position in the
# JSON (SignalBase's field order) even though they're redeclared here.


class CheckpointFailureSignal(SignalBase[JobScope, CheckpointFailureObserved]):
    tool: Literal["flink.checkpoint_failure"] = "flink.checkpoint_failure"
    signal_type: Literal["checkpoint_failure"] = "checkpoint_failure"
    system: ClassVar[DiagnosedSystem | None] = "flink"


class BackpressureRatioSignal(SignalBase[JobVertexScope, BackpressureRatioObserved]):
    tool: Literal["flink.backpressure_ratio"] = "flink.backpressure_ratio"
    signal_type: Literal["backpressure_ratio"] = "backpressure_ratio"
    system: ClassVar[DiagnosedSystem | None] = "flink"


class WatermarkLagSignal(SignalBase[JobVertexScope, WatermarkLagObserved]):
    tool: Literal["flink.watermark_lag"] = "flink.watermark_lag"
    signal_type: Literal["watermark_lag"] = "watermark_lag"
    system: ClassVar[DiagnosedSystem | None] = "flink"


class StateBackendDiskPressureSignal(SignalBase[JobVertexScope, StateBackendDiskPressureObserved]):
    tool: Literal["flink.state_backend_disk_pressure"] = "flink.state_backend_disk_pressure"
    signal_type: Literal["state_backend_disk_pressure"] = "state_backend_disk_pressure"
    system: ClassVar[DiagnosedSystem | None] = "flink"


class SavepointRestoreFailureSignal(SignalBase[JobScope, SavepointRestoreFailureObserved]):
    tool: Literal["flink.savepoint_restore_failure"] = "flink.savepoint_restore_failure"
    signal_type: Literal["savepoint_restore_failure"] = "savepoint_restore_failure"
    system: ClassVar[DiagnosedSystem | None] = "flink"
