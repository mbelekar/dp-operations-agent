"""Typed signals, per system. See base.py for the wire-format rules they
follow.

Signal is the discriminated union of every concrete signal class, keyed on
signal_type: validating a signal (e.g. a Diagnosis's signals, or a tool
result read back from JSON) yields the concrete class with typed scope and
observed payloads.
"""

from typing import Annotated

from pydantic import Field

from dp_ops_agent.evidence.signals.dbt import (
    DependencyGraphCompileErrorSignal,
    FreshnessCheckFailureSignal,
    IncrementalModelDriftSignal,
    ModelRunFailureSignal,
    TestFailureSignal,
)
from dp_ops_agent.evidence.signals.flink import (
    BackpressureRatioSignal,
    CheckpointFailureSignal,
    SavepointRestoreFailureSignal,
    StateBackendDiskPressureSignal,
    WatermarkLagSignal,
)
from dp_ops_agent.evidence.signals.kafka import (
    ConsumerLagTrendSignal,
    HotPartitionSkewSignal,
    IsrChurnSignal,
    RebalanceFrequencySignal,
    SchemaRegistryCompatSignal,
    UnderReplicatedPartitionsSignal,
)
from dp_ops_agent.evidence.signals.lineage import LineageUpstreamSignal

Signal = Annotated[
    UnderReplicatedPartitionsSignal
    | IsrChurnSignal
    | ConsumerLagTrendSignal
    | RebalanceFrequencySignal
    | HotPartitionSkewSignal
    | SchemaRegistryCompatSignal
    | CheckpointFailureSignal
    | BackpressureRatioSignal
    | WatermarkLagSignal
    | StateBackendDiskPressureSignal
    | SavepointRestoreFailureSignal
    | LineageUpstreamSignal
    | TestFailureSignal
    | ModelRunFailureSignal
    | FreshnessCheckFailureSignal
    | IncrementalModelDriftSignal
    | DependencyGraphCompileErrorSignal,
    Field(discriminator="signal_type"),
]
