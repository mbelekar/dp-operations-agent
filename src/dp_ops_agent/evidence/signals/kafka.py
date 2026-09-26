"""Scope and observed payloads for the Kafka signals. Field order is key
order in the JSON; see base.py."""

from __future__ import annotations

from typing import Literal

from dp_ops_agent.evidence.signals.base import (
    Absent,
    PassThrough,
    Payload,
    Scope,
    SignalBase,
    SignalTargets,
)


class TopicsScope(Scope):
    topics: str  # comma-joined, as the tool was called

    def targets(self) -> SignalTargets:
        return SignalTargets(topics=frozenset(self.topics.split(",")))


class BrokerScope(Scope):
    broker_id: str


class GroupTopicScope(Scope):
    group: str
    topic: str

    def targets(self) -> SignalTargets:
        return SignalTargets(groups=frozenset({self.group}), topics=frozenset({self.topic}))


class GroupScope(Scope):
    group: str

    def targets(self) -> SignalTargets:
        return SignalTargets(groups=frozenset({self.group}))


class TopicScope(Scope):
    topic: str

    def targets(self) -> SignalTargets:
        return SignalTargets(topics=frozenset({self.topic}))


class SubjectScope(Scope):
    subject: str


class PartitionState(Payload):
    topic: str
    partition: int
    replicas: list[int]
    isr: list[int]
    under_replicated: bool
    offline: bool


class UnderReplicatedPartitionsObserved(Payload):
    partitions: list[PartitionState]
    no_data_reason: Absent[str] = None
    known_topics: Absent[list[str]] = None


class IsrChurnObserved(Payload):
    isr_shrinks_per_sec: list[float]
    isr_expands_per_sec: list[float]
    max_shrink_rate: float
    no_data_reason: Absent[str] = None
    known_brokers: Absent[list[int]] = None


class ConsumerLagTrendObserved(Payload):
    lag_by_partition: dict[str, int]
    max_lag: int
    partitions_without_committed_offset: list[str]
    no_data_reason: Absent[str] = None
    known_topics: Absent[list[str]] = None
    known_groups: Absent[list[str]] = None


class RebalanceFrequencyObserved(Payload):
    state_history: list[PassThrough]
    rebalance_count: int
    no_data_reason: Absent[str] = None
    known_groups: Absent[list[str]] = None


class HotPartitionSkewObserved(Payload):
    # Per-partition values come from a gateway dict, not a pydantic model,
    # so they keep whatever numeric type the gateway returned.
    throughput_by_partition: dict[str, int | float]
    avg_throughput: float
    skew_ratio: float
    no_data_reason: Absent[str] = None
    known_topics: Absent[list[str]] = None


class SchemaRegistryCompatObserved(Payload):
    raw: PassThrough
    is_compatible: bool | None
    no_data_reason: Absent[str] = None
    known_subjects: Absent[list[str]] = None


# --- signals ------------------------------------------------------------------
# tool and signal_type are fixed per class; they keep their position in the
# JSON (SignalBase's field order) even though they're redeclared here.


class UnderReplicatedPartitionsSignal(SignalBase[TopicsScope, UnderReplicatedPartitionsObserved]):
    tool: Literal["kafka.under_replicated_partitions"] = "kafka.under_replicated_partitions"
    signal_type: Literal["under_replicated_partitions"] = "under_replicated_partitions"


class IsrChurnSignal(SignalBase[BrokerScope, IsrChurnObserved]):
    tool: Literal["kafka.isr_churn"] = "kafka.isr_churn"
    signal_type: Literal["isr_churn"] = "isr_churn"


class ConsumerLagTrendSignal(SignalBase[GroupTopicScope, ConsumerLagTrendObserved]):
    tool: Literal["kafka.consumer_lag_trend"] = "kafka.consumer_lag_trend"
    signal_type: Literal["consumer_lag_trend"] = "consumer_lag_trend"


class RebalanceFrequencySignal(SignalBase[GroupScope, RebalanceFrequencyObserved]):
    tool: Literal["kafka.rebalance_frequency"] = "kafka.rebalance_frequency"
    signal_type: Literal["rebalance_frequency"] = "rebalance_frequency"


class HotPartitionSkewSignal(SignalBase[TopicScope, HotPartitionSkewObserved]):
    tool: Literal["kafka.hot_partition_skew"] = "kafka.hot_partition_skew"
    signal_type: Literal["hot_partition_skew"] = "hot_partition_skew"


class SchemaRegistryCompatSignal(SignalBase[SubjectScope, SchemaRegistryCompatObserved]):
    tool: Literal["kafka.schema_registry_compat"] = "kafka.schema_registry_compat"
    signal_type: Literal["schema_registry_compat"] = "schema_registry_compat"
