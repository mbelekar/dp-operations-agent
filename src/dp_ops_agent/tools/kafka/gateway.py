"""Gateway abstraction over Kafka metadata/metrics sources.

Two implementations share this Protocol: LiveKafkaGateway (confluent-kafka
AdminClient + JMX/Prometheus exporter + Schema Registry REST) and
FixtureKafkaGateway (canned JSON snapshots for tests/demos). This is the
single seam that makes the diagnostic loop testable without live infra.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from pydantic import BaseModel


class PartitionMetadata(BaseModel):
    topic: str
    id: int
    replicas: list[int]
    isr: list[int]
    leader: int  # -1 means offline/no leader


class ClusterMetadataView(BaseModel):
    partitions: list[PartitionMetadata]


class MetricSample(BaseModel):
    ts: datetime
    value: float


class KafkaMetricsGateway(Protocol):
    # The list_* methods are only called when a lookup found nothing, to tell
    # the model which identifiers do exist (see ADR-0009).
    def list_topics(self) -> list[str]: ...

    def list_consumer_groups(self) -> list[str]: ...

    def list_brokers(self) -> list[int]: ...

    def list_schema_subjects(self) -> list[str]: ...

    def cluster_metadata(self, topics: list[str]) -> ClusterMetadataView: ...

    def broker_jmx_metrics(
        self, broker_id: int, metric_names: list[str], window_minutes: int
    ) -> dict[str, list[MetricSample]]: ...

    def consumer_group_offsets(self, group: str) -> dict[int, int]: ...

    def topic_high_watermarks(self, topic: str) -> dict[int, int]: ...

    def consumer_group_state_history(
        self, group: str, window_minutes: int
    ) -> list[dict[str, Any]]: ...

    def partition_throughput(self, topic: str, window_minutes: int) -> dict[int, float]: ...

    def schema_registry_subject(self, subject: str) -> dict[str, Any]: ...
