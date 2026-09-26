"""Deterministic KafkaMetricsGateway backed by a JSON snapshot file.

Snapshot shape (all keys optional; missing lookups return empty results):

{
  "cluster_metadata": {"<topic>": [{"topic", "id", "replicas", "isr", "leader"}, ...]},
  "broker_jmx_metrics": {"<broker_id>": {"<metric_name>": [{"ts", "value"}, ...]}},
  "consumer_group_offsets": {"<group>": {"<partition>": <offset>}},
  "topic_high_watermarks": {"<topic>": {"<partition>": <watermark>}},
  "consumer_group_state_history": {"<group>": [{"ts": ..., "state": ...}, ...]},
  "partition_throughput": {"<topic>": {"<partition>": <bytes_per_sec>}},
  "schema_registry_subject": {"<subject>": {...}}
}
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dp_ops_agent.tools.kafka.gateway import ClusterMetadataView, MetricSample, PartitionMetadata


class FixtureKafkaGateway:
    def __init__(self, snapshot_path: str | Path) -> None:
        self._data: dict[str, Any] = json.loads(Path(snapshot_path).read_text())

    def _keys(self, *sections: str) -> list[str]:
        return sorted({k for s in sections for k in self._data.get(s, {})})

    def list_topics(self) -> list[str]:
        return self._keys("cluster_metadata", "topic_high_watermarks", "partition_throughput")

    def list_consumer_groups(self) -> list[str]:
        return self._keys("consumer_group_offsets", "consumer_group_state_history")

    def list_brokers(self) -> list[int]:
        return sorted(int(b) for b in self._keys("broker_jmx_metrics"))

    def list_schema_subjects(self) -> list[str]:
        return self._keys("schema_registry_subject")

    def cluster_metadata(self, topics: list[str]) -> ClusterMetadataView:
        by_topic: dict[str, Any] = self._data.get("cluster_metadata", {})
        partitions: list[PartitionMetadata] = []
        for topic in topics:
            for p in by_topic.get(topic, []):
                partitions.append(PartitionMetadata(**p))
        return ClusterMetadataView(partitions=partitions)

    def broker_jmx_metrics(
        self, broker_id: int, metric_names: list[str], window_minutes: int
    ) -> dict[str, list[MetricSample]]:
        by_broker: dict[str, Any] = self._data.get("broker_jmx_metrics", {})
        broker_metrics = by_broker.get(str(broker_id), {})
        return {
            name: [MetricSample(**s) for s in broker_metrics.get(name, [])] for name in metric_names
        }

    def consumer_group_offsets(self, group: str) -> dict[int, int]:
        raw = self._data.get("consumer_group_offsets", {}).get(group, {})
        return {int(k): v for k, v in raw.items()}

    def topic_high_watermarks(self, topic: str) -> dict[int, int]:
        raw = self._data.get("topic_high_watermarks", {}).get(topic, {})
        return {int(k): v for k, v in raw.items()}

    def consumer_group_state_history(self, group: str, window_minutes: int) -> list[dict[str, Any]]:
        return self._data.get("consumer_group_state_history", {}).get(group, [])

    def partition_throughput(self, topic: str, window_minutes: int) -> dict[int, float]:
        raw = self._data.get("partition_throughput", {}).get(topic, {})
        return {int(k): float(v) for k, v in raw.items()}

    def schema_registry_subject(self, subject: str) -> dict[str, Any]:
        return self._data.get("schema_registry_subject", {}).get(subject, {})
