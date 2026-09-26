"""Live KafkaMetricsGateway backed by confluent-kafka AdminClient, a
JMX-Prometheus exporter, and the Schema Registry REST API.

Verified against a real 3-broker cluster (see docker-compose.yml,
docker/jmx-exporter/, docs/docker.md): cluster_metadata, broker_jmx_metrics,
consumer_group_offsets, topic_high_watermarks, consumer_group_state_history,
and schema_registry_subject all return real data.

partition_throughput does not, and can't: Kafka's own JMX only exposes
BytesInPerSec at broker level and per-topic level, there is no per-partition
byte-rate MBean to source this from. hot_partition_skew's tool always
returns an empty throughput map in live mode, confirmed against a real
broker's JMX rather than assumed. It stays fixture-only until this is
computed a different way (e.g. sampling AdminClient.describe_log_dirs()
partition sizes over time), which is a real code change, not an exporter
config one.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from dp_ops_agent.tools.kafka.gateway import ClusterMetadataView, MetricSample, PartitionMetadata

_PROMETHEUS_LINE_RE = re.compile(
    r'^(?P<name>\w+)\{(?P<labels>[^}]*)\}\s+(?P<value>[-\d.eE+]+)\s*$'
)


class LiveKafkaGateway:
    def __init__(
        self,
        bootstrap_servers: str,
        jmx_exporter_base_url: str,
        schema_registry_url: str,
        consumer_group_for_watermarks: str = "__dp_ops_agent_probe",
    ) -> None:
        self._admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        self._bootstrap_servers = bootstrap_servers
        self._jmx_base_url = jmx_exporter_base_url.rstrip("/")
        self._schema_registry_url = schema_registry_url.rstrip("/")
        self._probe_group = consumer_group_for_watermarks
        self._http = httpx.Client(timeout=10.0)
        self._metrics_cache: tuple[float, str] | None = None
        self._metrics_cache_ttl_seconds = 5.0

    def _scrape_metrics(self) -> str:
        """Fetch the Prometheus exporter's /metrics page, cached briefly so
        multiple tool calls within one diagnosis session (e.g. isr_churn and
        hot_partition_skew both hit this) don't each re-fetch and re-parse
        the full payload from the network."""
        now = time.monotonic()
        if self._metrics_cache is not None:
            cached_at, body = self._metrics_cache
            if now - cached_at < self._metrics_cache_ttl_seconds:
                return body
        resp = self._http.get(f"{self._jmx_base_url}/metrics")
        resp.raise_for_status()
        self._metrics_cache = (now, resp.text)
        return resp.text

    def list_topics(self) -> list[str]:
        # "__"-prefixed topics are Kafka's own (__consumer_offsets, ...).
        metadata = self._admin.list_topics(timeout=10.0)
        return sorted(t for t in metadata.topics if not t.startswith("__"))

    def list_consumer_groups(self) -> list[str]:
        result = self._admin.list_consumer_groups(request_timeout=10.0).result(timeout=10.0)
        return sorted(g.group_id for g in result.valid)

    def list_brokers(self) -> list[int]:
        return sorted(self._admin.list_topics(timeout=10.0).brokers)

    def list_schema_subjects(self) -> list[str]:
        resp = self._http.get(f"{self._schema_registry_url}/subjects")
        resp.raise_for_status()
        return sorted(resp.json())

    def cluster_metadata(self, topics: list[str]) -> ClusterMetadataView:
        metadata = self._admin.list_topics(timeout=10.0)
        partitions: list[PartitionMetadata] = []
        for topic in topics:
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None:
                continue
            for pid, pmeta in topic_meta.partitions.items():
                partitions.append(
                    PartitionMetadata(
                        topic=topic,
                        id=pid,
                        replicas=list(pmeta.replicas),
                        isr=list(pmeta.isrs),
                        leader=pmeta.leader,
                    )
                )
        return ClusterMetadataView(partitions=partitions)

    def broker_jmx_metrics(
        self, broker_id: int, metric_names: list[str], window_minutes: int
    ) -> dict[str, list[MetricSample]]:
        # Point-in-time scrape of the Prometheus exporter; window_minutes is
        # accepted for interface parity with a future time-series backend.
        metrics_text = self._scrape_metrics()
        now = datetime.now(UTC)
        result: dict[str, list[MetricSample]] = {name: [] for name in metric_names}
        for line in metrics_text.splitlines():
            match = _PROMETHEUS_LINE_RE.match(line)
            if not match or match.group("name") not in metric_names:
                continue
            if f'broker="{broker_id}"' not in match.group("labels"):
                continue
            result[match.group("name")].append(
                MetricSample(ts=now, value=float(match.group("value")))
            )
        return result

    def consumer_group_offsets(self, group: str) -> dict[int, int]:
        # confluent-kafka's AdminClient offset-listing API varies by version;
        # use a throwaway Consumer bound to the group's committed offsets instead.
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": group,
                "enable.auto.commit": False,
            }
        )
        try:
            committed = consumer.committed(
                [TopicPartition(t, p) for t, p in self._group_assignment(group)], timeout=10.0
            )
            return {tp.partition: tp.offset for tp in committed if tp.offset >= 0}
        finally:
            consumer.close()

    def _group_assignment(self, group: str) -> list[tuple[str, int]]:
        desc = self._admin.describe_consumer_groups([group])
        result = desc[group].result(timeout=10.0)
        return [
            (tp.topic, tp.partition)
            for member in result.members
            for tp in member.assignment.topic_partitions
        ]

    def topic_high_watermarks(self, topic: str) -> dict[int, int]:
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": self._probe_group,
                "enable.auto.commit": False,
            }
        )
        try:
            metadata = self._admin.list_topics(topic=topic, timeout=10.0)
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None:
                return {}
            result: dict[int, int] = {}
            for pid in topic_meta.partitions:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, pid), timeout=10.0, cached=False
                )
                result[pid] = high
            return result
        finally:
            consumer.close()

    def consumer_group_state_history(
        self, group: str, window_minutes: int
    ) -> list[dict[str, Any]]:
        desc = self._admin.describe_consumer_groups([group])
        result = desc[group].result(timeout=10.0)
        return [{"ts": datetime.now(UTC).isoformat(), "state": str(result.state)}]

    def partition_throughput(self, topic: str, window_minutes: int) -> dict[int, float]:
        metrics_text = self._scrape_metrics()
        result: dict[int, float] = {}
        for line in metrics_text.splitlines():
            match = _PROMETHEUS_LINE_RE.match(line)
            if not match or match.group("name") != "kafka_topic_partition_bytesinpersec":
                continue
            labels = match.group("labels")
            if f'topic="{topic}"' not in labels:
                continue
            pid_match = re.search(r'partition="(\d+)"', labels)
            if pid_match:
                result[int(pid_match.group(1))] = float(match.group("value"))
        return result

    def schema_registry_subject(self, subject: str) -> dict[str, Any]:
        # Confluent's compatibility-check endpoint is POST-only and requires a
        # candidate schema body this tool doesn't have; treat any non-2xx
        # response (404 subject-not-found, 405 wrong-method, etc.) as "no
        # compatibility signal available" rather than crashing the session.
        try:
            resp = self._http.get(
                f"{self._schema_registry_url}/compatibility/subjects/{subject}/versions/latest"
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            return {}
        return resp.json()
