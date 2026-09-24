from concurrent.futures import Future
from pathlib import Path

import httpx
from confluent_kafka.admin import (
    BrokerMetadata,
    ClusterMetadata,
    ConsumerGroupListing,
    ListConsumerGroupsResult,
    TopicMetadata,
)

from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.kafka.live_gateway import LiveKafkaGateway

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "kafka"


def test_fixture_lists_identifiers_from_the_snapshot():
    gateway = FixtureKafkaGateway(FIXTURE_DIR / "healthy_baseline.json")

    assert gateway.list_topics() == ["orders"]
    assert gateway.list_consumer_groups() == ["billing-svc"]
    assert gateway.list_brokers() == [1]
    assert gateway.list_schema_subjects() == ["orders-value"]


class _FakeAdmin:
    """Returns real confluent-kafka result types, so field names are the
    library's, not assumed."""

    def list_topics(self, timeout=None):
        metadata = ClusterMetadata()
        for name in ("orders", "__consumer_offsets", "orders-sink"):
            topic = TopicMetadata()
            topic.topic = name
            metadata.topics[name] = topic
        for broker_id in (2, 1, 3):
            broker = BrokerMetadata()
            broker.id = broker_id
            metadata.brokers[broker_id] = broker
        return metadata

    def list_consumer_groups(self, **kwargs):
        future: Future = Future()
        future.set_result(
            ListConsumerGroupsResult(
                valid=[
                    ConsumerGroupListing("flink-orders-processing", False),
                    ConsumerGroupListing("billing-svc", False),
                ]
            )
        )
        return future


def _live() -> LiveKafkaGateway:
    gateway = LiveKafkaGateway("localhost:1", "http://jmx:5559", "http://schema-registry:8081")
    gateway._admin = _FakeAdmin()
    gateway._http = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=["orders-value", "orders-sink-value"])
            if request.url.path == "/subjects"
            else httpx.Response(404)
        )
    )
    return gateway


def test_live_lists_topics_without_internal_ones():
    assert _live().list_topics() == ["orders", "orders-sink"]


def test_live_lists_brokers_and_groups_sorted():
    gateway = _live()

    assert gateway.list_brokers() == [1, 2, 3]
    assert gateway.list_consumer_groups() == ["billing-svc", "flink-orders-processing"]


def test_live_lists_schema_subjects():
    assert _live().list_schema_subjects() == ["orders-sink-value", "orders-value"]
