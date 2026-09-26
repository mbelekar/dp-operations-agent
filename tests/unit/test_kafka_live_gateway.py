"""LiveKafkaGateway's Schema Registry lookup, against a mocked HTTP server."""

import httpx
import pytest

from dp_ops_agent.tools.kafka.live_gateway import LiveKafkaGateway


def _live(response: httpx.Response) -> LiveKafkaGateway:
    gateway = LiveKafkaGateway("localhost:1", "http://jmx:5559", "http://schema-registry:8081")
    gateway._http = httpx.Client(transport=httpx.MockTransport(lambda request: response))
    return gateway


def test_schema_registry_object_body_is_returned_as_is():
    body = {"is_compatible": False, "messages": ["field type changed"]}
    assert _live(httpx.Response(200, json=body)).schema_registry_subject("orders-value") == body


@pytest.mark.parametrize("body", [b"null", b"[]", b'["orders-value"]', b'"compatible"', b"1"])
def test_schema_registry_non_object_body_is_no_verdict(body):
    """Anything but a JSON object is no compatibility verdict, same as a 404:
    the tool reports unknown instead of failing validation mid-session."""
    response = httpx.Response(200, content=body, headers={"content-type": "application/json"})
    assert _live(response).schema_registry_subject("orders-value") == {}


def test_schema_registry_error_status_is_no_verdict():
    assert _live(httpx.Response(405)).schema_registry_subject("orders-value") == {}
