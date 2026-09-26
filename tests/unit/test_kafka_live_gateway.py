"""LiveKafkaGateway against mocked backends: its Schema Registry lookup, and
that its three kinds of I/O (HTTP, AdminClient futures, blocking
AdminClient/Consumer calls) never hold the event loop."""

import asyncio
import threading
import time
from concurrent.futures import Future
from types import SimpleNamespace

import httpx
import pytest

from dp_ops_agent.tools.kafka.live_gateway import LiveKafkaGateway


def _live(response: httpx.Response) -> LiveKafkaGateway:
    gateway = LiveKafkaGateway("localhost:1", "http://jmx:5559", "http://schema-registry:8081")
    gateway._http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response))
    return gateway


async def test_schema_registry_object_body_is_returned_as_is():
    body = {"is_compatible": False, "messages": ["field type changed"]}
    gateway = _live(httpx.Response(200, json=body))
    assert await gateway.schema_registry_subject("orders-value") == body


@pytest.mark.parametrize("body", [b"null", b"[]", b'["orders-value"]', b'"compatible"', b"1"])
async def test_schema_registry_non_object_body_is_no_verdict(body):
    """Anything but a JSON object is no compatibility verdict, same as a 404:
    the tool reports unknown instead of failing validation mid-session."""
    response = httpx.Response(200, content=body, headers={"content-type": "application/json"})
    assert await _live(response).schema_registry_subject("orders-value") == {}


async def test_schema_registry_error_status_is_no_verdict():
    assert await _live(httpx.Response(405)).schema_registry_subject("orders-value") == {}


_METRICS = 'kafka_server_replicamanager_isrshrinkspersec{broker="1"} 0.5\n'


def _gateway_with(handler, admin=None) -> LiveKafkaGateway:
    gateway = LiveKafkaGateway("localhost:1", "http://jmx:5559", "http://schema-registry:8081")
    gateway._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    if admin is not None:
        gateway._admin = admin
    return gateway


async def _ticks_while(awaitable) -> int:
    """How often a concurrent task got to run while `awaitable` was pending:
    0 if the awaitable held the event loop."""
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.02)
            ticks += 1

    task = asyncio.create_task(ticker())
    await awaitable
    task.cancel()
    return ticks


async def test_http_calls_overlap_instead_of_queuing():
    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.3)
        if request.url.path == "/metrics":
            return httpx.Response(200, text=_METRICS)
        return httpx.Response(200, json={"is_compatible": True})

    gateway = _gateway_with(slow)

    start = time.perf_counter()
    await asyncio.gather(
        gateway.schema_registry_subject("orders-value"),
        gateway.broker_jmx_metrics(1, ["kafka_server_replicamanager_isrshrinkspersec"], 10),
    )

    assert time.perf_counter() - start < 0.5  # 0.6s if they queued


class _SlowDescribeAdmin:
    """describe_consumer_groups returns at once, like librdkafka, and its
    futures complete later on another thread."""

    def describe_consumer_groups(self, groups):
        futures = {}
        for group in groups:
            future: Future = Future()
            threading.Timer(
                0.3, future.set_result, [SimpleNamespace(state="STABLE", members=[])]
            ).start()
            futures[group] = future
        return futures


async def test_admin_futures_are_awaited_not_blocked_on():
    gateway = _gateway_with(lambda request: httpx.Response(404), admin=_SlowDescribeAdmin())

    start = time.perf_counter()
    histories = await asyncio.gather(
        gateway.consumer_group_state_history("billing-svc", 10),
        gateway.consumer_group_state_history("flink-orders-processing", 10),
    )

    assert time.perf_counter() - start < 0.5  # 0.6s if each future was waited on in turn
    assert [h[0]["state"] for h in histories] == ["STABLE", "STABLE"]


async def test_an_admin_future_that_never_completes_times_out():
    class _HangingAdmin:
        def list_consumer_groups(self, **kwargs):
            return Future()  # never completed

    gateway = _gateway_with(lambda request: httpx.Response(404), admin=_HangingAdmin())
    gateway._admin_timeout_seconds = 0.1

    # TimeoutError is a backend error to the tool-error middleware.
    with pytest.raises(TimeoutError):
        await gateway.list_consumer_groups()


async def test_blocking_list_topics_runs_off_the_event_loop():
    class _SlowListTopicsAdmin:
        def list_topics(self, timeout=None, **kwargs):
            time.sleep(0.3)
            return SimpleNamespace(topics={"orders": None}, brokers={1: None})

    gateway = _gateway_with(lambda request: httpx.Response(404), admin=_SlowListTopicsAdmin())

    assert await _ticks_while(gateway.list_topics()) >= 5


async def test_concurrent_calls_share_one_metrics_scrape():
    """The JMX aggregator can take seconds per scrape; concurrent tool calls
    must not each start their own."""
    scrapes = 0

    async def slow_metrics(request: httpx.Request) -> httpx.Response:
        nonlocal scrapes
        scrapes += 1
        await asyncio.sleep(0.2)
        return httpx.Response(200, text=_METRICS)

    gateway = _gateway_with(slow_metrics)
    names = ["kafka_server_replicamanager_isrshrinkspersec"]

    first, second = await asyncio.gather(
        gateway.broker_jmx_metrics(1, names, 10), gateway.broker_jmx_metrics(1, names, 10)
    )

    assert scrapes == 1
    assert first[names[0]][0].value == second[names[0]][0].value == 0.5


async def test_aclose_closes_the_http_client():
    gateway = LiveKafkaGateway("localhost:1", "http://jmx:5559", "http://schema-registry:8081")
    await gateway.aclose()
    assert gateway._http.is_closed
