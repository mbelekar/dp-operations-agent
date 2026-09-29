import json

import httpx

from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.flink.live_gateway import LiveFlinkGateway

# Metric ids as Flink 1.18 reports them for the demo job's chained
# source vertex (checked against the live stack, see docs/docker.md).
_SOURCE_METRICS = {
    "0.Source__orders-source.pendingRecords": "0",
    "1.Source__orders-source.pendingRecords": "3",
    "0.Source__orders-source.sourceIdleTime": "121939",
    "1.Source__orders-source.sourceIdleTime": "450",
    "0.Map.currentInputWatermark": "1790692860928",
}


def _live(metrics: dict[str, str]) -> LiveFlinkGateway:
    def handler(request: httpx.Request) -> httpx.Response:
        wanted = request.url.params.get("get")
        ids = wanted.split(",") if wanted else list(metrics)
        if wanted:
            return httpx.Response(200, json=[{"id": i, "value": metrics[i]} for i in ids])
        return httpx.Response(200, json=[{"id": i} for i in ids])

    gateway = LiveFlinkGateway("http://flink:8081")
    gateway._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return gateway


async def test_live_sums_pending_and_takes_the_smallest_idle_time():
    activity = await _live(_SOURCE_METRICS).source_activity("job", "vertex")

    assert activity is not None
    assert (activity.pending_records, activity.idle_ms) == (3, 450)


async def test_live_vertex_without_a_source_has_no_activity():
    metrics = {"0.Map.currentInputWatermark": "1790692860928"}

    assert await _live(metrics).source_activity("job", "vertex") is None


async def test_fixture_reads_source_activity_when_present(tmp_path):
    fixture = tmp_path / "flink.json"
    fixture.write_text(
        json.dumps(
            {"source_activity": {"job": {"source": {"pending_records": 0, "idle_ms": 90000}}}}
        )
    )
    gateway = FixtureFlinkGateway(fixture)

    activity = await gateway.source_activity("job", "source")

    assert activity is not None
    assert (activity.pending_records, activity.idle_ms) == (0, 90000)
    assert await gateway.source_activity("job", "sink") is None
