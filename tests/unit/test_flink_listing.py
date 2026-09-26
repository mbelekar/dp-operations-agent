import asyncio
import time
from pathlib import Path

import httpx

from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.flink.live_gateway import LiveFlinkGateway

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "flink"


async def test_fixture_lists_jobs_and_vertices_from_the_snapshot():
    gateway = FixtureFlinkGateway(FIXTURE_DIR / "backpressure_incident.json")

    assert [j.id for j in await gateway.list_jobs()] == ["orders-processing-job"]
    vertices = await gateway.list_vertices("orders-processing-job")
    assert {v.id for v in vertices} >= {"sink"}
    assert all(v.name == v.id for v in vertices)
    assert await gateway.list_vertices("no-such-job") == []


# Shapes from Flink's REST API: /jobs/overview lists JobDetails ("jid",
# "name"); /jobs/:jid has "vertices" with "id" and "name" (the same payload
# docker/app-live-entrypoint.py reads the vertex id from).
_OVERVIEW = {"jobs": [{"jid": "a1b2c3", "name": "orders-processing-job", "state": "RUNNING"}]}
_JOB = {
    "jid": "a1b2c3",
    "name": "orders-processing-job",
    "vertices": [
        {"id": "cbc357ccb763df2852fee8c4fc7d55f2", "name": "Source: orders", "parallelism": 1},
        {
            "id": "90bea66de1c231edf33913ecd54406c1",
            "name": "Map -> Sink: orders-sink",
            "parallelism": 1,
        },
    ],
}


def _live() -> LiveFlinkGateway:
    def handler(request: httpx.Request) -> httpx.Response:
        routes = {"/jobs/overview": _OVERVIEW, "/jobs/a1b2c3": _JOB}
        body = routes.get(request.url.path)
        return httpx.Response(200, json=body) if body else httpx.Response(404, json={"errors": []})

    gateway = LiveFlinkGateway("http://flink:8081")
    gateway._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return gateway


async def test_live_lists_jobs_with_names():
    [job] = await _live().list_jobs()

    assert (job.id, job.name) == ("a1b2c3", "orders-processing-job")


async def test_live_lists_vertex_ids_with_names():
    vertices = await _live().list_vertices("a1b2c3")

    assert [(v.id, v.name) for v in vertices] == [
        ("cbc357ccb763df2852fee8c4fc7d55f2", "Source: orders"),
        ("90bea66de1c231edf33913ecd54406c1", "Map -> Sink: orders-sink"),
    ]


_CHECKPOINTS = {
    "counts": {"completed": 12, "failed": 1, "in_progress": 0, "restored": 0, "total": 13},
    "history": [],
}
_BACKPRESSURE = {"status": "ok", "backpressure-level": "ok", "subtasks": []}


async def test_concurrent_calls_overlap_instead_of_queuing():
    """The tool node runs a turn's tool calls concurrently; a call waiting on
    the Flink REST API must not hold the event loop."""

    async def slow_flink(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.3)
        if request.url.path.endswith("/checkpoints"):
            return httpx.Response(200, json=_CHECKPOINTS)
        return httpx.Response(200, json=_BACKPRESSURE)

    gateway = LiveFlinkGateway("http://flink:8081")
    gateway._http = httpx.AsyncClient(transport=httpx.MockTransport(slow_flink))

    start = time.perf_counter()
    checkpoints, backpressure = await asyncio.gather(
        gateway.checkpoint_history("a1b2c3"), gateway.backpressure("a1b2c3", "v1")
    )

    assert time.perf_counter() - start < 0.5  # 0.6s if they queued
    assert checkpoints.counts.failed == 1
    assert backpressure.backpressure_level == "ok"


async def test_aclose_closes_the_http_client():
    gateway = LiveFlinkGateway("http://flink:8081")
    await gateway.aclose()
    assert gateway._http.is_closed
