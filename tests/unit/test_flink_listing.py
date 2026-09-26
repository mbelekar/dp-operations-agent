from pathlib import Path

import httpx

from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.flink.live_gateway import LiveFlinkGateway

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "flink"


def test_fixture_lists_jobs_and_vertices_from_the_snapshot():
    gateway = FixtureFlinkGateway(FIXTURE_DIR / "backpressure_incident.json")

    assert [j.id for j in gateway.list_jobs()] == ["orders-processing-job"]
    vertices = gateway.list_vertices("orders-processing-job")
    assert {v.id for v in vertices} >= {"sink"}
    assert all(v.name == v.id for v in vertices)
    assert gateway.list_vertices("no-such-job") == []


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
    gateway._http = httpx.Client(transport=httpx.MockTransport(handler))
    return gateway


def test_live_lists_jobs_with_names():
    [job] = _live().list_jobs()

    assert (job.id, job.name) == ("a1b2c3", "orders-processing-job")


def test_live_lists_vertex_ids_with_names():
    vertices = _live().list_vertices("a1b2c3")

    assert [(v.id, v.name) for v in vertices] == [
        ("cbc357ccb763df2852fee8c4fc7d55f2", "Source: orders"),
        ("90bea66de1c231edf33913ecd54406c1", "Map -> Sink: orders-sink"),
    ]
