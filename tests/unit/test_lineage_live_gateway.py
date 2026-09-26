import asyncio
import time

import httpx
import pytest

from dp_ops_agent.tools.lineage.live_gateway import LiveLineageGateway

# Shape captured from a real Marquez 0.51.1 seeded with
# docker/marquez-seed/events.jsonl.
MARQUEZ_GRAPH = {
    "graph": [
        {
            "id": "dataset:kafka:orders",
            "type": "DATASET",
            "data": {},
            "inEdges": [],
            "outEdges": [
                {"origin": "dataset:kafka:orders", "destination": "job:flink:orders-processing-job"}
            ],
        },
        {
            "id": "job:flink:orders-processing-job",
            "type": "JOB",
            "data": {},
            "inEdges": [
                {"origin": "dataset:kafka:orders", "destination": "job:flink:orders-processing-job"}
            ],
            "outEdges": [
                {
                    "origin": "job:flink:orders-processing-job",
                    "destination": "dataset:kafka:orders-sink",
                }
            ],
        },
        {
            "id": "dataset:kafka:orders-sink",
            "type": "DATASET",
            "data": {},
            "inEdges": [
                {
                    "origin": "job:flink:orders-processing-job",
                    "destination": "dataset:kafka:orders-sink",
                }
            ],
            "outEdges": [],
        },
    ]
}


def _gateway(status_code: int, body: dict | None = None) -> LiveLineageGateway:
    gateway = LiveLineageGateway("http://marquez:5000")
    gateway._http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status_code, json=body or {}))
    )
    return gateway


@pytest.mark.parametrize(
    "message",
    # Verbatim from Marquez 0.51.1 for an unknown job / dataset node id.
    ["Job 'does-not-exist' not found.", "Dataset 'does-not-exist' not found."],
)
async def test_unknown_node_returns_empty_view(message):
    view = await _gateway(404, {"code": 404, "message": message}).upstream_lineage(
        "job:flink:does-not-exist"
    )

    assert view.nodes == []
    assert view.node_found is False


@pytest.mark.parametrize(
    "body",
    [
        # Marquez itself, but a wrong path prefix in MARQUEZ_URL.
        {"code": 404, "message": "HTTP 404 Not Found"},
        # Some other service entirely (Flink REST's 404), i.e. a wrong host/port.
        {"errors": ["Unable to load requested file /api/v1/lineage."]},
    ],
    ids=["wrong-path", "wrong-host"],
)
async def test_404_that_is_not_an_unknown_node_raises(body):
    # Must surface as a tool error, not an empty graph the model could cite
    # as evidence that nothing is upstream.
    with pytest.raises(httpx.HTTPStatusError):
        await _gateway(404, body).upstream_lineage("job:flink:orders-processing-job")


async def test_server_error_still_raises():
    with pytest.raises(httpx.HTTPStatusError):
        await _gateway(500).upstream_lineage("job:flink:orders-processing-job")


async def test_two_hop_walk_returns_ancestors_only():
    gateway = _gateway(200, MARQUEZ_GRAPH)

    from_sink = await gateway.upstream_lineage("dataset:kafka:orders-sink")
    from_job = await gateway.upstream_lineage("job:flink:orders-processing-job")

    assert sorted(n.id for n in from_sink.nodes) == [
        "dataset:kafka:orders",
        "job:flink:orders-processing-job",
    ]
    assert [n.id for n in from_job.nodes] == ["dataset:kafka:orders"]
    assert from_sink.node_found and from_job.node_found


async def test_concurrent_lookups_overlap_instead_of_queuing():
    """The tool node runs a turn's tool calls concurrently; a lookup waiting
    on Marquez must not hold the event loop."""

    async def slow_marquez(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.3)
        return httpx.Response(200, json=MARQUEZ_GRAPH)

    gateway = LiveLineageGateway("http://marquez:5000")
    gateway._http = httpx.AsyncClient(transport=httpx.MockTransport(slow_marquez))

    start = time.perf_counter()
    await asyncio.gather(
        gateway.upstream_lineage("dataset:kafka:orders-sink"),
        gateway.upstream_lineage("job:flink:orders-processing-job"),
    )

    assert time.perf_counter() - start < 0.5  # 0.6s if they queued


async def test_aclose_closes_the_http_client():
    gateway = LiveLineageGateway("http://marquez:5000")
    await gateway.aclose()
    assert gateway._http.is_closed
