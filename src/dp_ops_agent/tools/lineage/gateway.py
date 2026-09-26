"""Gateway abstraction over lineage graph queries.

Two implementations share this Protocol: LiveLineageGateway (Marquez's REST
API) and FixtureLineageGateway (canned JSON snapshots for tests/demos),
mirroring tools/kafka/gateway.py and tools/flink/gateway.py's split.

Node ID convention: Marquez doesn't dictate one, so this project picks a
single consistent scheme and every gateway implementation and fixture must
use it, so a node_id computed from an alert (e.g. a topic or job name)
round-trips correctly into a lineage query. A Kafka topic is
"dataset:kafka:{topic}", a Flink job is "job:flink:{job_name}", a dbt model
is "job:dbt:{model_name}", and a warehouse table (a dbt source, or a model's
output) is "dataset:warehouse:{schema}.{table}". The dbt tools compute these
ids themselves and hand them to the model in each signal's scope.
"""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel

NodeType = Literal["JOB", "DATASET"]


class LineageEdge(BaseModel):
    origin: str
    destination: str


class LineageNode(BaseModel):
    id: str
    type: NodeType
    in_edges: list[LineageEdge]
    out_edges: list[LineageEdge]


class LineageGraphView(BaseModel):
    """The subgraph upstream of the node a query started from (not the full
    bidirectional graph Marquez's raw API returns, the gateway implementation
    is responsible for walking inEdges to filter to just the upstream side).
    """

    nodes: list[LineageNode]
    # False when the lineage backend has never seen the queried node, as
    # opposed to a known node with nothing upstream (both have no nodes).
    node_found: bool = True


class LineageQueryGateway(Protocol):
    async def upstream_lineage(self, node_id: str, depth: int = 5) -> LineageGraphView: ...
