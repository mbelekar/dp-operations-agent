"""Live LineageQueryGateway backed by Marquez's lineage API.

Verified endpoint (see gateway.py's module docstring for the node ID
convention): GET /api/v1/lineage?nodeId=...&depth=..., returning
{"graph": [GraphNode, ...]}, GraphNode = {id, type, data, inEdges, outEdges},
inEdges/outEdges = [{origin, destination}, ...]. Verified against a real
Marquez 0.51.1 in the compose stack (see docs/docker.md and docs/lineage.md).

Marquez's raw response is the full graph (upstream and downstream) within
depth hops of node_id, not just the upstream side. upstream_lineage() walks
inEdges backward from node_id to filter to ancestors only, and excludes
node_id itself from the result, the caller already knows what it queried,
this returns what's upstream of it.

Marquez answers 404 for a node_id it has never seen. That's returned as an
empty view rather than raised, matching FixtureLineageGateway's contract for
an unknown node; otherwise a mistyped node_id from the model would crash
the whole session instead of reading as "nothing upstream". Only that 404
though, recognized by Marquez's body ({"message": "Job 'x' not found."} or
"Dataset 'x' not found."). Any other 404 (a wrong path prefix or host in
MARQUEZ_URL) raises, so it reaches the model as a tool error rather than an
empty graph it could cite as evidence that nothing is upstream.
"""

from __future__ import annotations

import re

import httpx

from dp_ops_agent.tools.lineage.gateway import LineageEdge, LineageGraphView, LineageNode


_UNKNOWN_NODE_MESSAGE_RE = re.compile(r"^(Job|Dataset) '.*' not found\.$")


def _is_unknown_node(resp: httpx.Response) -> bool:
    if resp.status_code != 404:
        return False
    try:
        message = resp.json().get("message", "")
    except (ValueError, AttributeError):
        return False
    return isinstance(message, str) and bool(_UNKNOWN_NODE_MESSAGE_RE.match(message))


class LiveLineageGateway:
    def __init__(self, marquez_base_url: str) -> None:
        self._base_url = marquez_base_url.rstrip("/")
        self._http = httpx.Client(timeout=10.0)

    def upstream_lineage(self, node_id: str, depth: int = 5) -> LineageGraphView:
        resp = self._http.get(
            f"{self._base_url}/api/v1/lineage", params={"nodeId": node_id, "depth": depth}
        )
        if _is_unknown_node(resp):
            return LineageGraphView(nodes=[], node_found=False)
        resp.raise_for_status()
        data = resp.json()

        nodes_by_id: dict[str, LineageNode] = {}
        for raw in data.get("graph", []):
            nodes_by_id[raw["id"]] = LineageNode(
                id=raw["id"],
                type=raw["type"],
                in_edges=[
                    LineageEdge(origin=e["origin"], destination=e["destination"])
                    for e in raw.get("inEdges", [])
                ],
                out_edges=[
                    LineageEdge(origin=e["origin"], destination=e["destination"])
                    for e in raw.get("outEdges", [])
                ],
            )

        visited = {node_id}
        frontier = [node_id]
        while frontier:
            next_frontier: list[str] = []
            for current_id in frontier:
                node = nodes_by_id.get(current_id)
                if node is None:
                    continue
                for edge in node.in_edges:
                    if edge.origin not in visited:
                        visited.add(edge.origin)
                        next_frontier.append(edge.origin)
            frontier = next_frontier

        upstream_ids = visited - {node_id}
        return LineageGraphView(nodes=[nodes_by_id[i] for i in upstream_ids if i in nodes_by_id])
