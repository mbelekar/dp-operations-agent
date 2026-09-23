"""Live LineageQueryGateway backed by Marquez's lineage API.

Verified endpoint (see gateway.py's module docstring for the node ID
convention): GET /api/v1/lineage?nodeId=...&depth=..., returning
{"graph": [GraphNode, ...]}, GraphNode = {id, type, data, inEdges, outEdges},
inEdges/outEdges = [{origin, destination}, ...]. Written to this documented
API shape; not tested against a live Marquez instance in this environment
(see docs/docker.md), same limitation as every other live gateway here that
hasn't been run against real infra.

Marquez's raw response is the full graph (upstream and downstream) within
depth hops of node_id, not just the upstream side. upstream_lineage() walks
inEdges backward from node_id to filter to ancestors only, and excludes
node_id itself from the result, the caller already knows what it queried,
this returns what's upstream of it.
"""

from __future__ import annotations

import httpx

from dp_ops_agent.tools.lineage.gateway import LineageEdge, LineageGraphView, LineageNode


class LiveLineageGateway:
    def __init__(self, marquez_base_url: str) -> None:
        self._base_url = marquez_base_url.rstrip("/")
        self._http = httpx.Client(timeout=10.0)

    def upstream_lineage(self, node_id: str, depth: int = 5) -> LineageGraphView:
        resp = self._http.get(
            f"{self._base_url}/api/v1/lineage", params={"nodeId": node_id, "depth": depth}
        )
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
