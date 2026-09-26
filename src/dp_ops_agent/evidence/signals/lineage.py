"""Scope and observed payloads for the lineage signal. Field order is key
order in the JSON; see base.py."""

from __future__ import annotations

from dp_ops_agent.evidence.signals.base import Absent, Payload, Scope


class LineageNodeScope(Scope):
    node_id: str


class UpstreamNode(Payload):
    id: str
    type: str


class LineageUpstreamObserved(Payload):
    upstream_nodes: list[UpstreamNode]
    no_data_reason: Absent[str] = None
