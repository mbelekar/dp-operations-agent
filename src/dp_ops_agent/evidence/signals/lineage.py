"""Scope and observed payloads for the lineage signal. Field order is key
order in the JSON; see base.py."""

from __future__ import annotations

from typing import ClassVar, Literal

from dp_ops_agent.evidence.signals.base import Absent, DiagnosedSystem, Payload, Scope, SignalBase


class LineageNodeScope(Scope):
    node_id: str


class UpstreamNode(Payload):
    id: str
    type: str


class LineageUpstreamObserved(Payload):
    upstream_nodes: list[UpstreamNode]
    no_data_reason: Absent[str] = None


# tool and signal_type are fixed; they keep their position in the JSON
# (SignalBase's field order) even though they're redeclared here.
class LineageUpstreamSignal(SignalBase[LineageNodeScope, LineageUpstreamObserved]):
    tool: Literal["lineage.walk_lineage_upstream"] = "lineage.walk_lineage_upstream"
    signal_type: Literal["lineage_upstream"] = "lineage_upstream"
    # Lineage shows where to look; it can't be a root cause itself.
    system: ClassVar[DiagnosedSystem | None] = None
