"""Deterministic LineageQueryGateway backed by a JSON snapshot file.

Snapshot shape (a node_id missing from the snapshot is an unknown node,
node_found=False; list a known node with nothing upstream as "nodes": []):

{
  "upstream_lineage": {
    "<node_id>": {
      "nodes": [
        {"id": "...", "type": "DATASET"|"JOB", "in_edges": [...], "out_edges": [...]}
      ]
    }
  }
}
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dp_ops_agent.tools.lineage.gateway import LineageGraphView


class FixtureLineageGateway:
    def __init__(self, snapshot_path: str | Path) -> None:
        self._data: dict[str, Any] = json.loads(Path(snapshot_path).read_text())

    def upstream_lineage(self, node_id: str, depth: int = 5) -> LineageGraphView:
        raw = self._data.get("upstream_lineage", {}).get(node_id)
        if raw is None:
            return LineageGraphView(nodes=[], node_found=False)
        return LineageGraphView(**raw)
