"""Deterministic FlinkMetricsGateway backed by a JSON snapshot file.

Snapshot shape (all keys optional; missing lookups return empty results):

{
  "checkpoint_history": {"<job_id>": {"counts": {...}, "history": [...]}},
  "backpressure": {"<job_id>": {"<vertex_id>": {"status", "backpressure_level", "subtasks"}}},
  "watermark_lag": {"<job_id>": {"<vertex_id>": {"<subtask>": <lag_ms>}}},
  "task_manager_disk_metrics": {"<job_id>": {"<vertex_id>": {"<metric_name>": <value>}}},
  "job_exceptions": {"<job_id>": [{"timestamp": ..., "exception": ...}, ...]}
}
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dp_ops_agent.tools.flink.gateway import (
    BackpressureView,
    CheckpointCounts,
    CheckpointHistoryView,
)


class FixtureFlinkGateway:
    def __init__(self, snapshot_path: str | Path) -> None:
        self._data: dict[str, Any] = json.loads(Path(snapshot_path).read_text())

    def checkpoint_history(self, job_id: str) -> CheckpointHistoryView:
        raw = self._data.get("checkpoint_history", {}).get(
            job_id, {"counts": {"completed": 0, "failed": 0, "in_progress": 0, "restored": 0, "total": 0}, "history": []}
        )
        return CheckpointHistoryView(
            counts=CheckpointCounts(**raw["counts"]),
            history=raw.get("history", []),
        )

    def backpressure(self, job_id: str, vertex_id: str) -> BackpressureView:
        raw = self._data.get("backpressure", {}).get(job_id, {}).get(
            vertex_id, {"status": "not_found", "backpressure_level": "unknown", "subtasks": []}
        )
        return BackpressureView(**raw)

    def watermark_lag(self, job_id: str, vertex_id: str) -> dict[int, float]:
        raw = self._data.get("watermark_lag", {}).get(job_id, {}).get(vertex_id, {})
        return {int(k): float(v) for k, v in raw.items()}

    def task_manager_disk_metrics(self, job_id: str, vertex_id: str) -> dict[str, float]:
        raw = self._data.get("task_manager_disk_metrics", {}).get(job_id, {}).get(vertex_id, {})
        return {k: float(v) for k, v in raw.items()}

    def job_exceptions(self, job_id: str, window_minutes: int) -> list[dict[str, Any]]:
        return self._data.get("job_exceptions", {}).get(job_id, [])
