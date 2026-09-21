"""Live FlinkMetricsGateway backed by the Flink JobManager REST API.

Verified endpoints (see gateway.py's module docstring for sources):
GET /jobs/:jobid/checkpoints, GET /jobs/:jobid/vertices/:vertexid/backpressure,
GET /jobs/:jobid/vertices/:vertexid/metrics, GET /jobs/:jobid/exceptions.

Two things are genuinely best-effort here, not verified against a real
cluster, and should be checked against your Flink version before relying on
them: the per-subtask field names inside a backpressure response's
"subtasks" array, and the watermark metric's naming convention
(assumed "<subtask>.currentInputWatermark", Flink's usual pattern, but not
guaranteed across versions/connectors).
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from dp_ops_agent.tools.flink.gateway import (
    BackpressureView,
    CheckpointCounts,
    CheckpointHistoryEntry,
    CheckpointHistoryView,
    SubtaskBackpressure,
)


class LiveFlinkGateway:
    def __init__(self, rest_base_url: str) -> None:
        self._base_url = rest_base_url.rstrip("/")
        self._http = httpx.Client(timeout=10.0)

    def checkpoint_history(self, job_id: str) -> CheckpointHistoryView:
        resp = self._http.get(f"{self._base_url}/jobs/{job_id}/checkpoints")
        resp.raise_for_status()
        data = resp.json()
        return CheckpointHistoryView(
            counts=CheckpointCounts(**data.get("counts", {})),
            history=[
                CheckpointHistoryEntry(
                    id=h["id"],
                    status=h["status"],
                    trigger_timestamp=h["trigger_timestamp"],
                    end_to_end_duration=h.get("end_to_end_duration"),
                )
                for h in data.get("history", [])
            ],
        )

    def backpressure(self, job_id: str, vertex_id: str) -> BackpressureView:
        resp = self._http.get(
            f"{self._base_url}/jobs/{job_id}/vertices/{vertex_id}/backpressure"
        )
        resp.raise_for_status()
        data = resp.json()
        subtasks = [
            SubtaskBackpressure(subtask=s.get("subtask", i), ratio=s.get("ratio", 0.0))
            for i, s in enumerate(data.get("subtasks", []))
        ]
        return BackpressureView(
            status=data.get("status", "unknown"),
            backpressure_level=data.get("backpressure-level", "unknown"),
            subtasks=subtasks,
        )

    def _available_metric_ids(self, job_id: str, vertex_id: str) -> list[str]:
        resp = self._http.get(
            f"{self._base_url}/jobs/{job_id}/vertices/{vertex_id}/metrics"
        )
        resp.raise_for_status()
        return [m["id"] for m in resp.json()]

    def watermark_lag(self, job_id: str, vertex_id: str) -> dict[int, float]:
        metric_ids = [
            m for m in self._available_metric_ids(job_id, vertex_id)
            if m.endswith("currentInputWatermark")
        ]
        if not metric_ids:
            return {}
        resp = self._http.get(
            f"{self._base_url}/jobs/{job_id}/vertices/{vertex_id}/metrics",
            params={"get": ",".join(metric_ids)},
        )
        resp.raise_for_status()
        now_ms = time.time() * 1000
        result: dict[int, float] = {}
        for entry in resp.json():
            subtask_str = entry["id"].split(".")[0]
            if not subtask_str.isdigit():
                continue
            watermark_ms = float(entry["value"])
            if watermark_ms <= 0:
                continue  # no watermark emitted yet
            result[int(subtask_str)] = max(0.0, now_ms - watermark_ms)
        return result

    def task_manager_disk_metrics(self, job_id: str, vertex_id: str) -> dict[str, float]:
        metric_ids = [
            m for m in self._available_metric_ids(job_id, vertex_id)
            if "disk" in m.lower() or "rocksdb" in m.lower()
        ]
        if not metric_ids:
            return {}
        resp = self._http.get(
            f"{self._base_url}/jobs/{job_id}/vertices/{vertex_id}/metrics",
            params={"get": ",".join(metric_ids)},
        )
        resp.raise_for_status()
        return {entry["id"]: float(entry["value"]) for entry in resp.json()}

    def job_exceptions(self, job_id: str, window_minutes: int) -> list[dict[str, Any]]:
        resp = self._http.get(f"{self._base_url}/jobs/{job_id}/exceptions")
        resp.raise_for_status()
        data = resp.json()
        now_ms = time.time() * 1000
        cutoff_ms = now_ms - (window_minutes * 60 * 1000)
        return [
            e for e in data.get("all-exceptions", [])
            if e.get("timestamp", now_ms) >= cutoff_ms
        ]
