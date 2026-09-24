from __future__ import annotations

from datetime import datetime, timedelta, timezone

from langchain_core.tools import BaseTool, tool

from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import Signal
from dp_ops_agent.tools.flink.gateway import FlinkMetricsGateway, NamedId
from dp_ops_agent.tools.known_identifiers import capped, describe

_SAVEPOINT_FAILURE_KEYWORDS = ("savepoint", "incompatible state", "state schema")


def _record_signal(
    audit: AuditSink, session_id: str, collected_signals: list[Signal], signal: Signal
) -> str:
    """Log the signal to the audit trail, add it to this session's collected
    signals, and return the tool result text the model sees. Mirrors
    tools/kafka/tools.py's helper of the same name; kept local rather than
    shared since each module owns its own tool-building logic."""
    collected_signals.append(signal)
    audit.append(
        AuditEvent(
            event_type="signal_collected",
            timestamp=datetime.now(timezone.utc),
            session_id=session_id,
            actor="tool",
            payload=signal.model_dump(mode="json"),
        )
    )
    return signal.model_dump_json()


def _refs(items: list[NamedId]) -> list[dict[str, str]]:
    return [i.model_dump() for i in capped(items)]


def _describe_refs(items: list[NamedId]) -> str:
    return describe([i.id if i.id == i.name else f"{i.id} ({i.name})" for i in items])


def build_flink_tools(
    gateway: FlinkMetricsGateway,
    audit: AuditSink,
    session_id: str,
    collected_signals: list[Signal],
) -> list[BaseTool]:
    def _no_vertex_data(job_id: str, vertex_id: str, what: str) -> dict:
        """observed fields for an unknown result on a vertex lookup: whether
        the vertex exists (then don't retry it) or which ones do."""
        vertices = gateway.list_vertices(job_id)
        if any(v.id == vertex_id for v in vertices):
            return {
                "no_data_reason": (
                    f"vertex {vertex_id!r} of job {job_id!r} exists but reports no {what}; "
                    "don't retry it"
                ),
                "known_vertices": _refs(vertices),
            }
        if vertices:
            return {
                "no_data_reason": (
                    f"no vertex {vertex_id!r} in job {job_id!r}; its vertices (pass the id): "
                    f"{_describe_refs(vertices)}"
                ),
                "known_vertices": _refs(vertices),
            }
        jobs = gateway.list_jobs()
        return {
            "no_data_reason": (
                f"no vertices found for job {job_id!r}; known jobs (pass the id): "
                f"{_describe_refs(jobs)}"
            ),
            "known_vertices": [],
            "known_jobs": _refs(jobs),
        }

    @tool
    async def checkpoint_failure(job_id: str) -> str:
        """Report checkpoint failure history for a Flink job. Signals growing
        state size, a slow sink, or backpressure upstream of the barrier."""
        now = datetime.now(timezone.utc)
        view = gateway.checkpoint_history(job_id)
        most_recent_failed = bool(view.history) and view.history[-1].status == "FAILED"
        critical = most_recent_failed or view.counts.failed >= 3
        observed = {
            "counts": view.counts.model_dump(),
            "most_recent_status": view.history[-1].status if view.history else None,
        }
        if view.counts.total == 0 and not view.history:
            severity = "unknown"
            jobs = gateway.list_jobs()
            observed["known_jobs"] = _refs(jobs)
            if any(j.id == job_id for j in jobs):
                observed["no_data_reason"] = (
                    f"job {job_id!r} exists but has no checkpoints recorded; don't retry it"
                )
            else:
                observed["no_data_reason"] = (
                    f"no job {job_id!r}; known jobs (pass the id): {_describe_refs(jobs)}"
                )
        else:
            severity = "critical" if critical else ("warn" if view.counts.failed >= 1 else "ok")
        signal = Signal(
            tool="flink.checkpoint_failure",
            signal_type="checkpoint_failure",
            collected_at=now,
            window_start=now,
            window_end=now,
            scope={"job_id": job_id},
            observed=observed,
            severity=severity,
            raw_source_ref=f"flink:/jobs/{job_id}/checkpoints",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def backpressure_ratio(job_id: str, vertex_id: str) -> str:
        """Report backpressure level for a specific Flink job vertex
        (operator). Localizes the actual bottleneck operator rather than
        just indicating "the job is slow"."""
        now = datetime.now(timezone.utc)
        view = gateway.backpressure(job_id, vertex_id)
        level = view.backpressure_level.lower()
        observed = {
            "status": view.status,
            "backpressure_level": view.backpressure_level,
            "subtasks": [s.model_dump() for s in view.subtasks],
        }
        if not view.subtasks:
            severity = "unknown"
            observed.update(_no_vertex_data(job_id, vertex_id, "backpressure samples"))
        else:
            severity = "critical" if level == "high" else ("warn" if level == "low" else "ok")
        signal = Signal(
            tool="flink.backpressure_ratio",
            signal_type="backpressure_ratio",
            collected_at=now,
            window_start=now,
            window_end=now,
            scope={"job_id": job_id, "vertex_id": vertex_id},
            observed=observed,
            severity=severity,
            raw_source_ref=f"flink:/jobs/{job_id}/vertices/{vertex_id}/backpressure",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def watermark_lag(job_id: str, vertex_id: str) -> str:
        """Report event-time watermark lag per subtask for a Flink job
        vertex. Signals event-time skew, often caused by a stalled upstream
        Kafka partition."""
        now = datetime.now(timezone.utc)
        lag_by_subtask = gateway.watermark_lag(job_id, vertex_id)
        max_lag_ms = max(lag_by_subtask.values(), default=0.0)
        observed = {
            "lag_ms_by_subtask": {str(k): v for k, v in lag_by_subtask.items()},
            "max_lag_ms": max_lag_ms,
        }
        if not lag_by_subtask:
            severity = "unknown"
            observed.update(_no_vertex_data(job_id, vertex_id, "watermark metrics"))
        else:
            severity = (
                "critical" if max_lag_ms > 60_000 else ("warn" if max_lag_ms > 10_000 else "ok")
            )
        signal = Signal(
            tool="flink.watermark_lag",
            signal_type="watermark_lag",
            collected_at=now,
            window_start=now,
            window_end=now,
            scope={"job_id": job_id, "vertex_id": vertex_id},
            observed=observed,
            severity=severity,
            raw_source_ref=f"flink:/jobs/{job_id}/vertices/{vertex_id}/metrics (currentInputWatermark)",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def state_backend_disk_pressure(job_id: str, vertex_id: str) -> str:
        """Report state backend (RocksDB/TaskManager) disk pressure for a
        Flink job vertex. Signals state growth from a skewed key, missing
        TTL, or an unbounded window."""
        now = datetime.now(timezone.utc)
        metrics = gateway.task_manager_disk_metrics(job_id, vertex_id)
        disk_used_ratio = metrics.get("disk_used_ratio")
        observed = {"metrics": metrics}
        if disk_used_ratio is None:
            severity = "unknown"
            observed.update(_no_vertex_data(job_id, vertex_id, "disk_used_ratio metric"))
        elif disk_used_ratio > 0.9:
            severity = "critical"
        elif disk_used_ratio > 0.7:
            severity = "warn"
        else:
            severity = "ok"
        signal = Signal(
            tool="flink.state_backend_disk_pressure",
            signal_type="state_backend_disk_pressure",
            collected_at=now,
            window_start=now,
            window_end=now,
            scope={"job_id": job_id, "vertex_id": vertex_id},
            observed=observed,
            severity=severity,
            raw_source_ref=f"flink:/jobs/{job_id}/vertices/{vertex_id}/metrics (disk/rocksdb)",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    @tool
    async def savepoint_restore_failure(job_id: str, window_minutes: int) -> str:
        """Check a Flink job's recent exception history for a savepoint or
        state-schema restore failure. Signals an incompatible state schema
        after a job graph or operator UID change. Heuristic: inferred from
        exception text, since Flink has no dedicated REST field for this."""
        now = datetime.now(timezone.utc)
        exceptions = gateway.job_exceptions(job_id, window_minutes)
        matches = [
            e for e in exceptions
            if any(kw in str(e.get("exception", "")).lower() for kw in _SAVEPOINT_FAILURE_KEYWORDS)
        ]
        signal = Signal(
            tool="flink.savepoint_restore_failure",
            signal_type="savepoint_restore_failure",
            collected_at=now,
            window_start=now - timedelta(minutes=window_minutes),
            window_end=now,
            scope={"job_id": job_id},
            observed={"matching_exceptions": matches, "total_exceptions": len(exceptions)},
            severity="critical" if matches else "ok",
            raw_source_ref=f"flink:/jobs/{job_id}/exceptions",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    return [
        checkpoint_failure,
        backpressure_ratio,
        watermark_lag,
        state_backend_disk_pressure,
        savepoint_restore_failure,
    ]
