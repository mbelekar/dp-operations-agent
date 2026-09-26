from __future__ import annotations

from datetime import UTC, datetime

from langchain_core.tools import BaseTool, tool

from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import Severity, Signal
from dp_ops_agent.evidence.signals.lineage import (
    LineageNodeScope,
    LineageUpstreamObserved,
    LineageUpstreamSignal,
    UpstreamNode,
)
from dp_ops_agent.tools.lineage.gateway import LineageQueryGateway


def _record_signal(
    audit: AuditSink, session_id: str, collected_signals: list[Signal], signal: Signal
) -> str:
    """Mirrors tools/kafka/tools.py and tools/flink/tools.py's helper of the
    same name; kept local rather than shared since each module owns its own
    tool-building logic."""
    collected_signals.append(signal)
    audit.append(
        AuditEvent(
            event_type="signal_collected",
            timestamp=datetime.now(UTC),
            session_id=session_id,
            actor="tool",
            payload=signal.model_dump(mode="json"),
        )
    )
    return signal.model_dump_json()


def build_lineage_tools(
    gateway: LineageQueryGateway,
    audit: AuditSink,
    session_id: str,
    collected_signals: list[Signal],
) -> list[BaseTool]:
    @tool
    async def walk_lineage_upstream(node_id: str) -> str:
        """Find what's upstream of a Kafka topic, Flink job, dbt model, or
        warehouse table, to trace a root cause from where an alert fired to a
        different system entirely. node_id must follow the convention: a
        Kafka topic is "dataset:kafka:{topic}", a Flink job is
        "job:flink:{job_name}", a dbt model is "job:dbt:{model_name}", a
        warehouse table is "dataset:warehouse:{schema}.{table}" (dbt tool
        results give it as scope.lineage_node_id). Call
        this before concluding a diagnosis when the alert's system might not
        be where the root cause actually lives, then investigate whatever
        upstream system this returns using that system's own tools."""
        now = datetime.now(UTC)
        view = await gateway.upstream_lineage(node_id)
        observed = LineageUpstreamObserved(
            upstream_nodes=[UpstreamNode(id=n.id, type=n.type) for n in view.nodes]
        )
        if view.node_found:
            severity: Severity = "ok"
        else:
            severity = "unknown"
            observed.no_data_reason = (
                f"lineage has no node {node_id!r}; not the same as nothing upstream"
            )
        signal = LineageUpstreamSignal(
            collected_at=now,
            window_start=now,
            window_end=now,
            scope=LineageNodeScope(node_id=node_id),
            observed=observed,
            severity=severity,
            raw_source_ref=f"marquez:/api/v1/lineage?nodeId={node_id}",
        )
        return _record_signal(audit, session_id, collected_signals, signal)

    return [walk_lineage_upstream]
