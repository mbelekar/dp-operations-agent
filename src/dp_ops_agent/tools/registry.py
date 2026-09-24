"""Assembles the tool list for a diagnosis session.

IMPORTANT for future phases: every tool returned here is auto-executed by
create_agent's built-in ToolNode with no gate. Every tool registered so far
(Kafka, Flink, lineage, dbt) is a read-only diagnostic, so that's correct. When Phase 4
adds the execution tool, it MUST be gated by an AgentMiddleware.wrap_tool_call
(or awrap_tool_call) hook that checks the approval store before calling
handler(request) — short-circuiting with a denial ToolMessage instead of
calling handler() when no valid, unexpired approval record exists. Without
that hook, create_agent will execute a state-changing tool the moment the
model calls it, silently bypassing the permission boundary the design doc
requires.

That gate goes *after* orchestrator/tool_errors.py's middleware in
session.py's create_agent middleware list (later = inner). The error
middleware must stay outermost, so that a backend failure during an
approved execution comes back as an error the model sees, instead of being
masked or aborting the session mid-action.
"""

from __future__ import annotations

from langchain_core.tools import BaseTool

from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import Diagnosis, Signal
from dp_ops_agent.tools.dbt.gateway import DbtArtifactsGateway
from dp_ops_agent.tools.dbt.tools import build_dbt_tools
from dp_ops_agent.tools.diagnosis_output.tools import build_diagnosis_output_tools
from dp_ops_agent.tools.flink.gateway import FlinkMetricsGateway
from dp_ops_agent.tools.flink.tools import build_flink_tools
from dp_ops_agent.tools.kafka.gateway import KafkaMetricsGateway
from dp_ops_agent.tools.kafka.tools import build_kafka_tools
from dp_ops_agent.tools.lineage.gateway import LineageQueryGateway
from dp_ops_agent.tools.lineage.tools import build_lineage_tools

TOOL_NAMES: list[str] = [
    "under_replicated_partitions",
    "isr_churn",
    "consumer_lag_trend",
    "rebalance_frequency",
    "hot_partition_skew",
    "schema_registry_compat",
    "checkpoint_failure",
    "backpressure_ratio",
    "watermark_lag",
    "state_backend_disk_pressure",
    "savepoint_restore_failure",
    "walk_lineage_upstream",
    "test_failure",
    "model_run_failure",
    "freshness_check_failure",
    "incremental_model_drift",
    "dependency_graph_compile_error",
    "submit_diagnosis",
]


def build_tools(
    kafka_gateway: KafkaMetricsGateway,
    flink_gateway: FlinkMetricsGateway,
    lineage_gateway: LineageQueryGateway,
    dbt_gateway: DbtArtifactsGateway,
    audit: AuditSink,
    session_id: str,
    model_name: str,
    result_holder: dict[str, Diagnosis],
) -> list[BaseTool]:
    collected_signals: list[Signal] = []
    kafka_tools = build_kafka_tools(kafka_gateway, audit, session_id, collected_signals)
    flink_tools = build_flink_tools(flink_gateway, audit, session_id, collected_signals)
    lineage_tools = build_lineage_tools(lineage_gateway, audit, session_id, collected_signals)
    dbt_tools = build_dbt_tools(dbt_gateway, audit, session_id, collected_signals)
    diagnosis_tools = build_diagnosis_output_tools(
        audit, session_id, collected_signals, result_holder, model_name
    )
    return [*kafka_tools, *flink_tools, *lineage_tools, *dbt_tools, *diagnosis_tools]
