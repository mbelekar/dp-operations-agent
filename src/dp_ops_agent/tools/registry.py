"""Assembles the tool list for a diagnosis session.

IMPORTANT for future phases: every tool returned here is auto-executed by
create_agent's built-in ToolNode with no gate. All Phase 1 tools are
read-only diagnostics, so that's correct. When Phase 4 adds the execution
tool, it MUST be gated by an AgentMiddleware.wrap_tool_call (or
awrap_tool_call) hook that checks the approval store before calling
handler(request) — short-circuiting with a denial ToolMessage instead of
calling handler() when no valid, unexpired approval record exists. Without
that hook, create_agent will execute a state-changing tool the moment the
model calls it, silently bypassing the permission boundary the design doc
requires.
"""

from __future__ import annotations

from langchain_core.tools import BaseTool

from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import Diagnosis, Signal
from dp_ops_agent.tools.diagnosis_output.tools import build_diagnosis_output_tools
from dp_ops_agent.tools.kafka.gateway import KafkaMetricsGateway
from dp_ops_agent.tools.kafka.tools import build_kafka_tools

PHASE1_TOOL_NAMES: list[str] = [
    "under_replicated_partitions",
    "isr_churn",
    "consumer_lag_trend",
    "rebalance_frequency",
    "hot_partition_skew",
    "schema_registry_compat",
    "submit_diagnosis",
]


def build_tools(
    gateway: KafkaMetricsGateway,
    audit: AuditSink,
    session_id: str,
    model_name: str,
    result_holder: dict[str, Diagnosis],
) -> list[BaseTool]:
    collected_signals: list[Signal] = []
    kafka_tools = build_kafka_tools(gateway, audit, session_id, collected_signals)
    diagnosis_tools = build_diagnosis_output_tools(
        audit, session_id, collected_signals, result_holder, model_name
    )
    return [*kafka_tools, *diagnosis_tools]
