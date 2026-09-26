"""The Signal envelope and the building blocks its typed payloads share.

A signal's scope and observed payloads are typed per signal type (the
sibling modules), but the JSON they serialize to is exactly what the tools
produced when they were plain dicts: same keys, same order, same keys
present or absent in each branch, same number encoding. The model, the
audit log, and the system prompt all read that JSON, and
tests/integration/test_signal_json_snapshot.py pins it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Generic, Literal, TypeVar
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

SignalType = Literal[
    "consumer_lag_trend",
    "under_replicated_partitions",
    "isr_churn",
    "rebalance_frequency",
    "hot_partition_skew",
    "schema_registry_compat",
    "checkpoint_failure",
    "backpressure_ratio",
    "watermark_lag",
    "state_backend_disk_pressure",
    "savepoint_restore_failure",
    "lineage_upstream",
    "test_failure",
    "model_run_failure",
    "freshness_check_failure",
    "incremental_model_drift",
    "dependency_graph_compile_error",
]

# "unknown": the tool found no data for the identifiers it was asked about
# (an unknown vertex, topic, node...). Not evidence of health, see ADR-0009.
Severity = Literal["ok", "warn", "critical", "unknown"]

T = TypeVar("T")
ScopeT = TypeVar("ScopeT")
ObservedT = TypeVar("ObservedT")


def _is_none(value: object) -> bool:
    return value is None


# A key that some branches leave out entirely (not emitted as null), like a
# no_data_reason that only exists when there's no data. Declare it with a
# plain `= None` default so mypy sees it's optional. A key that's always
# present but can be null is a plain `X | None` instead.
Absent = Annotated[T | None, Field(exclude_if=_is_none)]


class Payload(BaseModel):
    """Base for scope and observed models. Extra keys are rejected, so a
    payload can't quietly carry a key its model doesn't declare, and a
    union of payload shapes always resolves to exactly one of them."""

    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True)
class SignalTargets:
    """The identifiers a signal covered, for checking a proposed action acts
    only on something that was investigated (ADR-0010)."""

    job_ids: frozenset[str] = frozenset()
    models: frozenset[str] = frozenset()
    groups: frozenset[str] = frozenset()
    topics: frozenset[str] = frozenset()


class Scope(Payload):
    def targets(self) -> SignalTargets:
        return SignalTargets()


class SignalBase(BaseModel, Generic[ScopeT, ObservedT]):
    """Field order here is the order of keys in every signal's JSON."""

    signal_id: str = Field(default_factory=lambda: str(uuid4()))
    tool: str
    signal_type: SignalType
    collected_at: datetime
    window_start: datetime
    window_end: datetime
    scope: ScopeT
    observed: ObservedT
    severity: Severity
    raw_source_ref: str | None = None


# Level-3 data: external API responses passed through unchanged (Kafka
# AdminClient, Flink REST, Schema Registry, dbt's sources.json). Their shape
# is set by those systems, so typing them belongs in the gateways.
PassThrough = dict[str, Any]
