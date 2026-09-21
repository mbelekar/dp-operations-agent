"""Typed evidence, diagnosis, proposal, and approval schemas.

Proposal and ApprovalRecord are fully modeled now but stay unused (None) until
Phase 3 (proposals) and Phase 4 (execution + approval) so those phases add
data, not a schema migration.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

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
]

Severity = Literal["ok", "warn", "critical"]
Tier = Literal[0, 1, 2]


class Signal(BaseModel):
    signal_id: str = Field(default_factory=lambda: str(uuid4()))
    tool: str
    signal_type: SignalType
    collected_at: datetime
    window_start: datetime
    window_end: datetime
    scope: dict[str, str] = Field(default_factory=dict)
    observed: dict[str, Any]
    severity: Severity
    raw_source_ref: str | None = None


class EvidenceChainEntry(BaseModel):
    step: int
    signal_id: str
    interpretation: str


class Proposal(BaseModel):
    proposal_id: str = Field(default_factory=lambda: str(uuid4()))
    tier: Tier
    action: dict[str, Any] | None = None
    expected_outcome: str | None = None
    rollback_step: str | None = None
    downstream_consumers: list[str] | None = None
    estimated_volume: str | None = None
    idempotency_rating: Literal["idempotent", "dedupable", "append-only"] | None = None
    warnings: list[str] = Field(default_factory=list)


class ApprovalRecord(BaseModel):
    approval_id: str = Field(default_factory=lambda: str(uuid4()))
    proposal_id: str
    decision: Literal["approved", "rejected", "expired"]
    reviewer: str
    decided_at: datetime
    reasoning: str | None = None
    expires_at: datetime | None = None


class Diagnosis(BaseModel):
    diagnosis_id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    system: Literal["kafka", "flink"]
    root_cause_hypothesis: str
    confidence: Literal["low", "medium", "high"]
    evidence_chain: list[EvidenceChainEntry]
    signals: list[Signal]
    tier: Tier = 0
    proposal: Proposal | None = None
    created_at: datetime
    model: str

    @model_validator(mode="after")
    def evidence_chain_is_grounded(self) -> "Diagnosis":
        if not self.signals or not self.evidence_chain:
            raise ValueError(
                "a diagnosis must cite at least one collected signal; "
                "signals and evidence_chain cannot be empty"
            )
        signal_ids = {s.signal_id for s in self.signals}
        for entry in self.evidence_chain:
            if entry.signal_id not in signal_ids:
                raise ValueError(
                    f"evidence_chain references unknown signal_id {entry.signal_id!r} "
                    "(not present in signals collected this session)"
                )
        return self
