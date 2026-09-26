"""Typed evidence, diagnosis, proposal, and approval schemas.

Proposals (Phase 3b) are Tier 0/1 only: the model picks one action from the
closed catalog below and fills its parameters; everything else about a
proposal (tier, rollback, command preview) is derived in code, and Diagnosis
validation checks the action against the session's evidence (ADR-0010).
ApprovalRecord records a human's decision on a proposal (Phase 4, ADR-0011).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

# Severity and SignalType are defined with the signal envelope and
# re-exported here, where the rest of the code imports them from.
from dp_ops_agent.evidence.signals.base import Severity as Severity
from dp_ops_agent.evidence.signals.base import SignalBase
from dp_ops_agent.evidence.signals.base import SignalType as SignalType

Tier = Literal[0, 1, 2]
# The systems a diagnosis can be about. Lineage signals aren't one of them:
# they show where to look, and the root cause is on the system they point to.
DiagnosedSystem = Literal["kafka", "flink", "dbt"]


# Unparametrized while the tools migrate to typed payloads (ADR-0012 once
# complete): scope and observed are validated as Any until each system's
# tools construct their typed Signal subclasses.
Signal = SignalBase


class EvidenceChainEntry(BaseModel):
    step: int
    signal_id: str
    interpretation: str


# A Kafka replay is Tier 1 ("reversible, narrow") only up to this many
# offsets on one partition of one group; anything wider is Tier 2, which is
# not supported yet and is rejected rather than escalated.
MAX_TIER1_REPLAY_OFFSETS = 100_000


class RestartFlinkJobFromCheckpoint(BaseModel):
    action_type: Literal["restart_flink_job_from_checkpoint"]
    job_id: str


class RerunDbtModel(BaseModel):
    action_type: Literal["rerun_dbt_model"]
    model: str


class ReplayKafkaOffsets(BaseModel):
    action_type: Literal["replay_kafka_offsets"]
    group: str
    topic: str
    partition: int = Field(ge=0)
    from_offset: int = Field(ge=0)
    to_offset: int

    @model_validator(mode="after")
    def is_narrow(self) -> ReplayKafkaOffsets:
        if self.to_offset <= self.from_offset:
            raise ValueError("to_offset must be greater than from_offset")
        if self.to_offset - self.from_offset > MAX_TIER1_REPLAY_OFFSETS:
            raise ValueError(
                f"a replay of {self.to_offset - self.from_offset} offsets is wider than "
                f"Tier 1 allows ({MAX_TIER1_REPLAY_OFFSETS}); broader replays are Tier 2, "
                "not supported yet"
            )
        return self


ProposedAction = Annotated[
    RestartFlinkJobFromCheckpoint | RerunDbtModel | ReplayKafkaOffsets,
    Field(discriminator="action_type"),
]

# The system an action changes; it must match the root-cause signal's system.
ACTION_SYSTEM: dict[str, str] = {
    "restart_flink_job_from_checkpoint": "flink",
    "rerun_dbt_model": "dbt",
    "replay_kafka_offsets": "kafka",
}


def derive_tier(action: ProposedAction | None) -> Tier:
    """Every catalog action is Tier 1; no action is Tier 0."""
    return 0 if action is None else 1


def _action_targets(action: ProposedAction) -> list[tuple[str, str]]:
    """(scope key, value) pairs a collected signal must cover. A Kafka
    replay's partition and offsets aren't in any signal's scope, so they're
    bounded by MAX_TIER1_REPLAY_OFFSETS instead of grounded."""
    if isinstance(action, RestartFlinkJobFromCheckpoint):
        return [("job_id", action.job_id)]
    if isinstance(action, RerunDbtModel):
        return [("model", action.model)]
    return [("group", action.group), ("topic", action.topic)]


def _scope_values(signal: Signal, key: str) -> set[str]:
    values = {signal.scope[key]} if key in signal.scope else set()
    if key == "topic" and "topics" in signal.scope:  # under_replicated_partitions
        values |= set(signal.scope["topics"].split(","))
    return values


class Proposal(BaseModel):
    proposal_id: str = Field(default_factory=lambda: str(uuid4()))
    tier: Tier
    action: ProposedAction | None = None
    # A literal command for a human to review; nothing runs it.
    command: str | None = None
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
    # SHA-256 of the approved action; execution refuses an action that no
    # longer matches it (approvals/store.py).
    action_digest: str | None = None


class Diagnosis(BaseModel):
    diagnosis_id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    system: DiagnosedSystem
    root_cause_hypothesis: str
    root_cause_signal_id: str
    confidence: Literal["low", "medium", "high"]
    evidence_chain: list[EvidenceChainEntry]
    signals: list[Signal]
    tier: Tier = 0
    proposal: Proposal | None = None
    created_at: datetime
    model: str

    @model_validator(mode="after")
    def evidence_chain_is_grounded(self) -> Diagnosis:
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
        if self.root_cause_signal_id not in signal_ids:
            raise ValueError(
                f"root_cause_signal_id {self.root_cause_signal_id!r} is not a signal "
                "collected this session"
            )
        cited_ids = {entry.signal_id for entry in self.evidence_chain}
        if self.root_cause_signal_id not in cited_ids:
            raise ValueError(
                f"root_cause_signal_id {self.root_cause_signal_id!r} must also be cited "
                "in evidence_chain; naming a root cause without citing it as evidence "
                "is a contradiction"
            )
        root_cause = next(s for s in self.signals if s.signal_id == self.root_cause_signal_id)
        if root_cause.severity == "unknown":
            raise ValueError(
                f"root_cause_signal_id {self.root_cause_signal_id!r} has severity 'unknown': "
                "the tool found no data for it, so it can't be what a hypothesis rests on"
            )
        self._check_proposal()
        return self

    def _check_proposal(self) -> None:
        """Tier is derived, never asserted; an action must change the root
        cause's own system, and act only on identifiers some signal collected
        this session actually covered (ADR-0010)."""
        action = self.proposal.action if self.proposal else None
        tier = derive_tier(action)
        if self.tier != tier or (self.proposal and self.proposal.tier != tier):
            raise ValueError(
                f"tier must be {tier} for this proposal (tier is derived from the "
                "action, not chosen)"
            )
        if action is None:
            return
        action_system = ACTION_SYSTEM[action.action_type]
        if action_system != self.system:
            raise ValueError(
                f"{action.action_type} changes {action_system}, but the root cause is on "
                f"system {self.system!r}; propose an action on the root cause's system, "
                "or no action (Tier 0)"
            )
        for key, value in _action_targets(action):
            if not any(value in _scope_values(s, key) for s in self.signals):
                raise ValueError(
                    f"{action.action_type} targets {key} {value!r}, but no signal collected "
                    f"this session covered that {key}; act only on what was investigated"
                )
