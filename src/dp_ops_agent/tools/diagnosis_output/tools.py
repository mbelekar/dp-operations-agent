from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from langchain_core.tools import BaseTool, tool
from pydantic import BaseModel, Field, ValidationError

from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import (
    DiagnosedSystem,
    Diagnosis,
    EvidenceChainEntry,
    Proposal,
    ReplayKafkaOffsets,
    RerunDbtModel,
    RestartFlinkJobFromCheckpoint,
    Signal,
    derive_tier,
)
from dp_ops_agent.tools.diagnosis_output.proposals import render


class _EvidenceChainEntryInput(BaseModel):
    step: int
    signal_id: str = Field(
        description="Must be the signal_id of a signal actually returned by a "
        "diagnostic tool call earlier in this session."
    )
    interpretation: str


class _ProposalInput(BaseModel):
    """Only what the model chooses. Tier, command preview, rollback step, and
    warnings are derived in code from the action (ADR-0010)."""

    # A plain union, not schema.ProposedAction's discriminated one: the
    # Anthropic tool converter inlines the variants but keeps pydantic's
    # discriminator mapping, which then points at $defs the tool schema no
    # longer has. Each variant's action_type is a const, so validation still
    # picks the right one.
    action: RestartFlinkJobFromCheckpoint | RerunDbtModel | ReplayKafkaOffsets = Field(
        description="One action from the catalog. Every identifier in it must be one "
        "you collected a signal about this session, and the action must change the "
        "root cause's own system."
    )
    expected_outcome: str


class SubmitDiagnosisInput(BaseModel):
    root_cause_hypothesis: str
    root_cause_signal_id: str = Field(
        description="The signal_id, from evidence_chain, of the single signal that is "
        "the actual root cause. Not just any cited signal — the one the hypothesis "
        "rests on. Cross-system diagnoses (e.g. a Flink alert traced to a Kafka root "
        "cause via lineage) cite multiple systems' signals in evidence_chain, so this "
        "field is what disambiguates which system the diagnosis is actually about."
    )
    confidence: Literal["low", "medium", "high"]
    evidence_chain: list[_EvidenceChainEntryInput]
    proposal: _ProposalInput | None = Field(
        default=None,
        description="Optional Tier 1 remediation. Leave it out (Tier 0) when no catalog "
        "action fixes the root cause.",
    )


def _build_proposal(proposal: _ProposalInput | None) -> Proposal | None:
    if proposal is None:
        return None
    rendered = render(proposal.action)
    return Proposal(
        tier=derive_tier(proposal.action),
        action=proposal.action,
        expected_outcome=proposal.expected_outcome,
        rollback_step=rendered.rollback_step,
        command=rendered.command,
        warnings=rendered.warnings,
    )


def _derive_system(root_cause: Signal) -> DiagnosedSystem | None:
    """The diagnosed system is derived from the root-cause signal's class
    (each concrete signal class declares it, matching its Signal.tool prefix,
    e.g. "flink.checkpoint_failure" -> "flink"), not asserted by the model or
    fixed by the caller. Same philosophy as evidence grounding:
    don't trust a claim that can be derived from real collected data. Takes
    the root_cause_signal_id's signal specifically (not "whichever
    evidence_chain entry comes first"), since a cross-system diagnosis can
    legitimately cite signals from more than one system, only the root
    cause's system is what Diagnosis.system means. None for a signal that
    can't be a root cause (lineage, see DiagnosedSystem).
    """
    return root_cause.system


def build_diagnosis_output_tools(
    audit: AuditSink,
    session_id: str,
    collected_signals: list[Signal],
    result_holder: dict[str, Diagnosis],
    model_name: str,
) -> list[BaseTool]:
    """`result_holder` is a caller-owned dict; on success this tool sets
    result_holder["diagnosis"] so the orchestrator session can retrieve it
    without parsing the model's free-text reply."""

    @tool(args_schema=SubmitDiagnosisInput)
    async def submit_diagnosis(
        root_cause_hypothesis: str,
        root_cause_signal_id: str,
        confidence: Literal["low", "medium", "high"],
        evidence_chain: list[_EvidenceChainEntryInput],
        proposal: _ProposalInput | None = None,
    ) -> str:
        """Conclude the diagnosis. Call this exactly once, after gathering
        sufficient evidence via the diagnostic tools. This is the only way to
        end the session — do not just describe your conclusion in a text
        reply. The signals you collected this session are attached
        automatically; evidence_chain entries must cite a signal_id you
        actually received from a tool call, and root_cause_signal_id must be
        one of those cited signal_ids. An optional proposal names one catalog
        action to remediate the root cause; nothing is executed."""
        root_cause = next(
            (s for s in collected_signals if s.signal_id == root_cause_signal_id), None
        )
        if root_cause is None:
            # Rejected here, not by Diagnosis: without the signal there is no
            # system to derive, and a missing system would fail validation on
            # a field the model doesn't control instead of on grounding.
            return (
                f"submit_diagnosis rejected: root_cause_signal_id {root_cause_signal_id!r} "
                "is not a signal collected this session"
            )
        system = _derive_system(root_cause)
        if system is None:
            return (
                f"submit_diagnosis rejected: root_cause_signal_id {root_cause_signal_id!r} is a "
                "lineage signal; lineage shows where to look, not a root cause. Cite a signal "
                "from the upstream system it points to."
            )
        try:
            built_proposal = _build_proposal(proposal)
            diagnosis = Diagnosis(
                session_id=session_id,
                system=system,
                root_cause_hypothesis=root_cause_hypothesis,
                root_cause_signal_id=root_cause_signal_id,
                confidence=confidence,
                evidence_chain=[EvidenceChainEntry(**e.model_dump()) for e in evidence_chain],
                signals=list(collected_signals),
                tier=built_proposal.tier if built_proposal else 0,
                proposal=built_proposal,
                created_at=datetime.now(UTC),
                model=model_name,
            )
        except ValidationError as exc:
            # Validation failure (e.g. an ungrounded signal_id) surfaces as a
            # tool error so the model must retry with real evidence. Anything
            # else is a bug in our code and propagates (see tool_errors.py).
            return f"submit_diagnosis rejected: {exc}"

        result_holder["diagnosis"] = diagnosis
        audit.append(
            AuditEvent(
                event_type="diagnosis_completed",
                timestamp=datetime.now(UTC),
                session_id=session_id,
                actor="orchestrator",
                diagnosis_id=diagnosis.diagnosis_id,
                tier=diagnosis.tier,
                payload=diagnosis.model_dump(mode="json"),
            )
        )
        if diagnosis.proposal is None:
            return f"Diagnosis {diagnosis.diagnosis_id} recorded (Tier 0, no action proposed)."
        audit.append(
            AuditEvent(
                event_type="proposal_created",
                timestamp=datetime.now(UTC),
                session_id=session_id,
                actor="orchestrator",
                diagnosis_id=diagnosis.diagnosis_id,
                proposal_id=diagnosis.proposal.proposal_id,
                tier=diagnosis.proposal.tier,
                payload=diagnosis.proposal.model_dump(mode="json"),
            )
        )
        return (
            f"Diagnosis {diagnosis.diagnosis_id} recorded with Tier {diagnosis.proposal.tier} "
            f"proposal {diagnosis.proposal.proposal_id} (for human review; nothing was executed)."
        )

    return [submit_diagnosis]
