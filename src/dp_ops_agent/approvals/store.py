"""Approval decisions and execution results, stored in the session audit log.

The audit log is the approval store (ADR-0011): a proposal's
proposal_created event, every approval_decision on it, and every
execution_result live in the same append-only session log, so one file
records an incident from alert to execution. Everything here re-reads that
log rather than trusting state passed in, which is what lets the execute
command and the executor each check an approval independently.

An approval is usable only if it is the latest decision, is an approval,
hasn't expired, hasn't been used by an execution that changed anything, and
still matches the digest of the action it approved.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.evidence.schema import ApprovalRecord, Proposal, ProposedAction

Decision = Literal["approved", "rejected", "expired"]
# "refused" means the executor stopped before changing anything (e.g. the
# consumer group was still active), so the approval is not used up.
ExecutionOutcome = Literal["succeeded", "failed", "refused"]
_CONSUMING_OUTCOMES = ("succeeded", "failed")


@dataclass(frozen=True)
class ProposalRecord:
    session_id: str
    audit_path: Path
    diagnosis_id: str | None
    proposal: Proposal
    decisions: list[ApprovalRecord]
    executions: list[dict[str, Any]]


@dataclass(frozen=True)
class Usability:
    ok: bool
    reason: str
    approval: ApprovalRecord | None = None
    approval_action: ProposedAction | None = None
    # True when the latest approval ran out; the caller records an
    # "expired" decision so it shows in the audit trail.
    expired: bool = False


def action_digest(action: ProposedAction) -> str:
    canonical = json.dumps(action.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def find_proposal(audit_dir: str | Path, proposal_id: str) -> ProposalRecord | None:
    for path in sorted(Path(audit_dir).glob("*.jsonl")):
        events = [AuditEvent.model_validate_json(line) for line in path.read_text().splitlines() if line]
        created = next(
            (e for e in events if e.event_type == "proposal_created" and e.proposal_id == proposal_id),
            None,
        )
        if created is None:
            continue
        return ProposalRecord(
            session_id=created.session_id,
            audit_path=path,
            diagnosis_id=created.diagnosis_id,
            proposal=Proposal.model_validate(created.payload),
            decisions=[
                ApprovalRecord.model_validate(e.payload)
                for e in events
                if e.event_type == "approval_decision" and e.proposal_id == proposal_id
            ],
            executions=[
                e.payload
                for e in events
                if e.event_type == "execution_result" and e.proposal_id == proposal_id
            ],
        )
    return None


def _sink(record: ProposalRecord) -> JsonlAuditSink:
    return JsonlAuditSink(record.audit_path.parent, record.session_id)


def record_decision(
    record: ProposalRecord,
    decision: Decision,
    reviewer: str,
    now: datetime,
    reason: str | None = None,
    expires_in: timedelta = timedelta(hours=1),
) -> ApprovalRecord:
    latest = record.decisions[-1] if record.decisions else None
    if decision != "expired" and latest is not None and latest.decision != "expired":
        raise ValueError(
            f"proposal {record.proposal.proposal_id} was already {latest.decision} by "
            f"{latest.reviewer}"
        )
    if decision == "approved" and record.proposal.action is None:
        raise ValueError("a Tier 0 proposal has no action to approve")
    approval = ApprovalRecord(
        proposal_id=record.proposal.proposal_id,
        decision=decision,
        reviewer=reviewer,
        decided_at=now,
        reasoning=reason,
        expires_at=now + expires_in if decision == "approved" else None,
        action_digest=action_digest(record.proposal.action) if record.proposal.action else None,
    )
    _sink(record).append(
        AuditEvent(
            event_type="approval_decision",
            timestamp=now,
            session_id=record.session_id,
            actor="human",
            diagnosis_id=record.diagnosis_id,
            proposal_id=record.proposal.proposal_id,
            tier=record.proposal.tier,
            approval_id=approval.approval_id,
            payload=approval.model_dump(mode="json"),
        )
    )
    return approval


def record_execution(
    record: ProposalRecord,
    outcome: ExecutionOutcome,
    now: datetime,
    details: dict[str, Any],
    approval_id: str | None = None,
) -> None:
    _sink(record).append(
        AuditEvent(
            event_type="execution_result",
            timestamp=now,
            session_id=record.session_id,
            actor="human",
            diagnosis_id=record.diagnosis_id,
            proposal_id=record.proposal.proposal_id,
            tier=record.proposal.tier,
            approval_id=approval_id,
            payload={"outcome": outcome, **details},
        )
    )


def check_usable(record: ProposalRecord, now: datetime) -> Usability:
    action = record.proposal.action
    if action is None or record.proposal.tier != 1:
        return Usability(False, "Tier 0: this proposal has no action to execute")
    latest = record.decisions[-1] if record.decisions else None
    if latest is None:
        return Usability(False, "not approved: run `dp-ops-agent approve` first")
    if latest.decision == "rejected":
        return Usability(False, f"rejected by {latest.reviewer}: {latest.reasoning or 'no reason given'}")
    if latest.decision == "expired":
        return Usability(False, "the approval expired; approve it again to execute")
    if latest.expires_at is not None and now > latest.expires_at:
        return Usability(
            False, f"the approval expired at {latest.expires_at.isoformat()}", latest, action, expired=True
        )
    if any(e.get("outcome") in _CONSUMING_OUTCOMES for e in record.executions):
        return Usability(False, "already executed: an approval can be used once")
    if latest.action_digest != action_digest(action):
        return Usability(False, "the action does not match the one that was approved")
    return Usability(True, f"approved by {latest.reviewer}", latest, action)
