import json
from datetime import datetime, timedelta, timezone

import pytest

from dp_ops_agent.approvals.store import (
    action_digest,
    check_usable,
    find_proposal,
    record_decision,
    record_execution,
)
from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.evidence.schema import Proposal, ReplayKafkaOffsets
from dp_ops_agent.tools.diagnosis_output.tools import _build_proposal, _ProposalInput

NOW = datetime(2026, 9, 25, 9, 0, tzinfo=timezone.utc)
_REPLAY = {
    "action_type": "replay_kafka_offsets", "group": "billing-svc", "topic": "orders",
    "partition": 1, "from_offset": 100, "to_offset": 500,
}


def _write_proposal(audit_dir, session_id="s1", proposal: Proposal | None = None) -> Proposal:
    proposal = proposal or _build_proposal(_ProposalInput(action=_REPLAY, expected_outcome="x"))
    JsonlAuditSink(audit_dir, session_id).append(
        AuditEvent(
            event_type="proposal_created", timestamp=NOW, session_id=session_id,
            actor="orchestrator", diagnosis_id="d1", proposal_id=proposal.proposal_id,
            tier=proposal.tier, payload=proposal.model_dump(mode="json"),
        )
    )
    return proposal


def test_find_proposal_scans_every_session_log(tmp_path):
    _write_proposal(tmp_path, "other-session")
    proposal = _write_proposal(tmp_path, "s2")

    record = find_proposal(tmp_path, proposal.proposal_id)

    assert record.session_id == "s2"
    assert record.proposal == proposal
    assert find_proposal(tmp_path, "no-such-id") is None


def test_digest_is_stable_and_changes_with_the_action():
    a = ReplayKafkaOffsets(**_REPLAY)
    b = ReplayKafkaOffsets(**{**_REPLAY, "from_offset": 101})

    assert action_digest(a) == action_digest(ReplayKafkaOffsets(**_REPLAY))
    assert action_digest(a) != action_digest(b)


def _approved(tmp_path, expires_in=timedelta(hours=1)):
    proposal = _write_proposal(tmp_path)
    record = find_proposal(tmp_path, proposal.proposal_id)
    record_decision(record, "approved", reviewer="alice", now=NOW, expires_in=expires_in)
    return find_proposal(tmp_path, proposal.proposal_id)


def test_fresh_approval_is_usable(tmp_path):
    result = check_usable(_approved(tmp_path), NOW + timedelta(minutes=5))

    assert result.ok, result.reason
    assert result.approval.reviewer == "alice"
    assert result.approval.action_digest == action_digest(result.approval_action)


def test_no_decision_is_not_usable(tmp_path):
    proposal = _write_proposal(tmp_path)

    result = check_usable(find_proposal(tmp_path, proposal.proposal_id), NOW)

    assert not result.ok and "not approved" in result.reason


def test_rejected_is_not_usable(tmp_path):
    proposal = _write_proposal(tmp_path)
    record = find_proposal(tmp_path, proposal.proposal_id)
    record_decision(record, "rejected", reviewer="bob", now=NOW, reason="too risky")

    result = check_usable(find_proposal(tmp_path, proposal.proposal_id), NOW)

    assert not result.ok and "rejected" in result.reason


def test_approval_past_its_expiry_is_not_usable(tmp_path):
    result = check_usable(_approved(tmp_path), NOW + timedelta(hours=1, seconds=1))

    assert not result.ok and "expired" in result.reason
    assert result.expired


def test_an_executed_approval_cannot_be_used_again(tmp_path):
    record = _approved(tmp_path)
    record_execution(record, outcome="succeeded", now=NOW, details={})

    result = check_usable(find_proposal(tmp_path, record.proposal.proposal_id), NOW)

    assert not result.ok and "already executed" in result.reason


def test_a_refused_execution_does_not_use_up_the_approval(tmp_path):
    # e.g. the consumer group was still active: nothing changed.
    record = _approved(tmp_path)
    record_execution(record, outcome="refused", now=NOW, details={"reason": "group active"})

    assert check_usable(find_proposal(tmp_path, record.proposal.proposal_id), NOW).ok


def test_an_action_edited_after_approval_is_not_usable(tmp_path):
    record = _approved(tmp_path)
    log = record.audit_path
    lines = log.read_text().splitlines()
    event = json.loads(lines[0])
    event["payload"]["action"]["from_offset"] = 0  # tampered after approval
    log.write_text("\n".join([json.dumps(event), *lines[1:]]) + "\n")

    result = check_usable(find_proposal(tmp_path, record.proposal.proposal_id), NOW)

    assert not result.ok and "does not match" in result.reason


def test_tier0_proposal_has_nothing_to_execute(tmp_path):
    proposal = _write_proposal(tmp_path, proposal=Proposal(tier=0))
    record = find_proposal(tmp_path, proposal.proposal_id)

    result = check_usable(record, NOW)

    assert not result.ok and "Tier 0" in result.reason


@pytest.mark.parametrize("first", ["approved", "rejected"])
def test_a_proposal_can_be_decided_only_once(tmp_path, first):
    proposal = _write_proposal(tmp_path)
    record = find_proposal(tmp_path, proposal.proposal_id)
    record_decision(record, first, reviewer="alice", now=NOW)

    with pytest.raises(ValueError, match="already"):
        record_decision(find_proposal(tmp_path, proposal.proposal_id), "approved", reviewer="bob", now=NOW)


def test_an_expired_approval_can_be_approved_again(tmp_path):
    record = _approved(tmp_path, expires_in=timedelta(minutes=1))
    later = NOW + timedelta(hours=2)
    record_decision(record, "expired", reviewer="system", now=later)

    record_decision(find_proposal(tmp_path, record.proposal.proposal_id), "approved", reviewer="alice", now=later)

    assert check_usable(find_proposal(tmp_path, record.proposal.proposal_id), later).ok
