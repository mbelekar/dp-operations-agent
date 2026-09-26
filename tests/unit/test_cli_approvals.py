from datetime import UTC, datetime

from typer.testing import CliRunner

from dp_ops_agent.approvals.store import find_proposal
from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.cli import app
from dp_ops_agent.evidence.schema import Proposal
from dp_ops_agent.tools.diagnosis_output.tools import _build_proposal, _ProposalInput

_REPLAY = {
    "action_type": "replay_kafka_offsets", "group": "billing-svc", "topic": "orders",
    "partition": 1, "from_offset": 100, "to_offset": 500,
}


def _write(tmp_path, proposal: Proposal | None = None) -> Proposal:
    proposal = proposal or _build_proposal(_ProposalInput(action=_REPLAY, expected_outcome="catch up"))
    JsonlAuditSink(tmp_path, "s1").append(
        AuditEvent(
            event_type="proposal_created", timestamp=datetime.now(UTC), session_id="s1",
            actor="orchestrator", proposal_id=proposal.proposal_id, tier=proposal.tier,
            payload=proposal.model_dump(mode="json"),
        )
    )
    return proposal


def _invoke(*args):
    return CliRunner().invoke(app, list(args))


def test_approve_shows_the_proposal_and_records_the_approval(tmp_path):
    proposal = _write(tmp_path)

    result = _invoke(
        "approve", proposal.proposal_id, "--reviewer", "alice", "--expires-in", "30m",
        "--log-dir", str(tmp_path),
    )

    assert result.exit_code == 0, result.output
    assert "Action: replay_kafka_offsets" in result.output
    assert "--reset-offsets --to-offset 100 --execute" in result.output
    assert "Approved by alice" in result.output
    [decision] = find_proposal(tmp_path, proposal.proposal_id).decisions
    assert decision.decision == "approved" and decision.reviewer == "alice"
    assert (decision.expires_at - decision.decided_at).total_seconds() == 30 * 60


def test_reject_records_the_reason(tmp_path):
    proposal = _write(tmp_path)

    result = _invoke(
        "reject", proposal.proposal_id, "--reviewer", "bob", "--reason", "replay window too wide",
        "--log-dir", str(tmp_path),
    )

    assert result.exit_code == 0, result.output
    [decision] = find_proposal(tmp_path, proposal.proposal_id).decisions
    assert (decision.decision, decision.reasoning) == ("rejected", "replay window too wide")


def test_unknown_proposal_is_refused(tmp_path):
    result = _invoke("approve", "no-such-id", "--reviewer", "alice", "--log-dir", str(tmp_path))

    assert result.exit_code == 1
    assert "no proposal no-such-id" in result.output


def test_tier0_proposal_cannot_be_approved(tmp_path):
    proposal = _write(tmp_path, Proposal(tier=0))

    result = _invoke("approve", proposal.proposal_id, "--reviewer", "alice", "--log-dir", str(tmp_path))

    assert result.exit_code == 1
    assert "Tier 0" in result.output
    assert find_proposal(tmp_path, proposal.proposal_id).decisions == []


def test_already_decided_proposal_is_refused(tmp_path):
    proposal = _write(tmp_path)
    _invoke("reject", proposal.proposal_id, "--reviewer", "bob", "--reason", "no", "--log-dir", str(tmp_path))

    result = _invoke("approve", proposal.proposal_id, "--reviewer", "alice", "--log-dir", str(tmp_path))

    assert result.exit_code == 1
    assert "already rejected" in result.output


def test_bad_expiry_is_refused(tmp_path):
    proposal = _write(tmp_path)

    result = _invoke(
        "approve", proposal.proposal_id, "--reviewer", "alice", "--expires-in", "soon",
        "--log-dir", str(tmp_path),
    )

    assert result.exit_code != 0
    assert find_proposal(tmp_path, proposal.proposal_id).decisions == []
