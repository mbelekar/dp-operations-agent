from datetime import UTC
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dp_ops_agent.cli import _augment_alert_text, app


def test_job_name_listed_as_known_identifier():
    text = _augment_alert_text(
        "watermark lag alert",
        kafka_topics="orders",
        consumer_group=None,
        flink_job_id="a1b2c3",
        flink_vertex_id=None,
        flink_job_name="orders-processing-job",
    )

    assert "- Flink job name: orders-processing-job" in text
    assert "- Flink job_id: a1b2c3" in text


def test_output_unchanged_without_job_name():
    without = _augment_alert_text("alert", "orders", "group", "a1b2c3", "v1")
    explicit_none = _augment_alert_text("alert", "orders", "group", "a1b2c3", "v1", None)

    assert "job name" not in without
    assert (
        without
        == explicit_none
        == (
            "alert\n\nKnown identifiers for this incident:\n"
            "- Kafka topics: orders\n"
            "- Kafka consumer group: group\n"
            "- Flink job_id: a1b2c3\n"
            "- Flink vertex_id: v1"
        )
    )


def test_no_identifiers_returns_alert_unchanged():
    assert _augment_alert_text("alert", None, None, None, None, None) == "alert"


def test_fixture_mode_requires_a_dbt_fixture(tmp_path):
    fixtures = Path(__file__).resolve().parents[1] / "fixtures"
    result = CliRunner().invoke(
        app,
        [
            "diagnose",
            "--fixture",
            str(fixtures / "kafka" / "healthy_baseline.json"),
            "--flink-fixture",
            str(fixtures / "flink" / "healthy_baseline.json"),
            "--lineage-fixture",
            str(fixtures / "lineage" / "empty.json"),
            "--alert-text",
            "alert",
            "--log-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "--dbt-fixture" in result.output


def _fixture_args(tmp_path):
    fixtures = Path(__file__).resolve().parents[1] / "fixtures"
    return [
        "diagnose",
        "--fixture",
        str(fixtures / "kafka" / "healthy_baseline.json"),
        "--flink-fixture",
        str(fixtures / "flink" / "healthy_baseline.json"),
        "--lineage-fixture",
        str(fixtures / "lineage" / "empty.json"),
        "--dbt-fixture",
        str(fixtures / "dbt" / "healthy_baseline.json"),
        "--alert-text",
        "alert",
        "--log-dir",
        str(tmp_path),
    ]


def _stub_run(monkeypatch, proposal_input):
    from datetime import datetime

    from dp_ops_agent import cli
    from dp_ops_agent.evidence.schema import Diagnosis, EvidenceChainEntry
    from dp_ops_agent.orchestrator.session import DiagnosisRunResult, TokenUsage
    from dp_ops_agent.tools.diagnosis_output.tools import _build_proposal
    from tests.signal_factory import make_signal

    now = datetime.now(UTC)
    signal = make_signal("checkpoint_failure", scope={"job_id": "orders-processing-job"})
    proposal = _build_proposal(proposal_input)
    diagnosis = Diagnosis(
        session_id="s",
        system="flink",
        root_cause_hypothesis="checkpoints failing",
        root_cause_signal_id=signal.signal_id,
        confidence="high",
        evidence_chain=[EvidenceChainEntry(step=1, signal_id=signal.signal_id, interpretation="x")],
        signals=[signal],
        tier=proposal.tier if proposal else 0,
        proposal=proposal,
        created_at=now,
        model="m",
    )

    async def fake_run_diagnosis(**kwargs):
        return DiagnosisRunResult(
            diagnosis=diagnosis, session_id="s", audit_log_path=None, usage=TokenUsage()
        )

    monkeypatch.setattr(cli, "run_diagnosis", fake_run_diagnosis)


def test_diagnose_prints_the_proposal_for_review(tmp_path, monkeypatch):
    from dp_ops_agent.tools.diagnosis_output.tools import _ProposalInput

    _stub_run(
        monkeypatch,
        _ProposalInput(
            action={
                "action_type": "restart_flink_job_from_checkpoint",
                "job_id": "orders-processing-job",
            },
            expected_outcome="checkpoints complete again",
        ),
    )

    result = CliRunner().invoke(app, _fixture_args(tmp_path))

    assert result.exit_code == 0, result.output
    out = result.output
    assert "Proposal (Tier 1, for human review; nothing was executed):" in out
    assert "Action: restart_flink_job_from_checkpoint" in out
    assert "flink cancel orders-processing-job" in out
    assert "Rollback:" in out and "checkpoint is not modified" in out
    assert "Warnings:" in out and "<latest-completed-checkpoint-path>" in out
    assert "Expected outcome: checkpoints complete again" in out


def test_diagnose_says_when_no_action_is_proposed(tmp_path, monkeypatch):
    _stub_run(monkeypatch, None)

    result = CliRunner().invoke(app, _fixture_args(tmp_path))

    assert result.exit_code == 0, result.output
    assert "Proposal: none (Tier 0, root cause identified, no action proposed)" in result.output


def _live_args(tmp_path):
    return ["diagnose", "--live", "--alert-text", "alert", "--log-dir", str(tmp_path)]


def _spy_on_lineage_close(monkeypatch) -> list[bool]:
    """Records, for each LiveLineageGateway the CLI builds, whether its HTTP
    client was closed once the diagnosis finished."""
    from dp_ops_agent import cli
    from dp_ops_agent.tools.lineage.live_gateway import LiveLineageGateway

    built: list[LiveLineageGateway] = []

    class _Recording(LiveLineageGateway):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            built.append(self)

    monkeypatch.setattr(cli, "LiveLineageGateway", _Recording)
    # No broker in unit tests: don't let librdkafka start connecting.
    monkeypatch.setattr(cli, "LiveKafkaGateway", lambda **kwargs: object())
    return built


def test_live_diagnosis_closes_the_gateways_http_clients(tmp_path, monkeypatch):
    _stub_run(monkeypatch, None)
    built = _spy_on_lineage_close(monkeypatch)

    result = CliRunner().invoke(app, _live_args(tmp_path))

    assert result.exit_code == 0, result.output
    [gateway] = built
    assert gateway._http.is_closed


@pytest.mark.parametrize("error", ["not_submitted", "unexpected"])
def test_live_diagnosis_closes_the_http_clients_when_it_fails(tmp_path, monkeypatch, error):
    from dp_ops_agent import cli
    from dp_ops_agent.orchestrator.session import DiagnosisNotSubmittedError

    async def failing_run_diagnosis(**kwargs):
        if error == "not_submitted":
            raise DiagnosisNotSubmittedError("no submit_diagnosis call")
        raise RuntimeError("bug")

    monkeypatch.setattr(cli, "run_diagnosis", failing_run_diagnosis)
    built = _spy_on_lineage_close(monkeypatch)

    result = CliRunner().invoke(app, _live_args(tmp_path))

    assert result.exit_code != 0
    [gateway] = built
    assert gateway._http.is_closed
