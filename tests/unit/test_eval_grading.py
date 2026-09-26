"""Grading with the Phase 3b proposal expectation, against hand-built
diagnoses (no model calls)."""

from dataclasses import replace
from datetime import UTC, datetime

from dp_ops_agent.evidence.schema import Diagnosis, EvidenceChainEntry, Signal, SignalType
from dp_ops_agent.tools.diagnosis_output.tools import _build_proposal, _ProposalInput
from evals.grading import grade
from evals.scenarios import SCENARIOS
from tests.signal_factory import make_signal

_BY_NAME = {s.name: s for s in SCENARIOS}


def _diagnosis(
    signal_type: SignalType,
    scope: dict,
    action: dict | None,
    supporting: tuple[Signal, ...] = (),
) -> Diagnosis:
    """A diagnosis rooted in one signal of signal_type; supporting signals are
    cited after it (e.g. the lag signal that grounds a replay's group)."""
    now = datetime.now(UTC)
    signal = make_signal(signal_type, scope=scope)
    signals = [signal, *supporting]
    proposal = (
        _build_proposal(_ProposalInput(action=action, expected_outcome="x")) if action else None
    )
    return Diagnosis(
        session_id="s",
        system=signal.system,
        root_cause_hypothesis="h",
        root_cause_signal_id=signal.signal_id,
        confidence="high",
        evidence_chain=[
            EvidenceChainEntry(step=i, signal_id=s.signal_id, interpretation="x")
            for i, s in enumerate(signals, start=1)
        ],
        signals=signals,
        tier=proposal.tier if proposal else 0,
        proposal=proposal,
        created_at=now,
        model="m",
    )


_CHECKPOINT = ("checkpoint_failure", {"job_id": "orders-processing-job"})
_MODEL_ERROR = ("model_run_failure", {"model": "fct_orders"})
_RERUN = {"action_type": "rerun_dbt_model", "model": "fct_orders"}
_ISR = ("isr_churn", {"broker_id": "1"})


def test_expected_action_proposed_passes():
    result = grade(_diagnosis(*_MODEL_ERROR, _RERUN), _BY_NAME["dbt_transient_model_run_failure"])

    assert result.passed, result.reason


def test_right_root_cause_but_no_proposal_fails_when_an_action_is_expected():
    result = grade(_diagnosis(*_MODEL_ERROR, None), _BY_NAME["dbt_transient_model_run_failure"])

    assert not result.passed
    assert "rerun_dbt_model" in result.reason


def test_recovery_action_is_not_expected_where_it_cannot_fix_the_cause():
    # A code regression isn't fixed by re-running the same model, and a
    # running job with a transient checkpoint blip isn't fixed by a restart.
    assert _BY_NAME["dbt_model_logic_regression"].expected_action_type == "none"
    assert _BY_NAME["flink_checkpoint_failure"].expected_action_type == "none"


def test_tier0_expected_and_no_proposal_passes():
    result = grade(_diagnosis(*_ISR, None), _BY_NAME["urp_lag_spike"])

    assert result.passed, result.reason


def test_tier0_expected_but_an_action_proposed_fails():
    replay = {
        "action_type": "replay_kafka_offsets",
        "group": "g",
        "topic": "orders",
        "partition": 0,
        "from_offset": 0,
        "to_offset": 10,
    }

    lag = make_signal("consumer_lag_trend", scope={"group": "g", "topic": "orders"})

    result = grade(_diagnosis(*_ISR, replay, supporting=(lag,)), _BY_NAME["urp_lag_spike"])

    assert not result.passed
    assert "Tier 0" in result.reason


def test_no_expectation_ignores_the_proposal():
    scenario = replace(_BY_NAME["flink_checkpoint_failure"], expected_action_type=None)

    assert grade(_diagnosis(*_CHECKPOINT, None), scenario).passed


def test_wrong_root_cause_still_fails_regardless_of_proposal():
    result = grade(_diagnosis(*_ISR, None), _BY_NAME["flink_checkpoint_failure"])

    assert not result.passed
    assert "checkpoint_failure" in result.reason
