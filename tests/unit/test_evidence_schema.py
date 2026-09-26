from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from dp_ops_agent.evidence.schema import Diagnosis, EvidenceChainEntry, Signal
from dp_ops_agent.tools.diagnosis_output.tools import _derive_system


def _make_signal(**overrides) -> Signal:
    now = datetime.now(timezone.utc)
    defaults = dict(
        tool="kafka.under_replicated_partitions",
        signal_type="under_replicated_partitions",
        collected_at=now,
        window_start=now,
        window_end=now,
        scope={"topic": "orders"},
        observed={"partitions": []},
        severity="critical",
    )
    defaults.update(overrides)
    return Signal(**defaults)


def test_signal_round_trips_through_json():
    signal = _make_signal()
    restored = Signal.model_validate_json(signal.model_dump_json())
    assert restored == signal


def test_diagnosis_accepts_evidence_chain_grounded_in_collected_signals():
    signal = _make_signal()
    diagnosis = Diagnosis(
        session_id="s1",
        system="kafka",
        root_cause_hypothesis="Broker 1 ISR churn caused partition 7 to under-replicate",
        root_cause_signal_id=signal.signal_id,
        confidence="high",
        evidence_chain=[
            EvidenceChainEntry(step=1, signal_id=signal.signal_id, interpretation="URP detected")
        ],
        signals=[signal],
        created_at=datetime.now(timezone.utc),
        model="claude-sonnet-5",
    )
    assert diagnosis.tier == 0
    assert diagnosis.proposal is None


def test_diagnosis_rejects_empty_evidence_chain_and_signals():
    with pytest.raises(ValidationError, match="cannot be empty"):
        Diagnosis(
            session_id="s1",
            system="kafka",
            root_cause_hypothesis="bogus, no evidence gathered",
            root_cause_signal_id="doesnt-matter",
            confidence="low",
            evidence_chain=[],
            signals=[],
            created_at=datetime.now(timezone.utc),
            model="claude-sonnet-5",
        )


def test_diagnosis_rejects_evidence_chain_citing_unknown_signal_id():
    signal = _make_signal()
    with pytest.raises(ValidationError, match="unknown signal_id"):
        Diagnosis(
            session_id="s1",
            system="kafka",
            root_cause_hypothesis="bogus",
            root_cause_signal_id=signal.signal_id,
            confidence="low",
            evidence_chain=[
                EvidenceChainEntry(step=1, signal_id="not-a-real-signal-id", interpretation="x")
            ],
            signals=[signal],
            created_at=datetime.now(timezone.utc),
            model="claude-sonnet-5",
        )


def test_diagnosis_rejects_root_cause_signal_id_not_cited_in_evidence_chain():
    cited_signal = _make_signal()
    uncited_signal = _make_signal()
    with pytest.raises(ValidationError, match="must also be cited"):
        Diagnosis(
            session_id="s1",
            system="kafka",
            root_cause_hypothesis="bogus",
            root_cause_signal_id=uncited_signal.signal_id,
            confidence="low",
            evidence_chain=[
                EvidenceChainEntry(step=1, signal_id=cited_signal.signal_id, interpretation="x"),
            ],
            signals=[cited_signal, uncited_signal],
            created_at=datetime.now(timezone.utc),
            model="claude-sonnet-5",
        )


@pytest.mark.parametrize(
    "signal_type",
    [
        "test_failure",
        "model_run_failure",
        "freshness_check_failure",
        "incremental_model_drift",
        "dependency_graph_compile_error",
    ],
)
def test_diagnosis_accepts_dbt_root_cause(signal_type):
    signal = _make_signal(tool=f"dbt.{signal_type}", signal_type=signal_type, scope={})
    diagnosis = Diagnosis(
        session_id="s1",
        system=_derive_system(signal),
        root_cause_hypothesis="dbt-side root cause",
        root_cause_signal_id=signal.signal_id,
        confidence="high",
        evidence_chain=[
            EvidenceChainEntry(step=1, signal_id=signal.signal_id, interpretation="dbt signal")
        ],
        signals=[signal],
        created_at=datetime.now(timezone.utc),
        model="claude-sonnet-5",
    )
    assert diagnosis.system == "dbt"


def _diagnosis(root: Signal, signals: list[Signal]) -> Diagnosis:
    return Diagnosis(
        session_id="s1",
        system="flink",
        root_cause_hypothesis="h",
        root_cause_signal_id=root.signal_id,
        confidence="low",
        evidence_chain=[
            EvidenceChainEntry(step=i, signal_id=s.signal_id, interpretation="x")
            for i, s in enumerate(signals, start=1)
        ],
        signals=signals,
        created_at=datetime.now(timezone.utc),
        model="claude-sonnet-5",
    )


def test_unknown_signal_cannot_be_the_root_cause():
    no_data = _make_signal(severity="unknown", observed={"no_data_reason": "no such vertex"})

    with pytest.raises(ValidationError, match="severity 'unknown'"):
        _diagnosis(no_data, [no_data])


def test_unknown_signal_can_still_be_cited_as_evidence():
    no_data = _make_signal(severity="unknown", observed={"no_data_reason": "no such vertex"})
    root = _make_signal()

    diagnosis = _diagnosis(root, [no_data, root])

    assert diagnosis.evidence_chain[0].signal_id == no_data.signal_id


# --- Proposals (Phase 3b) -----------------------------------------------------

from dp_ops_agent.evidence.schema import (  # noqa: E402
    MAX_TIER1_REPLAY_OFFSETS,
    Proposal,
    ReplayKafkaOffsets,
    RerunDbtModel,
    RestartFlinkJobFromCheckpoint,
)

_FLINK = dict(tool="flink.checkpoint_failure", signal_type="checkpoint_failure", scope={"job_id": "orders-job"})
_DBT = dict(tool="dbt.test_failure", signal_type="test_failure", scope={"model": "fct_orders"})
_KAFKA_LAG = dict(
    tool="kafka.consumer_lag_trend",
    signal_type="consumer_lag_trend",
    scope={"group": "billing-svc", "topic": "orders"},
)


def _with_proposal(root_fields: dict, action, tier: int = 1, extra_signals=()) -> Diagnosis:
    root = _make_signal(**root_fields)
    signals = [root, *extra_signals]
    return Diagnosis(
        session_id="s1",
        system=root_fields["tool"].split(".")[0],
        root_cause_hypothesis="h",
        root_cause_signal_id=root.signal_id,
        confidence="high",
        evidence_chain=[EvidenceChainEntry(step=1, signal_id=root.signal_id, interpretation="x")],
        signals=signals,
        tier=tier,
        proposal=Proposal(tier=tier, action=action, expected_outcome="recovers"),
        created_at=datetime.now(timezone.utc),
        model="claude-sonnet-5",
    )


@pytest.mark.parametrize(
    ("root", "action"),
    [
        (_FLINK, RestartFlinkJobFromCheckpoint(action_type="restart_flink_job_from_checkpoint", job_id="orders-job")),
        (_DBT, RerunDbtModel(action_type="rerun_dbt_model", model="fct_orders")),
        (
            _KAFKA_LAG,
            ReplayKafkaOffsets(
                action_type="replay_kafka_offsets", group="billing-svc", topic="orders",
                partition=7, from_offset=1000, to_offset=5000,
            ),
        ),
    ],
    ids=["flink-restart", "dbt-rerun", "kafka-replay"],
)
def test_grounded_tier1_action_matching_the_root_system_is_accepted(root, action):
    diagnosis = _with_proposal(root, action)

    assert diagnosis.tier == 1
    assert diagnosis.proposal.tier == 1


@pytest.mark.parametrize(
    ("root", "action", "match"),
    [
        # The job/model/group was never investigated this session.
        (_FLINK, RestartFlinkJobFromCheckpoint(action_type="restart_flink_job_from_checkpoint", job_id="other-job"), "other-job"),
        (_DBT, RerunDbtModel(action_type="rerun_dbt_model", model="stg_orders"), "stg_orders"),
        (
            _KAFKA_LAG,
            ReplayKafkaOffsets(
                action_type="replay_kafka_offsets", group="billing-svc", topic="payments",
                partition=0, from_offset=0, to_offset=10,
            ),
            "payments",
        ),
    ],
    ids=["flink-job", "dbt-model", "kafka-topic"],
)
def test_action_on_an_identifier_no_signal_covered_is_rejected(root, action, match):
    with pytest.raises(ValidationError, match=match):
        _with_proposal(root, action)


def test_action_for_a_different_system_than_the_root_cause_is_rejected():
    # e.g. "re-run a dbt model" proposed for a Kafka root cause.
    action = RerunDbtModel(action_type="rerun_dbt_model", model="fct_orders")

    with pytest.raises(ValidationError, match="system"):
        _with_proposal(_KAFKA_LAG, action, extra_signals=[_make_signal(**_DBT)])


def test_replay_wider_than_tier1_is_rejected_not_escalated():
    with pytest.raises(ValidationError, match="Tier 2"):
        ReplayKafkaOffsets(
            action_type="replay_kafka_offsets", group="g", topic="t", partition=0,
            from_offset=0, to_offset=MAX_TIER1_REPLAY_OFFSETS + 1,
        )


def test_replay_range_must_move_forward():
    with pytest.raises(ValidationError, match="to_offset"):
        ReplayKafkaOffsets(
            action_type="replay_kafka_offsets", group="g", topic="t", partition=0,
            from_offset=500, to_offset=500,
        )


def test_tier_is_derived_not_asserted():
    action = RerunDbtModel(action_type="rerun_dbt_model", model="fct_orders")

    with pytest.raises(ValidationError, match="tier"):
        _with_proposal(_DBT, action, tier=0)
