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
        system=_derive_system(signal.signal_id, [signal]),
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
