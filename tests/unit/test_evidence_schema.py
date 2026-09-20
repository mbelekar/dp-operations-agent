from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from dp_ops_agent.evidence.schema import Diagnosis, EvidenceChainEntry, Signal


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
            confidence="low",
            evidence_chain=[
                EvidenceChainEntry(step=1, signal_id="not-a-real-signal-id", interpretation="x")
            ],
            signals=[signal],
            created_at=datetime.now(timezone.utc),
            model="claude-sonnet-5",
        )
