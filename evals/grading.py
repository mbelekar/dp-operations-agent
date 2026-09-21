"""Deterministic grading: no LLM-as-judge. A diagnosis passes if the
scenario's expected root-cause signal type was actually cited in the final
evidence_chain, not merely collected during the session. This reuses typed
fields already on Diagnosis/Signal instead of matching free text."""

from __future__ import annotations

from dataclasses import dataclass

from dp_ops_agent.evidence.schema import Diagnosis
from evals.scenarios import EvalScenario


@dataclass(frozen=True)
class GradeResult:
    passed: bool
    reason: str


def grade(diagnosis: Diagnosis, scenario: EvalScenario) -> GradeResult:
    signals_by_id = {s.signal_id: s for s in diagnosis.signals}
    cited_signal_types = {
        signals_by_id[entry.signal_id].signal_type
        for entry in diagnosis.evidence_chain
        if entry.signal_id in signals_by_id
    }

    if scenario.expected_signal_type in cited_signal_types:
        return GradeResult(
            passed=True,
            reason=f"evidence_chain cites a {scenario.expected_signal_type} signal",
        )

    return GradeResult(
        passed=False,
        reason=(
            f"expected a {scenario.expected_signal_type} signal in evidence_chain, "
            f"got {sorted(cited_signal_types) or 'none'}"
        ),
    )
