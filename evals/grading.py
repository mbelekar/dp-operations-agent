"""Deterministic grading: no LLM-as-judge. A diagnosis passes if the
scenario's expected root-cause signal type was actually cited in the final
evidence_chain, not merely collected during the session, and, when the
scenario sets expected_action_type, its proposal matches: the expected
catalog action, or no proposal (Tier 0) for "none". This reuses typed fields
already on Diagnosis/Signal/Proposal instead of matching free text."""

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

    if scenario.expected_signal_type not in cited_signal_types:
        return GradeResult(
            passed=False,
            reason=(
                f"expected a {scenario.expected_signal_type} signal in evidence_chain, "
                f"got {sorted(cited_signal_types) or 'none'}"
            ),
        )
    reason = f"evidence_chain cites a {scenario.expected_signal_type} signal"

    expected_action = scenario.expected_action_type
    if expected_action is None:
        return GradeResult(passed=True, reason=reason)
    proposed = diagnosis.proposal.action.action_type if diagnosis.proposal else "none"
    if proposed != expected_action:
        wanted = "no proposal (Tier 0)" if expected_action == "none" else expected_action
        got = "no proposal (Tier 0)" if proposed == "none" else proposed
        return GradeResult(passed=False, reason=f"{reason}, but expected {wanted}, got {got}")
    got = "no proposal (Tier 0)" if proposed == "none" else f"proposes {proposed}"
    return GradeResult(passed=True, reason=f"{reason}; {got}")
