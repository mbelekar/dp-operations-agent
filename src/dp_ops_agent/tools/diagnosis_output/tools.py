from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from langchain_core.tools import BaseTool, tool
from pydantic import BaseModel, Field

from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import Diagnosis, EvidenceChainEntry, Signal


class _EvidenceChainEntryInput(BaseModel):
    step: int
    signal_id: str = Field(
        description="Must be the signal_id of a signal actually returned by a "
        "diagnostic tool call earlier in this session."
    )
    interpretation: str


class SubmitDiagnosisInput(BaseModel):
    root_cause_hypothesis: str
    confidence: Literal["low", "medium", "high"]
    evidence_chain: list[_EvidenceChainEntryInput]


def build_diagnosis_output_tools(
    audit: AuditSink,
    session_id: str,
    collected_signals: list[Signal],
    result_holder: dict[str, Diagnosis],
    model_name: str,
    system: str = "kafka",
) -> list[BaseTool]:
    """`result_holder` is a caller-owned dict; on success this tool sets
    result_holder["diagnosis"] so the orchestrator session can retrieve it
    without parsing the model's free-text reply."""

    @tool(args_schema=SubmitDiagnosisInput)
    async def submit_diagnosis(
        root_cause_hypothesis: str,
        confidence: Literal["low", "medium", "high"],
        evidence_chain: list[_EvidenceChainEntryInput],
    ) -> str:
        """Conclude the diagnosis. Call this exactly once, after gathering
        sufficient evidence via the diagnostic tools. This is the only way to
        end the session — do not just describe your conclusion in a text
        reply. The signals you collected this session are attached
        automatically; evidence_chain entries must cite a signal_id you
        actually received from a tool call."""
        try:
            diagnosis = Diagnosis(
                session_id=session_id,
                system=system,  # type: ignore[arg-type]
                root_cause_hypothesis=root_cause_hypothesis,
                confidence=confidence,
                evidence_chain=[
                    EvidenceChainEntry(**e.model_dump()) for e in evidence_chain
                ],
                signals=list(collected_signals),
                created_at=datetime.now(timezone.utc),
                model=model_name,
            )
        except Exception as exc:
            # Validation failure (e.g. an ungrounded signal_id) surfaces as a
            # tool error so the model must retry with real evidence.
            return f"submit_diagnosis rejected: {exc}"

        result_holder["diagnosis"] = diagnosis
        audit.append(
            AuditEvent(
                event_type="diagnosis_completed",
                timestamp=datetime.now(timezone.utc),
                session_id=session_id,
                actor="orchestrator",
                diagnosis_id=diagnosis.diagnosis_id,
                tier=diagnosis.tier,
                payload=diagnosis.model_dump(mode="json"),
            )
        )
        return f"Diagnosis {diagnosis.diagnosis_id} recorded."

    return [submit_diagnosis]
