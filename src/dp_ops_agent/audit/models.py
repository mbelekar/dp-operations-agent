"""Audit event schema. tier/proposal_id/approval_id are reserved for Phase 3/4
so those phases add new event_type values rather than redesigning this model."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field

EventType = Literal[
    "diagnosis_run_started",
    "signal_collected",
    "tool_error",
    "diagnosis_completed",
    "session_usage",
    "proposal_created",
    "approval_decision",
    "execution_result",
]


class AuditEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    timestamp: datetime
    session_id: str
    actor: Literal["orchestrator", "tool", "human"]
    diagnosis_id: str | None = None
    proposal_id: str | None = None
    tier: Literal[0, 1, 2] | None = None
    approval_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
