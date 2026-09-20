from __future__ import annotations

from typing import Any, Protocol

from dp_ops_agent.audit.models import AuditEvent


class AuditSink(Protocol):
    def append(self, event: AuditEvent) -> None: ...

    def query(self, **filters: Any) -> list[AuditEvent]: ...
