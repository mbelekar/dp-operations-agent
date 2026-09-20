from __future__ import annotations

from pathlib import Path
from typing import Any

from dp_ops_agent.audit.models import AuditEvent


class JsonlAuditSink:
    """Append-only JSONL audit log, one file per session.

    Phase 4 will likely swap this for a SQLite/Postgres-backed sink so
    approval-record lookups can use indexed "exact action + not expired"
    queries. Any replacement need only implement the AuditSink protocol.
    """

    def __init__(self, log_dir: str | Path, session_id: str) -> None:
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._path = self._log_dir / f"{session_id}.jsonl"

    @property
    def path(self) -> Path:
        return self._path

    def append(self, event: AuditEvent) -> None:
        with self._path.open("a", encoding="utf-8") as f:
            f.write(event.model_dump_json())
            f.write("\n")

    def query(self, **filters: Any) -> list[AuditEvent]:
        if not self._path.exists():
            return []
        events: list[AuditEvent] = []
        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = AuditEvent.model_validate_json(line)
                if all(getattr(event, key, None) == value for key, value in filters.items()):
                    events.append(event)
        return events
