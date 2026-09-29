"""Percent-encoding for identifiers placed in a live backend's URL path.

Identifiers (a Flink job_id, a schema subject) come from the model, so one
like "../jobmanager/logs" or "abc?x=1" must stay a single path segment
instead of reaching a different endpoint. quote() alone isn't enough: it
leaves a bare "." or ".." as is, and httpx resolves those as dot segments.
"""

from __future__ import annotations

from urllib.parse import quote

_DOT_SEGMENTS = {".": "%2E", "..": "%2E%2E"}


def segment(value: str) -> str:
    return _DOT_SEGMENTS.get(value) or quote(value, safe="")
