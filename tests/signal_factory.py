"""Valid typed signals for tests that care about a signal's envelope or scope,
not its payload (grounding, diagnosis validation, CLI output).

Signal is a discriminated union of concrete classes whose observed payloads
are fully typed, so a test can't build one from `observed={}`. make_signal
starts from a known-good payload for the signal type, taken from the signal
JSON snapshot, and applies the test's overrides.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import TypeAdapter

from dp_ops_agent.evidence.schema import Severity, Signal, SignalType

_SNAPSHOT = Path(__file__).resolve().parent / "fixtures" / "snapshots" / "signals.json"
_SIGNAL: TypeAdapter[Signal] = TypeAdapter(Signal)

# The snapshot case whose payload each signal type starts from.
_BASE_CASE: dict[str, str] = {
    "under_replicated_partitions": "under_replicated_partitions/normal",
    "isr_churn": "isr_churn/normal",
    "consumer_lag_trend": "consumer_lag_trend/normal",
    "rebalance_frequency": "rebalance_frequency/normal",
    "hot_partition_skew": "hot_partition_skew/normal",
    "schema_registry_compat": "schema_registry_compat/incompatible",
    "checkpoint_failure": "checkpoint_failure/failing",
    "backpressure_ratio": "backpressure_ratio/normal",
    "watermark_lag": "watermark_lag/normal",
    "state_backend_disk_pressure": "state_backend_disk_pressure/normal",
    "savepoint_restore_failure": "savepoint_restore_failure/match",
    "lineage_upstream": "walk_lineage_upstream/found",
    "test_failure": "test_failure/failing-previously-passed",
    "model_run_failure": "model_run_failure/error",
    "freshness_check_failure": "freshness_check_failure/stale",
    "incremental_model_drift": "incremental_model_drift/sharp-drop",
    "dependency_graph_compile_error": "dependency_graph_compile_error/parent-changed",
}


def make_signal(
    signal_type: SignalType,
    *,
    scope: dict[str, str] | None = None,
    severity: Severity = "critical",
    observed: dict[str, Any] | None = None,
) -> Signal:
    """scope replaces the base case's scope; observed keys are merged into
    its observed payload."""
    base = json.loads(json.loads(_SNAPSHOT.read_text())[_BASE_CASE[signal_type]])
    now = datetime.now(UTC).isoformat()
    base.update(
        signal_id=str(uuid4()),
        collected_at=now,
        window_start=now,
        window_end=now,
        severity=severity,
    )
    if scope is not None:
        base["scope"] = scope
    if observed:
        base["observed"] = {**base["observed"], **observed}
    return _SIGNAL.validate_python(base)
