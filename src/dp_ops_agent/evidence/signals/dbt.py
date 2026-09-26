"""Scope and observed payloads for the dbt signals. Field order is key
order in the JSON; see base.py.

Unlike the other systems, a dbt payload leads with no_data_reason when
there is one. A model or source the manifest doesn't have gets a
different payload altogether (DbtModelNotFound, DbtSourceNotFound), so
each observed type is a union with its not-found shape.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Literal

from dp_ops_agent.evidence.signals.base import (
    Absent,
    DiagnosedSystem,
    PassThrough,
    Payload,
    Scope,
    SignalBase,
    SignalTargets,
)

# Artifact name (run_results, manifest, sources, catalog) -> its generated_at.
GeneratedAt = dict[str, datetime | None]


class DbtModelScope(Scope):
    model: str
    lineage_node_id: Absent[str] = None  # absent when the model isn't found

    def targets(self) -> SignalTargets:
        return SignalTargets(models=frozenset({self.model}))


class DbtSourceScope(Scope):
    source: str
    lineage_node_id: Absent[str] = None  # absent when the source isn't found


class DbtModelNotFound(Payload):
    model_found: Literal[False] = False
    known_models: list[str]
    no_data_reason: str


class DbtSourceNotFound(Payload):
    source_found: Literal[False] = False
    known_sources: list[str]
    no_data_reason: str


class DbtTestResult(Payload):
    test: str
    status: str
    failures: int | None
    message: str | None
    previously_passed: bool | None


class ChangedParent(Payload):
    parent: str
    lineage_node_id: str
    added: list[str]
    removed: list[str]
    retyped: dict[str, list[str]]  # column -> [previous type, current type]


class TestFailureObserved(Payload):
    __test__ = False  # not a pytest test class, despite the name

    no_data_reason: Absent[str] = None
    model_code_changed: bool | None
    failing_tests: list[DbtTestResult]
    warning_tests: list[DbtTestResult]
    skipped_tests: list[str]
    not_run_tests: list[str]
    passing_tests: int
    all_failing_previously_passed: bool | None
    artifacts_generated_at: GeneratedAt


class ModelRunFailureObserved(Payload):
    no_data_reason: Absent[str] = None
    status: str
    message: str | None
    previous_status: str | None
    model_code_changed: bool | None
    artifacts_generated_at: GeneratedAt


class FreshnessCheckFailureObserved(Payload):
    no_data_reason: Absent[str] = None
    status: str
    max_loaded_at: datetime | None
    age_seconds: float | None
    criteria: PassThrough
    artifacts_generated_at: GeneratedAt


class IncrementalModelDriftObserved(Payload):
    no_data_reason: Absent[str] = None
    materialized: str | None
    current_rows_affected: int | None
    previous_rows_affected: int | None
    current_adapter_code: str | None
    previous_adapter_code: str | None
    rows_affected_available: bool
    comparable: bool
    ratio: float | None
    artifacts_generated_at: GeneratedAt


class DependencyGraphCompileErrorObserved(Payload):
    no_data_reason: Absent[str] = None
    model_status: str | None
    model_code_changed: bool | None
    catalog_available: bool
    changed_parents: list[ChangedParent]
    parents_not_in_catalog: list[str]
    artifacts_generated_at: GeneratedAt


# --- signals ------------------------------------------------------------------
# tool and signal_type are fixed per class; they keep their position in the
# JSON (SignalBase's field order) even though they're redeclared here.


class TestFailureSignal(SignalBase[DbtModelScope, TestFailureObserved | DbtModelNotFound]):
    __test__ = False  # not a pytest test class, despite the name

    tool: Literal["dbt.test_failure"] = "dbt.test_failure"
    signal_type: Literal["test_failure"] = "test_failure"
    system: ClassVar[DiagnosedSystem | None] = "dbt"


class ModelRunFailureSignal(SignalBase[DbtModelScope, ModelRunFailureObserved | DbtModelNotFound]):
    tool: Literal["dbt.model_run_failure"] = "dbt.model_run_failure"
    signal_type: Literal["model_run_failure"] = "model_run_failure"
    system: ClassVar[DiagnosedSystem | None] = "dbt"


class FreshnessCheckFailureSignal(
    SignalBase[DbtSourceScope, FreshnessCheckFailureObserved | DbtSourceNotFound]
):
    tool: Literal["dbt.freshness_check_failure"] = "dbt.freshness_check_failure"
    signal_type: Literal["freshness_check_failure"] = "freshness_check_failure"
    system: ClassVar[DiagnosedSystem | None] = "dbt"


class IncrementalModelDriftSignal(
    SignalBase[DbtModelScope, IncrementalModelDriftObserved | DbtModelNotFound]
):
    tool: Literal["dbt.incremental_model_drift"] = "dbt.incremental_model_drift"
    signal_type: Literal["incremental_model_drift"] = "incremental_model_drift"
    system: ClassVar[DiagnosedSystem | None] = "dbt"


class DependencyGraphCompileErrorSignal(
    SignalBase[DbtModelScope, DependencyGraphCompileErrorObserved | DbtModelNotFound]
):
    tool: Literal["dbt.dependency_graph_compile_error"] = "dbt.dependency_graph_compile_error"
    signal_type: Literal["dependency_graph_compile_error"] = "dependency_graph_compile_error"
    system: ClassVar[DiagnosedSystem | None] = "dbt"
