"""dbt diagnostic tools.

Each tool compares the current dbt run against the previous one (the
gateway's "state" run) wherever that comparison is what separates "the
model's logic is wrong" from "the model is correct but its inputs are bad":
model_code_changed is decided the way dbt's own state:modified decides it
(the manifest checksum differs), never inferred by the model. With no
previous run, those fields are None, meaning unknown, not False. Likewise a
tool that has nothing to judge (an unknown model, no catalog, no row counts)
reports severity "unknown" with a no_data_reason, never "ok" (ADR-0009).

Every signal's scope carries the lineage_node_id to hand to
walk_lineage_upstream (see tools/lineage/gateway.py for the convention), so
tracing a dbt symptom upstream never depends on the model building an id.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from langchain_core.tools import BaseTool, tool

from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.audit.sink import AuditSink
from dp_ops_agent.evidence.schema import Severity, Signal
from dp_ops_agent.evidence.signals.dbt import (
    ChangedParent,
    DbtModelNotFound,
    DbtModelScope,
    DbtSourceNotFound,
    DbtSourceScope,
    DbtTestResult,
    DependencyGraphCompileErrorObserved,
    DependencyGraphCompileErrorSignal,
    FreshnessCheckFailureObserved,
    FreshnessCheckFailureSignal,
    IncrementalModelDriftObserved,
    IncrementalModelDriftSignal,
    ModelRunFailureObserved,
    ModelRunFailureSignal,
    TestFailureObserved,
    TestFailureSignal,
)
from dp_ops_agent.tools.dbt.gateway import (
    DbtArtifactsGateway,
    ManifestNode,
    ManifestView,
    RunResultsView,
)
from dp_ops_agent.tools.known_identifiers import capped, describe

_FAILING_TEST_STATUSES = ("fail", "error")
_STALE_FRESHNESS_STATUSES = ("error", "runtime error")
# incremental_model_drift: current run's rows_affected as a fraction of the
# previous run's.
_DRIFT_WARN_RATIO = 0.5
_DRIFT_CRITICAL_RATIO = 0.1


def _record_signal(
    audit: AuditSink, session_id: str, collected_signals: list[Signal], signal: Signal
) -> str:
    """Log the signal to the audit trail, add it to this session's collected
    signals, and return the tool result text the model sees. Mirrors
    tools/flink/tools.py's helper of the same name; kept local rather than
    shared since each module owns its own tool-building logic."""
    collected_signals.append(signal)
    audit.append(
        AuditEvent(
            event_type="signal_collected",
            timestamp=datetime.now(UTC),
            session_id=session_id,
            actor="tool",
            payload=signal.model_dump(mode="json"),
        )
    )
    return signal.model_dump_json()


def _lineage_node_id(node: ManifestNode) -> str:
    if node.resource_type == "source":
        return f"dataset:warehouse:{node.schema_name}.{node.table_name}"
    return f"job:dbt:{node.name}"


def _display_name(node: ManifestNode) -> str:
    if node.resource_type == "source":
        return f"{node.source_name}.{node.name}"
    return node.name


def _find_model(manifest: ManifestView, name: str) -> ManifestNode | None:
    return next(
        (n for n in manifest.nodes.values() if n.resource_type == "model" and n.name == name),
        None,
    )


def _current[View](view: View | None, artifact: str) -> View:
    """Narrows a current-run artifact. The gateway contract (gateway.py)
    raises DbtArtifactsUnavailable when one is missing, so None here is a
    gateway bug and propagates as one, not as missing data."""
    if view is None:
        raise RuntimeError(
            f"dbt gateway returned no current {artifact}; it must raise "
            "DbtArtifactsUnavailable instead"
        )
    return view


def _code_changed(node: ManifestNode, state_manifest: ManifestView | None) -> bool | None:
    if state_manifest is None:
        return None
    previous = state_manifest.nodes.get(node.unique_id)
    if previous is None:
        return True  # new since the previous run
    return previous.checksum != node.checksum


def _status(run_results: RunResultsView | None, unique_id: str) -> str | None:
    if run_results is None:
        return None
    result = run_results.results.get(unique_id)
    return result.status if result is not None else None


def _generated_at(**views: Any) -> dict[str, datetime | None]:
    """Surfaced in every signal: a Jinja compile error aborts a dbt
    invocation without writing new artifacts, so stale files look exactly
    like fresh ones unless their timestamps are shown."""
    return {name: view.generated_at for name, view in views.items() if view is not None}


def build_dbt_tools(
    gateway: DbtArtifactsGateway,
    audit: AuditSink,
    session_id: str,
    collected_signals: list[Signal],
) -> list[BaseTool]:
    def _signal(signal: Signal) -> str:
        return _record_signal(audit, session_id, collected_signals, signal)

    def _model_not_found(
        signal_class: type[TestFailureSignal]
        | type[ModelRunFailureSignal]
        | type[IncrementalModelDriftSignal]
        | type[DependencyGraphCompileErrorSignal],
        model: str,
        manifest: ManifestView,
    ) -> str:
        known = sorted(n.name for n in manifest.nodes.values() if n.resource_type == "model")
        now = datetime.now(UTC)
        return _signal(
            signal_class(
                collected_at=now,
                window_start=now,
                window_end=now,
                scope=DbtModelScope(model=model),
                observed=DbtModelNotFound(
                    known_models=capped(known),
                    no_data_reason=(
                        f"no dbt model named {model!r} in the manifest; "
                        f"known models: {describe(known)}"
                    ),
                ),
                severity="unknown",
                raw_source_ref="dbt:target/manifest.json",
            )
        )

    @tool
    async def test_failure(model: str) -> str:
        """Report dbt test results for a model (by model name, e.g.
        "stg_orders"), compared against the previous run. A test that
        previously_passed on a model whose code did not change
        (model_code_changed: false) means the model's inputs went bad, not
        its logic: trace upstream from scope.lineage_node_id. A null
        previously_passed or model_code_changed means there is no previous
        run to compare against."""
        manifest = _current(await gateway.manifest("current"), "manifest")
        node = _find_model(manifest, model)
        if node is None:
            return _model_not_found(TestFailureSignal, model, manifest)
        run_results = _current(await gateway.run_results("current"), "run_results")
        state_manifest = await gateway.manifest("state")
        state_run_results = await gateway.run_results("state")

        tests = [
            n
            for n in manifest.nodes.values()
            if n.resource_type == "test"
            and (n.attached_node == node.unique_id or node.unique_id in n.depends_on)
        ]
        failing: list[DbtTestResult] = []
        warning: list[DbtTestResult] = []
        skipped: list[str] = []
        not_run: list[str] = []
        passing = 0
        for t in sorted(tests, key=lambda t: t.name):
            result = run_results.results.get(t.unique_id)
            if result is None:
                not_run.append(t.name)
                continue
            if result.status == "skipped":
                skipped.append(t.name)
                continue
            if result.status == "pass":
                passing += 1
                continue
            previous = _status(state_run_results, t.unique_id)
            entry = DbtTestResult(
                test=t.name,
                status=result.status,
                failures=result.failures,
                message=result.message,
                previously_passed=None if previous is None else previous == "pass",
            )
            (failing if result.status in _FAILING_TEST_STATUSES else warning).append(entry)

        previously_passed = [f.previously_passed for f in failing]
        if not failing or None in previously_passed:
            all_failing_previously_passed = None
        else:
            all_failing_previously_passed = all(previously_passed)

        no_data_reason = None
        if failing:
            severity: Severity = "critical"
        elif warning:
            severity = "warn"
        elif passing:
            severity = "ok"
        else:
            severity = "unknown"
            no_data_reason = (
                f"no test on {model!r} ran in the latest run "
                f"({len(not_run)} not run, {len(skipped)} skipped)"
            )
        now = datetime.now(UTC)
        return _signal(
            TestFailureSignal(
                collected_at=now,
                window_start=now,
                window_end=now,
                scope=DbtModelScope(model=model, lineage_node_id=_lineage_node_id(node)),
                observed=TestFailureObserved(
                    no_data_reason=no_data_reason,
                    model_code_changed=_code_changed(node, state_manifest),
                    failing_tests=failing,
                    warning_tests=warning,
                    skipped_tests=skipped,
                    not_run_tests=not_run,
                    passing_tests=passing,
                    all_failing_previously_passed=all_failing_previously_passed,
                    artifacts_generated_at=_generated_at(
                        run_results=run_results, manifest=manifest
                    ),
                ),
                severity=severity,
                raw_source_ref="dbt:target/run_results.json + manifest.json (vs. state/)",
            )
        )

    @tool
    async def model_run_failure(model: str) -> str:
        """Report whether a dbt model (by model name) failed to build in the
        latest run, and whether its code changed since the previous run. An
        error on unchanged code points at a schema change, broken ref, or
        warehouse limit rather than the model's own SQL. A skipped model
        didn't run because something it depends on failed first."""
        manifest = _current(await gateway.manifest("current"), "manifest")
        node = _find_model(manifest, model)
        if node is None:
            return _model_not_found(ModelRunFailureSignal, model, manifest)
        run_results = _current(await gateway.run_results("current"), "run_results")
        result = run_results.results.get(node.unique_id)
        status = result.status if result is not None else "not_run"

        if status == "error":
            severity: Severity = "critical"
        elif status == "skipped":
            severity = "warn"
        elif status == "not_run":
            severity = "unknown"
        else:
            severity = "ok"
        now = datetime.now(UTC)
        return _signal(
            ModelRunFailureSignal(
                collected_at=now,
                window_start=now,
                window_end=now,
                scope=DbtModelScope(model=model, lineage_node_id=_lineage_node_id(node)),
                observed=ModelRunFailureObserved(
                    no_data_reason=(
                        f"{model!r} is not in the latest run's results"
                        if status == "not_run"
                        else None
                    ),
                    status=status,
                    message=result.message if result is not None else None,
                    previous_status=_status(await gateway.run_results("state"), node.unique_id),
                    model_code_changed=_code_changed(node, await gateway.manifest("state")),
                    artifacts_generated_at=_generated_at(
                        run_results=run_results, manifest=manifest
                    ),
                ),
                severity=severity,
                raw_source_ref="dbt:target/run_results.json + manifest.json (vs. state/)",
            )
        )

    @tool
    async def freshness_check_failure(source: str) -> str:
        """Report `dbt source freshness` results for a source, given as
        "source_name.table_name" (e.g. "raw.orders_sink"). A stale source is
        almost always an upstream problem: the loader or streaming sink
        stopped landing data. Trace upstream from scope.lineage_node_id."""
        manifest = _current(await gateway.manifest("current"), "manifest")
        node = next(
            (
                n
                for n in manifest.nodes.values()
                if n.resource_type == "source" and _display_name(n) == source
            ),
            None,
        )
        if node is None:
            known_sources = sorted(
                _display_name(n) for n in manifest.nodes.values() if n.resource_type == "source"
            )
            now = datetime.now(UTC)
            return _signal(
                FreshnessCheckFailureSignal(
                    collected_at=now,
                    window_start=now,
                    window_end=now,
                    scope=DbtSourceScope(source=source),
                    observed=DbtSourceNotFound(
                        known_sources=capped(known_sources),
                        no_data_reason=(
                            f"no dbt source named {source!r} in the manifest; known sources "
                            f"(source_name.table_name): {describe(known_sources)}"
                        ),
                    ),
                    severity="unknown",
                    raw_source_ref="dbt:target/manifest.json",
                )
            )
        freshness = _current(await gateway.source_freshness("current"), "source_freshness")
        result = freshness.results.get(node.unique_id)
        status = result.status if result is not None else "not_checked"

        if status in _STALE_FRESHNESS_STATUSES:
            severity: Severity = "critical"
        elif status == "warn":
            severity = "warn"
        elif status == "not_checked":
            severity = "unknown"
        else:
            severity = "ok"
        now = datetime.now(UTC)
        return _signal(
            FreshnessCheckFailureSignal(
                collected_at=now,
                window_start=now,
                window_end=now,
                scope=DbtSourceScope(source=source, lineage_node_id=_lineage_node_id(node)),
                observed=FreshnessCheckFailureObserved(
                    no_data_reason=(
                        f"`dbt source freshness` has no result for {source!r}"
                        if status == "not_checked"
                        else None
                    ),
                    status=status,
                    max_loaded_at=result.max_loaded_at if result is not None else None,
                    age_seconds=result.age_seconds if result is not None else None,
                    criteria=result.criteria if result is not None else {},
                    artifacts_generated_at=_generated_at(sources=freshness, manifest=manifest),
                ),
                severity=severity,
                raw_source_ref="dbt:target/sources.json",
            )
        )

    @tool
    async def incremental_model_drift(model: str) -> str:
        """Compare an incremental dbt model's rows_affected in the latest run
        against the previous run. A sharp drop suggests a gap in the
        upstream stream (e.g. a replay window that didn't fully backfill).
        Heuristic: based only on dbt artifacts, not a warehouse row count;
        some adapters don't report rows_affected, and runs of different
        kinds (e.g. the initial full build vs. an incremental insert) are
        reported as not comparable."""
        manifest = _current(await gateway.manifest("current"), "manifest")
        node = _find_model(manifest, model)
        if node is None:
            return _model_not_found(IncrementalModelDriftSignal, model, manifest)
        run_results = _current(await gateway.run_results("current"), "run_results")
        state_run_results = await gateway.run_results("state")
        current = run_results.results.get(node.unique_id)
        previous = (
            state_run_results.results.get(node.unique_id) if state_run_results is not None else None
        )

        current_rows = current.rows_affected if current is not None else None
        previous_rows = previous.rows_affected if previous is not None else None
        current_code = current.adapter_code if current is not None else None
        previous_code = previous.adapter_code if previous is not None else None
        rows_available = current_rows is not None and previous_rows is not None
        comparable = rows_available and current_code == previous_code
        ratio = (
            current_rows / previous_rows
            if comparable and current_rows is not None and previous_rows
            else None
        )

        severity: Severity = "ok"
        no_data_reason = None
        if node.materialized == "incremental":
            if not rows_available:
                no_data_reason = (
                    "rows_affected missing for the current or previous run (no previous "
                    "run, or the adapter doesn't report it)"
                )
            elif not comparable:
                no_data_reason = (
                    f"runs aren't comparable: adapter code {previous_code!r} previously, "
                    f"{current_code!r} now (e.g. the initial full build vs. an incremental run)"
                )
            elif ratio is None:
                no_data_reason = "previous run affected 0 rows, so there's no ratio"
            elif ratio < _DRIFT_CRITICAL_RATIO:
                severity = "critical"
            elif ratio < _DRIFT_WARN_RATIO:
                severity = "warn"
            if no_data_reason:
                severity = "unknown"
        now = datetime.now(UTC)
        return _signal(
            IncrementalModelDriftSignal(
                collected_at=now,
                window_start=now,
                window_end=now,
                scope=DbtModelScope(model=model, lineage_node_id=_lineage_node_id(node)),
                observed=IncrementalModelDriftObserved(
                    no_data_reason=no_data_reason,
                    materialized=node.materialized,
                    current_rows_affected=current_rows,
                    previous_rows_affected=previous_rows,
                    current_adapter_code=current_code,
                    previous_adapter_code=previous_code,
                    rows_affected_available=rows_available,
                    comparable=comparable,
                    ratio=ratio,
                    artifacts_generated_at=_generated_at(
                        run_results=run_results, manifest=manifest
                    ),
                ),
                severity=severity,
                raw_source_ref="dbt:target/run_results.json adapter_response (vs. state/)",
            )
        )

    @tool
    async def dependency_graph_compile_error(model: str) -> str:
        """Check whether any of a dbt model's direct parents (models or
        sources) changed shape since the previous run: columns added,
        removed, or retyped, from catalog.json. A parent that changed shape
        while the model's own code did not, followed by the model failing,
        means the upstream table changed without a matching model update.
        catalog.json only exists if `dbt docs generate` ran."""
        manifest = _current(await gateway.manifest("current"), "manifest")
        node = _find_model(manifest, model)
        if node is None:
            return _model_not_found(DependencyGraphCompileErrorSignal, model, manifest)
        run_results = _current(await gateway.run_results("current"), "run_results")
        catalog = await gateway.catalog("current")
        state_catalog = await gateway.catalog("state")
        model_status = _status(run_results, node.unique_id)
        catalog_available = catalog is not None and state_catalog is not None

        changed_parents: list[ChangedParent] = []
        not_in_catalog: list[str] = []
        if catalog is not None and state_catalog is not None:
            for parent_id in node.depends_on:
                parent = manifest.nodes.get(parent_id)
                current_cols = catalog.columns.get(parent_id)
                previous_cols = state_catalog.columns.get(parent_id)
                if parent is None or current_cols is None or previous_cols is None:
                    not_in_catalog.append(parent_id)
                    continue
                added = sorted(set(current_cols) - set(previous_cols))
                removed = sorted(set(previous_cols) - set(current_cols))
                retyped = {
                    col: [previous_cols[col], current_cols[col]]
                    for col in sorted(set(current_cols) & set(previous_cols))
                    if previous_cols[col] != current_cols[col]
                }
                if added or removed or retyped:
                    changed_parents.append(
                        ChangedParent(
                            parent=_display_name(parent),
                            lineage_node_id=_lineage_node_id(parent),
                            added=added,
                            removed=removed,
                            retyped=retyped,
                        )
                    )

        no_data_reason = None
        if changed_parents and model_status == "error":
            severity: Severity = "critical"
        elif changed_parents:
            severity = "warn"
        elif not catalog_available:
            severity = "unknown"
            no_data_reason = (
                "catalog.json missing for the current or previous run "
                "(`dbt docs generate` didn't run)"
            )
        elif node.depends_on and len(not_in_catalog) == len(node.depends_on):
            severity = "unknown"
            no_data_reason = "none of the model's parents are in both runs' catalogs"
        else:
            severity = "ok"
        now = datetime.now(UTC)
        return _signal(
            DependencyGraphCompileErrorSignal(
                collected_at=now,
                window_start=now,
                window_end=now,
                scope=DbtModelScope(model=model, lineage_node_id=_lineage_node_id(node)),
                observed=DependencyGraphCompileErrorObserved(
                    no_data_reason=no_data_reason,
                    model_status=model_status,
                    model_code_changed=_code_changed(node, await gateway.manifest("state")),
                    catalog_available=catalog_available,
                    changed_parents=changed_parents,
                    parents_not_in_catalog=not_in_catalog,
                    artifacts_generated_at=_generated_at(
                        run_results=run_results, manifest=manifest, catalog=catalog
                    ),
                ),
                severity=severity,
                raw_source_ref="dbt:target/catalog.json + manifest.json (vs. state/)",
            )
        )

    return [
        test_failure,
        model_run_failure,
        freshness_check_failure,
        incremental_model_drift,
        dependency_graph_compile_error,
    ]
