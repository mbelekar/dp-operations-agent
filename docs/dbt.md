# dbt diagnostic module

> **Status: Planned (Phase 3), not yet implemented.**
> This document describes the intended design so it can be built without
> rearchitecting the orchestrator or tool registry. See [`kafka.md`](kafka.md)
> for the pattern this will follow, since it's already built and working.

## Why Phase 3, and why paired with proposals

By Phase 3, the agent covers all four systems the design doc scopes (Kafka, Flink, dbt, data quality) and gains the ability to *propose* a remediation, not just diagnose. `evidence/schema.py`'s `Proposal` type already exists (reserved since Phase 1) but stays unused until this phase populates it. dbt is the layer where most alerts actually fire (a failing test, a stale source) despite the root cause usually living upstream, which makes it the natural point to test the agent's core discipline: distinguishing "the model's logic is wrong" from "the model is correct but its inputs are bad."

## Intended signals

| Tool | Source | What it detects |
| --- | --- | --- |
| `test_failure` | dbt run artifacts (`run_results.json`) | Data-quality issue in the model or its inputs, usually upstream, not the model's SQL |
| `model_run_failure` | dbt run artifacts / warehouse logs | Schema change, broken `ref`, or warehouse resource limit |
| `freshness_check_failure` | `sources.json` from `dbt source freshness` | Almost always upstream: the loader or streaming sink stopped landing data on schedule |
| `incremental_model_drift` | Row-count / max-timestamp comparison vs. expected | A gap in the upstream stream (e.g. a replay window that didn't fully backfill) |
| `dependency_graph_compile_error` | `manifest.json` diff after a schema change | An upstream table changed shape without a corresponding model update |

The key discipline this module has to encode: check whether the same test passed on the previous run with no code change to the model. If so, the fault is almost certainly upstream, and the agent should route the investigation there via lineage rather than suggesting a model edit. That's exactly the trap a naive "the test on model X failed, so fix model X" agent would fall into.

## How it plugs into the existing architecture

Same pattern as Kafka and the planned Flink module:

- **`tools/dbt/gateway.py`**: a `DbtArtifactsGateway` Protocol (reads `run_results.json`, `sources.json`, `manifest.json`, either from disk or the dbt Cloud API).
- **`tools/dbt/live_gateway.py`** / **`tools/dbt/fixture_gateway.py`**: real vs. canned-snapshot implementations, mirroring the Kafka module.
- **`tools/dbt/tools.py`**: `build_dbt_tools(...)`, emitting `Signal`s into the same shared `collected_signals` list every other system's tools already write to.
- **`tools/data_quality/`**: a related but separate module (schema drift, statistical anomalies, cross-system contract violations) that's mostly a **signal source** feeding the core loop rather than a standalone diagnostic path. Per the design doc, a data-quality alert is almost always a symptom, and the loop's job is to trace it to a Kafka, Flink, or dbt root cause via lineage.
- **`runbook/`**: currently an empty package stub. This phase is where it becomes a real `runbook_search` tool backed by a RAG index over past incident write-ups, so recurring failure signatures get faster, more consistent proposals over time.
- **`tools/registry.py`**: concatenates the new tool lists onto the existing ones. `orchestrator/session.py` still doesn't change.
- **`evidence/schema.py`**: `Diagnosis.system` widens further to include `"dbt"`. `submit_diagnosis`'s input schema starts accepting a populated `Proposal` (the field already exists, just `None` today), and `audit/models.py`'s `AuditEvent.event_type` gains `"proposal_created"`. Both are additive, not a migration, since those fields were reserved from Phase 1.

## What's actually new in this phase: proposals, not just diagnoses

Unlike Phase 2 (which only adds a system), Phase 3 changes what `submit_diagnosis` accepts. A `Diagnosis` can now carry a `Proposal` (tier, action, expected outcome, rollback step, downstream consumers, idempotency rating: see the design doc's remediation-tiering table). There's still no execution tool yet (that's Phase 4), so a proposal at this stage is informational, reviewed by a human, and logged, not acted on.
