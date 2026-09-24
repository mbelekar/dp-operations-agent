# ADR-0008: dbt's "previous run" is its `--state` directory, and column shapes come from `catalog.json`

**Status:** Accepted
**Date:** 2026-09-24

## Context

The dbt module's core discipline (see [`docs/dbt.md`](../dbt.md)) is telling "the model's logic is wrong" apart from "the model is correct but its inputs are bad", by checking whether a failing test passed on the previous run with no change to the model's code. That needs two runs' artifacts and a definition of "code changed". Separately, `dependency_graph_compile_error` needs each upstream table's actual columns in both runs, and Design.md specified a `manifest.json` diff for that.

## Decision

**The previous run is the directory dbt's own `--state` flag points at**, holding a copy of an earlier run's `target/` artifacts. "Code changed" means the model's `checksum` in the current `manifest.json` differs from the one in the state manifest, the same comparison dbt's `state:modified` selector makes. With no previous run, the tools report `previously_passed` and `model_code_changed` as `null`, never `false`: `false` would quietly steer the model toward "the model's logic is fine".

**Column shapes come from `catalog.json`**, which `dbt docs generate` writes from the warehouse itself. Upstream nodes and checksums still come from `manifest.json`.

## Alternatives considered

- **Diff `manifest.json` columns, as Design.md specified.** Rejected. A manifest's `columns` list only the columns someone documented in YAML, often none, with declared rather than actual types. An upstream table gaining, losing, or retyping an undocumented column would diff as "no change", a false all-clear the model could cite as evidence.
- **Let the model judge whether code changed**, e.g. from compiled SQL. Rejected, for the same reason as ADR-0002 and ADR-0006: a deterministic fact that can be computed from artifacts shouldn't be delegated to the model.
- **A dbt Cloud API client** for run history. Deferred, not rejected: Cloud's run artifacts have the same shapes, so a Cloud gateway can sit behind the same Protocol later. The local `target/` + `--state` pair can be tested offline against real artifact files; a Cloud client can't be without an account.

## Consequences

A live run needs a disciplined dbt run order, verified against real dbt-core 1.12.5: every dbt command overwrites `run_results.json`, `dbt docs generate` included, so the usable order is `source freshness` → `docs generate` → `build`, and the gateway rejects a `run_results.json` not written by `build`/`run`/`test` instead of misreading it. Under that order `catalog.json` lags one build behind, enough to catch an upstream schema change. Without `docs generate` at all, `dependency_graph_compile_error` reports the catalog as unavailable rather than guessing. And someone, or some scheduler step, has to copy `target/` aside after each run for the next run to have a previous one.
