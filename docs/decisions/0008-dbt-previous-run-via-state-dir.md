# ADR-0008: Compare dbt runs through state artifacts

| Status | Date |
| --- | --- |
| Accepted | 2026-09-24 |

## Decision

Use dbt's state directory as the previous run and `catalog.json` as the source of actual column shapes.

More specifically:

- The previous run is an earlier `target/` directory saved for dbt's `--state` workflow.
- A model changed when its current `manifest.json` checksum differs from the state manifest.
- Actual columns and types come from `catalog.json`.
- Upstream relationships and checksums continue to come from `manifest.json`.

## Context

The dbt module must distinguish between two cases:

```text
Model logic changed → investigate the dbt model
Model unchanged      → investigate upstream inputs
```

That decision requires previous-run results and a deterministic definition of "code changed." Schema-change diagnosis also needs actual warehouse column shapes.

## Unknown previous state

If no previous run exists:

- `previously_passed` is `null`;
- `model_code_changed` is `null`.

The values are not set to `false`, because false would incorrectly suggest that the model is unchanged and its logic is safe.

## Alternatives considered

| Alternative | Why it was rejected or deferred |
| --- | --- |
| Diff columns declared in `manifest.json` | Rejected. Manifest columns reflect YAML documentation, not necessarily the actual warehouse schema. Undocumented changes could appear unchanged. |
| Ask the model whether compiled SQL changed | Rejected. A deterministic artifact comparison should not be delegated to an LLM. |
| Use the dbt Cloud API for history | Deferred. Cloud artifacts can later fit behind the same gateway protocol, while local files are testable without an account. |

## Operational consequence

dbt commands overwrite `run_results.json`, including `dbt docs generate`. Use this order:

```bash
dbt source freshness
dbt docs generate
dbt build
cp -R target/ <state-dir>
```

The gateway accepts run results produced by `build`, `run`, or `test`. It rejects artifacts overwritten by another command rather than interpreting them incorrectly.

Under this order, `catalog.json` is one build behind. That is sufficient to detect an upstream schema change. Without `catalog.json`, the relevant tool returns `unknown` rather than guessing.

See the [dbt module documentation](../dbt.md).
