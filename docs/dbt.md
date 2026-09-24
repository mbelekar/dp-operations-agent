# dbt diagnostic module

**Status: Implemented (Phase 3a).** Diagnose-only, same as Kafka, Flink, and lineage: there is no proposal or execution capability yet (proposals are Phase 3b, the data-quality module and runbook Phase 3c). See [`kafka.md`](kafka.md) for the pattern this module follows, and [`lineage.md`](lineage.md) for how a dbt symptom gets traced to another system.

## What it does

dbt is where most alerts in this stack fire, a failing test or a stale source, even though the root cause usually lives upstream. This module's job is the discipline a naive "the test on model X failed, so fix model X" agent lacks: telling "the model's logic is wrong" apart from "the model is correct but its inputs are bad".

It does that by comparing the latest dbt run against the previous one:

- If a failing test **passed on the previous run** and the model's **code did not change** since, the model's logic isn't the problem, its inputs are. The agent is instructed to walk lineage upstream from the model and investigate there before concluding.
- If the model's **code did change**, that change is the prime suspect, and the agent is instructed not to blame an upstream system without an upstream signal showing a problem.
- If there's **no previous run** to compare against, both fields are `null` (unknown, not `false`), and the agent investigates both directions.

"Code changed" is decided exactly the way dbt's own `state:modified` selector decides it: the model's `checksum` in the current `manifest.json` differs from the previous run's. The model never infers it. The grounding mechanism is the same shared code as every other module, see [`kafka.md`'s grounding section](kafka.md#grounding-how-the-evidence-chain-is-enforced).

## Signals collected

| Tool | Reads | What it detects |
| --- | --- | --- |
| `test_failure` | `run_results.json`, `manifest.json` (current and previous) | Failing tests on a model, each with `previously_passed`, plus `model_code_changed` for the model |
| `model_run_failure` | `run_results.json`, `manifest.json` | A model that errored (critical) or was skipped because a parent failed (warn), and whether its code changed |
| `freshness_check_failure` | `sources.json` | A stale source, almost always upstream: the loader or streaming sink stopped landing data |
| `incremental_model_drift` | `run_results.json` `adapter_response` (current and previous) | An incremental model whose `rows_affected` collapsed vs. the previous run: a gap in the upstream stream |
| `dependency_graph_compile_error` | `catalog.json` (current and previous), `manifest.json` | A direct parent (model or source) whose columns were added, removed, or retyped, critical if the model then errored |

Every signal carries, in its `scope`, the `lineage_node_id` to pass to `walk_lineage_upstream`, so the model never builds one itself. Every signal also carries `artifacts_generated_at`, the timestamps of the artifacts it was computed from (see "Stale artifacts" below).

Two signals are weaker than the rest, worth knowing before trusting them:

- **`incremental_model_drift` is a heuristic.** The honest version of "row count vs. expected" needs a warehouse query; this compares only what dbt artifacts record. `rows_affected` is adapter-specific: dbt-postgres reports it, dbt-duckdb doesn't (then `rows_affected_available: false`, severity `unknown`). And an incremental model's first full build reports every row (`code: SELECT`, 1000 rows on dbt-postgres) while later incremental runs report only the new ones (`code: INSERT`, 10 rows), so runs whose adapter `code` differs are reported `comparable: false` with severity `unknown` rather than as a false 99% drop. Thresholds: `warn` below 50% of the previous run, `critical` below 10%.
- **`dependency_graph_compile_error` needs `catalog.json`**, which only exists if `dbt docs generate` ran. Without it the tool reports `catalog_available: false`, severity `unknown`, rather than guessing. Column shapes come from the catalog, not `manifest.json`: a manifest's `columns` list only what someone documented in YAML, so an undocumented column would silently look unchanged. See [ADR-0008](decisions/0008-dbt-previous-run-via-state-dir.md).

## The gateway abstraction: one seam, two implementations

Same pattern as the other modules: every dbt tool talks through the `DbtArtifactsGateway` Protocol (`tools/dbt/gateway.py`). Each method takes a run: `"current"` (the latest run) or `"state"` (the previous run).

- **`LiveDbtGateway`** (`tools/dbt/live_gateway.py`): reads a dbt-core `target/` directory for the current run and a second directory for the previous run, the directory dbt's own `--state` flag would point at. There's no dbt Cloud client. Verified against real dbt-core 1.12.5 output (dbt-duckdb and dbt-postgres): `run-results/v6`, `sources/v3`, `manifest/v12`, `catalog/v1`. Unlike the other live gateways, it's fully testable offline, because its "API" is files: `tests/unit/test_dbt_live_gateway.py` writes artifacts shaped like that real output into a temp directory per test.
- **`FixtureDbtGateway`** (`tools/dbt/fixture_gateway.py`): loads a JSON snapshot in the gateway's own view shapes (not raw dbt artifacts), same contract as the other fixture gateways. Used by every test, the eval suite, and the CLI's `--dbt-fixture` flag.

Missing artifacts follow one rule set in both implementations:

- The current run's `run_results.json`, `manifest.json`, or `sources.json` missing raises `DbtArtifactsUnavailable`. That's one of the backend errors the tool-error middleware turns into a tool error the model sees (see [`docker.md`](docker.md#backend-failures-during-a-diagnosis)), so the session carries on without that signal.
- Anything missing from the previous run means there is no previous run: `previously_passed` and `model_code_changed` come back `null`.
- A missing `catalog.json`, in either run, is reported as unavailable (severity `unknown`), not an error.

More generally, whenever a tool has nothing to judge (an unknown model or source, no test on the model ran, a model missing from the latest run, a source with no freshness result, no row counts to compare) it reports severity `unknown` with an `observed.no_data_reason`, never `ok`. See [ADR-0009](decisions/0009-no-data-is-unknown-not-ok.md).

## Running dbt so its artifacts are usable

dbt overwrites `run_results.json` on every command, including `dbt docs generate` and each of `dbt run` / `dbt test` separately. `LiveDbtGateway` only accepts a `run_results.json` written by `build`, `run`, or `test`, and reports anything else (e.g. one left by `docs generate`) as unavailable, naming the invocation that overwrote it. The run order that leaves all four artifacts usable:

```
dbt source freshness && dbt docs generate && dbt build
cp -R target/ <state dir>    # before the next run, to keep this one as "the previous run"
```

With that order `catalog.json` describes relations as of the previous build, one build behind. That's enough to spot an upstream schema change, and running `docs generate` after `build` instead would destroy the build's `run_results.json`.

Other behavior verified against real dbt worth knowing:

- `dbt build` marks a failing test's downstream models and their tests `skipped`. `test_failure` lists skipped tests separately and never counts them as failures; `model_run_failure` reports a skipped model as `warn`.
- **Stale artifacts.** A Jinja compile error aborts the whole invocation without writing any new artifacts, leaving the previous run's files in place, indistinguishable from fresh ones except by their timestamps. That's why every signal carries `artifacts_generated_at`.

## Node ID convention

Added to the lineage convention in `tools/lineage/gateway.py`: a dbt model is `job:dbt:{model_name}`, a warehouse table (a dbt source, or a model's output) is `dataset:warehouse:{schema}.{table}`. Real dbt OpenLineage events use warehouse-connection namespaces (e.g. `postgres://host:5432`); mapping to those is part of running dbt against live Marquez, not built yet (see [`lineage.md`](lineage.md#node-id-convention)).

## The two scenarios

**Flagship, Design.md's three-hop cascade.** The alert is a dbt source freshness failure on `raw.orders_sink`. `stg_orders`' recency test fails and `fct_orders`' incremental row count collapses from 1210 to 40, but the tests passed last run and no model code changed. Lineage from the warehouse table leads to the Flink job (watermark lag critical) and from there to the `orders` topic (broker 1 ISR churn critical). A correct diagnosis cites `isr_churn` as root cause, `system: "kafka"`.

**Counter-case.** `not_null_fct_orders_amount` fails. It passed last run, but `fct_orders`' checksum changed since, and everything upstream is healthy. A correct diagnosis cites the dbt `test_failure` itself, `system: "dbt"`. This scenario exists to catch an agent that has learned "always blame upstream" instead of the actual rule.

## Example run

```
dp-ops-agent diagnose \
  --fixture tests/fixtures/kafka/isr_churn_upstream_incident.json \
  --flink-fixture tests/fixtures/flink/watermark_lag_cross_system_incident.json \
  --lineage-fixture tests/fixtures/lineage/warehouse_table_to_kafka_topic.json \
  --dbt-fixture tests/fixtures/dbt/freshness_failure_upstream_incident.json \
  --alert-text "PagerDuty: dbt source freshness failed for raw.orders_sink"
```

Against live dbt artifacts instead of a fixture, add `--live` with `--dbt-target-dir` and `--dbt-state-dir` (or the `DBT_TARGET_DIR` / `DBT_STATE_DIR` env vars).

## Testing without live dbt

- `tests/unit/test_dbt_tools.py`: calls each tool's `.ainvoke(args)` directly against `FixtureDbtGateway`, each test building the current and previous run it compares. Covers both directions of the previously-passed / code-changed rule, the no-previous-run case, every severity threshold, and the adapter-specific `rows_affected` cases.
- `tests/unit/test_dbt_live_gateway.py`: parses artifacts shaped like real dbt output, including overwritten `run_results.json`, skipped nodes, and missing or empty previous runs.
- `tests/integration/test_registry_wiring.py`: walks both scenarios tool by tool, confirming each diagnosis derives the expected `system`.
- `evals/scenarios.py`: `cross_system_dbt_freshness_to_isr_churn` and `dbt_model_logic_regression`, run via `./auto/eval` against a live model.

## Known gaps

- No dbt project in the Docker live stack: the `app` service's dbt tools report artifacts unavailable (see [`docker.md`](docker.md#known-gaps)).
- No dbt Cloud API client, and no warehouse queries (real row counts for drift would need one).
- dbt nodes aren't emitted to Marquez; the lineage between dbt and the rest of the stack exists only in fixtures so far.
