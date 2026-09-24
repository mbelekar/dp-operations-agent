# Lineage module

**Status: Implemented (Phase 2b).** See [`kafka.md`](kafka.md) and [`flink.md`](flink.md) for the two modules this one connects, and [`dbt.md`](dbt.md) (Phase 3a) for the dbt module, whose symptoms get traced upstream through this one.

## What it does

Kafka and Flink diagnostics investigate one system at a time. Lineage is what lets a diagnosis cross the boundary: given a Kafka topic or a Flink job, `walk_lineage_upstream` finds what's upstream of it, so an alert that fires on one system can be traced back to a root cause that actually lives on another. This is Phase 2's actual point. Phase 2a proved the Kafka and Flink tools individually; this module proves cross-system localization, not just single-system diagnosis done twice.

## The flagship scenario

An alert fires on Flink watermark lag. Every Flink-internal signal (backpressure, checkpoints, disk pressure, savepoint history) comes back healthy, there's nothing wrong inside Flink itself. The correct move is to call `walk_lineage_upstream` on the job, find the Kafka topic it reads from, and investigate that topic's own signals. There, `isr_churn` is critical: a broker is flaky, its ISR is shrinking, and that instability is what's starving the Flink source of timely data. A correct diagnosis cites the Kafka `isr_churn` signal as `root_cause_signal_id`, despite the alert being entirely Flink-worded. This is both a fixture (`tests/fixtures/{flink,kafka,lineage}/*cross_system*` / `*isr_churn_upstream*` / `flink_job_to_kafka_topic.json`) and the `cross_system_watermark_lag_to_isr_churn` eval scenario, verified against a live model, not just asserted structurally, the model does correctly cross the system boundary rather than stopping at "Flink watermark lag, cause unknown."

## Signals collected

| Tool | File | What it detects |
| --- | --- | --- |
| `walk_lineage_upstream` | `tools/lineage/tools.py` | What's upstream of a Kafka topic or Flink job, the trace step that makes cross-system root-causing possible |

Unlike every other tool in this project, `walk_lineage_upstream`'s severity is always `"ok"`. A lineage lookup isn't itself a health signal, it's graph structure. The actual health signal comes from whatever tool investigates the node lineage points to next.

## The gateway abstraction: one seam, two implementations

Same pattern as Kafka and Flink: every call goes through the `LineageQueryGateway` Protocol (`tools/lineage/gateway.py`), never calling Marquez's REST API directly.

- **`LiveLineageGateway`** (`tools/lineage/live_gateway.py`): the real implementation. Uses `httpx` against Marquez's `GET /api/v1/lineage?nodeId=...&depth=...`. Marquez's raw response is the full graph (upstream and downstream) within `depth` hops, not just the upstream side, so this walks `inEdges` backward from the queried node to filter to ancestors only. Verified against a real Marquez 0.51.1 in the compose stack (see [`docs/docker.md`](docker.md)), seeded with the demo topology: the job resolves to `[dataset:kafka:orders]`, `dataset:kafka:orders-sink` to the job plus `orders` (two hops, downstream excluded), and `dataset:kafka:orders` to nothing. Marquez answers **404** for a node it has never seen; the gateway returns an empty view for that, matching `FixtureLineageGateway`, rather than raising and ending the whole session over a mistyped node id. Only that 404, recognized by Marquez's `"Job '…' not found."` / `"Dataset '…' not found."` body: any other 404 (a wrong path prefix or host in `MARQUEZ_URL`) raises and reaches the model as a tool error, not as an empty graph it could cite as "nothing upstream".
- **`FixtureLineageGateway`** (`tools/lineage/fixture_gateway.py`): loads a JSON snapshot keyed by `node_id`, same contract as the other fixture gateways. Used by every test, the eval suite, and the CLI's `--lineage-fixture` flag.

## Node ID convention

Marquez doesn't dictate a node-naming scheme, so this project picks one and uses it everywhere, gateway implementations, fixtures, and the system prompt all agree on it: a Kafka topic is `dataset:kafka:{topic}`, a Flink job is `job:flink:{job_name}`, a dbt model is `job:dbt:{model_name}`, and a warehouse table (a dbt source, or a model's output) is `dataset:warehouse:{schema}.{table}`. The dbt tools compute these ids themselves and return them in each signal's `scope.lineage_node_id`, so a dbt symptom can be walked upstream without the model building an id (see [`dbt.md`](dbt.md#node-id-convention)). The live Marquez stack only has Kafka and Flink nodes; dbt nodes exist in fixtures so far. Documented once in `tools/lineage/gateway.py`'s module docstring rather than scattered across call sites.

## Why `Diagnosis.system` needed a real fix here

Before this module, `Diagnosis.system` was derived from whichever `evidence_chain` entry came first in the model's list, safe only because every session's cited evidence was single-system (see [ADR-0006](decisions/0006-diagnosis-system-derived-not-asserted.md)). The flagship scenario breaks that assumption on purpose: it cites both a Flink signal and a Kafka signal in one evidence chain. `submit_diagnosis` now requires an explicit `root_cause_signal_id`, and `system` derives from that signal specifically, not from list position. See [ADR-0007](decisions/0007-root-cause-signal-id.md) for the full reasoning.

## Example run

```
dp-ops-agent diagnose \
  --fixture tests/fixtures/kafka/isr_churn_upstream_incident.json \
  --flink-fixture tests/fixtures/flink/watermark_lag_cross_system_incident.json \
  --lineage-fixture tests/fixtures/lineage/flink_job_to_kafka_topic.json \
  --dbt-fixture tests/fixtures/dbt/healthy_baseline.json \
  --alert-text "PagerDuty: watermark lag alert on orders-processing-job"
```

Against this fixture, the agent investigated Flink first (matching the alert), found only watermark lag critical and everything else clean, called `walk_lineage_upstream` and found the upstream `orders` topic, then investigated Kafka and found broker 1's ISR-shrink rate critical while its peers were quiet. The resulting diagnosis correctly set `system: "kafka"` and `root_cause_signal_id` to the `isr_churn` signal, not the Flink signal that triggered the alert.

## Testing without live Marquez

- `tests/unit/test_lineage_tools.py`: calls the tool's `.ainvoke(args)` directly against `FixtureLineageGateway`. No LLM involved, mirrors the Kafka and Flink unit test pattern.
- `tests/integration/test_registry_wiring.py`: includes a flagship-fixture wiring test, confirming the full three-fixture set produces the expected severities and that a diagnosis citing the Kafka signal as `root_cause_signal_id` correctly derives `system == "kafka"`.
- `evals/scenarios.py`: the `cross_system_watermark_lag_to_isr_churn` scenario, run via `./auto/eval` against a live model, the one eval scenario that actually validates cross-system localization rather than single-system diagnosis.
- `tests/unit/test_lineage_live_gateway.py`: `LiveLineageGateway` against an `httpx.MockTransport` serving a response shape captured from real Marquez: unknown-node 404 → empty view, any other 404 (wrong path/host) → raises, 500 → raises, two-hop walk → ancestors only.

## Running against live Marquez

`./auto/live-up` stands up Marquez seeded with the demo job's lineage, see [`docs/docker.md`](docker.md). In a live run with the alert `"PagerDuty: watermark lag alert on orders-processing-job"`, the agent found Flink watermark lag critical and backpressure clean, called `walk_lineage_upstream("job:flink:orders-processing-job")`, got `dataset:kafka:orders` back from Marquez, and went on to investigate that topic with the Kafka tools, concluding with `system: "kafka"`. Unlike the fixture scenario, there's no injected fault in the live stack, so what the model concludes about the cause varies; the point of the live run is that the lineage step crosses the system boundary against real infra.
