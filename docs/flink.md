# Flink diagnostic module

**Status: Implemented (Phase 2a).** Cross-system localization is not yet available, that needs the lineage tool, which is Phase 2b and hasn't landed. Until then, Flink and Kafka tools run in the same session but each investigation stays single-system: if the alert names one system, the model is instructed to investigate that system's own tools, since there's no way yet to trace a root cause from one system to another. See [`kafka.md`](kafka.md) for the module this one mirrors, and [`dbt.md`](dbt.md) for the still-planned Phase 3 module.

## What it does

Given an incident alert naming a Flink job (e.g. "repeated checkpoint failures on orders-processing-job"), the agent investigates using five read-only Flink diagnostic tools, on top of the six Kafka tools from Phase 1, all in the same session. The grounding mechanism is identical to Kafka's: every claim in the final diagnosis has to trace back to a real tool call. See [`kafka.md`'s grounding section](kafka.md#grounding-how-the-evidence-chain-is-enforced) rather than repeating it here, the mechanism is shared code (`evidence/schema.py`, `tools/registry.py`), not reimplemented per module.

## Signals collected

| Tool | File | What it detects |
| --- | --- | --- |
| `checkpoint_failure` | `tools/flink/tools.py` | Growing state size, slow sink, or backpressure upstream of the barrier (repeated or most-recent checkpoint failures) |
| `backpressure_ratio` | `tools/flink/tools.py` | Localizes the actual bottleneck operator, not just "the job is slow" (per-vertex backpressure level) |
| `watermark_lag` | `tools/flink/tools.py` | Event-time skew, often caused by a stalled upstream Kafka partition (per-subtask watermark lag) |
| `state_backend_disk_pressure` | `tools/flink/tools.py` | State growth from a skewed key, missing TTL, or an unbounded window (RocksDB/TaskManager disk usage ratio) |
| `savepoint_restore_failure` | `tools/flink/tools.py` | Incompatible state schema after a job graph or operator UID change |

**One signal is weaker than the other four, worth knowing before trusting it.** Flink's REST API has no dedicated field for "savepoint restore failure", there's no clean status check the way `checkpoint_failure` has `counts.failed`. It's inferred from the job's exception history (`GET /jobs/:id/exceptions`), matching exception text against a small keyword list (`savepoint`, `incompatible state`, `state schema`). This is a heuristic, not a structured signal, and it's documented as such in the tool's own docstring, not just here.

## The gateway abstraction: one seam, two implementations

Same pattern as Kafka: every Flink tool talks through the `FlinkMetricsGateway` Protocol (`tools/flink/gateway.py`), never calling the Flink REST API directly.

- **`LiveFlinkGateway`** (`tools/flink/live_gateway.py`): the real implementation, `httpx` against the Flink JobManager REST API (`/jobs/:id/checkpoints`, `/jobs/:id/vertices/:id/backpressure`, `/jobs/:id/vertices/:id/metrics`, `/jobs/:id/exceptions`), all four endpoints verified against Flink's real REST API docs before writing this module, not guessed. Two things inside it are still best-effort rather than verified: the exact per-subtask field names in a backpressure response, and the watermark metric's naming convention (assumed `<subtask>.currentInputWatermark`, Flink's usual pattern, not guaranteed across versions). Both are flagged in the file's own docstring.
- **`FixtureFlinkGateway`** (`tools/flink/fixture_gateway.py`): loads a JSON snapshot, same contract as `FixtureKafkaGateway`, deterministic responses, safe empty defaults for anything not in the snapshot.

## Why Kafka and Flink tools are both always available

A session's tool list always includes both Kafka and Flink tools, regardless of which system the alert names. This matters for one reason: `Diagnosis.system` isn't set by the caller or asserted by the model, it's derived from which system's signal actually got cited in the final `evidence_chain` (see `_derive_system` in `tools/diagnosis_output/tools.py`). That only works correctly if both systems' tools are genuinely available in every session, a Kafka-only tool list would make `system` trivially always `"kafka"`, which defeats the point once a real cross-system session (Phase 2b) needs to derive it correctly instead of trusting a caller-supplied constant.

## Example run

```
dp-ops-agent diagnose \
  --fixture tests/fixtures/kafka/healthy_baseline.json \
  --flink-fixture tests/fixtures/flink/checkpoint_failure_incident.json \
  --alert-text "PagerDuty: repeated checkpoint failures on orders-processing-job"
```

Kafka is healthy in this fixture; the model has to notice that and still correctly attribute the root cause to Flink, not because Kafka tools are unavailable, but because their own signals come back clean.

## Testing without live Flink

- `tests/unit/test_flink_tools.py`: calls each tool's `.ainvoke(args)` directly against `FixtureFlinkGateway`, no LLM involved, mirroring `test_kafka_tools.py`.
- `tests/integration/test_registry_wiring.py`: includes a Flink-specific wiring test alongside the Kafka ones, confirming a Flink signal is citable in `submit_diagnosis` and that `Diagnosis.system` derives to `"flink"` when a Flink signal is what's actually cited.
- `evals/scenarios.py`: one Flink scenario (`flink_checkpoint_failure`) alongside the four Kafka scenarios, run via `./auto/eval` against a live model.
