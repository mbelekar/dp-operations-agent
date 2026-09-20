# Flink diagnostic module

> **Status: Planned (Phase 2), not yet implemented.**
> This document describes the intended design so it can be built without
> rearchitecting the orchestrator or tool registry. See [`kafka.md`](kafka.md)
> for the pattern this will follow, since it's already built and working.

## Why Phase 2, and why paired with lineage

Phase 1 proved the tool-calling loop and evidence format on a single system (Kafka). Phase 2's job is to prove the design doc's actual differentiator: cross-system, lineage-aware localization, which requires at least a second system to localize across. Flink is the natural second system. The design doc's cascading-failure example (Kafka → Flink → dbt → data-quality test) starts with a Kafka broker issue that manifests as a Flink watermark stall, which is exactly the kind of symptom-vs-cause distinction a single-system agent can't make.

## Intended signals

| Tool | Source | What it detects |
| --- | --- | --- |
| `checkpoint_failure` | Flink REST API `/jobs/:id/checkpoints` | Growing state size, slow sink, or backpressure upstream of the barrier |
| `backpressure_ratio` | Flink REST API `/jobs/:id/vertices/:id/backpressure` | Localizes the actual bottleneck operator, not just "the job is slow" |
| `watermark_lag` | Flink metrics (`currentInputWatermark`) | Event-time skew, often caused by a stalled upstream Kafka partition. This is the signal that would catch the design doc's cascading example |
| `state_backend_disk_pressure` | RocksDB metrics / TaskManager disk metrics | State growth from a skewed key, missing TTL, or unbounded window |
| `savepoint_restore_failure` | Job manager logs | Incompatible state schema after a job graph or operator UID change |

Backpressure localization matters most: rather than restarting the whole job (a common but often ineffective first response), the agent should walk the operator chain from sink to source using the per-vertex backpressure ratio to find which operator is actually saturated.

## How it plugs into the existing architecture

Follows the exact pattern `kafka.md` documents:

- **`tools/flink/gateway.py`**: a `FlinkMetricsGateway` Protocol, mirroring `KafkaMetricsGateway`.
- **`tools/flink/live_gateway.py`**: real implementation over the Flink REST API.
- **`tools/flink/fixture_gateway.py`**: canned JSON snapshots for tests, mirroring `FixtureKafkaGateway`.
- **`tools/flink/tools.py`**: `build_flink_tools(gateway, audit, session_id, collected_signals)`, returning `BaseTool`s that emit `Signal`s into the same shared `collected_signals` list Kafka tools already write to.
- **`tools/registry.py`**: `build_tools()` concatenates `build_flink_tools(...)` onto the existing list. `orchestrator/session.py` doesn't change at all.
- **`evidence/schema.py`**: `Diagnosis.system` widens from `Literal["kafka"]` to `Literal["kafka", "flink"]`. `Signal.signal_type` gains the five Flink signal types above, no other schema change, since `Signal.tool` is already a namespaced string (`"kafka.under_replicated_partitions"`, `"flink.watermark_lag"`, ...), not a hardcoded per-system enum.

## What's actually new in this phase: lineage

Adding a second system's tools is mechanical (see above). The real new work is the **lineage tool**: an OpenLineage/Marquez query client that, given a failing node (a Kafka topic, a Flink job), walks the lineage graph to find upstream nodes that changed or degraded within the incident window. This is what lets the orchestrator's system prompt include a "localize via lineage" instruction. Investigate the alerting system first, then follow lineage upstream before committing to a root-cause hypothesis, rather than bolting together two independent single-system investigations.
