# ADR-0009: Report missing data as `unknown`

| Status | Date |
| --- | --- |
| Accepted | 2026-09-24 |

## Decision

When a diagnostic tool finds no data for the requested identifier, return `severity: unknown` instead of `ok`.

An `unknown` signal may describe an evidence gap, but it cannot be selected as `root_cause_signal_id`.

## Context

Several tools originally applied thresholds to empty results. Empty collections therefore produced healthy-looking defaults such as zero lag or `backpressure_level: ok`.

One tool had the inverse problem: a missing committed offset was interpreted as offset zero, creating false critical lag.

The problem surfaced during a live-model evaluation:

1. The model queried a nonexistent Flink vertex named `sink`.
2. Three tools returned `ok` because their result sets were empty.
3. The diagnosis described Flink as healthy.
4. A real `source` vertex had critical watermark lag.

Grounding could not catch this. The signals had genuinely been collected, but the tools had assigned meaning to data they never observed.

## Behaviour

| Situation | Result |
| --- | --- |
| Identifier exists and observations are healthy | `ok` |
| Identifier exists and observations cross a threshold | `warn` or `critical` |
| Identifier is unknown or its metric is unavailable | `unknown` with `observed.no_data_reason` |
| Lineage node exists but has no upstream node | `ok`, with an empty upstream list |
| Lineage node does not exist | `unknown`, using `node_found: false` |

The lineage gateway needs `node_found` because an empty upstream list is valid for a source node.

## Root-cause validation

The grounding validator rejects an `unknown` signal as `root_cause_signal_id` because it contains no positive observation.

The signal may still appear elsewhere in the evidence chain, for example:

> No data existed for vertex `sink`, so the investigation retried with known vertex `source`.

## Identifier hints and retry policy

Making gaps visible exposed a second issue: the model guessed identifiers and retried repeatedly.

Unknown-identifier responses now include up to 20 valid identifiers where possible:

- `known_topics`
- `known_groups`
- `known_brokers`
- `known_subjects`
- `known_jobs`
- `known_vertices`
- `known_models`
- `known_sources`

The system prompt permits one retry after `unknown`, using only:

- an identifier from a `known_*` list;
- an identifier in the alert; or
- an identifier returned by another tool.

If an identifier exists but a metric is unavailable, the result says not to retry it.

Broker IDs are a special case: `isr_churn` may use the numeric broker IDs returned in `under_replicated_partitions` replica and ISR lists.

## Alternatives considered

| Alternative | Why it was rejected or deferred |
| --- | --- |
| Keep `ok` and add `data_available: false` | Rejected. Consumers naturally trust the severity field, so `ok` would remain misleading. |
| Raise a tool error | Rejected. "No data for this identifier" is different from a backend failure, and an error would leave no citable signal describing the gap. |
| Change every gateway method to return `None` | Deferred. It would require broad protocol and fixture changes without being necessary for this correction. |
| Add separate discovery tools | Rejected for now. They add a round trip, still require the model to call them first, and duplicate hints that can be returned only when needed. |

## Consequences

### Improvements

- Missing observations no longer look healthy.
- Live data-source gaps are visible rather than silently passing.
- The model receives valid identifiers without unrestricted guessing.
- The evaluation suite still passed 8 of 8 scenarios immediately after the change.

### Visible live-mode gaps

The following tools now honestly report `unknown` when their live backend lacks the required data:

- Kafka `hot_partition_skew`
- Kafka `schema_registry_compat`
- Flink `state_backend_disk_pressure` for the demo job

### Remaining limitations

- A real outage that prevents metrics from arriving also appears as `unknown`; `no_data_reason` states what could not be observed.
- Marquez does not return a convenient node inventory, so lineage gets the retry cap but no `known_nodes` list.
- The dbt flagship evaluation sometimes stops at critical Flink watermark lag instead of continuing to Kafka ISR churn. It passed 4 of 6 recorded runs after the retry-scope fix. See [dbt known gaps](../dbt.md#known-gaps).
- `savepoint_restore_failure` remains intentionally different: an existing job with an empty exception history is healthy.
- Live behaviour for `rebalance_frequency` on a nonexistent consumer group remains unverified.
