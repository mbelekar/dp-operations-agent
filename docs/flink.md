# Flink diagnostics

**Status:** Phase 2a

The agent can inspect a Flink job, identify likely failures, and compare the result with Kafka evidence in the same diagnosis session.

Cross-system localization through lineage is planned for Phase 2b. dbt support is planned for Phase 3a.

## What the agent checks

The agent exposes five read-only Flink tools:

| Check | What it can reveal |
| --- | --- |
| Checkpoint failures | Repeated or recent failures, growing state, a slow sink, or upstream barrier backpressure |
| Backpressure ratio | The vertex causing a processing bottleneck |
| Watermark lag | Event-time skew or a stalled Kafka partition |
| State backend disk pressure | RocksDB or TaskManager disk pressure, state skew, missing TTL, or an unbounded window |
| Savepoint restore failures | State that became incompatible after changing the job graph or operator UIDs |

Kafka and Flink tools are available together, so the agent can distinguish an upstream Kafka problem from a Flink processing problem.

## How a diagnosis works

1. The agent reads the alert.
2. It checks relevant Kafka and Flink signals.
3. It compares the evidence across systems.
4. It reports the most likely root cause and cites the signals that support it.

The diagnosis system is derived from the selected `root_cause_signal_id`. The caller or model cannot set it independently. See [ADR-0007](decisions/0007-root-cause-signal-id.md).

## Missing data

Missing data does not automatically mean the system is healthy.

If a job or vertex cannot be found, the tool returns:

- severity `unknown`;
- an `observed.no_data_reason` value; and
- known job or vertex IDs, so the agent can retry once with a valid ID.

Flink vertex IDs are hexadecimal. Always pass the live vertex ID, not its display name.

If the vertex exists but the requested metric is absent, the response explains that retrying will not help.

Savepoint checks are slightly different: an existing job with an empty exception history is considered healthy.

For the full design rationale, see [ADR-0009](decisions/0009-no-data-is-unknown-not-ok.md).

## Data sources

The Flink tools use one of two gateways:

| Gateway | Use case | Behavior |
| --- | --- | --- |
| `LiveFlinkGateway` | Live infrastructure | Reads four Flink REST endpoints. It has been verified against the API documentation and a real Flink job. |
| `FixtureFlinkGateway` | Local demos, tests, and evaluations | Reads deterministic JSON fixtures. A missing vertex returns `not_found`, which becomes an `unknown` signal. |

### Savepoint restore detection

Flink's REST API has no direct “savepoint restore failed” field. The tool therefore scans `/jobs/:id/exceptions` for terms such as `savepoint`, `incompatible state`, and `state schema`.

This is a heuristic, so treat it as supporting evidence rather than a perfect classifier.

### Watermark metric names

Flink exposes input watermarks with a metric name similar to:

```text
<subtask>.<operatorName>.currentInputWatermark
```

The operator-name segment makes discovery harder when several operators are chained into one vertex. This does not affect the current demo topology, which has one relevant operator per vertex.

### Watermark lag on a quiet source

Watermark lag is the current time minus the newest event time the job has seen. When no new input arrives, the watermark stops moving and the lag keeps growing, even though the job has processed everything.

On a vertex that contains the Kafka source, `watermark_lag` also reads two of Flink's standard source metrics:

| Field | Flink metric | Meaning |
| --- | --- | --- |
| `source_pending_records` | `pendingRecords`, summed over subtasks | Records in Kafka the source hasn't fetched yet |
| `source_idle_ms` | `sourceIdleTime`, smallest over subtasks | How long Flink reports the source as idle |

If nothing is waiting and the source is idle, the job is caught up. A large lag then means no new input, which is either a quiet stream or an upstream stall. The tool caps severity at `warn` in that case. With records waiting, or on a vertex without a source, severity follows the usual thresholds.

## Try it with fixtures

Run a deterministic checkpoint-failure scenario:

```bash
dp-ops-agent diagnose \
  --fixture tests/fixtures/kafka/healthy_baseline.json \
  --flink-fixture tests/fixtures/flink/checkpoint_failure_incident.json \
  --lineage-fixture tests/fixtures/lineage/empty.json \
  --dbt-fixture tests/fixtures/dbt/healthy_baseline.json \
  --alert-text "PagerDuty: repeated checkpoint failures on orders-processing-job"
```

In this example, Kafka is healthy. The agent should therefore attribute the incident to Flink using the checkpoint evidence.

## Tests

| Coverage | Location |
| --- | --- |
| Flink tool behavior | `tests/unit/test_flink_tools.py` |
| Tool registry wiring | `tests/integration/test_registry_wiring.py` |
| Checkpoint-failure evaluation | `flink_checkpoint_failure` scenario |

Run the evaluation suite with:

```bash
./auto/eval
```
