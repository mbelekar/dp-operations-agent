# ADR-0006: Derive `Diagnosis.system` from evidence

| Status | Date |
| --- | --- |
| Accepted | 2026-09-21 |

## Decision

Derive `Diagnosis.system` from the cited root-cause evidence instead of accepting a caller or model assertion.

> ADR-0007 later refined the derivation to use an explicit `root_cause_signal_id` rather than the first evidence-chain entry.

## Context

When Kafka was the only module, the caller always set `Diagnosis.system` to `kafka`. After Flink tools were added to every session, that default became unsafe.

A Flink diagnosis would still succeed but be silently recorded as Kafka. This would be a data-quality failure rather than a crash.

## Initial implementation

The diagnosis handler looked up the first cited `Signal` and derived the system from its tool namespace:

```text
flink.checkpoint_failure → flink
kafka.isr_churn          → kafka
```

The field was no longer provided by the caller or model.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Ask the model to provide `system` | The field could disagree with the model's own evidence. |
| Keep a caller-supplied constant | No single constant is correct when several systems' tools are available in one session. |

## Consequences

- System attribution became verifiable against collected evidence.
- A Flink-specific registry test now guards against silent regression.
- The change exposed a broader rule: when new capabilities invalidate an old assumption, re-derive the value from data instead of preserving the stale default.
- Cross-system lineage later invalidated the "first evidence entry" assumption, leading to [ADR-0007](0007-root-cause-signal-id.md).
