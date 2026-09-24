# ADR-0007: Require `root_cause_signal_id`

| Status | Date |
| --- | --- |
| Accepted | 2026-09-23 |

## Decision

Require `submit_diagnosis` to identify one cited signal as `root_cause_signal_id`.

Derive `Diagnosis.system` from that signal's tool namespace.

## Context

Lineage allows one diagnosis to cite signals from several systems. For example:

- Flink watermark lag explains why the alert fired.
- Kafka ISR churn identifies the upstream root cause.

ADR-0006 initially derived the system from the first evidence-chain entry. That worked for single-system diagnoses but became order-dependent once evidence crossed system boundaries.

## Validation rules

The grounding validator rejects a diagnosis when `root_cause_signal_id`:

- was not collected during the session; or
- does not also appear in the evidence chain.

This prevents a diagnosis from naming a root cause without citing it as evidence.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Derive the system from the last evidence entry | This replaces one ordering assumption with another. The prompt cannot guarantee evidence order. |
| Restore a caller-supplied system | Already rejected in ADR-0006 because several systems coexist in one session. |

## Consequences

### Benefits

- Root-cause selection is explicit and independently checkable.
- System attribution no longer depends on evidence ordering.
- Cross-system diagnoses can include symptom and cause signals without ambiguity.

### Cost

All tests and fixtures that construct a `Diagnosis` or call `submit_diagnosis` required the new field. This was a mechanical migration accepted in exchange for removing a silent classification bug.
