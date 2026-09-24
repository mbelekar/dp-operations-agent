# ADR-0010: Proposals are model-chosen and code-checked

| Status | Date |
| --- | --- |
| Accepted | 2026-09-24 |

## Decision

A remediation proposal is one action from a closed, typed catalog, chosen by the model and checked by code.

- The model chooses the action and its parameters, and writes the expected outcome.
- Code derives the tier, the command preview, the rollback step, and the warnings.
- Code rejects a proposal whose action targets a different system than the root cause, or an identifier no collected signal covered.
- Phase 3b supports Tier 0 (no action) and Tier 1 (reversible, narrow) only.

## Context

The design's Phase 3 autonomy level is "diagnose + propose". Earlier decisions refuse to trust model-asserted facts that code can check or derive: evidence grounding (ADR-0002), the diagnosed system (ADR-0006), and the root-cause signal (ADR-0007). A proposal is the first model output a human may act on, so the same rule applies with more at stake.

## Rules

| Rule | Why |
| --- | --- |
| Tier is derived from the action | A model-chosen tier would be an unchecked claim about blast radius |
| Action system must match the root-cause system | Stops, for example, a Kafka replay proposed for a Flink root cause |
| Targets must appear in a collected signal's scope | "Act only on real targets", the proposal equivalent of evidence grounding |
| A Kafka replay is Tier 1 only up to 100,000 offsets on one partition of one group | Wider replays are Tier 2; they are rejected, not escalated |
| No proposal means Tier 0 | "No catalog action fixes this" is a correct answer |

A Kafka replay's partition and offsets aren't in any signal's scope, so they are bounded by the replay limit rather than grounded.

## Alternatives considered

| Alternative | Why it was rejected or deferred |
| --- | --- |
| The model writes every proposal field, including the tier | Rejected. The tier and rollback would be unchecked claims, contrary to ADR-0002 and ADR-0006. |
| A fixed table maps each root-cause signal type to a proposal | Rejected. It can't choose incident-specific parameters such as the offset range or the model to re-run. |
| A separate `propose_remediation` tool | Rejected. It needs its own "which diagnosis?" plumbing and allows a proposal for a diagnosis later rejected. |
| Include Tier 2 now | Deferred. It needs downstream lineage and a per-sink idempotency registry, neither of which exists. |

## Consequences

### Benefits

- A reviewer sees a command, rollback, and warnings that code generated and that were checked against the real CLIs.
- A proposal can't target something the agent didn't investigate.
- Tier 0 is explicit, so "nothing to do automatically" is visible rather than silent.

### Costs

- Every new action type needs a typed model, a system mapping, rendering, and tests.
- Command previews contain placeholders (bootstrap servers, checkpoint path, job jar) that a human must fill in.
- The model can still choose a valid but unhelpful action. Evaluation scenarios with `expected_action_type` are the check.
