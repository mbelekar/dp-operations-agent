# ADR-0007: `submit_diagnosis` requires an explicit `root_cause_signal_id`

**Status:** Accepted
**Date:** 2026-09-23

## Context

Adding the lineage tool (Phase 2b) means a single diagnosis can now legitimately cite signals from more than one system: the flagship scenario cites both a Flink watermark-lag signal (why the alert fired) and a Kafka ISR-churn signal (the actual root cause). Before lineage, `Diagnosis.system` was derived by taking whichever `evidence_chain` entry came first in the model's list (see ADR-0006), and that was safe only because every pre-lineage session's cited evidence was single-system, "first entry" and "the system the root cause is on" were the same thing by construction. Once evidence can span systems, they aren't. A model that investigates the alert's own system first and the upstream root cause second would have `system` silently resolve to the wrong (alert-origin) system under the old logic.

## Decision

`submit_diagnosis` gains a required field, `root_cause_signal_id`: the model must state explicitly which cited signal its hypothesis actually rests on. `Diagnosis.system` is derived from that signal's `tool` prefix, not from evidence_chain position. The grounding validator (`evidence_chain_is_grounded`) is extended to reject a `root_cause_signal_id` that isn't a real collected signal, and separately to reject one that isn't actually cited in `evidence_chain`, naming something as the root cause without citing it as evidence is itself a contradiction, and the validator catches it the same way it already catches an ungrounded `signal_id`.

## Alternatives considered

- **Derive `system` from the last `evidence_chain` entry instead of the first.** Rejected. No schema change, but it just relocates the same problem: it assumes the model always lists evidence causally, ending on the root cause, which is a convention the prompt can ask for but not enforce. `root_cause_signal_id` makes the claim explicit and checkable instead of inferred from list order.
- **Leave `system` as a caller-supplied constant.** Already rejected once, in ADR-0006, for the same reason: not viable once more than one system's tools coexist in a session.

## Consequences

This is the same pattern as ADR-0006 and as evidence grounding generally: don't trust a claim that can be derived from explicit, verifiable data instead. Every existing test and fixture that constructs a `Diagnosis` or calls `submit_diagnosis` needed updating for the new required field, a real but mechanical cost, worth paying once, up front, rather than discovering the positional-inference bug later the way ADR-0006's bug was discovered after Flink shipped.
