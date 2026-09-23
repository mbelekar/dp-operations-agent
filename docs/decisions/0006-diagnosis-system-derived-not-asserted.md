# ADR-0006: `Diagnosis.system` is derived from cited evidence, not asserted

**Status:** Accepted
**Date:** 2026-09-21

## Context

Adding the Flink module meant Kafka and Flink tools now coexist in every session (see the gateway-wiring decision in the Phase 2a plan: both gateways are required, not optional, so diagnostic tools stay "always available" regardless of which system an alert names). Before that change, `Diagnosis.system` was a caller-supplied constant, fixed to `"kafka"` at tool-build time. That was correct when only Kafka tools existed. It was never revisited when Flink tools were added, and nothing would have caught it. It is a silent mislabel, not a crash: every Flink diagnosis would have been recorded as `system: "kafka"` with no error anywhere.

## Decision

`Diagnosis.system` is derived from the cited evidence at construction time. The handler looks up the `Signal` referenced by the first grounded `evidence_chain` entry and reads its `tool` field's namespace prefix (`"flink.checkpoint_failure"` becomes `"flink"`). It is not supplied by the caller and not self-reported by the model.

## Alternatives considered

- **Have the model state `system` explicitly as a `submit_diagnosis` argument.** Rejected. This is the same category of problem evidence-grounding already solves: a model-asserted field with nothing verifying it against the model's own cited evidence is a claim, not a guarantee, and it can drift from what the evidence chain actually supports.
- **Leave it as a caller-supplied constant, set per module combination.** Not viable once two systems' tools coexist in one session. There is no single caller-known value that is correct for every possible diagnosis a session could produce.

## Consequences

This bug was found and fixed during Phase 2a's implementation, not caught by a test written in advance, because no test exercised a session with more than one system's tools available until Flink was added. It is evidence for a pattern worth repeating: whenever a new capability makes a previously-safe assumption stop holding, here, "there is only one possible system" stopped being true, that assumption needs to be re-derived from real data, not left as a stale default. The registry wiring tests now include a Flink-specific case asserting `Diagnosis.system` derives correctly, specifically so this does not regress silently again.
