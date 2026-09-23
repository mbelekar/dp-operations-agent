# ADR-0004: Diagnose and propose only, no autonomous execution in v1

**Status:** Accepted
**Date:** 2026-09-20

## Context

The agent's diagnostic tools are read-only, but the design was always intended to eventually support remediation: restarting a job, replaying a Kafka offset range, backfilling a dbt model. Those are state-changing, sometimes hard-to-reverse actions. The question was where to draw the autonomy line for the first version.

## Decision

v1's autonomy level is diagnose and propose. The agent always stops at a proposal. A human approves or rejects before anything touches production state. This is a scope boundary set in the original design, before any implementation, and it shapes the architecture even in phases where no execution tool exists yet. The permission boundary is planned to sit outside the LLM, in the tool layer itself: a future execution tool checks for a valid, unexpired approval record before running, rather than trusting the model's judgment that approval was given.

## Alternatives considered

- **Autonomous execution from the start, gated only by a confidence score.** Rejected. A confidence score is still a model output. Trusting it to gate a state-changing action repeats the same problem evidence-grounding was built to avoid: an LLM's self-reported certainty is not a verifiable guarantee.
- **No remediation capability at all, diagnosis-only, permanently.** Rejected as the end state, though it is the current state. The design explicitly scopes remediation as a later phase, tiered by blast radius and reversibility, once diagnostic accuracy is established. Not something to build before the diagnosis itself is trustworthy.

## Consequences

Every phase built so far, Kafka and Flink, is diagnose-only by construction. There is no execution tool to accidentally over-trust. When an execution tool is eventually built, this decision requires two independent gates: a middleware hook checking the approval store before a tool call executes, and a redundant in-handler check inside the tool itself, so a prompt-level mistake alone cannot skip the gate. Neither exists yet. This ADR is why that is still true.
