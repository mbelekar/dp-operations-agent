# ADR-0002: Evidence grounding is enforced in code, not requested in the prompt

**Status:** Accepted
**Date:** 2026-09-20

## Context

An LLM asked to "only make claims backed by evidence" will still sometimes assert something it never actually checked. That's a prompting instruction, not a guarantee. This project's core safety requirement is that a diagnosis can be trusted to cite only real, collected data.

## Decision

The model cannot just assert a diagnosis. Every diagnostic tool returns a typed, pydantic-validated `Signal`, never free text. All signals collected during a session are tracked server-side in a shared `collected_signals` list, not restated by the model. When the model calls `submit_diagnosis`, the final `Diagnosis` is assembled from that list. The model only supplies `signal_id` references in its `evidence_chain`. A `model_validator` on `Diagnosis` (`evidence_chain_is_grounded`) runs at construction time and rejects the diagnosis, as a tool error that forces a retry, if `evidence_chain` or `signals` is empty, or if any cited `signal_id` was not actually collected that session.

## Alternatives considered

- **Prompt instruction only** ("cite only real evidence"). Rejected as the sole mechanism. It is the same LLM-nondeterminism problem the rest of the design is built to avoid. Kept as a secondary layer, the system prompt does instruct this, but never relied on alone.
- **Trusting the model to restate signal payloads it wants to cite.** Rejected. Restating invites transcription drift, the model paraphrasing a number instead of quoting it, and makes grounding unverifiable, since there would be no independent record to check the restated value against.

## Consequences

This is the one property every diagnostic module is built around. Adding Flink or dbt tools does not require touching this mechanism, they just emit `Signal`s into the same shared list. The cost is a small amount of plumbing (`collected_signals` closures, the shared audit sink) that has to be wired correctly by every tool module. The registry wiring tests were written specifically to catch a tool that doesn't wire in correctly.
