# ADR-0005: Deterministic eval grading, not LLM-as-judge, for the first version

**Status:** Accepted
**Date:** 2026-09-21

## Context

Unit and integration tests check code correctness, does a tool compute severity correctly for known fixture data, and grounding enforcement, does the validator reject ungrounded evidence. Neither checks whether the agent's actual diagnosis is right. Closing that gap needed a way to grade a live model's output against a known-correct answer, automatically.

## Decision

Each eval scenario declares an `expected_signal_type`, the signal type that represents the true root cause. A run passes if that signal type appears among the signals actually cited in the final `evidence_chain`, not merely collected during the session. This reuses the same typed evidence contracts the grounding validator already enforces, instead of judging the model's prose.

## Alternatives considered

- **LLM-as-judge grading** (a second model call scoring the hypothesis text). Deferred, not rejected outright. It adds another model call, another prompt to maintain, and non-determinism inside the grader itself, on top of the non-determinism already being measured in the thing under test. Worth adding later for dimensions that cannot be reduced to a structured field, is this hypothesis actually a reasonable explanation a human would trust. Not needed to get a working, trustworthy eval loop first.
- **Keyword matching on `root_cause_hypothesis` free text.** Rejected. Fragile to wording changes, would need constant retuning as the system prompt evolves, and produces false confidence: a keyword match does not confirm the model actually grounded that claim in real evidence, only that it used the right word.

## Consequences

Every eval scenario doubles as a fixture used elsewhere in the test suite. One incident definition serves both code-level and agent-level testing, instead of two parallel, drifting sets of incident data. The known limitation, stated plainly: this checks "cited the right root-cause signal," a single pass or fail per run, not a richer breakdown (root system correct vs. causal chain correct vs. unsupported claims). It is also single-shot per scenario, not repeated and averaged to smooth over the model's own sampling variance. Both are reasonable next steps if the eval suite needs to carry more weight than a starting signal.
