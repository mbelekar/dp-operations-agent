# ADR-0005: Start with deterministic evaluation grading

| Status | Date |
| --- | --- |
| Accepted | 2026-09-21 |

## Decision

Grade each evaluation by checking whether the final evidence chain cites the expected root-cause signal type.

Do not use an LLM judge in the first evaluation version.

## Context

The test suite verifies that tools calculate signals correctly and that grounding rejects unsupported evidence. It does not verify that a live model chooses the correct diagnosis.

Each labeled incident therefore defines an `expected_signal_type`. An evaluation passes only when that signal type appears in the submitted `evidence_chain`.

Merely collecting the right signal is not enough. The model must use it in the diagnosis.

## Alternatives considered

| Alternative | Decision |
| --- | --- |
| LLM-as-judge over the hypothesis text | Deferred. It adds cost, another prompt, and nondeterminism to the grader. It may later assess qualities that structured fields cannot capture. |
| Keyword matching over `root_cause_hypothesis` | Rejected. It is fragile to wording and does not prove that the claim was grounded in cited evidence. |

## Consequences

### Benefits

- Grading is deterministic.
- It reuses the typed evidence contract.
- Incident fixtures support both code tests and agent evaluations.

### Limitations

The initial grade answers one narrow question: **did the diagnosis cite the expected root-cause signal type?**

It does not separately score:

- root system accuracy;
- causal-chain quality;
- hypothesis usefulness; or
- repeated-run reliability.

Those dimensions can be added when the evaluation suite needs to provide stronger evidence than a starting pass/fail signal.
