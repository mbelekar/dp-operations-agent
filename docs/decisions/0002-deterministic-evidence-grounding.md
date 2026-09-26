# ADR-0002: Enforce evidence grounding in code

| Status | Date |
| --- | --- |
| Accepted | 2026-09-20 |

## Decision

Require every diagnosis to cite typed signals collected through tools during the current session. Validate this requirement in code rather than relying on the prompt.

## Context

An instruction such as "only make claims supported by evidence" does not guarantee that an LLM will comply. The project needs a deterministic boundary between model reasoning and accepted output.

## How it works

1. Each diagnostic tool returns a Pydantic-validated `Signal`.
2. The application stores every signal in a server-side `collected_signals` list.
3. The model supplies only `signal_id` references in its `evidence_chain`.
4. The application builds the final `Diagnosis` from the stored signals.
5. `Diagnosis.evidence_chain_is_grounded` rejects the diagnosis if:
   - no signals were collected;
   - the evidence chain is empty; or
   - any cited ID was not collected in that session.

A rejected `submit_diagnosis` call becomes a tool error, allowing the model to retry with valid evidence.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Prompt instruction only | Prompt compliance is probabilistic and cannot provide a guarantee. The instruction remains as a secondary layer. |
| Let the model restate signal values | Restating values allows transcription errors and paraphrasing, and makes independent verification harder. |

## Consequences

### Benefits

- Unsupported evidence IDs cannot enter an accepted diagnosis.
- New modules reuse the same grounding mechanism by emitting `Signal` objects.
- The audit trail retains the original evidence independently of the model's wording.

### Cost

Every tool module must use the shared signal registry and audit sink correctly. Registry-wiring tests protect this integration point.
