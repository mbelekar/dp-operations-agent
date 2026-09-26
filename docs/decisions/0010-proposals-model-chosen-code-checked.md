# ADR-0010: Let the model choose proposals, then validate them in code

| Status | Date |
| --- | --- |
| Accepted | 2026-09-24 |

## Decision

A remediation proposal must use one action from a closed, typed catalog.

The model chooses the action, parameters, and expected outcome. Code derives and validates everything that can be checked deterministically.

| Model chooses | Code derives or validates |
| --- | --- |
| Action and parameters | Remediation tier |
| Expected outcome | Command preview |
| Whether to propose an action | Rollback instructions and warnings |
|  | Target system and investigated identifiers |

Phase 3b supports:

- **Tier 0:** no action proposed;
- **Tier 1:** narrow, reversible action; and
- no Tier 2 actions.

## Context

The project already validates model claims about evidence, diagnosed system, and root-cause signal. A proposal may lead to a production change, so it needs the same separation between model judgment and code-enforced guarantees.

## Validation rules

| Rule | Purpose |
| --- | --- |
| Derive the tier from the action | Prevents the model from understating blast radius |
| Match the action system to the root-cause system | Prevents, for example, a Kafka replay for a Flink root cause |
| Require targets to appear in collected signal scope | The agent can act only on identifiers it investigated |
| Limit a Kafka replay to 100,000 offsets on one partition and group | Wider replays are Tier 2 and are rejected |
| Treat no proposal as Tier 0 | “No safe catalog action applies” remains a valid conclusion |

Kafka replay partition and offset values do not appear in signal scope. Their risk is controlled by the Tier 1 replay limit instead.

## Alternatives considered

| Alternative | Why it was rejected or deferred |
| --- | --- |
| Let the model write the tier, command, rollback, and warnings | These would be unchecked claims about risk and execution. |
| Map every root-cause signal to a fixed action | A static mapping cannot select incident-specific parameters. |
| Add a separate proposal tool | It would need extra diagnosis-selection plumbing and could propose against a diagnosis that is later rejected. |
| Support Tier 2 actions | Deferred until downstream lineage and sink-idempotency information exist. |

## Consequences

### Benefits

- Proposals are bounded by a small, typed action catalog.
- Commands, rollback instructions, and warnings come from reviewed code.
- A proposal cannot target an uninvestigated identifier.
- Tier 0 makes the absence of a safe action explicit.

### Limitations

- Every new action requires a model, renderer, system mapping, and tests.
- Some command previews contain placeholders that a human must complete.
- A proposal may be valid but still unhelpful. Evaluation scenarios check expected action types.

Related decisions: [ADR-0002](0002-deterministic-evidence-grounding.md), [ADR-0006](0006-diagnosis-system-derived-not-asserted.md), and [ADR-0007](0007-root-cause-signal-id.md).
