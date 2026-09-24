# ADR-0004: Require human approval before remediation

| Status | Date |
| --- | --- |
| Accepted | 2026-09-20 |

## Decision

The agent may diagnose an incident and propose a remediation, but it must not execute a state-changing action without explicit human approval.

The approval boundary must be enforced outside the LLM.

> **Current implementation:** diagnosis plus Tier 0/1 proposals for human review ([ADR-0010](0010-proposals-model-chosen-code-checked.md)). Execution and approval records have not been built yet.

## Context

Future remediation actions may include:

- restarting a job;
- replaying Kafka offsets; or
- backfilling a dbt model.

These actions change production state and may be difficult to reverse. Model confidence alone is not a sufficient permission mechanism.

## Planned enforcement

A future execution path must pass two independent checks:

1. Middleware verifies that a valid, unexpired approval record exists.
2. The execution tool repeats the check inside its handler.

The model cannot grant approval to itself.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Autonomous execution gated by model confidence | Confidence is also model output. It is not an independently verifiable safety boundary. |
| Remain diagnosis-only permanently | Rejected as the end state. Low-risk remediation may be added after diagnostic reliability is demonstrated. |

## Consequences

- Existing diagnostic tools remain read-only.
- There is currently no execution tool that can be accidentally over-trusted.
- Future remediation must be tiered by blast radius and reversibility.
- Trust must be earned from audit history rather than assumed from model confidence.
