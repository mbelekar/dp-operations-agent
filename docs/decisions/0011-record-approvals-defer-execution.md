# ADR-0011: Record approvals, defer execution

| Status | Date |
| --- | --- |
| Accepted | 2026-09-25 |

## Decision

Phase 4 records human decisions on proposals and stops there. Nothing in the system executes a proposal.

- `dp-ops-agent approve` and `dp-ops-agent reject` record a reviewer's decision on a Tier 1 proposal.
- Humans run the reviewed command themselves, using the command preview, rollback, and warnings from the proposal (ADR-0010).
- An executor is deferred, not rejected.

## Context

The project's focus is finding where an incident started and proposing a grounded fix. A proposal already carries a CLI-checked command, rollback step, and warnings, so an executor would add little beyond automated pre-checks, and would:

- cover one action in three (only the Kafka replay can be verified end to end here);
- be the only code that changes a real system; and
- rest on self-declared reviewers and approvals stored in plain files.

Approval records are worth having on their own. Design.md treats every approve, reject, or expire decision as the audit trail and as the evidence Phase 5 needs before trusting any autonomy.

## How approvals work

| Rule | Detail |
| --- | --- |
| Store | The session audit log: `approval_decision` events next to the proposal's `proposal_created` event |
| Binding | Each decision records a SHA-256 digest of the approved action |
| Expiry | 1 hour by default (`--expires-in`) |
| One decision | A decided proposal can't be decided again, except re-approval after an approval expired |
| Tier 0 | Can't be approved: there is no action |
| Reviewer | `--reviewer` is recorded as given, not authenticated |

`approvals/store.py` also provides the usability check (approved, not rejected, not expired, not used, digest still matches) that a future executor would run.

## Alternatives considered

| Alternative | Why it was rejected or deferred |
| --- | --- |
| A human-run `execute` command with a Kafka replay executor | Deferred. It was built and tested (two approval checks, inactive-group check, offset backup, read-back, dry run) and set aside. |
| An execution tool the agent calls, gated by middleware (ADR-0004's original picture) | Rejected. A diagnosis session ends before a human approves, and keeping the model out of execution removes the prompt-to-state-change path entirely. |
| Defer all of Phase 4, including approval records | Rejected. Without recorded decisions, Phase 5 has no evidence to assess. |

## Consequences

- The agent still never changes a system; ADR-0004's boundary holds trivially.
- The audit log now records the full path from alert to human decision.
- Approvals stored in plain files can be forged by anyone who can write to `logs/audit/`. The digest detects an edited action, not a forged decision. This is acceptable while nothing executes; an executor would need signed approvals or an access-controlled store first.
- If execution is picked up later, it should be a human-run command, not an agent tool, with the checks listed above.
