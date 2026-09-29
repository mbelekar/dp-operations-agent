# ADR-0011: Record approvals, but defer execution

| Status | Date |
| --- | --- |
| Accepted | 2026-09-25 |

## Decision

Record human approval and rejection decisions, but do not execute proposals automatically.

- `dp-ops-agent approve` records approval of a Tier 1 proposal.
- `dp-ops-agent reject` records rejection.
- A human reviews and runs the proposed command separately.
- Automated execution is deferred, not permanently rejected.

## Context

The project currently focuses on grounded diagnosis and safe proposals. Adding an executor would introduce the first component capable of changing a real system.

That would be premature because:

- only the Kafka replay can currently be verified end to end;
- reviewers are self-declared rather than authenticated; and
- approvals are stored in local audit files.

Approval records are still valuable. They complete the audit trail and provide evidence for deciding whether greater autonomy is justified later.

## Approval rules

| Rule | Behaviour |
| --- | --- |
| Storage | `approval_decision` events sit beside the proposal in the session audit log |
| Binding | Approval includes a SHA-256 digest of the exact action |
| Expiry | Approval lasts one hour by default and is configurable with `--expires-in` |
| Re-decision | A decided proposal cannot be decided again, except after approval expires |
| Tier 0 | Cannot be approved because it contains no action |
| Reviewer identity | Recorded as provided; not authenticated |

`approvals/store.py` also checks whether a proposal is approved, unexpired, unused, and unchanged. A future executor could reuse this logic.

## Alternatives considered

| Alternative | Decision |
| --- | --- |
| Add a human-run Kafka replay executor | Deferred. It was built and tested, but only covered one of the three actions. |
| Let the agent call an execution tool after approval | Rejected. Diagnosis ends before approval, and keeping execution outside the agent removes the prompt-to-state-change path. |
| Defer approval records as well | Rejected. Without recorded decisions, there is no evidence for evaluating future autonomy. |

## Consequences

### Benefits

- The agent remains read-only.
- The audit log covers the path from alert to human decision.
- Future execution can reuse the action digest, expiry, and single-use checks.

### Limitations

- Anyone able to edit `logs/audit/` can forge an approval.
- The action digest detects a changed action, not a forged reviewer decision.
If execution is added later, it should be a human-run command with independent approval and safety checks, not an agent tool.

## Preconditions for execution

Any executor must first have:

1. **Authenticated reviewers:** a reviewer's identity comes from something other than a CLI flag they type.
2. **An approval store outside the operator's reach:** the executor can trust approval records that the person running the agent or the executor cannot write. For example, an approval service, or records signed with a key held outside that person's access.

Two protections for the local audit log were considered on 2026-09-29, after the security audit, and neither meets requirement 2:

| Option | Why it falls short |
| --- | --- |
| Hash-chain the audit events | Anyone who can edit the file can recompute the chain. It helps only if the head hash is stored somewhere they can't write, which is requirement 2 again. |
| HMAC-sign approval events with a key from the environment | The operator who runs the agent holds the key, so they can sign forged records. |

Neither is built now: there is no executor for them to protect.

Related decisions: [ADR-0004](0004-human-approved-remediation.md) and [ADR-0010](0010-proposals-model-chosen-code-checked.md).
