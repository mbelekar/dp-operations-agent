# Remediation proposals

> **Status:** Implemented in Phase 3b · Tier 0 and Tier 1 only · Nothing is executed

After a diagnosis, the agent may propose one remediation for a human to review. It never runs anything. Execution and approval records are Phase 4 ([ADR-0004](decisions/0004-human-approved-remediation.md)).

Related: [Kafka](kafka.md) · [Flink](flink.md) · [dbt](dbt.md) · [ADR-0010](decisions/0010-proposals-model-chosen-code-checked.md)

## Who decides what

| Part of a proposal | Decided by |
| --- | --- |
| Which catalog action, and its parameters | The model |
| Expected outcome | The model (descriptive text) |
| Tier | Code: derived from the action |
| Command preview, rollback step, warnings | Code: rendered from the action's parameters |
| Whether the proposal is accepted | Code: validated with the diagnosis |

The proposal is an optional `proposal` argument to `submit_diagnosis`. It is validated together with the diagnosis, so a proposal can't exist without the grounded diagnosis it rests on. A rejected proposal rejects the whole submission, and the model retries.

## Tiers

| Tier | Meaning | When |
| --- | --- | --- |
| 0 | Root cause identified, no action proposed | No catalog action fixes the root cause, or the model omits a proposal |
| 1 | Reversible, narrow action | One of the catalog actions below |
| 2 | Broad or hard to undo | Not supported. Too-broad actions are rejected, not escalated |

Tier 0 is a correct answer, not a failure. For example, ISR churn on a broker is a broker-health problem that no catalog action fixes.

## Action catalog

| `action_type` | Parameters | Allowed for a root cause on |
| --- | --- | --- |
| `restart_flink_job_from_checkpoint` | `job_id` | Flink |
| `rerun_dbt_model` | `model` | dbt |
| `replay_kafka_offsets` | `group`, `topic`, `partition`, `from_offset`, `to_offset` | Kafka |

A Kafka replay is Tier 1 only for one group, one partition, and at most 100,000 offsets (`MAX_TIER1_REPLAY_OFFSETS`). A wider replay is rejected as Tier 2.

## Validation rules

A proposal is rejected when:

- its action changes a different system than the root-cause signal's system;
- it targets a job, model, consumer group, or topic that no signal collected this session covered; or
- its tier doesn't match the tier derived from the action.

The second rule extends evidence grounding from "cite only real evidence" to "act only on real targets". A Kafka replay's partition and offsets aren't in any signal's scope, so they are bounded by the Tier 1 limit rather than grounded.

## What a reviewer sees

`dp-ops-agent diagnose` prints the proposal after the evidence chain. Values the tool can't know stay as `<placeholders>`.

| Action | Command preview | Rollback |
| --- | --- | --- |
| Flink restart | `flink cancel <job_id>`, then `flink run -s <latest-completed-checkpoint-path> <job-jar>` | The checkpoint is not modified by the restore; cancel and restore from it again |
| dbt re-run | `dbt run --select <model>` | No generic undo; restore from warehouse time travel or a snapshot if available |
| Kafka replay | Back up offsets (`--reset-offsets --to-current --dry-run --export > offsets-backup.csv`), then `--reset-offsets --to-offset <from_offset> --execute` | `--reset-offsets --from-file offsets-backup.csv --execute` |

Each proposal also carries warnings. For example: stop the consumer group before a Kafka reset, and re-running an incremental dbt model won't fill a gap.

The commands were checked against the real tools: the Kafka commands were run end to end against a `cp-kafka:7.6.1` broker, and the Flink and dbt commands against the `--help` output of `flink:1.18.1` and dbt-core 1.12.5.

## Audit trail

An accepted proposal writes a `proposal_created` event with its `proposal_id` and tier, alongside `diagnosis_completed`. `Diagnosis.tier` and `Diagnosis.proposal` carry the same data in the diagnosis itself.

## Evaluation

Scenarios can set `expected_action_type`, graded alongside the root-cause signal:

| Scenario | Expected proposal | Why |
| --- | --- | --- |
| `dbt_transient_model_run_failure` | `rerun_dbt_model` | A database deadlock with unchanged code and healthy inputs: retrying is the fix |
| `dbt_model_logic_regression` | None (Tier 0) | A code regression; re-running the same model reproduces it |
| `flink_checkpoint_failure` | None (Tier 0) | The job is running and its latest checkpoint completed; a restart fixes nothing |
| `urp_lag_spike` and both ISR-churn cross-system scenarios | None (Tier 0) | Broker health needs a human |

The catalog actions are recovery actions: they help once a cause is gone or was transient. Most scenarios' root causes need a human fix, so they expect Tier 0.

Only `rerun_dbt_model` has a live-model scenario. `restart_flink_job_from_checkpoint` and `replay_kafka_offsets` are tested offline only, through validation, rendering, and wiring tests, until scenarios exist where they are the right fix (a failed Flink job, a consumer that skipped data).

## Testing

| Test area | File | What it verifies |
| --- | --- | --- |
| Validation rules | `tests/unit/test_evidence_schema.py` | Grounded targets, system match, replay limit, derived tier |
| Rendered commands | `tests/unit/test_proposal_rendering.py` | Exact command, rollback, and warning text |
| Tool wiring | `tests/integration/test_registry_wiring.py` | Accepted and rejected proposals, Tier 0, audit event, tool schema |
| CLI output | `tests/unit/test_cli_live_alert.py` | The printed proposal block |
| Grading | `tests/unit/test_eval_grading.py` | Expected action and Tier 0 expectations |

## Reviewing a proposal

A reviewer records a decision on a Tier 1 proposal. Both commands print the full proposal first.

```bash
dp-ops-agent approve <proposal_id> --reviewer <name> [--expires-in 1h] [--reason "..."]
dp-ops-agent reject <proposal_id> --reviewer <name> --reason "..."
```

The decision is appended to the session audit log as an `approval_decision` event, with a digest of the approved action and, for an approval, an expiry. A proposal can be decided once (re-approval is allowed after an approval expires). Tier 0 proposals can't be approved. `--reviewer` is recorded as given, not authenticated.

Nothing executes an approved proposal: the reviewer runs the previewed command. See [ADR-0011](decisions/0011-record-approvals-defer-execution.md).

## Not built yet

- Executing approved proposals (deferred, ADR-0011)
- Tier 2 actions, which need downstream lineage and a per-sink idempotency registry
- Volume and time-to-replay estimates
- An approval UI beyond the CLI
