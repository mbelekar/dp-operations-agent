"""What a reviewer sees for each catalog action: the literal command to run,
how to undo it, and what to check first. Derived in code from the action's
parameters, never written by the model (ADR-0010). Nothing here runs a
command; they're previews for a human.

Every command was checked against the real CLIs before being written here:
Kafka's run end to end against cp-kafka 7.6.1 (backup, reset, rollback);
Flink's and dbt's against the --help of the stack's own flink:1.18.1 image and
dbt-core 1.12.5. Values the tool can't know (bootstrap servers, the
checkpoint path, the job jar) stay as <placeholders> rather than guesses.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dp_ops_agent.evidence.schema import (
    ProposedAction,
    ReplayKafkaOffsets,
    RerunDbtModel,
    RestartFlinkJobFromCheckpoint,
)


@dataclass(frozen=True)
class RenderedAction:
    command: str
    rollback_step: str
    warnings: list[str] = field(default_factory=list)


def _flink_restart(action: RestartFlinkJobFromCheckpoint) -> RenderedAction:
    return RenderedAction(
        command=(
            f"flink cancel {action.job_id}\n"
            "flink run -s <latest-completed-checkpoint-path> <job-jar>"
        ),
        rollback_step=(
            "The checkpoint is not modified by restoring from it. If the restarted job "
            "misbehaves, cancel it and restore from the same checkpoint again."
        ),
        warnings=[
            "Replace <latest-completed-checkpoint-path> with the job's last COMPLETED "
            "checkpoint and <job-jar> with the deployed job jar before running.",
            "The job reprocesses input from that checkpoint onwards; its sinks must "
            "tolerate replayed records.",
        ],
    )


def _dbt_rerun(action: RerunDbtModel) -> RenderedAction:
    return RenderedAction(
        command=f"dbt run --select {action.model}",
        rollback_step=(
            f"A re-run has no generic undo: it rebuilds {action.model} from its current "
            "inputs. Restore the previous table from warehouse time travel or a snapshot "
            "if available, or re-run again once the inputs are corrected."
        ),
        warnings=[
            f"If {action.model} is incremental, `dbt run` only processes new rows and will "
            "not fill an existing gap; that needs --full-refresh or a targeted backfill, "
            "which is Tier 2 and not supported.",
            f"Consumers of {action.model} may read partial results until the run completes.",
        ],
    )


def _kafka_replay(action: ReplayKafkaOffsets) -> RenderedAction:
    base = f"kafka-consumer-groups --bootstrap-server <bootstrap-servers> --group {action.group}"
    scope = f"--topic {action.topic}:{action.partition}"
    return RenderedAction(
        command=(
            f"{base} {scope} --reset-offsets --to-current --dry-run --export > offsets-backup.csv\n"
            f"{base} {scope} --reset-offsets --to-offset {action.from_offset} --execute"
        ),
        rollback_step=f"{base} --reset-offsets --from-file offsets-backup.csv --execute",
        warnings=[
            f"Stop every consumer in group {action.group} first: offsets can only be "
            "reset while the group is inactive.",
            f"The group re-reads offsets {action.from_offset}-{action.to_offset} of "
            f"{action.topic}:{action.partition}; its sinks must tolerate replayed records.",
        ],
    )


def render(action: ProposedAction) -> RenderedAction:
    if isinstance(action, RestartFlinkJobFromCheckpoint):
        return _flink_restart(action)
    if isinstance(action, RerunDbtModel):
        return _dbt_rerun(action)
    return _kafka_replay(action)
