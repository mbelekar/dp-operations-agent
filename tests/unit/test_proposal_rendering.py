"""The command previews and rollbacks a reviewer sees. Pinned exactly: a
plausible but wrong command is worse than none. Kafka syntax was run for real
against cp-kafka 7.6.1; Flink and dbt checked against the stack's images'
--help (see docs/plans/phase3b-proposals.md, "Task 3 findings")."""

import shlex

import pytest

from dp_ops_agent.evidence.schema import (
    ReplayKafkaOffsets,
    RerunDbtModel,
    RestartFlinkJobFromCheckpoint,
)
from dp_ops_agent.tools.diagnosis_output.proposals import render


def test_flink_restart():
    rendered = render(
        RestartFlinkJobFromCheckpoint(
            action_type="restart_flink_job_from_checkpoint", job_id="a1b2c3"
        )
    )

    assert rendered.command == (
        "flink cancel a1b2c3\nflink run -s <latest-completed-checkpoint-path> <job-jar>"
    )
    assert "checkpoint is not modified" in rendered.rollback_step
    assert any("<latest-completed-checkpoint-path>" in w for w in rendered.warnings)
    assert any("replayed" in w for w in rendered.warnings)


def test_dbt_rerun():
    rendered = render(RerunDbtModel(action_type="rerun_dbt_model", model="fct_orders"))

    assert rendered.command == "dbt run --select fct_orders"
    # No fake undo: --state selects models, it restores nothing.
    assert "--state" not in rendered.rollback_step
    assert "no generic undo" in rendered.rollback_step
    assert any("incremental" in w and "--full-refresh" in w for w in rendered.warnings)


def test_kafka_replay_backs_up_offsets_first_and_rolls_back_from_the_file():
    rendered = render(
        ReplayKafkaOffsets(
            action_type="replay_kafka_offsets",
            group="billing-svc",
            topic="orders",
            partition=1,
            from_offset=100,
            to_offset=500,
        )
    )

    assert rendered.command == (
        "kafka-consumer-groups --bootstrap-server <bootstrap-servers> --group billing-svc "
        "--topic orders:1 --reset-offsets --to-current --dry-run --export > offsets-backup.csv\n"
        "kafka-consumer-groups --bootstrap-server <bootstrap-servers> --group billing-svc "
        "--topic orders:1 --reset-offsets --to-offset 100 --execute"
    )
    assert rendered.rollback_step == (
        "kafka-consumer-groups --bootstrap-server <bootstrap-servers> --group billing-svc "
        "--reset-offsets --from-file offsets-backup.csv --execute"
    )
    assert any("inactive" in w for w in rendered.warnings)


_HOSTILE = "x $(touch pwned); 'q' #"


@pytest.mark.parametrize(
    "action",
    [
        RestartFlinkJobFromCheckpoint(
            action_type="restart_flink_job_from_checkpoint", job_id=_HOSTILE
        ),
        RerunDbtModel(action_type="rerun_dbt_model", model=_HOSTILE),
        ReplayKafkaOffsets(
            action_type="replay_kafka_offsets",
            group=_HOSTILE,
            topic="orders",
            partition=0,
            from_offset=0,
            to_offset=10,
        ),
    ],
    ids=["flink-restart", "dbt-rerun", "kafka-replay"],
)
def test_identifiers_are_shell_quoted_as_one_argument(action):
    """An identifier can hold anything its backend allows (a Kafka group id
    can contain any character), so it must reach the shell as one argument."""
    rendered = render(action)

    lines = rendered.command.splitlines()
    if isinstance(action, ReplayKafkaOffsets):  # the only rollback that's a command
        lines.append(rendered.rollback_step)
    with_identifier = [line for line in lines if "touch pwned" in line]
    assert with_identifier
    for line in with_identifier:
        assert _HOSTILE in shlex.split(line)


def test_kafka_topic_partition_is_quoted_together():
    rendered = render(
        ReplayKafkaOffsets(
            action_type="replay_kafka_offsets",
            group="g",
            topic="a topic",
            partition=3,
            from_offset=0,
            to_offset=10,
        )
    )

    assert all("a topic:3" in shlex.split(line) for line in rendered.command.splitlines())
