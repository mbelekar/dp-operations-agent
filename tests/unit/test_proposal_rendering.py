"""The command previews and rollbacks a reviewer sees. Pinned exactly: a
plausible but wrong command is worse than none. Kafka syntax was run for real
against cp-kafka 7.6.1; Flink and dbt checked against the stack's images'
--help (see docs/plans/phase3b-proposals.md, "Task 3 findings")."""

from dp_ops_agent.evidence.schema import (
    ReplayKafkaOffsets,
    RerunDbtModel,
    RestartFlinkJobFromCheckpoint,
)
from dp_ops_agent.tools.diagnosis_output.proposals import render


def test_flink_restart():
    rendered = render(
        RestartFlinkJobFromCheckpoint(action_type="restart_flink_job_from_checkpoint", job_id="a1b2c3")
    )

    assert rendered.command == (
        "flink cancel a1b2c3\n"
        "flink run -s <latest-completed-checkpoint-path> <job-jar>"
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
            action_type="replay_kafka_offsets", group="billing-svc", topic="orders",
            partition=1, from_offset=100, to_offset=500,
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
