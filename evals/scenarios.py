"""Eval scenarios: incident fixtures paired with the root-cause signal a
correct diagnosis should cite. Reuses the fixtures already built for the
unit test suites rather than authoring new incident data.

Every scenario needs a Kafka, a Flink, and a lineage fixture, since all
three tool sets are always available in a session (see the gateway-wiring
decisions in docs/decisions/) — a single-system incident still runs with a
healthy Flink fixture and an empty lineage graph so the model has to notice
there's nothing else worth investigating, not because those tools don't
exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dp_ops_agent.evidence.schema import SignalType

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
KAFKA_FIXTURE_DIR = FIXTURE_ROOT / "kafka"
FLINK_FIXTURE_DIR = FIXTURE_ROOT / "flink"
LINEAGE_FIXTURE_DIR = FIXTURE_ROOT / "lineage"

KAFKA_HEALTHY = KAFKA_FIXTURE_DIR / "healthy_baseline.json"
FLINK_HEALTHY = FLINK_FIXTURE_DIR / "healthy_baseline.json"
LINEAGE_EMPTY = LINEAGE_FIXTURE_DIR / "empty.json"


@dataclass(frozen=True)
class EvalScenario:
    name: str
    kafka_fixture_path: Path
    flink_fixture_path: Path
    lineage_fixture_path: Path
    alert_text: str
    expected_signal_type: SignalType
    description: str


SCENARIOS: list[EvalScenario] = [
    EvalScenario(
        name="urp_lag_spike",
        kafka_fixture_path=KAFKA_FIXTURE_DIR / "urp_lag_spike_incident.json",
        flink_fixture_path=FLINK_HEALTHY,
        lineage_fixture_path=LINEAGE_EMPTY,
        alert_text="PagerDuty: consumer lag alert on billing-svc/orders",
        expected_signal_type="isr_churn",
        description=(
            "Cascading failure: broker 1 ISR churn is the earliest signal, "
            "causing under-replication on partition 7 and the consumer lag "
            "that actually triggered the alert. The correct diagnosis cites "
            "isr_churn as root cause, not the downstream symptoms."
        ),
    ),
    EvalScenario(
        name="rebalance_storm",
        kafka_fixture_path=KAFKA_FIXTURE_DIR / "rebalance_storm_incident.json",
        flink_fixture_path=FLINK_HEALTHY,
        lineage_fixture_path=LINEAGE_EMPTY,
        alert_text="PagerDuty: billing-svc consumer group is unstable",
        expected_signal_type="rebalance_frequency",
        description="Single-cause incident: a consumer group rebalance storm.",
    ),
    EvalScenario(
        name="hot_partition_skew",
        kafka_fixture_path=KAFKA_FIXTURE_DIR / "hot_partition_skew_incident.json",
        flink_fixture_path=FLINK_HEALTHY,
        lineage_fixture_path=LINEAGE_EMPTY,
        alert_text="PagerDuty: uneven throughput reported on the orders topic",
        expected_signal_type="hot_partition_skew",
        description="Single-cause incident: one hot partition dominating throughput.",
    ),
    EvalScenario(
        name="schema_incompatibility",
        kafka_fixture_path=KAFKA_FIXTURE_DIR / "schema_incompatibility_incident.json",
        flink_fixture_path=FLINK_HEALTHY,
        lineage_fixture_path=LINEAGE_EMPTY,
        alert_text="PagerDuty: producer errors reported for the orders-value schema",
        expected_signal_type="schema_registry_compat",
        description="Single-cause incident: an incompatible schema change.",
    ),
    EvalScenario(
        name="flink_checkpoint_failure",
        kafka_fixture_path=KAFKA_HEALTHY,
        flink_fixture_path=FLINK_FIXTURE_DIR / "checkpoint_failure_incident.json",
        lineage_fixture_path=LINEAGE_EMPTY,
        alert_text="PagerDuty: repeated checkpoint failures on orders-processing-job",
        expected_signal_type="checkpoint_failure",
        description=(
            "Single-cause incident, first Flink scenario: repeated checkpoint "
            "failures. Kafka is healthy, so the model has to notice that and "
            "still correctly attribute the root cause to Flink."
        ),
    ),
    EvalScenario(
        name="cross_system_watermark_lag_to_isr_churn",
        kafka_fixture_path=KAFKA_FIXTURE_DIR / "isr_churn_upstream_incident.json",
        flink_fixture_path=FLINK_FIXTURE_DIR / "watermark_lag_cross_system_incident.json",
        lineage_fixture_path=LINEAGE_FIXTURE_DIR / "flink_job_to_kafka_topic.json",
        alert_text="PagerDuty: watermark lag alert on orders-processing-job",
        expected_signal_type="isr_churn",
        description=(
            "The flagship cross-system scenario: alert fires on Flink watermark "
            "lag, but every Flink-internal signal is healthy. The correct "
            "diagnosis calls walk_lineage_upstream, finds the job's upstream "
            "Kafka topic, and cites that topic's isr_churn signal as root "
            "cause, despite the alert being entirely Flink-worded. This is the "
            "one scenario that actually validates cross-system localization, "
            "not just whether the lineage tool returns a graph."
        ),
    ),
]
