"""Eval scenarios: incident fixtures paired with the root-cause signal a
correct diagnosis should cite. Reuses the fixtures already built for
tests/unit/test_kafka_tools.py rather than authoring new incident data.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dp_ops_agent.evidence.schema import SignalType

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "kafka"


@dataclass(frozen=True)
class EvalScenario:
    name: str
    fixture_path: Path
    alert_text: str
    expected_signal_type: SignalType
    description: str


SCENARIOS: list[EvalScenario] = [
    EvalScenario(
        name="urp_lag_spike",
        fixture_path=FIXTURE_DIR / "urp_lag_spike_incident.json",
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
        fixture_path=FIXTURE_DIR / "rebalance_storm_incident.json",
        alert_text="PagerDuty: billing-svc consumer group is unstable",
        expected_signal_type="rebalance_frequency",
        description="Single-cause incident: a consumer group rebalance storm.",
    ),
    EvalScenario(
        name="hot_partition_skew",
        fixture_path=FIXTURE_DIR / "hot_partition_skew_incident.json",
        alert_text="PagerDuty: uneven throughput reported on the orders topic",
        expected_signal_type="hot_partition_skew",
        description="Single-cause incident: one hot partition dominating throughput.",
    ),
    EvalScenario(
        name="schema_incompatibility",
        fixture_path=FIXTURE_DIR / "schema_incompatibility_incident.json",
        alert_text="PagerDuty: producer errors reported for the orders-value schema",
        expected_signal_type="schema_registry_compat",
        description="Single-cause incident: an incompatible schema change.",
    ),
]
