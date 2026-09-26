"""Offline guard for evals/scenarios.py: ./auto/eval is billed, so a
mistyped fixture path shouldn't first surface there."""

from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from evals.scenarios import SCENARIOS


def test_every_scenario_fixture_loads():
    for scenario in SCENARIOS:
        FixtureKafkaGateway(scenario.kafka_fixture_path)
        FixtureFlinkGateway(scenario.flink_fixture_path)
        FixtureLineageGateway(scenario.lineage_fixture_path)
        FixtureDbtGateway(scenario.dbt_fixture_path)


def test_scenario_names_are_unique():
    names = [s.name for s in SCENARIOS]
    assert len(names) == len(set(names))


def test_dbt_scenarios_are_registered():
    by_name = {s.name: s for s in SCENARIOS}

    assert by_name["cross_system_dbt_freshness_to_isr_churn"].expected_signal_type == "isr_churn"
    assert by_name["dbt_model_logic_regression"].expected_signal_type == "test_failure"
