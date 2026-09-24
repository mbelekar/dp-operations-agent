"""The eval runner's selection, repeat, and reporting logic, with
run_scenario stubbed out so nothing here calls a model."""

import pytest

from dp_ops_agent.orchestrator.session import TokenUsage
from evals import runner
from evals.grading import GradeResult
from evals.scenarios import SCENARIOS


def test_no_names_selects_every_scenario():
    assert runner.select_scenarios(None) == SCENARIOS


def test_names_select_a_subset_in_the_order_given():
    names = ["dbt_model_logic_regression", "urp_lag_spike"]

    assert [s.name for s in runner.select_scenarios(names)] == names


def test_unknown_name_lists_the_valid_ones():
    with pytest.raises(ValueError, match="urp_lag_spike") as exc:
        runner.select_scenarios(["no_such_scenario"])
    assert "no_such_scenario" in str(exc.value)


@pytest.mark.parametrize("repeat", [0, 3])
def test_repeat_outside_one_to_two_is_rejected(repeat):
    with pytest.raises(SystemExit):
        runner.parse_args(["--repeat", str(repeat)])


def _result(scenario, passed: bool, usage: TokenUsage | None) -> runner.EvalRunResult:
    return runner.EvalRunResult(
        scenario=scenario,
        grade=GradeResult(passed=passed, reason="ok" if passed else "expected isr_churn"),
        duration_seconds=10.0,
        session_id="s",
        usage=usage,
    )


@pytest.mark.asyncio
async def test_run_all_runs_each_scenario_repeat_times(monkeypatch):
    calls = []

    async def fake_run_scenario(scenario, model):
        calls.append(scenario.name)
        return _result(scenario, True, TokenUsage())

    monkeypatch.setattr(runner, "run_scenario", fake_run_scenario)
    scenarios = runner.select_scenarios(["urp_lag_spike", "rebalance_storm"])

    results = await runner.run_all(scenarios, "m", repeat=2)

    assert calls == ["urp_lag_spike", "urp_lag_spike", "rebalance_storm", "rebalance_storm"]
    assert len(results) == 4


def test_report_shows_k_of_n_tokens_and_totals(capsys):
    scenario = runner.select_scenarios(["cross_system_dbt_freshness_to_isr_churn"])[0]
    usage = TokenUsage(input_tokens=1000, output_tokens=100, cache_read_tokens=800, model_calls=5)
    results = [_result(scenario, True, usage), _result(scenario, False, None)]

    all_passed = runner.print_report(results, repeat=2)

    out = capsys.readouterr().out
    assert all_passed is False
    assert "cross_system_dbt_freshness_to_isr_churn" in out
    assert "1/2" in out
    assert "expected isr_churn" in out
    # Tokens: the one run with usage recorded; the other is counted as unrecorded.
    assert "in=1,000" in out and "cached=800" in out and "out=100" in out
    assert "1 session(s) without recorded usage" in out
    # 2/2 must not be read as "reliable".
    assert "2/2" in out and "not proof" in out
