from dp_ops_agent.orchestrator.prompts import render_system_prompt
from dp_ops_agent.tools.registry import TOOL_NAMES


def test_session_prompt_names_every_dbt_tool():
    prompt = render_system_prompt(phase=3)

    for name in (
        "test_failure",
        "model_run_failure",
        "freshness_check_failure",
        "incremental_model_drift",
        "dependency_graph_compile_error",
    ):
        assert name in TOOL_NAMES
        assert name in prompt


def test_session_prompt_states_the_dbt_upstream_rule_and_node_ids():
    prompt = render_system_prompt(phase=3)

    assert "model_code_changed" in prompt
    assert "previously_passed" in prompt
    assert "lineage_node_id" in prompt
    assert '"job:dbt:{model_name}"' in prompt
    assert '"dataset:warehouse:{schema}.{table}"' in prompt


def test_session_prompt_says_unknown_severity_is_not_health():
    prompt = render_system_prompt(phase=3)

    assert 'severity "unknown"' in prompt
    assert "no_data_reason" in prompt
    assert "not evidence" in prompt
