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


def test_session_prompt_caps_retries_after_unknown_and_forbids_invented_names():
    prompt = render_system_prompt(phase=3)

    assert "at most once" in prompt
    assert "known_" in prompt
    assert "Never make up" in prompt


def test_retry_cap_is_scoped_to_retries_and_says_where_broker_ids_come_from():
    # Regression: an unscoped "never make up a broker" stopped the model from
    # ever calling isr_churn, since no tool result labels anything a broker.
    prompt = render_system_prompt(phase=3)

    assert "retrying after an unknown result" in prompt
    assert "replicas" in prompt and "broker ids" in prompt


def test_isr_churn_description_says_where_broker_ids_come_from(tmp_path):
    from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
    from dp_ops_agent.tools.kafka.tools import build_kafka_tools

    tools = {t.name: t for t in build_kafka_tools(None, JsonlAuditSink(tmp_path, "s"), "s", [])}

    assert "replicas" in tools["isr_churn"].description
