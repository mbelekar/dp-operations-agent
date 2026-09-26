from datetime import UTC, datetime

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.audit.models import AuditEvent


def _event(**overrides) -> AuditEvent:
    defaults = dict(
        event_type="diagnosis_run_started",
        timestamp=datetime.now(UTC),
        session_id="s1",
        actor="orchestrator",
        payload={},
    )
    defaults.update(overrides)
    return AuditEvent(**defaults)


def test_append_writes_one_json_line_per_event(tmp_path):
    sink = JsonlAuditSink(tmp_path, "s1")
    sink.append(_event(event_type="diagnosis_run_started"))
    sink.append(_event(event_type="signal_collected"))

    lines = sink.path.read_text().strip().splitlines()
    assert len(lines) == 2


def test_append_does_not_truncate_existing_entries(tmp_path):
    sink = JsonlAuditSink(tmp_path, "s1")
    sink.append(_event())
    # A second sink instance pointed at the same session must append, not overwrite.
    sink2 = JsonlAuditSink(tmp_path, "s1")
    sink2.append(_event())
    assert len(sink.path.read_text().strip().splitlines()) == 2


def test_query_filters_by_event_type_and_session(tmp_path):
    sink = JsonlAuditSink(tmp_path, "s1")
    sink.append(_event(event_type="diagnosis_run_started", session_id="s1"))
    sink.append(_event(event_type="signal_collected", session_id="s1"))
    sink.append(_event(event_type="signal_collected", session_id="s1"))

    results = sink.query(event_type="signal_collected")
    assert len(results) == 2
    assert all(e.event_type == "signal_collected" for e in results)


def test_query_on_missing_file_returns_empty_list(tmp_path):
    sink = JsonlAuditSink(tmp_path, "does-not-exist")
    assert sink.query() == []


def test_tool_error_event_round_trips(tmp_path):
    sink = JsonlAuditSink(tmp_path, "s1")
    sink.append(
        _event(
            event_type="tool_error",
            actor="tool",
            payload={"tool": "hot_partition_skew", "error_type": "RemoteProtocolError"},
        )
    )

    [event] = sink.query(event_type="tool_error")
    assert event.payload["tool"] == "hot_partition_skew"
