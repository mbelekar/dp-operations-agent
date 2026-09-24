from pathlib import Path

from typer.testing import CliRunner

from dp_ops_agent.cli import _augment_alert_text, app


def test_job_name_listed_as_known_identifier():
    text = _augment_alert_text(
        "watermark lag alert",
        kafka_topics="orders",
        consumer_group=None,
        flink_job_id="a1b2c3",
        flink_vertex_id=None,
        flink_job_name="orders-processing-job",
    )

    assert "- Flink job name: orders-processing-job" in text
    assert "- Flink job_id: a1b2c3" in text


def test_output_unchanged_without_job_name():
    without = _augment_alert_text("alert", "orders", "group", "a1b2c3", "v1")
    explicit_none = _augment_alert_text("alert", "orders", "group", "a1b2c3", "v1", None)

    assert "job name" not in without
    assert without == explicit_none == (
        "alert\n\nKnown identifiers for this incident:\n"
        "- Kafka topics: orders\n"
        "- Kafka consumer group: group\n"
        "- Flink job_id: a1b2c3\n"
        "- Flink vertex_id: v1"
    )


def test_no_identifiers_returns_alert_unchanged():
    assert _augment_alert_text("alert", None, None, None, None, None) == "alert"


def test_fixture_mode_requires_a_dbt_fixture(tmp_path):
    fixtures = Path(__file__).resolve().parents[1] / "fixtures"
    result = CliRunner().invoke(
        app,
        [
            "diagnose",
            "--fixture", str(fixtures / "kafka" / "healthy_baseline.json"),
            "--flink-fixture", str(fixtures / "flink" / "healthy_baseline.json"),
            "--lineage-fixture", str(fixtures / "lineage" / "empty.json"),
            "--alert-text", "alert",
            "--log-dir", str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "--dbt-fixture" in result.output
