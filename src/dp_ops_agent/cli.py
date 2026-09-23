from __future__ import annotations

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import typer
from dotenv import load_dotenv

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.orchestrator.session import DiagnosisNotSubmittedError, run_diagnosis
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.flink.live_gateway import LiveFlinkGateway
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.kafka.live_gateway import LiveKafkaGateway
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from dp_ops_agent.tools.lineage.live_gateway import LiveLineageGateway

load_dotenv()

app = typer.Typer(add_completion=False)


@app.callback()
def _root() -> None:
    """Data Platform Operations Agent CLI."""
    # A no-op callback keeps Typer in subcommand-group mode even with a
    # single command registered today (Typer collapses to a flat command
    # with no subcommand name otherwise) — needed so `diagnose` stays valid
    # as later phases add more subcommands (e.g. approve, execute).


def _augment_alert_text(
    alert_text: str,
    kafka_topics: str | None,
    consumer_group: str | None,
    flink_job_id: str | None,
    flink_vertex_id: str | None,
    flink_job_name: str | None = None,
) -> str:
    """Live mode has no fixture pointing the model at the right topic/job.
    Tool schemas require concrete ids (job_id, vertex_id, ...) the model has
    no way to guess from prose alone, so any identifiers the caller already
    knows are appended as an explicit block rather than left to the model to
    invent. The job name matters separately from job_id: lineage node ids
    are keyed by name (job:flink:{job_name}), not by Flink's random job_id."""
    known = []
    if kafka_topics:
        known.append(f"Kafka topics: {kafka_topics}")
    if consumer_group:
        known.append(f"Kafka consumer group: {consumer_group}")
    if flink_job_name:
        known.append(f"Flink job name: {flink_job_name}")
    if flink_job_id:
        known.append(f"Flink job_id: {flink_job_id}")
    if flink_vertex_id:
        known.append(f"Flink vertex_id: {flink_vertex_id}")
    if not known:
        return alert_text
    return alert_text + "\n\nKnown identifiers for this incident:\n" + "\n".join(
        f"- {k}" for k in known
    )


@app.command()
def diagnose(
    fixture: Path | None = typer.Option(
        None, help="Path to a Kafka fixture snapshot JSON file"
    ),
    flink_fixture: Path | None = typer.Option(
        None, help="Path to a Flink fixture snapshot JSON file"
    ),
    lineage_fixture: Path | None = typer.Option(
        None, help="Path to a lineage fixture snapshot JSON file"
    ),
    live: bool = typer.Option(
        False,
        "--live",
        help="Use live Kafka/Flink infra (see ./auto/live-up) instead of fixtures",
    ),
    alert_text: str = typer.Option(..., help="The incident alert text to hand the agent"),
    log_dir: str = typer.Option(
        os.environ.get("AUDIT_LOG_DIR", "logs/audit"), help="Directory for the audit log"
    ),
    model: str = typer.Option(
        os.environ.get("CLAUDE_MODEL", "claude-sonnet-5"), help="Model id to use"
    ),
    kafka_bootstrap_servers: str = typer.Option(
        os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"
        ),
        help="Live mode only: Kafka bootstrap servers",
    ),
    kafka_jmx_url: str = typer.Option(
        os.environ.get("KAFKA_JMX_URL", "http://localhost:5559"),
        help="Live mode only: JMX-exporter (aggregated) base URL",
    ),
    schema_registry_url: str = typer.Option(
        os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        help="Live mode only: Schema Registry base URL",
    ),
    flink_rest_url: str = typer.Option(
        os.environ.get("FLINK_REST_URL", "http://localhost:8082"),
        help="Live mode only: Flink JobManager REST base URL",
    ),
    marquez_url: str = typer.Option(
        os.environ.get("MARQUEZ_URL", "http://localhost:5000"),
        help="Live mode only: Marquez base URL",
    ),
    kafka_topics: str | None = typer.Option(
        None, help="Live mode only: comma-separated topics relevant to this incident"
    ),
    consumer_group: str | None = typer.Option(
        None, help="Live mode only: Kafka consumer group relevant to this incident"
    ),
    flink_job_id: str | None = typer.Option(
        None, help="Live mode only: Flink job_id relevant to this incident"
    ),
    flink_job_name: str | None = typer.Option(
        None, help="Live mode only: Flink job name, used to build lineage node ids"
    ),
    flink_vertex_id: str | None = typer.Option(
        None, help="Live mode only: Flink vertex_id relevant to this incident"
    ),
) -> None:
    """Run a Kafka + Flink + lineage diagnosis, against fixtures by default
    or live infra with --live."""
    session_id = str(uuid4())
    audit = JsonlAuditSink(log_dir, session_id)

    if live:
        kafka_gateway = LiveKafkaGateway(
            bootstrap_servers=kafka_bootstrap_servers,
            jmx_exporter_base_url=kafka_jmx_url,
            schema_registry_url=schema_registry_url,
        )
        flink_gateway = LiveFlinkGateway(flink_rest_url)
        lineage_gateway = LiveLineageGateway(marquez_url)
        alert_text = _augment_alert_text(
            alert_text,
            kafka_topics,
            consumer_group,
            flink_job_id,
            flink_vertex_id,
            flink_job_name,
        )
    else:
        if fixture is None or flink_fixture is None or lineage_fixture is None:
            typer.echo(
                "--fixture, --flink-fixture, and --lineage-fixture are required "
                "unless --live is set",
                err=True,
            )
            raise typer.Exit(code=1)
        kafka_gateway = FixtureKafkaGateway(fixture)
        flink_gateway = FixtureFlinkGateway(flink_fixture)
        lineage_gateway = FixtureLineageGateway(lineage_fixture)

    try:
        result = asyncio.run(
            run_diagnosis(
                session_id=session_id,
                alert_text=alert_text,
                kafka_gateway=kafka_gateway,
                flink_gateway=flink_gateway,
                lineage_gateway=lineage_gateway,
                audit=audit,
                model=model,
            )
        )
    except DiagnosisNotSubmittedError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    diagnosis = result.diagnosis
    typer.echo(diagnosis.model_dump_json(indent=2))
    typer.echo("\nEvidence chain:")
    for entry in diagnosis.evidence_chain:
        typer.echo(f"  {entry.step}. [{entry.signal_id}] {entry.interpretation}")
    typer.echo(f"\nAudit log: {result.audit_log_path}")


if __name__ == "__main__":
    app()
