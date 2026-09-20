from __future__ import annotations

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import typer
from dotenv import load_dotenv

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.orchestrator.session import DiagnosisNotSubmittedError, run_diagnosis
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway

load_dotenv()

app = typer.Typer(add_completion=False)


@app.callback()
def _root() -> None:
    """Data Platform Operations Agent CLI."""
    # A no-op callback keeps Typer in subcommand-group mode even with a
    # single command registered today (Typer collapses to a flat command
    # with no subcommand name otherwise) — needed so `diagnose` stays valid
    # as later phases add more subcommands (e.g. approve, execute).


@app.command()
def diagnose(
    fixture: Path = typer.Option(..., help="Path to a Kafka fixture snapshot JSON file"),
    alert_text: str = typer.Option(..., help="The incident alert text to hand the agent"),
    log_dir: str = typer.Option(
        os.environ.get("AUDIT_LOG_DIR", "logs/audit"), help="Directory for the audit log"
    ),
    model: str = typer.Option(
        os.environ.get("CLAUDE_MODEL", "claude-sonnet-5"), help="Model id to use"
    ),
) -> None:
    """Run a Phase 1 Kafka-only diagnosis against a fixture incident."""
    session_id = str(uuid4())
    audit = JsonlAuditSink(log_dir, session_id)
    gateway = FixtureKafkaGateway(fixture)

    try:
        result = asyncio.run(
            run_diagnosis(
                session_id=session_id,
                alert_text=alert_text,
                gateway=gateway,
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
