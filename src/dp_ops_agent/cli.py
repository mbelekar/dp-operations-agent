from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Coroutine
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import typer
from dotenv import load_dotenv

from dp_ops_agent.approvals.store import ProposalRecord, find_proposal, record_decision
from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.evidence.schema import Proposal
from dp_ops_agent.orchestrator.session import (
    DiagnosisNotSubmittedError,
    DiagnosisRunResult,
    run_diagnosis,
)
from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.dbt.gateway import DbtArtifactsGateway
from dp_ops_agent.tools.dbt.live_gateway import LiveDbtGateway
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
    return (
        alert_text
        + "\n\nKnown identifiers for this incident:\n"
        + "\n".join(f"- {k}" for k in known)
    )


@app.command()
def diagnose(
    fixture: Path | None = typer.Option(None, help="Path to a Kafka fixture snapshot JSON file"),
    flink_fixture: Path | None = typer.Option(
        None, help="Path to a Flink fixture snapshot JSON file"
    ),
    lineage_fixture: Path | None = typer.Option(
        None, help="Path to a lineage fixture snapshot JSON file"
    ),
    dbt_fixture: Path | None = typer.Option(None, help="Path to a dbt fixture snapshot JSON file"),
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
    dbt_target_dir: Path = typer.Option(
        Path(os.environ.get("DBT_TARGET_DIR", "target")),  # noqa: B008 (env default, like the options above)
        help="Live mode only: dbt target/ directory with the latest run's artifacts",
    ),
    dbt_state_dir: Path | None = typer.Option(
        os.environ.get("DBT_STATE_DIR"),  # noqa: B008 (env default, like the options above)
        help="Live mode only: directory with the previous run's dbt artifacts (dbt --state)",
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
    """Run a Kafka + Flink + lineage + dbt diagnosis, against fixtures by
    default or live infra with --live."""
    session_id = str(uuid4())
    audit = JsonlAuditSink(log_dir, session_id)

    run: Coroutine[Any, Any, DiagnosisRunResult]
    if live:
        run = _run_live(
            session_id=session_id,
            alert_text=_augment_alert_text(
                alert_text,
                kafka_topics,
                consumer_group,
                flink_job_id,
                flink_vertex_id,
                flink_job_name,
            ),
            audit=audit,
            model=model,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_jmx_url=kafka_jmx_url,
            schema_registry_url=schema_registry_url,
            flink_rest_url=flink_rest_url,
            marquez_url=marquez_url,
            dbt_target_dir=dbt_target_dir,
            dbt_state_dir=dbt_state_dir,
        )
    else:
        if (
            fixture is None
            or flink_fixture is None
            or lineage_fixture is None
            or dbt_fixture is None
        ):
            typer.echo(
                "--fixture, --flink-fixture, --lineage-fixture, and --dbt-fixture "
                "are required unless --live is set",
                err=True,
            )
            raise typer.Exit(code=1)
        run = run_diagnosis(
            session_id=session_id,
            alert_text=alert_text,
            kafka_gateway=FixtureKafkaGateway(fixture),
            flink_gateway=FixtureFlinkGateway(flink_fixture),
            lineage_gateway=FixtureLineageGateway(lineage_fixture),
            dbt_gateway=FixtureDbtGateway(dbt_fixture),
            audit=audit,
            model=model,
        )

    try:
        result = asyncio.run(run)
    except DiagnosisNotSubmittedError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    diagnosis = result.diagnosis
    typer.echo(diagnosis.model_dump_json(indent=2))
    typer.echo("\nEvidence chain:")
    for entry in diagnosis.evidence_chain:
        typer.echo(f"  {entry.step}. [{entry.signal_id}] {entry.interpretation}")
    _echo_proposal(diagnosis.proposal)
    typer.echo(f"\nAudit log: {result.audit_log_path}")


async def _run_live(
    *,
    session_id: str,
    alert_text: str,
    audit: JsonlAuditSink,
    model: str,
    kafka_bootstrap_servers: str,
    kafka_jmx_url: str,
    schema_registry_url: str,
    flink_rest_url: str,
    marquez_url: str,
    dbt_target_dir: Path,
    dbt_state_dir: Path | None,
) -> DiagnosisRunResult:
    """Builds the live gateways inside the running event loop and closes
    their HTTP clients when the diagnosis ends, however it ends: an
    httpx.AsyncClient must be closed in the loop that used it."""
    async with AsyncExitStack() as stack:
        kafka = LiveKafkaGateway(
            bootstrap_servers=kafka_bootstrap_servers,
            jmx_exporter_base_url=kafka_jmx_url,
            schema_registry_url=schema_registry_url,
        )
        stack.push_async_callback(kafka.aclose)
        flink = LiveFlinkGateway(flink_rest_url)
        stack.push_async_callback(flink.aclose)
        lineage = LiveLineageGateway(marquez_url)
        stack.push_async_callback(lineage.aclose)
        dbt_gateway: DbtArtifactsGateway = LiveDbtGateway(dbt_target_dir, dbt_state_dir)
        return await run_diagnosis(
            session_id=session_id,
            alert_text=alert_text,
            kafka_gateway=kafka,
            flink_gateway=flink,
            lineage_gateway=lineage,
            dbt_gateway=dbt_gateway,
            audit=audit,
            model=model,
        )


def _echo_proposal(proposal: Proposal | None) -> None:
    """The part a human reviews before anything is run by hand. The command,
    rollback, and warnings are derived in code, not written by the model."""
    if proposal is None:
        typer.echo("\nProposal: none (Tier 0, root cause identified, no action proposed)")
        return
    typer.echo(f"\nProposal (Tier {proposal.tier}, for human review; nothing was executed):")
    action_type = proposal.action.action_type if proposal.action is not None else "none"
    typer.echo(f"  Action: {action_type}")
    typer.echo(f"  Expected outcome: {proposal.expected_outcome}")
    typer.echo("  Command:")
    for line in (proposal.command or "").splitlines():
        typer.echo(f"    {line}")
    typer.echo(f"  Rollback: {proposal.rollback_step}")
    typer.echo("  Warnings:")
    for warning in proposal.warnings:
        typer.echo(f"    - {warning}")


_LOG_DIR_OPTION = typer.Option(
    os.environ.get("AUDIT_LOG_DIR", "logs/audit"), help="Directory of session audit logs"
)


def _parse_duration(text: str) -> timedelta:
    match = re.fullmatch(r"(\d+)([mh])", text.strip())
    if not match:
        raise typer.BadParameter("use minutes or hours, e.g. 30m or 2h")
    amount, unit = int(match.group(1)), match.group(2)
    return timedelta(minutes=amount) if unit == "m" else timedelta(hours=amount)


def _load_proposal(proposal_id: str, log_dir: str) -> ProposalRecord:
    record = find_proposal(log_dir, proposal_id)
    if record is None:
        typer.echo(f"FAILED: no proposal {proposal_id} in {log_dir}", err=True)
        raise typer.Exit(code=1)
    return record


def _decide(
    record: ProposalRecord,
    decision: Literal["approved", "rejected"],
    reviewer: str,
    reason: str | None,
    expires_in: timedelta = timedelta(hours=1),
):
    # --reviewer is recorded as given: there is no authentication (ADR-0011).
    _echo_proposal(record.proposal)
    try:
        return record_decision(
            record,
            decision,
            reviewer=reviewer,
            now=datetime.now(UTC),
            reason=reason,
            expires_in=expires_in,
        )
    except ValueError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def approve(
    proposal_id: str = typer.Argument(..., help="The proposal to approve"),
    reviewer: str = typer.Option(..., help="Who is approving (recorded as given, not verified)"),
    expires_in: str = typer.Option("1h", help="How long the approval stays usable, e.g. 30m or 2h"),
    reason: str | None = typer.Option(None, help="Optional reasoning, kept in the audit log"),
    log_dir: str = _LOG_DIR_OPTION,
) -> None:
    """Approve a Tier 1 proposal so `execute` may run it. Nothing runs yet."""
    duration = _parse_duration(expires_in)
    record = _load_proposal(proposal_id, log_dir)
    approval = _decide(record, "approved", reviewer, reason, duration)
    typer.echo(
        f"\nApproved by {approval.reviewer}; usable until {approval.expires_at.isoformat()} "
        f"(approval {approval.approval_id})."
    )


@app.command()
def reject(
    proposal_id: str = typer.Argument(..., help="The proposal to reject"),
    reviewer: str = typer.Option(..., help="Who is rejecting (recorded as given, not verified)"),
    reason: str = typer.Option(..., help="Why, kept in the audit log"),
    log_dir: str = _LOG_DIR_OPTION,
) -> None:
    """Reject a proposal. It can't be approved or executed afterwards."""
    record = _load_proposal(proposal_id, log_dir)
    approval = _decide(record, "rejected", reviewer, reason)
    typer.echo(f"\nRejected by {approval.reviewer} (decision {approval.approval_id}).")


if __name__ == "__main__":
    app()
