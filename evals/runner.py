"""Runs the eval scenario suite against a live model. Always makes real,
billed Anthropic API calls, one per scenario, since evaluating the agent's
own reasoning is the entire point; nothing here can be faked with a fixture.

    $ ./auto/eval
    $ ./auto/eval --model claude-sonnet-5
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from dataclasses import dataclass
from uuid import uuid4

from dotenv import load_dotenv

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.orchestrator.session import DiagnosisNotSubmittedError, run_diagnosis
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from evals.grading import GradeResult, grade
from evals.scenarios import SCENARIOS, EvalScenario

load_dotenv()


@dataclass
class EvalRunResult:
    scenario: EvalScenario
    grade: GradeResult
    duration_seconds: float
    session_id: str
    error: str | None = None


async def run_scenario(
    scenario: EvalScenario, model: str, log_dir: str = "logs/evals"
) -> EvalRunResult:
    session_id = str(uuid4())
    audit = JsonlAuditSink(log_dir, session_id)
    kafka_gateway = FixtureKafkaGateway(scenario.kafka_fixture_path)
    flink_gateway = FixtureFlinkGateway(scenario.flink_fixture_path)
    lineage_gateway = FixtureLineageGateway(scenario.lineage_fixture_path)

    start = time.monotonic()
    try:
        result = await run_diagnosis(
            session_id=session_id,
            alert_text=scenario.alert_text,
            kafka_gateway=kafka_gateway,
            flink_gateway=flink_gateway,
            lineage_gateway=lineage_gateway,
            audit=audit,
            model=model,
        )
    except DiagnosisNotSubmittedError as exc:
        return EvalRunResult(
            scenario=scenario,
            grade=GradeResult(passed=False, reason=str(exc)),
            duration_seconds=time.monotonic() - start,
            session_id=session_id,
            error=str(exc),
        )

    return EvalRunResult(
        scenario=scenario,
        grade=grade(result.diagnosis, scenario),
        duration_seconds=time.monotonic() - start,
        session_id=session_id,
    )


async def run_all(scenarios: list[EvalScenario], model: str) -> list[EvalRunResult]:
    return [await run_scenario(scenario, model) for scenario in scenarios]


def _print_report(results: list[EvalRunResult]) -> bool:
    print(f"{'scenario':<22} {'result':<6} {'time':>6}  reason")
    print("-" * 90)
    for r in results:
        status = "PASS" if r.grade.passed else "FAIL"
        print(f"{r.scenario.name:<22} {status:<6} {r.duration_seconds:5.1f}s  {r.grade.reason}")

    passed_count = sum(1 for r in results if r.grade.passed)
    print("-" * 90)
    print(f"{passed_count}/{len(results)} passed")
    return passed_count == len(results)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the agent eval scenario suite against a live model."
    )
    parser.add_argument("--model", default=os.environ.get("CLAUDE_MODEL", "claude-sonnet-5"))
    args = parser.parse_args()

    results = asyncio.run(run_all(SCENARIOS, args.model))
    all_passed = _print_report(results)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
