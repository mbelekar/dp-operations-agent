"""Runs the eval scenario suite against a live model. Always makes real,
billed Anthropic API calls, one session per scenario run, since evaluating
the agent's own reasoning is the entire point; nothing here can be faked
with a fixture.

    $ ./auto/eval
    $ ./auto/eval --model claude-sonnet-5
    $ ./auto/eval --scenario dbt_model_logic_regression --repeat 2

Run only the scenarios a change can affect (--scenario, repeatable): most
changes don't touch most scenarios, and each run is billed. --repeat 2 runs
each one twice; see print_report for what that can and can't tell you.
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
from dp_ops_agent.orchestrator.session import (
    DiagnosisNotSubmittedError,
    TokenUsage,
    run_diagnosis,
)
from dp_ops_agent.tools.dbt.fixture_gateway import FixtureDbtGateway
from dp_ops_agent.tools.flink.fixture_gateway import FixtureFlinkGateway
from dp_ops_agent.tools.kafka.fixture_gateway import FixtureKafkaGateway
from dp_ops_agent.tools.lineage.fixture_gateway import FixtureLineageGateway
from evals.grading import GradeResult, grade
from evals.scenarios import SCENARIOS, EvalScenario

load_dotenv()

MAX_REPEAT = 2


@dataclass
class EvalRunResult:
    scenario: EvalScenario
    grade: GradeResult
    duration_seconds: float
    session_id: str
    error: str | None = None
    # None when the session recorded no usage (e.g. it hit the recursion limit).
    usage: TokenUsage | None = None


def _recorded_usage(audit: JsonlAuditSink) -> TokenUsage | None:
    events = audit.query(event_type="session_usage")
    return TokenUsage(**events[-1].payload) if events else None


async def run_scenario(
    scenario: EvalScenario, model: str, log_dir: str = "logs/evals"
) -> EvalRunResult:
    session_id = str(uuid4())
    audit = JsonlAuditSink(log_dir, session_id)
    kafka_gateway = FixtureKafkaGateway(scenario.kafka_fixture_path)
    flink_gateway = FixtureFlinkGateway(scenario.flink_fixture_path)
    lineage_gateway = FixtureLineageGateway(scenario.lineage_fixture_path)
    dbt_gateway = FixtureDbtGateway(scenario.dbt_fixture_path)

    start = time.monotonic()
    try:
        result = await run_diagnosis(
            session_id=session_id,
            alert_text=scenario.alert_text,
            kafka_gateway=kafka_gateway,
            flink_gateway=flink_gateway,
            lineage_gateway=lineage_gateway,
            dbt_gateway=dbt_gateway,
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
            usage=_recorded_usage(audit),
        )

    return EvalRunResult(
        scenario=scenario,
        grade=grade(result.diagnosis, scenario),
        duration_seconds=time.monotonic() - start,
        session_id=session_id,
        usage=result.usage,
    )


def select_scenarios(names: list[str] | None) -> list[EvalScenario]:
    if not names:
        return list(SCENARIOS)
    by_name = {s.name: s for s in SCENARIOS}
    unknown = [n for n in names if n not in by_name]
    if unknown:
        raise ValueError(
            f"unknown scenario(s) {unknown}; valid names: {', '.join(by_name)}"
        )
    return [by_name[n] for n in names]


async def run_all(
    scenarios: list[EvalScenario], model: str, repeat: int = 1
) -> list[EvalRunResult]:
    return [
        await run_scenario(scenario, model)
        for scenario in scenarios
        for _ in range(repeat)
    ]


def print_report(results: list[EvalRunResult], repeat: int = 1) -> bool:
    """One line per scenario: passes out of runs, time, tokens, and the
    reason for any failed run. Returns whether every run passed."""
    by_scenario: dict[str, list[EvalRunResult]] = {}
    for r in results:
        by_scenario.setdefault(r.scenario.name, []).append(r)

    width = max([len("scenario"), *(len(name) for name in by_scenario)])
    print(f"{'scenario':<{width}}  {'passed':>6}  {'time':>7}  tokens")
    print("-" * (width + 60))
    total = TokenUsage()
    unrecorded = 0
    for name, runs in by_scenario.items():
        passed = sum(r.grade.passed for r in runs)
        seconds = sum(r.duration_seconds for r in runs)
        usage = TokenUsage()
        for r in runs:
            if r.usage is None:
                unrecorded += 1
                continue
            for field in TokenUsage.model_fields:
                setattr(usage, field, getattr(usage, field) + getattr(r.usage, field))
                setattr(total, field, getattr(total, field) + getattr(r.usage, field))
        print(
            f"{name:<{width}}  {passed}/{len(runs):<4}  {seconds:6.1f}s  {_tokens(usage)}"
        )
        for r in runs:
            if not r.grade.passed:
                print(f"{'':<{width}}    FAIL: {r.grade.reason}")

    passed_runs = sum(r.grade.passed for r in results)
    print("-" * (width + 60))
    print(f"{passed_runs}/{len(results)} runs passed; total tokens: {_tokens(total)}")
    if unrecorded:
        print(f"{unrecorded} session(s) without recorded usage (not in the token totals)")
    if repeat > 1:
        print(
            f"Note: {repeat}/{repeat} is not proof a scenario is reliable (one that passes "
            "75% of the time still passes both runs more than half the time); a failed "
            "run is a real sign it isn't."
        )
    return passed_runs == len(results)


def _tokens(usage: TokenUsage) -> str:
    return (
        f"in={usage.input_tokens:,} (cached={usage.cache_read_tokens:,}, "
        f"cache-write={usage.cache_creation_tokens:,}) out={usage.output_tokens:,}"
    )


def _repeat(value: str) -> int:
    n = int(value)
    if not 1 <= n <= MAX_REPEAT:
        raise argparse.ArgumentTypeError(f"--repeat must be between 1 and {MAX_REPEAT}")
    return n


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the agent eval scenario suite against a live model (billed)."
    )
    parser.add_argument("--model", default=os.environ.get("CLAUDE_MODEL", "claude-sonnet-5"))
    parser.add_argument(
        "--scenario",
        action="append",
        dest="scenarios",
        metavar="NAME",
        help="Run only this scenario; repeat the flag for several. Default: all.",
    )
    parser.add_argument(
        "--repeat",
        type=_repeat,
        default=1,
        help=f"Run each scenario this many times (1-{MAX_REPEAT}). Default: 1.",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    try:
        scenarios = select_scenarios(args.scenarios)
    except ValueError as exc:
        sys.exit(str(exc))

    results = asyncio.run(run_all(scenarios, args.model, args.repeat))
    all_passed = print_report(results, args.repeat)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
