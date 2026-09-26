"""Entrypoint for the `app` compose service's default (fixture-free) run.

There's no discovery tool in the agent's toolset, and a Flink job_id is
random per submission, so the model has no way to guess it from prose alone.
This script finds the currently-running demo job via the Flink REST API and
execs into `dp-ops-agent diagnose --live` with those ids already filled in,
so `docker compose run app` works with no manual steps.
"""

from __future__ import annotations

import json
import os
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

FLINK_REST_URL = os.environ.get("FLINK_REST_URL", "http://flink-jobmanager:8081")
ALERT_TEXT = os.environ.get(
    "ALERT_TEXT", "PagerDuty: investigate orders-processing-job / orders topic"
)


def _get_json(url: str) -> dict:
    with urlopen(url, timeout=10) as resp:
        return json.load(resp)


def _wait_for_running_job(timeout_seconds: int = 60) -> str:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            jobs = _get_json(f"{FLINK_REST_URL}/jobs")["jobs"]
            running = [j["id"] for j in jobs if j["status"] == "RUNNING"]
            if running:
                return running[0]
        except URLError:
            pass
        time.sleep(2)
    raise SystemExit(f"No RUNNING Flink job found at {FLINK_REST_URL} within {timeout_seconds}s")


def main() -> None:
    job_id = _wait_for_running_job()
    job = _get_json(f"{FLINK_REST_URL}/jobs/{job_id}")
    vertex_id = job["vertices"][0]["id"]

    os.execvp(
        "dp-ops-agent",
        [
            "dp-ops-agent",
            "diagnose",
            "--live",
            "--kafka-topics",
            "orders",
            "--consumer-group",
            "flink-orders-processing",
            "--flink-job-id",
            job_id,
            "--flink-job-name",
            job["name"],
            "--flink-vertex-id",
            vertex_id,
            "--alert-text",
            ALERT_TEXT,
            *sys.argv[1:],
        ],
    )


if __name__ == "__main__":
    main()
