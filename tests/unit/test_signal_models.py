"""The typed scope and observed models reproduce, byte for byte, the JSON
the tools produced as plain dicts (tests/fixtures/snapshots/signals.json):
same keys, same order, same keys present or absent, same number encoding."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, get_args

import pytest
from pydantic import ValidationError

from dp_ops_agent.evidence.schema import SignalType
from dp_ops_agent.evidence.signals.base import SignalBase, SignalTargets
from dp_ops_agent.evidence.signals.dbt import (
    DbtModelNotFound,
    DbtModelScope,
    DbtSourceNotFound,
    DbtSourceScope,
    DependencyGraphCompileErrorObserved,
    FreshnessCheckFailureObserved,
    IncrementalModelDriftObserved,
    ModelRunFailureObserved,
    TestFailureObserved,
)
from dp_ops_agent.evidence.signals.flink import (
    BackpressureRatioObserved,
    CheckpointFailureObserved,
    JobScope,
    JobVertexScope,
    SavepointRestoreFailureObserved,
    StateBackendDiskPressureObserved,
    WatermarkLagObserved,
)
from dp_ops_agent.evidence.signals.kafka import (
    BrokerScope,
    ConsumerLagTrendObserved,
    GroupScope,
    GroupTopicScope,
    HotPartitionSkewObserved,
    IsrChurnObserved,
    RebalanceFrequencyObserved,
    SchemaRegistryCompatObserved,
    SubjectScope,
    TopicScope,
    TopicsScope,
    UnderReplicatedPartitionsObserved,
)
from dp_ops_agent.evidence.signals.lineage import LineageNodeScope, LineageUpstreamObserved

SNAPSHOT = Path(__file__).resolve().parents[1] / "fixtures" / "snapshots" / "signals.json"
_TIMESTAMP = re.compile(r'"(collected_at|window_start|window_end)":"<volatile>"')
_VOLATILE = re.compile(r'"(signal_id|collected_at|window_start|window_end)":"[^"]*"')

PAYLOADS: dict[str, tuple[Any, Any]] = {
    "under_replicated_partitions": (TopicsScope, UnderReplicatedPartitionsObserved),
    "isr_churn": (BrokerScope, IsrChurnObserved),
    "consumer_lag_trend": (GroupTopicScope, ConsumerLagTrendObserved),
    "rebalance_frequency": (GroupScope, RebalanceFrequencyObserved),
    "hot_partition_skew": (TopicScope, HotPartitionSkewObserved),
    "schema_registry_compat": (SubjectScope, SchemaRegistryCompatObserved),
    "checkpoint_failure": (JobScope, CheckpointFailureObserved),
    "backpressure_ratio": (JobVertexScope, BackpressureRatioObserved),
    "watermark_lag": (JobVertexScope, WatermarkLagObserved),
    "state_backend_disk_pressure": (JobVertexScope, StateBackendDiskPressureObserved),
    "savepoint_restore_failure": (JobScope, SavepointRestoreFailureObserved),
    "lineage_upstream": (LineageNodeScope, LineageUpstreamObserved),
    "test_failure": (DbtModelScope, TestFailureObserved | DbtModelNotFound),
    "model_run_failure": (DbtModelScope, ModelRunFailureObserved | DbtModelNotFound),
    "freshness_check_failure": (DbtSourceScope, FreshnessCheckFailureObserved | DbtSourceNotFound),
    "incremental_model_drift": (DbtModelScope, IncrementalModelDriftObserved | DbtModelNotFound),
    "dependency_graph_compile_error": (
        DbtModelScope,
        DependencyGraphCompileErrorObserved | DbtModelNotFound,
    ),
}

SNAPSHOTS: dict[str, str] = json.loads(SNAPSHOT.read_text())


def test_every_signal_type_has_payload_models():
    assert set(PAYLOADS) == set(get_args(SignalType))


@pytest.mark.parametrize("case_id", sorted(SNAPSHOTS))
def test_typed_payloads_reproduce_the_snapshot_exactly(case_id):
    expected = SNAPSHOTS[case_id]
    signal_type = json.loads(expected)["signal_type"]
    scope_model, observed_type = PAYLOADS[signal_type]
    typed = SignalBase[scope_model, observed_type]

    valid = _TIMESTAMP.sub(lambda m: f'"{m.group(1)}":"2026-09-24T00:00:00Z"', expected)
    signal = typed.model_validate_json(valid)

    assert not isinstance(signal.scope, dict)
    assert not isinstance(signal.observed, dict)
    redumped = _VOLATILE.sub(lambda m: f'"{m.group(1)}":"<volatile>"', signal.model_dump_json())
    assert redumped == expected


def test_not_found_payload_resolves_to_its_own_shape():
    observed = json.loads(SNAPSHOTS["test_failure/model-not-found"])["observed"]
    typed = SignalBase[DbtModelScope, TestFailureObserved | DbtModelNotFound]
    signal = typed.model_validate(
        {
            "tool": "dbt.test_failure",
            "signal_type": "test_failure",
            "collected_at": "2026-09-24T00:00:00Z",
            "window_start": "2026-09-24T00:00:00Z",
            "window_end": "2026-09-24T00:00:00Z",
            "scope": {"model": "no_such_model"},
            "observed": observed,
            "severity": "unknown",
        }
    )
    assert isinstance(signal.observed, DbtModelNotFound)


def test_payloads_reject_keys_they_do_not_declare():
    with pytest.raises(ValidationError, match="extra_forbidden"):
        GroupTopicScope.model_validate({"group": "g", "topic": "t", "jobId": "j"})


@pytest.mark.parametrize(
    ("scope", "targets"),
    [
        (
            TopicsScope(topics="orders,payments"),
            SignalTargets(topics=frozenset({"orders", "payments"})),
        ),
        (
            GroupTopicScope(group="billing", topic="orders"),
            SignalTargets(groups=frozenset({"billing"}), topics=frozenset({"orders"})),
        ),
        (GroupScope(group="billing"), SignalTargets(groups=frozenset({"billing"}))),
        (TopicScope(topic="orders"), SignalTargets(topics=frozenset({"orders"}))),
        (JobScope(job_id="j1"), SignalTargets(job_ids=frozenset({"j1"}))),
        (JobVertexScope(job_id="j1", vertex_id="v1"), SignalTargets(job_ids=frozenset({"j1"}))),
        (
            DbtModelScope(model="stg_orders", lineage_node_id="job:dbt:stg_orders"),
            SignalTargets(models=frozenset({"stg_orders"})),
        ),
        # Scopes no catalog action can act on cover nothing.
        (BrokerScope(broker_id="1"), SignalTargets()),
        (SubjectScope(subject="orders-value"), SignalTargets()),
        (LineageNodeScope(node_id="dataset:kafka:orders"), SignalTargets()),
        (DbtSourceScope(source="raw.orders_sink"), SignalTargets()),
    ],
)
def test_scope_targets(scope, targets):
    assert scope.targets() == targets
