"""The typed signals reproduce, byte for byte, the JSON the tools produced
as plain dicts (tests/fixtures/snapshots/signals.json): same keys, same
order, same keys present or absent, same number encoding. Also the rules
that keep the set of signal classes consistent."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import get_args

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from dp_ops_agent.evidence.schema import DiagnosedSystem, Signal, SignalType
from dp_ops_agent.evidence.signals.base import Payload, SignalBase, SignalTargets
from dp_ops_agent.evidence.signals.dbt import (
    DbtModelNotFound,
    DbtModelScope,
    DbtSourceScope,
    TestFailureSignal,
)
from dp_ops_agent.evidence.signals.flink import (
    JobScope,
    JobVertexScope,
)
from dp_ops_agent.evidence.signals.kafka import (
    BrokerScope,
    GroupScope,
    GroupTopicScope,
    IsrChurnObserved,
    SubjectScope,
    TopicScope,
    TopicsScope,
)
from dp_ops_agent.evidence.signals.lineage import (
    LineageNodeScope,
    LineageUpstreamSignal,
)

SNAPSHOT = Path(__file__).resolve().parents[1] / "fixtures" / "snapshots" / "signals.json"
_TIMESTAMP = re.compile(r'"(collected_at|window_start|window_end)":"<volatile>"')
_VOLATILE = re.compile(r'"(signal_id|collected_at|window_start|window_end)":"[^"]*"')

SNAPSHOTS: dict[str, str] = json.loads(SNAPSHOT.read_text())
_SIGNAL: TypeAdapter[Signal] = TypeAdapter(Signal)
# Every concrete signal class, from the Signal union.
_CLASSES: list[type[SignalBase]] = list(get_args(get_args(Signal)[0]))


def test_every_signal_type_has_exactly_one_concrete_class():
    signal_types = [get_args(c.model_fields["signal_type"].annotation)[0] for c in _CLASSES]
    assert sorted(signal_types) == sorted(get_args(SignalType))


@pytest.mark.parametrize("signal_class", _CLASSES, ids=lambda c: c.__name__)
def test_each_class_declares_the_system_its_tool_belongs_to(signal_class):
    tool = signal_class.model_fields["tool"].default
    prefix = tool.split(".", 1)[0]
    if signal_class is LineageUpstreamSignal:
        # Lineage shows where to look; it can't be a diagnosis's root cause.
        assert signal_class.system is None
    else:
        assert signal_class.system == prefix
        assert prefix in get_args(DiagnosedSystem)


def test_the_base_class_cannot_be_constructed():
    now = "2026-09-24T00:00:00Z"
    fields = dict(
        tool="kafka.isr_churn",
        signal_type="isr_churn",
        collected_at=now,
        window_start=now,
        window_end=now,
        scope={"broker_id": "1"},
        observed={"isr_shrinks_per_sec": [], "isr_expands_per_sec": [], "max_shrink_rate": 0.0},
        severity="ok",
    )
    with pytest.raises(TypeError, match="not a concrete signal class"):
        SignalBase(**fields)
    with pytest.raises(TypeError, match="not a concrete signal class"):
        SignalBase[BrokerScope, IsrChurnObserved].model_validate(fields)


@pytest.mark.parametrize("case_id", sorted(SNAPSHOTS))
def test_typed_signals_reproduce_the_snapshot_exactly(case_id):
    expected = SNAPSHOTS[case_id]
    valid = _TIMESTAMP.sub(lambda m: f'"{m.group(1)}":"2026-09-24T00:00:00Z"', expected)

    signal = _SIGNAL.validate_json(valid)

    assert signal.signal_type == json.loads(expected)["signal_type"]
    assert type(signal) is not SignalBase and isinstance(signal, SignalBase)
    assert not isinstance(signal.scope, dict)
    assert not isinstance(signal.observed, dict)
    redumped = _VOLATILE.sub(lambda m: f'"{m.group(1)}":"<volatile>"', signal.model_dump_json())
    assert redumped == expected


def test_not_found_payload_resolves_to_its_own_shape():
    valid = _TIMESTAMP.sub(
        lambda m: f'"{m.group(1)}":"2026-09-24T00:00:00Z"',
        SNAPSHOTS["test_failure/model-not-found"],
    )
    signal = _SIGNAL.validate_json(valid)
    assert type(signal) is TestFailureSignal
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


def _nested_models(annotation) -> set[type[BaseModel]]:
    """Every pydantic model reachable from a field annotation."""
    found: set[type[BaseModel]] = set()
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        found.add(annotation)
        for field in annotation.model_fields.values():
            found |= _nested_models(field.annotation)
    for arg in get_args(annotation):
        found |= _nested_models(arg)
    return found


@pytest.mark.parametrize("signal_class", _CLASSES, ids=lambda c: c.__name__)
def test_every_payload_model_is_owned_by_the_evidence_layer(signal_class):
    """Scope and observed models, and everything nested in them, are wire
    format: Payloads (extra keys forbidden) defined in evidence/signals, not
    models borrowed from a gateway, where adding a field for gateway reasons
    would silently change the JSON the model sees."""
    for name in ("scope", "observed"):
        for model in _nested_models(signal_class.model_fields[name].annotation):
            assert issubclass(model, Payload), f"{model.__module__}.{model.__name__}"
            assert model.__module__.startswith("dp_ops_agent.evidence.signals"), model.__module__
