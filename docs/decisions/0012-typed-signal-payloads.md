# ADR-0012: Typed signal payloads behind an unchanged wire format

| Status | Date |
| --- | --- |
| Accepted | 2026-09-26 |

## Decision

Every signal type has its own `Signal` class with typed `scope` and `observed` payloads. `Signal` is the discriminated union of those classes, keyed on `signal_type`. The JSON the tools return stays exactly what it was when the payloads were plain dicts.

- 17 concrete classes live in `evidence/signals/` (`kafka.py`, `flink.py`, `lineage.py`, `dbt.py`), on a generic `SignalBase[ScopeT, ObservedT]`. Each fixes its `tool` and `signal_type` as literals and declares the system it diagnoses.
- Proposal grounding checks actions against `scope.targets()`, a typed `SignalTargets`, instead of looking up scope keys by name.
- The pydantic mypy plugin runs with `init_typed = true`, so constructor arguments are type-checked.

## Context

The `Signal` envelope was typed, but the evidence it carried was `scope: dict[str, str]` and `observed: dict[str, Any]`, assembled by hand in 17 tools and extended with `.update()` on some branches. Nothing checked either:

- **Scope drift broke grounding silently.** Grounding looked up `job_id`, `model`, `group`, `topic` and `topics` by name. A tool that wrote a different key would have made every proposal on that target fail as "not investigated", with no error anywhere else.
- **Observed shapes could drift between branches.** For example, a key could be added to the normal branch and not the no-data one. The model, the audit log and the system prompt (`scope.lineage_node_id`, `no_data_reason`, `known_*`) all depend on these shapes.

mypy in CI didn't close the gap on its own. By default the pydantic plugin types every model constructor argument as `Any`, so wrong value types, and plain strings passed where a `Literal` is expected, were only caught at runtime. Turning on `init_typed` exposed 13 hidden errors: 11 `severity` values typed as plain `str`, dbt's signal type passed as a plain `str`, and the lineage root-cause case described below.

## The wire format is a constraint

The model reads the tool JSON, the audit log stores it, and the prompt names fields in it. So the typed models had to reproduce it byte for byte:

| Rule | How the models keep it |
| --- | --- |
| Key order | Model fields are declared in the order the tools emitted keys. That order differs by module: dbt leads with `no_data_reason`, and Flink's checkpoint tool puts `known_jobs` before it. |
| Absent vs null | A key some branches leave out is `Absent[T]` (omitted when `None`). A key that's always present but can be null is a plain `T \| None`. |
| Number encoding | A `float` field turns `3` into `3.0`. Values from plain gateway dicts use `int \| float`. |
| Different shapes | A dbt model or source the manifest doesn't have gets its own payload (`DbtModelNotFound`, `DbtSourceNotFound`). Payload models forbid extra keys, so a union always resolves to exactly one shape. |

`tests/integration/test_signal_json_snapshot.py` pins the JSON of every tool, branch by branch (63 cases), and was committed before any model existed. It passed unchanged through the whole migration.

## How far the typing goes

| Level | What | Typed? |
| --- | --- | --- |
| 1 | Every top-level `scope` and `observed` field | Yes |
| 2 | Nested structures the tools build themselves: partition entries, dbt test results and changed parents, `known_*` refs, lineage upstream nodes | Yes, with Flink's `CheckpointCounts` and `SubtaskBackpressure` reused from the gateway |
| 3 | External API data passed through unchanged: consumer-group `state_history`, Flink `matching_exceptions`, the Schema Registry `raw` response, dbt freshness `criteria` | No, `dict[str, Any]` |

Level 3's shape is set by Kafka, Flink, Schema Registry and dbt, not by this project. Typing it belongs in the gateways.

## Lineage can't be a root cause

Each concrete class declares `system: ClassVar[DiagnosedSystem | None]`. Lineage is `None`: it shows where to look, and the root cause is on the system it points to. `submit_diagnosis` rejects a lineage root cause with that explanation, instead of the pydantic error on `Diagnosis.system` the model used to get. `_derive_system` (ADR-0006) now reads the class's `system` instead of splitting the `tool` string. A test requires every class's `system` to match its tool prefix, with lineage the only exception.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| One `Signal` class with `scope` and `observed` as untagged unions, plus a validator pairing them with `signal_type` | Overlapping optional fields make pydantic's union matching ambiguous, and mypy can't narrow `signal.observed` from `signal_type`. |
| Validate each type's dict key sets without models | Catches missing or extra keys, but not value types, and gives mypy nothing to check where signals are built. |
| Nest the "no data" fields under one sub-object | Simpler to model, but changes the JSON the model reads. |
| Keep grounding on scope keys looked up by name (`getattr`) | Still a string lookup: renaming a scope field would break grounding silently, the problem this ADR set out to fix. |

## Consequences

- Building a payload with a wrong field name or value type, or a signal whose `tool` and `signal_type` disagree, fails type-checking or validation where the signal is built, not somewhere downstream.
- Renaming a scope field breaks `targets()` at type-check time instead of silently breaking grounding.
- Adding a signal type means adding its class to the union. A test fails if a `SignalType` has no class, or if a class's `system` doesn't match its tool.
- Tests build signals through `tests/signal_factory.py`, which starts from a known-good snapshot payload, since `observed={}` no longer validates.
- Drift inside level-3 pass-through data is still unchecked.
- **Lesson from the migration:** the payload models were checked against the JSON, but grounding also read `scope`, as a dict. Migrating the Kafka tools first silently broke Kafka replay grounding until the fix that introduced `targets()`. Before changing a field's type, find every reader of it, not only the serializer.
