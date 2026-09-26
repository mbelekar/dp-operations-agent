# ADR-0012: Use typed signal payloads without changing their JSON

| Status | Date |
| --- | --- |
| Accepted | 2026-09-26 |

## Decision

Give every signal type its own typed `scope` and `observed` models while preserving the existing tool JSON.

- Seventeen concrete signal classes live under `evidence/signals/`.
- `Signal` is a discriminated union keyed by `signal_type`.
- Each class fixes its `tool`, `signal_type`, and diagnosed system.
- Proposal grounding uses `scope.targets()` instead of dictionary-key lookups.
- Mypy checks Pydantic constructor arguments through `init_typed = true`.

## Context

The original `Signal` envelope was typed, but its contents were not:

```python
scope: dict[str, str]
observed: dict[str, Any]
```

This created two risks:

1. **Scope drift**  
   Proposal grounding depended on string keys such as `topic`, `group`, `job_id`, and `model`. A renamed key could silently make a valid proposal look ungrounded.

2. **Payload drift**  
   Normal and no-data branches could emit different or incorrectly typed fields without static checking.

Enabling typed Pydantic constructors exposed 13 existing type errors, including incorrectly widened severity and signal-type values.

## Design

```text
SignalBase[ScopeT, ObservedT]
              ↓
Concrete signal class
              ↓
Typed scope + typed observations
              ↓
Unchanged JSON for tools and audit logs
```

Proposal validation now obtains targets through a common typed interface:

```python
signal.scope.targets()
```

Renaming a relevant scope field therefore breaks type checking or tests instead of silently weakening grounding.

## Preserving the wire format

The model and audit log already depend on the tool JSON, so the migration preserved it exactly.

| Constraint | Implementation |
| --- | --- |
| Field order | Models declare fields in the existing JSON order |
| Missing versus `null` | `Absent[T]` omits optional keys when their value is `None` |
| Numeric representation | Gateway-derived numbers retain `int \| float` where required |
| Different result branches | Separate payload models represent distinct shapes |
| Unexpected fields | Payload models use `extra="forbid"` |

Sixty-three snapshot cases in `tests/integration/test_signal_json_snapshot.py` protect the existing JSON across normal, no-data, and not-found branches.

## Typing boundary

| Data | Typed? |
| --- | --- |
| Top-level scope and observation fields | Yes |
| Project-owned nested structures | Yes |
| External payloads passed through unchanged | No |

External structures such as Kafka state history, Flink exceptions, Schema Registry responses, and dbt freshness criteria remain `dict[str, Any]`. Their schemas belong at the gateway boundary rather than in the evidence model.

## Lineage signals

A lineage signal has `system = None` because it shows where to investigate rather than identifying a failing system.

`submit_diagnosis` therefore rejects lineage as a root cause and asks for a diagnostic signal from the upstream system. A test verifies that every other signal class has a system matching its tool namespace.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| One signal class with untagged scope and observation unions | Overlapping optional fields make validation ambiguous and prevent useful type narrowing. |
| Validate dictionary key sets only | Would detect missing keys but not incorrect value types. |
| Restructure no-data fields into a nested object | Simpler internally, but changes the model-facing JSON. |
| Continue looking up scope fields by string | Renaming a field could still break grounding silently. |

## Consequences

### Benefits

- Incorrect field names and values fail close to where a signal is built.
- Signal type, tool name, and diagnosed system remain aligned.
- Proposal grounding uses typed targets.
- Adding a new signal requires an explicit class and union entry.

### Costs and limits

- Tests need valid typed signal fixtures rather than empty observation dictionaries.
- External pass-through payloads remain unchecked at this layer.
- The migration briefly broke Kafka replay grounding because the serializer was updated before every reader of `scope` was found. The resulting `targets()` interface now protects that path.

The migration reinforced one rule: when changing a field's representation, identify every reader as well as every writer.
