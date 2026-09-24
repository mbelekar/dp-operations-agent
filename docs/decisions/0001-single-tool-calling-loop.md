# ADR-0001: Use one tool-calling agent

| Status | Date |
| --- | --- |
| Accepted | 2026-09-20 |

## Decision

Use one LangGraph `create_agent` loop with all diagnostic tools registered directly.

Do not add a supervisor, per-system agents, or a separate planning agent at the current scale.

## Context

Many agentic-diagnostics examples use several agents: a supervisor, one agent per system, a planner, and sometimes a critic.

This project instead needs to follow evidence across Kafka, Flink, dbt, and lineage. Splitting those tools between agents would introduce another coordination problem without improving the diagnosis itself.

The required loop is simpler:

```text
Collect evidence → reason → collect more evidence → diagnose
```

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Kafka, Flink, and dbt agents coordinated by a supervisor | The supervisor would still need to choose systems and reconcile cross-system evidence. A shared tool list already supports that. |
| A planning step before tool use | The evidence needed to make a useful plan comes from the tools. Planning first would mean guessing before observing. |

## Consequences

### Benefits

- One message history
- One tool list
- No inter-agent protocol
- Simpler testing and debugging

### Trade-off

A growing tool list may eventually reduce tool-selection accuracy.

If that happens, add a routing layer that filters the tools exposed for an incident. Do not introduce multiple agents unless there is evidence that routing alone is insufficient.
