# ADR-0001: A single tool-calling loop, not multi-agent orchestration

**Status:** Accepted
**Date:** 2026-09-20

## Context

Most public "agentic diagnostics" demos default to a multi-agent setup: a supervisor agent, per-system sub-agents, a planner, sometimes a critic. Before writing any code, this needed a decision: does this project need that shape?

## Decision

The orchestrator is one `create_agent` loop (LangGraph), with every diagnostic tool registered directly on it. No supervisor, no per-system sub-agent, no separate planning step. The loop calls tools until it has grounded evidence, then concludes.

## Alternatives considered

- **Per-system sub-agents (Kafka agent, Flink agent, dbt agent) coordinated by a supervisor.** Rejected for the current scope. The actual hard problem here is tracing a root cause across system boundaries. A supervisor architecture makes that harder, not easier: the supervisor would need to decide which sub-agent to consult and reconcile their outputs, which is the same cross-system correlation work a single agent with a shared tool list already does by calling whichever tool is relevant next. Splitting into sub-agents adds coordination overhead without adding capability.
- **A separate planning step before tool-calling begins.** Rejected. The evidence needed to plan is the same evidence the diagnostic tools return. A pre-planning step would just be guessing ahead of the data.

## Consequences

This keeps the system simple to reason about and test: one message history, one tool list, no inter-agent protocol to design. The tradeoff, flagged early and accepted: as more systems and tools get added, one flat tool list could eventually strain tool-selection accuracy. If that happens in practice, the fix is a routing layer that filters which tools are exposed per incident, not a rearchitecture into multiple agents.
