# ADR-0003: Put gateway protocols between tools and infrastructure

| Status | Date |
| --- | --- |
| Accepted | 2026-09-20 |

## Decision

Diagnostic tools call typed gateway protocols instead of calling Kafka, Flink, lineage, or dbt APIs directly.

Each protocol has two implementations:

| Implementation | Purpose |
| --- | --- |
| `Live*Gateway` | Reads real infrastructure through libraries, HTTP APIs, or artifact files |
| `Fixture*Gateway` | Returns deterministic data from JSON snapshots |

Tools depend only on the protocol and do not know which implementation they receive.

## Context

Direct infrastructure calls inside each tool would require live Kafka, Flink, Marquez, or dbt resources for most tests and demonstrations.

The project needs to support:

- deterministic offline tests;
- repeatable incident fixtures;
- live infrastructure without changing tool logic; and
- small, typed views over third-party API responses.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Mock third-party clients directly | Tests would become coupled to complex client-library response types and implementation details. |
| One gateway with a `fixture_mode` flag | Mode branches mix two responsibilities and make each path harder to reason about independently. |

## Consequences

### Benefits

- The same tools work with fixtures and live infrastructure.
- Most behaviour can be tested without external services.
- Third-party response formats are isolated behind project-owned types.

### Cost

Live and fixture implementations must remain behaviourally aligned. Each module documents the live implementation's verified behaviour and known gaps.
