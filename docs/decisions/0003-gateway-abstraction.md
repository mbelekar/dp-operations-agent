# ADR-0003: A gateway Protocol between tools and infrastructure, not direct API calls

**Status:** Accepted
**Date:** 2026-09-20

## Context

Diagnostic tools need to call real infrastructure: Kafka's Admin API, a JMX/Prometheus exporter, the Flink REST API. Calling those APIs directly from inside each `@tool` function would make the tool-calling loop untestable without live infrastructure. This project needed to be developed, tested, and demoed with no live Kafka cluster or Flink deployment available.

## Decision

Every tool talks to infrastructure through a Protocol (`KafkaMetricsGateway`, later `FlinkMetricsGateway`), never directly. Each Protocol has two implementations: a `Live*Gateway` (real API calls via `confluent-kafka`/`httpx`) and a `Fixture*Gateway` (loads a JSON snapshot, returns deterministic canned responses, safe empty defaults for anything not in the snapshot). Tools are written against the Protocol and do not know or care which implementation they are getting.

## Alternatives considered

- **Mocking the API client library directly in tests** (e.g. `unittest.mock` over `confluent-kafka`'s `AdminClient`). Rejected. This couples every test to the exact shape of a third-party client's return objects, which is more brittle than owning a small typed view (`PartitionMetadata`, `ClusterMetadataView`) that the gateway itself is responsible for producing correctly, whichever implementation is behind it.
- **A single gateway implementation with a "fixture mode" flag.** Rejected. Branching inside one class on a mode flag is harder to reason about and test than two small classes, each doing one thing. It also risks the fixture path silently drifting out of sync with the live path, since a shared "if fixture_mode" branch can break quietly.

## Consequences

This is the single seam that makes the whole diagnostic loop testable without live infrastructure. It is what let this project be built, tested (33 tests), and evaluated (5 live-model eval scenarios) entirely offline except for the model call itself. The cost: the `Live*Gateway` implementations are written to verified API documentation but have never been run against a real cluster in this environment. That is a known, stated limitation, not an oversight, tracked per module in each module's own docs.
