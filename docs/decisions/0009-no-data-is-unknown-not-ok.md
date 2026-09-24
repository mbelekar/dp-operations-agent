# ADR-0009: A tool that finds no data reports severity `unknown`, not `ok`

**Status:** Accepted
**Date:** 2026-09-24

## Context

Every diagnostic tool computes severity by thresholding whatever its gateway returned, e.g. `max(lag_by_subtask.values(), default=0.0) > 60_000`. An empty result (an unknown vertex, topic, consumer group, or lineage node, or a metric the backend simply doesn't report) passes every threshold and came back `ok`. Some tools went further and filled in a healthy-looking value: the Flink fixture gateway defaulted a missing vertex to `backpressure_level: "ok"`, and `schema_registry_compat` read an empty registry response as `is_compatible: true`.

This surfaced in a live eval run: coming from a dbt alert, the model queried Flink vertex `sink`, which the fixture has no data for. Three Flink tools returned `ok`, and the diagnosis described Flink as "nominal", while watermark lag on vertex `source` was critical. The grounding validator (ADR-0002) couldn't catch it: the signal really was collected, it just asserted a health it never observed. Checking every tool against unknown identifiers found the same pattern in 12 of 13 cases, plus one inverse: `consumer_lag_trend` read a consumer group's missing committed offsets as offset 0 and reported the whole topic as critical lag. In live mode `schema_registry_compat` reported `ok` on every call (its compatibility endpoint is POST-only, so every GET fails, and the failure became "compatible").

## Decision

`Severity` gains a fourth value, `unknown`: the tool found no data for the identifiers it was asked about. Every tool whose empty result can't mean healthy reports `unknown`, with `observed.no_data_reason` naming what was missing, instead of thresholding the empty result. The distinction is made in the tools, not by changing gateway Protocols, with one exception: lineage, where an empty upstream list *is* a legitimate answer for a source node, so `LineageGraphView` gains `node_found` to tell "never heard of this node" apart from "nothing upstream".

The grounding validator additionally rejects a `root_cause_signal_id` whose signal is `unknown`: a hypothesis can't rest on a signal that observed nothing. An `unknown` signal can still be cited elsewhere in `evidence_chain` ("no data for vertex sink, so checked source instead"). The system prompt says `unknown` is not evidence of health and should prompt a retry with other identifiers or be reported as a gap.

## Alternatives considered

- **Keep `ok` and add `observed.data_available: false`.** Rejected. The failure happened precisely because the model trusted the severity field; a flag it has to notice inside `observed` is the weaker fix, and every consumer of severity (including a human reading an audit log) would keep being misled.
- **Raise an error so the tool middleware reports it as a tool error.** Rejected. A tool error means "the backend is unavailable" (see `orchestrator/tool_errors.py`), which is wrong for "the backend answered, and has nothing for this identifier", and it records no signal, so the model couldn't cite the gap at all.
- **Every gateway method returns `None` for "entity not found".** Deferred. More precise, but it changes 13 methods across three Protocols and every fixture, and wasn't needed to fix the failure seen.

## Consequences

Tools that have no live data source now say so instead of silently passing: `hot_partition_skew` and `schema_registry_compat` report `unknown` on every live call, and `state_backend_disk_pressure` does for the demo job (no `disk_used_ratio` metric). Those were always gaps; they're just visible now. `unknown` also covers a real outage that yields no data (e.g. an exporter losing a broker), which is still the honest report; `no_data_reason` says what was asked for. `savepoint_restore_failure` is deliberately unchanged: for a job that exists, an empty exception history really is healthy. Live `rebalance_frequency` for a nonexistent group is unverified: the live gateway always returns one `describe_consumer_groups` state, and what Kafka reports for a group that doesn't exist wasn't checked.
