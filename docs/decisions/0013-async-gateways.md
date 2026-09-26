# ADR-0013: Make infrastructure gateways asynchronous

| Status | Date |
| --- | --- |
| Accepted | 2026-09-26 |

## Decision

Make every gateway protocol method asynchronous and keep blocking work off the event loop.

Tools now await their gateways without changing their inputs or returned signal JSON.

## I/O strategy

| Infrastructure call | Implementation |
| --- | --- |
| JMX, Schema Registry, Flink REST, and Marquez | `httpx.AsyncClient` |
| Kafka administrative futures | `asyncio.wrap_future` with `asyncio.wait_for` |
| Blocking Kafka metadata and consumer calls | `asyncio.to_thread` |
| dbt artifact reads and JSON parsing | `asyncio.to_thread` |

HTTP and Kafka administrative futures are natively non-blocking from the event loop's perspective. Kafka calls without asynchronous APIs and local file reads run in worker threads.

## Context

The tools were declared `async`, but their gateway calls were synchronous. LangGraph can run several tool calls concurrently, but a blocking gateway prevented that concurrency.

For example, three Kafka calls that each waited ten seconds could take approximately thirty seconds instead of ten. They could also block unrelated investigations sharing the same event loop.

## Resource and concurrency handling

### HTTP clients

Each HTTP-backed live gateway owns an `AsyncClient` and exposes `aclose()`. The CLI creates and closes live gateways inside the same event loop using `AsyncExitStack`, including error paths.

Fixture gateways own no resources, so `aclose()` is not part of the gateway protocols.

### Kafka clients

- Each Kafka `Consumer` is created, used, and closed in one worker thread.
- The shared `AdminClient` is safe for concurrent use through librdkafka.
- Administrative futures are awaited without blocking the event loop.

### JMX cache

An `asyncio.Lock` prevents concurrent cache misses from triggering duplicate JMX scrapes. All callers share the first in-flight result.

### Errors

Gateways continue to raise the same `httpx.HTTPError`, `KafkaException`, and `TimeoutError` types. Existing retry and tool-error middleware therefore remain unchanged.

## Verification

`tests/integration/test_parallel_tool_calls.py` runs the real agent loop with a scripted model that requests three systems in one turn.

The test verifies that all three calls start before any finishes. Replacing an asynchronous wait with blocking `time.sleep` causes the test to fail. Gateway-level tests cover the individual I/O mechanisms.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Keep synchronous gateways and wrap every call in `to_thread` from the tools | It would hide blocking behaviour at the wrong layer, rely more heavily on the thread pool, and weaken the gateway contract. |
| Replace `confluent-kafka` with `aiokafka` | Deferred. It would replace a client already verified against the live Docker environment. |

## Consequences

### Benefits

- Tool calls to different systems can overlap.
- A slow backend contributes roughly its own latency rather than adding serially to every other call.
- HTTP cancellation stops the request promptly.
- Concurrent sessions no longer block each other through synchronous gateway I/O.

### Limitations

- Cancelling a `to_thread` Kafka call returns control promptly, but the worker finishes the underlying operation, up to its timeout.
- `topic_high_watermarks` still queries partitions sequentially inside one worker thread.
- The concurrency tests cover representative paths, not every gateway method. A future synchronous call inside an async method could still reintroduce blocking outside those paths.

Related decision: [ADR-0003](0003-gateway-abstraction.md).
