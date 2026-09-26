# ADR-0013: Async gateways

| Status | Date |
| --- | --- |
| Accepted | 2026-09-26 |

## Decision

Every gateway Protocol method (ADR-0003) is `async def`, and each live gateway uses the most natively async I/O its library offers. The tools only `await` their gateway calls, so their specs and the JSON they return are unchanged.

| I/O | Mechanism | Genuinely non-blocking? |
| --- | --- | --- |
| HTTP: JMX aggregator, Schema Registry, Flink REST, Marquez | `httpx.AsyncClient` | Yes |
| Kafka `list_consumer_groups`, `describe_consumer_groups` | `asyncio.wait_for(asyncio.wrap_future(f), 10.0)` on the library's `concurrent.futures` | Yes: librdkafka completes them on its own threads |
| Kafka `AdminClient.list_topics`, `Consumer.committed`, `Consumer.get_watermark_offsets` | `asyncio.to_thread` | No: confluent-kafka has no async API for these, so they run in a worker thread, off the event loop |
| dbt artifact reads (`target/`, `state/`) | `asyncio.to_thread` for the read and JSON parse | Local file I/O in a worker thread |

## Context

The tools were declared `async def`, but every gateway call blocked: synchronous `httpx`, `AdminClient` futures waited on with `.result(timeout=10.0)`, blocking `Consumer` calls, and file reads. LangGraph's tool node runs a turn's tool calls concurrently (`asyncio.gather`), and the model does issue several per turn: a live run produced 9 signals in 6 model calls. But a blocking call inside an `async def` holds the event loop, so those calls ran one after another.

Against the local Docker stack the cost was under a second, because model latency dominates. Against a slow or unreachable backend it adds up: three Kafka tools that each hit a 10s timeout would take about 30s instead of 10s. It would also stall every other session if investigations ever shared a process.

## How it works

- **Client lifecycle.** An `httpx.AsyncClient` must be closed in the event loop that used it. Each HTTP-backed live gateway has `aclose()`. The CLI builds live gateways inside the coroutine `asyncio.run` executes and closes them with an `AsyncExitStack`, however the diagnosis ends. `aclose` isn't part of the Protocols, because fixture gateways own no resources.
- **Kafka `Consumer` calls** run in a single worker thread from creation to `close()`, so a `Consumer` is never shared across threads. The `AdminClient` is shared across threads, which librdkafka handles support.
- **One metrics scrape in flight.** Concurrent Kafka tool calls could each miss the 5-second JMX cache and all scrape the aggregator, which can take seconds. An `asyncio.Lock` makes them share one scrape.
- **Errors are unchanged.** The gateways still raise `httpx.HTTPError`, `KafkaException` or `TimeoutError`. `asyncio.wait_for` raises the builtin `TimeoutError`, and `asyncio.to_thread` re-raises the thread's exception, so the tool-error middleware and the retry middleware (whose async path already used `asyncio.sleep`) needed no change.

`tests/integration/test_parallel_tool_calls.py` runs the real agent loop with a scripted model that issues three tool calls, one per system, in one message. It checks that all three start before any finishes. Replacing one gateway's `await asyncio.sleep` with a blocking `time.sleep` makes it fail. Per-gateway tests check the same property for each I/O mechanism above.

## Alternatives considered

| Alternative | Why it was rejected |
| --- | --- |
| Keep the gateways synchronous and run each call in `asyncio.to_thread` from the tools | Much less change, and the same overlap today. But a cancelled or timed-out call keeps running in its thread until the library's own timeout, concurrency is capped by the default thread pool, and `async def` wouldn't mean what it says. |
| aiokafka instead of confluent-kafka | Would make the `Consumer` offset and watermark lookups natively async, but replaces the client the live gateway was verified against, including the `PLAINTEXT_HOST` listener setup in [docker.md](../docker.md). Left for a separate change. |

## Consequences

- A turn's tool calls to different systems overlap, and a slow backend costs roughly its own latency, not the sum.
- Cancelling a call to an HTTP backend actually cancels it. Cancelling a Kafka `list_topics` or `Consumer` call returns promptly, but its worker thread finishes the library call (up to 10s). An awaited admin future that's cancelled is discarded when librdkafka completes it.
- `topic_high_watermarks` still queries partitions one after another within its worker thread. Parallelizing that is a separate optimization.
- Gateway code has to stay non-blocking. A synchronous call slipped into an `async def` gateway method would reintroduce the problem silently. The overlap and ticker tests catch it for the methods they cover, not for every method.
