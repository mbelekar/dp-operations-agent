# Running against live infra

**Status: optional.** The fixture-based path (`./auto/run diagnose --fixture ...`) stays the default way to run this project. It's instant and needs no Docker. This page covers a separate, opt-in path: a real 3-broker Kafka cluster and a real Flink job, run locally with Docker, so the `Live*Gateway` implementations can be exercised against actual infra instead of canned JSON. It exists to prove those gateways work, not to replace the fixture demo.

## What gets stood up

`docker-compose.yml` defines:

- A 3-broker Kafka cluster (`confluentinc/cp-kafka`, KRaft mode, no Zookeeper), plus a `kafka-init` step that creates the `orders` and `orders-sink` topics with real replication (6 partitions, RF 3).
- A `kafka-seed` step that produces 200 deterministic order records (`docker/kafka-seed/orders.jsonl`) into `orders`, so there's real data to observe.
- Schema Registry.
- A JMX exporter per broker plus a small aggregator that merges them into one endpoint (`docker/jmx-exporter/`, `docker/metrics-aggregator/`). `LiveKafkaGateway` expects a single JMX-exporter URL; a standard `jmx_exporter` in HTTP-server mode only ever connects to one broker, so this merges three into one to keep that contract true without changing the gateway code.
- A Flink JobManager and TaskManager, running a small demo job (`docker/flink-job/`) that reads `orders`, applies a deliberately slow map (so there's something real for the backpressure tool to observe), and writes to `orders-sink`, with checkpointing on.
- `app` (opt-in, see below): the agent itself, containerized.

Only `kafka-1` is reachable from the host if you look at its `ports:` mapping. Every broker also gets a second listener (`PLAINTEXT_HOST`, ports `29092`/`29093`/`29094`), because a Kafka client that bootstraps through one broker still needs to open direct connections to whichever broker actually leads each partition. Without this, a host-side client can list topics but times out doing anything else, this bit me during testing and is the reason both listeners exist.

## Running it

```
$ ./auto/live-up
```

Brings up everything except `app` (it's profile-gated, so a plain `docker compose up` never starts it). Needs a few GB of free memory, this stack is meaningfully heavier than anything else in this repo. On a memory-constrained Docker Desktop setup, `kafka-1` getting OOM-killed is the actual failure mode I hit, not a code error, raising Docker's memory limit (and a bit of swap) fixed it.

From there, either point the CLI at it directly:

```
$ dp-ops-agent diagnose --live \
    --kafka-topics orders \
    --consumer-group flink-orders-processing \
    --flink-job-id <id from curl localhost:8082/jobs> \
    --flink-vertex-id <id from curl localhost:8082/jobs/<job-id>> \
    --alert-text "PagerDuty: checkpoint and lag alert on orders-processing-job"
```

or run the agent as a container, which finds the running Flink job on its own (`docker/app-live-entrypoint.py` polls the Flink REST API instead of needing the ids passed in):

```
$ docker compose --profile app run --rm --no-deps -T app
```

Two flags there aren't optional, both are workarounds for things that failed silently otherwise:

- `-T` disables pseudo-TTY allocation. Without it, `docker compose run` produced zero output and exited 1 in a non-interactive shell, no error message, just silence.
- `--no-deps` skips Compose's own dependency startup. The Compose CLI on the machine this was built on (v2.2.3) has a real bug where `run`/`up <service>` re-triggers already-completed one-shot containers (`kafka-init`, `kafka-seed`, `flink-job-submitter`) and then fails before ever starting the requested service. Since `./auto/live-up` is already the documented first step, infra is already up by the time you run `app`, so skipping dependency resolution is safe here, not a hack around a missing dependency.

`--live` needs `ANTHROPIC_API_KEY` in the environment either way (`export` it for the CLI path, pass it inline for the container: `ANTHROPIC_API_KEY=... docker compose --profile app run --rm --no-deps -T app`).

Tear down with:

```
$ ./auto/live-down
```

Removes containers and volumes. Kafka's KRaft log and Flink's checkpoint directory aren't meant to persist between runs of this demo stack.

## Known gaps

- **`hot_partition_skew` has no real live data source.** It needs per-partition throughput, and Kafka's own JMX only exposes `BytesInPerSec` at broker and topic level, never per-partition, confirmed directly against a running broker's JMX, not assumed. It returns an empty result in live mode rather than crashing. Fixing this for real means computing throughput a different way (e.g. sampling `AdminClient.describe_log_dirs()` partition sizes over time), which is a real code change, not something this demo stack can paper over.
- **`watermark_lag` can be wrong for a vertex chaining multiple operators.** See `live_gateway.py`'s docstring. Harmless for the demo job's topology, not fixed in general.

## Why this exists separately from the fixture demo

The fixture path is what the README leads with, and should stay that way: it's instant, deterministic, and needs nothing but Python. This page is for verifying the `Live*Gateway` code actually works, not for everyday use. If you're just trying the agent out, use `./auto/run`.
