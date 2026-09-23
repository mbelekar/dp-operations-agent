# Running against live infra

**Status: optional.** The fixture-based path (`./auto/run diagnose --fixture ...`) stays the default way to run this project. It's instant and needs no Docker. This page covers a separate, opt-in path: a real 3-broker Kafka cluster and a real Flink job, run locally with Docker, so the `Live*Gateway` implementations can be exercised against actual infra instead of canned JSON. It exists to prove those gateways work, not to replace the fixture demo.

## What gets stood up

`docker-compose.yml` defines:

- A 3-broker Kafka cluster (`confluentinc/cp-kafka`, KRaft mode, no Zookeeper), plus a `kafka-init` step that creates the `orders` and `orders-sink` topics with real replication (6 partitions, RF 3).
- A `kafka-seed` step that produces 200 deterministic order records (`docker/kafka-seed/orders.jsonl`) into `orders`, so there's real data to observe.
- Schema Registry.
- A JMX exporter per broker plus a small aggregator that merges them into one endpoint (`docker/jmx-exporter/`, `docker/metrics-aggregator/`). `LiveKafkaGateway` expects a single JMX-exporter URL; a standard `jmx_exporter` in HTTP-server mode only ever connects to one broker, so this merges three into one to keep that contract true without changing the gateway code.
- A Flink JobManager and TaskManager, running a small demo job (`docker/flink-job/`) that reads `orders`, applies a deliberately slow map (so there's something real for the backpressure tool to observe), and writes to `orders-sink`, with checkpointing on.
- Marquez (`marquezproject/marquez:0.51.1`, API only, no web UI) with its Postgres (`marquez-db`), plus a `marquez-seed` step that posts OpenLineage run events (`docker/marquez-seed/events.jsonl`) describing the demo job: `kafka/orders` → `flink/orders-processing-job` → `kafka/orders-sink`. The namespaces `kafka` and `flink` are what make Marquez's node ids come out as `dataset:kafka:{topic}` / `job:flink:{job_name}`, the convention the lineage tool uses (see [`lineage.md`](lineage.md#node-id-convention)). This lineage is static: it describes the job's topology, it isn't emitted by the running job, so the seed file has to change if the job's sources or sinks do.
- `app` (opt-in, see below): the agent itself, containerized.

Three Marquez details that aren't obvious from the compose file alone:

- **Host port 5002, not 5000.** macOS's AirPlay Receiver already listens on 5000. Inside the compose network it's still `marquez:5000`.
- **It runs under emulation on Apple Silicon.** Marquez publishes amd64-only images, despite [MarquezProject/marquez#2804](https://github.com/MarquezProject/marquez/issues/2804) promising arm images for `0.51.0`, so `platform: linux/amd64` is set explicitly. It works, but takes ~50s to become healthy, hence the healthcheck's 90s `start_period`.
- **`SEARCH_ENABLED=false`.** Marquez's default config expects an OpenSearch service; this stack doesn't run one.

Only `kafka-1` is reachable from the host if you look at its `ports:` mapping. Every broker also gets a second listener (`PLAINTEXT_HOST`, ports `29092`/`29093`/`29094`), because a Kafka client that bootstraps through one broker still needs to open direct connections to whichever broker actually leads each partition. Without this, a host-side client can list topics but times out doing anything else, this bit me during testing and is the reason both listeners exist.

## Running it

```
$ ./auto/live-up
```

Brings up everything except `app` (it's profile-gated, so a plain `docker compose up` never starts it). From scratch this takes ~4–5 minutes on an Apple Silicon Mac, most of it Kafka health checks and Marquez's emulated startup.

Memory, measured with `docker stats` on a 5 GiB Docker Desktop VM: the whole stack sits at ~2.8–3.1 GiB idle, of which Marquez + Postgres are ~0.5 GiB, and the `app` container adds a little more while it runs. Set Docker Desktop to **5 GB** or more. On a memory-constrained setup, `kafka-1` getting OOM-killed is the actual failure mode I hit earlier, not a code error.

From there, either point the CLI at it directly:

```
$ dp-ops-agent diagnose --live \
    --kafka-topics orders \
    --consumer-group flink-orders-processing \
    --flink-job-id <id from curl localhost:8082/jobs> \
    --flink-vertex-id <id from curl localhost:8082/jobs/<job-id>> \
    --flink-job-name orders-processing-job \
    --marquez-url http://localhost:5002 \
    --alert-text "PagerDuty: checkpoint and lag alert on orders-processing-job"
```

`--marquez-url` is needed from the host because the CLI's default is `localhost:5000`, which is AirPlay on macOS, not Marquez. `--flink-job-name` is what lets the model build the lineage node id (`job:flink:{job_name}`); the random `job_id` alone doesn't.

or run the agent as a container, which finds the running Flink job on its own (`docker/app-live-entrypoint.py` polls the Flink REST API instead of needing the ids passed in):

```
$ docker compose --profile app run --rm --no-deps -T app
```

Two flags there aren't optional, both are workarounds for things that failed silently otherwise:

- `-T` disables pseudo-TTY allocation. Without it, `docker compose run` produced zero output and exited 1 in a non-interactive shell, no error message, just silence.
- `--no-deps` skips Compose's own dependency startup. The Compose CLI on the machine this was built on (v2.2.3) has a real bug where `run`/`up <service>` re-triggers already-completed one-shot containers (`kafka-init`, `kafka-seed`, `flink-job-submitter`) and then fails before ever starting the requested service. Since `./auto/live-up` is already the documented first step, infra is already up by the time you run `app`, so skipping dependency resolution is safe here, not a hack around a missing dependency.

To steer the alert, pass `-e ALERT_TEXT="..."` to `docker compose run` (the `app` service doesn't forward it from your shell on its own). A Flink-worded alert such as `"PagerDuty: watermark lag alert on orders-processing-job"` is what exercises the lineage walk from Flink to Kafka. To keep the audit log after the container exits, add `-v "$PWD/logs:/home/dpops/logs"`; it lands in `./logs/audit/`.

**After changing Python code, rebuild the image:** `docker compose --profile app build app`. `./auto/live-up` never rebuilds `app` (it's profile-gated), but `docker/app-live-entrypoint.py` is mounted from the host, so the entrypoint can get ahead of the CLI baked into the image. That's exactly how a new CLI flag once failed with `No such option`.

`--live` needs `ANTHROPIC_API_KEY` in the environment either way (`export` it for the CLI path, pass it inline for the container: `ANTHROPIC_API_KEY=... docker compose --profile app run --rm --no-deps -T app`).

Tear down with:

```
$ ./auto/live-down
```

Removes containers and volumes. Kafka's KRaft log, Flink's checkpoint directory, and Marquez's database aren't meant to persist between runs of this demo stack.

## Backend failures during a diagnosis

If a live backend (JMX aggregator, Schema Registry, Flink REST, Marquez, or the Kafka brokers) is unreachable or returns an HTTP error mid-diagnosis, the session keeps going. A connection error or timeout is retried once. After that, the tool call comes back to the model as an error result naming the tool and exception, and the model carries on with its other tools. A failed call produces no signal, so it can't be cited as evidence, and each one is recorded in the audit log as a `tool_error` event (`./logs/audit/{session_id}.jsonl`, with the `-v` mount above). Any other exception is still treated as a bug and ends the session. See `src/dp_ops_agent/orchestrator/tool_errors.py`.

## Known gaps

- **`hot_partition_skew` has no real live data source.** It needs per-partition throughput, and Kafka's own JMX only exposes `BytesInPerSec` at broker and topic level, never per-partition, confirmed directly against a running broker's JMX, not assumed. It returns an empty result in live mode rather than crashing. Fixing this for real means computing throughput a different way (e.g. sampling `AdminClient.describe_log_dirs()` partition sizes over time), which is a real code change, not something this demo stack can paper over.
- **`watermark_lag` can be wrong for a vertex chaining multiple operators.** See `live_gateway.py`'s docstring. Harmless for the demo job's topology, not fixed in general.
- **JMX scrapes are slow right after startup.** Exporters have been seen taking 8–13s per scrape while the stack is still settling, against the aggregator's 8s per-exporter limit (`UPSTREAM_TIMEOUT_SECONDS`). A scrape that runs over now returns a `502` naming the exporter, and the agent reports it as a tool error (see above) instead of the session ending. This used to surface as `RemoteProtocolError: Server disconnected without sending a response`: the old aggregator fetched exporters one at a time with a 5s timeout, and Python's `BaseHTTPRequestHandler` answers a timeout by closing the socket, logging only through a `log_message` the aggregator had silenced. That's why no traceback was ever logged.
- **Re-running `./auto/live-up` on a running stack submits a second copy of the Flink job.** Run `./auto/live-down` first.

## Why this exists separately from the fixture demo

The fixture path is what the README leads with, and should stay that way: it's instant, deterministic, and needs nothing but Python. This page is for verifying the `Live*Gateway` code actually works, not for everyday use. If you're just trying the agent out, use `./auto/run`.
