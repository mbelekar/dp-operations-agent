# Running against live infrastructure

The Docker environment runs the agent against real Kafka, Flink, and Marquez services. Use it to verify the live gateways and observe real metrics.

For normal demos and automated tests, use fixtures instead. They are faster, deterministic, and require only Python.

## Before you start

You need:

- Docker Desktop with at least **5 GB of memory**;
- about **4–5 minutes** for the first startup on Apple Silicon; and
- `ANTHROPIC_API_KEY` if you want to run the agent.

The stack uses about 2.8–3.1 GiB while idle. With less memory, `kafka-1` may be killed by Docker.

## What the stack runs

| Service | Purpose |
| --- | --- |
| Three Kafka brokers | A KRaft cluster with `orders` and `orders-sink` topics. Each topic has six partitions and replication factor three. |
| `kafka-seed` | Publishes 200 deterministic JSON orders. |
| Schema Registry | Provides schema metadata. |
| JMX exporters and aggregator | Expose broker metrics through one endpoint. |
| Flink JobManager and TaskManager | Run a demo job that slowly processes `orders` and writes to `orders-sink`, with checkpointing enabled. |
| Marquez 0.51.1 and Postgres | Store a static lineage graph: `kafka/orders` → `flink/orders-processing-job` → `kafka/orders-sink`. |
| `app` profile | Runs the agent inside Docker when requested. |

Marquez is API-only and runs without OpenSearch (`SEARCH_ENABLED=false`). Its seed data is static, so update the seed if the demo topology changes. Lineage nodes are identified by namespace and name.

## Start the services

```bash
./auto/live-up
```

This starts everything except the optional `app` container.

Running `live-up` more than once can create duplicate Flink jobs. Run `./auto/live-down` before starting the stack again.

## Run the agent from your host

First, get the Flink job and vertex IDs:

```bash
curl localhost:8082/jobs
curl localhost:8082/jobs/<job-id>
```

Then run:

```bash
dp-ops-agent diagnose --live \
    --kafka-topics orders \
    --consumer-group flink-orders-processing \
    --flink-job-id <id from curl localhost:8082/jobs> \
    --flink-vertex-id <id from curl localhost:8082/jobs/<job-id>> \
    --flink-job-name orders-processing-job \
    --marquez-url http://localhost:5002 \
    --alert-text "PagerDuty: checkpoint and lag alert on orders-processing-job"
```

The host uses Marquez port `5002` because macOS AirPlay may already use port `5000`. Containers still reach it at `marquez:5000`.

Pass `--flink-job-name` as well as the job ID. The name is used to build the lineage node; the ID alone is not enough.

## Run the agent inside Docker

```bash
docker compose --profile app run --rm --no-deps -T app
```

Important options:

- `-T` disables the pseudo-terminal and is required for non-interactive runs. Without it, the command may fail silently.
- `--no-deps` prevents older Compose versions, including v2.2.3, from rerunning one-shot setup containers. Use it only after `./auto/live-up` has completed.

Set a custom alert:

```bash
docker compose --profile app run --rm --no-deps -T \
  -e ALERT_TEXT="PagerDuty: checkpoint alert on orders-processing-job" app
```

A Flink alert also exercises lineage lookup.

To keep audit logs on the host:

```bash
docker compose --profile app run --rm --no-deps -T \
  -v "$PWD/logs:/home/dpops/logs" app
```

Export `ANTHROPIC_API_KEY` on the host or pass it to the container explicitly.

After changing Python code, rebuild the app image:

```bash
docker compose --profile app build app
```

`./auto/live-up` does not rebuild the profile-gated app image. Rebuilding prevents a newly mounted entrypoint from running against old packages in the image.

## Stop and remove the stack

```bash
./auto/live-down
```

This removes the containers and volumes. The live environment is not intended to preserve data.

## Platform notes

### Marquez on Apple Silicon

The Marquez image runs through amd64 emulation. It may take about 50 seconds to become healthy, so its health check uses a 90-second start period.

### Kafka listeners

Only `kafka-1` exposes its main host port directly. All three brokers also advertise host listeners on ports `29092`, `29093`, and `29094`, allowing host clients to contact whichever broker leads a partition.

### Host ports

Every published port is bound to `127.0.0.1`, so the stack is reachable only from this machine. None of its services require authentication, and Flink's REST API accepts and runs uploaded jars.

### Updating pinned images

Every image, in the compose file and the Dockerfiles, is pinned as `image:tag@sha256:<digest>`: Docker pulls the digest, and the tag is there for readers. Nothing updates the pins automatically. To take a new build of a tag, get its current digest and replace the old one:

```bash
docker buildx imagetools inspect python:3.14-slim | awk '/^Digest:/{print $2}'
```

The JMX exporter jar is pinned the same way, by `JMX_EXPORTER_SHA256` in `docker/jmx-exporter/Dockerfile`. Its comment says how that hash was verified.

## What happens when a backend fails

For connection failures, timeouts, missing resources, and HTTP errors, the agent retries once when appropriate. If the retry also fails:

- the tool returns an error to the model;
- the rest of the diagnosis session continues;
- the failed call produces no signal and cannot be cited as evidence; and
- the audit log records a `tool_error` event.

An unexpected exception is treated as a software bug and ends the session. See `src/dp_operations_agent/tool_errors.py` for the classification rules.

## Known limitations

| Area | Current limitation |
| --- | --- |
| Kafka hot-partition skew | Kafka JMX does not expose the required per-partition `BytesIn` data, so the live check returns `unknown`. Sampling log-directory sizes could provide this later. |
| Schema compatibility | The live command provides neither a candidate schema nor a POST endpoint, so this check returns `unknown`. |
| Flink state-backend disk pressure | The demo job does not expose `disk_used_ratio` or RocksDB state, so this check returns `unknown`. |
| Flink watermarks | Metric discovery is less reliable when multiple operators are chained into one vertex. |
| JMX startup | Initial scrapes can take 8–13 seconds, longer than the aggregator's 8-second exporter limit. During startup this can produce a `502` and a recoverable tool error. |
| dbt | The stack has no dbt project, so dbt tools return errors while the rest of the session continues. Point the environment or command flags at a real dbt target to use them. See [dbt diagnostics](dbt.md). |
| Repeated startup | Rerunning `live-up` can submit another copy of the Flink job. Run `live-down` first. |

## Which mode should I use?

Use **fixtures** for local development, demos, tests, and evaluations.

Use the **Docker stack** when you need to verify the live gateways, connectivity, metric names, or behavior against real services.
