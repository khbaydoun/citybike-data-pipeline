# citybike-data-pipeline

A real-time pipeline that tracks activity at Citibike NYC stations and answers questions about it with low latency.

```
event_generator.py → Redpanda → ingestion → Redis → HTTP API (FastAPI)
```

> Status: in progress. Sections marked _TODO_ are filled in as components land.

## How to run

```bash
# 1. Start the full stack (Redpanda, topic, Redis, ingestion, API)
docker compose up -d --build

# 2. Local tools (generator client, tests). Each service installs its own requirements.txt
#    inside its Docker image; locally you only need requirements-dev.txt.
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

# 3. Publish events. Download a month from https://citibikenyc.com/system-data into data/
.venv/bin/python tools/run_generator.py --file data/202606-citibike-tripdata_*.csv --broker localhost:19092
#    default pace is ~10 events/s (real time); add --burst 1000 --interval 0.01 for a fast replay

# 4. Query the API (interactive docs: http://localhost:8000/docs)
curl localhost:8000/stations/busiest
curl localhost:8000/stations/6140.05/last-activity
curl localhost:8000/stations/6140.05/bike-balance
curl localhost:8000/stations/6140.05/trip-stats

# Tests (need Redis from step 1)
cd ingestion && ../.venv/bin/python -m pytest -q && cd ../api && ../.venv/bin/python -m pytest -q
```

_TODO: minikube via Terraform (files also work with OpenTofu)._

## Configuration

Set these as environment variables or in a `.env` file next to `docker-compose.yml` (e.g. `PARTITIONS=12 docker compose up -d`).

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `TOPIC` | `citibike-events` | topic-init | Topic the generator publishes to |
| `PARTITIONS` | `6` | topic-init | Partition count, which caps consumer parallelism |

The topic is created once. To change `PARTITIONS` afterwards, reset with `docker compose down -v` (this deletes all data).

## Scaling the consumers

All ingestion instances share one consumer group (`GROUP_ID`, default `citibike-ingestion`). Kafka assigns each partition to exactly one instance, so adding instances splits the partitions among them. No other configuration is needed.

```bash
# Docker Compose
docker compose up -d --scale ingestion=3

# Kubernetes (minikube)
kubectl scale deployment ingestion --replicas=3

# Check which instance owns which partition, and the lag
docker compose exec redpanda rpk group describe citibike-ingestion
```

- **Maximum useful instances = partitions (6).** Extra instances stay idle as hot standbys.
- **To go beyond 6,** add partitions first: `rpk topic add-partitions citibike-events --num 6`. This moves stations to new partitions, so per-station ordering is briefly lost (the logic doesn't depend on it).
- **Each scale event triggers a rebalance.** With cooperative-sticky assignment, only the moving partitions pause. An instance that dies without leaving cleanly is detected after the session timeout (45 s), and its partitions pause until then.
- **Autoscaling** would use consumer lag, not CPU (e.g. KEDA's Kafka scaler). At this traffic, one instance never lags.

## Architecture

- **Redpanda** holds the raw event stream (topic `citibike-events`, 6 partitions, keyed by `station_id` by the provided generator).
- **ingestion** is a stateless Python consumer. It validates events, drops duplicates, pairs `trip_start`/`trip_end`, and updates per-station aggregates in Redis. Offsets are committed only after Redis confirms the writes.
- **Redis** is the serving layer and the only place state lives: one hash per station plus a sorted set for "busiest".
- **API** is a stateless FastAPI service. Every endpoint is an O(1)/O(log N) Redis lookup.

The data profile behind these choices is in [`docs/data-profile.md`](docs/data-profile.md), and the endpoint responses are in [`docs/api-contract.md`](docs/api-contract.md).

## Design decisions (why?)

- **Why a plain Python consumer, not Kafka Streams / Flink?** Three of the four endpoints are per-event updates (newest timestamp, +1/−1 counters, a ranked score), so no framework is needed. Only trip-stats needs a join of each ride's start with its end, and a Redis hash with a 48 h TTL does that. The design still follows stream-processing concepts: the topic is the stream, each station hash is an aggregate per key (a KTable), the ride hash is a windowed join (48 h ≈ its grace period), and Redis is the state store. Kafka Streams would add a JVM, a repartition topic by `ride_id`, local state with changelogs, and a stateful deployment, and we'd still have to write the results into Redis. At about 13 events/s peak, the hard part is correctness, not scale.
- **Why keep state in Redis and make consumers stateless?** Redis is the required serving layer anyway. Stateless consumers restart and scale freely, with no local store or changelog to restore, and both halves of a ride meet in Redis whichever consumer processes them.
- **Why this Redis model?** One hash per station answers last-activity, bike-balance and trip-stats with a single O(1) read (we pre-aggregate on write). "Busiest" compares all stations, so it uses a sorted set whose top is one lookup. Averages are stored as sum + count, so they can be updated in any order.
- **Why a `ride:{ride_id}` hash?** A duration needs both events of a ride. They're on different partitions (the generator keys by station) and arrive in either order, so the first one waits here for the second. It also serves as the dedup marker, and a 48 h TTL keeps memory bounded.
- **Why a Lua script per event?** Dedup, counter updates and pairing must be all-or-nothing. A crash or two concurrent consumers must never lose or double-count an event. Lua runs atomically inside Redis in one round-trip.
- **Why 6 partitions, created explicitly?** Partitions cap consumer parallelism, so they're sized for the most consumers we'd want, not today's count. Throughput alone needs 1 (≈13 events/s peak). 6 adds scale-out headroom and splits evenly across 1, 2, 3 or 6 consumers. We don't start with 1 for its total ordering: redeliveries, replays and multi-file runs break that order anyway, so the logic is order-independent regardless. Adding partitions later remaps keys, so the count is fixed up front, and auto-creation is disabled so the broker default (1) can't sneak in.
- **Why no compacted topic, and infinite retention?** The key is `station_id`, so compaction would keep only the last event per station. Keeping the full history lets us rebuild Redis by replaying the topic.
- **Why Redis AOF `everysec` + `noeviction`?** Redis is our only copy of the state, not a cache. It must survive restarts (at most about 1 s lost, covered by replay) and never silently evict keys.
- **Why REST polling, not SSE or WebSocket?** The spec asks for queries, not subscriptions. Stateless request/response scales behind any load balancer.
- **Why separate `/health` and `/ready`?** Liveness (`/health`) never touches Redis. Otherwise a Redis outage would make Kubernetes restart every API pod, which can't fix Redis. Readiness (`/ready`) checks Redis, so an affected pod just stops receiving traffic. Redis calls time out after 1 s, so an outage becomes a fast `503`, not hanging requests.
- **Why one uvicorn worker per container?** We scale with replicas (compose `--scale`, K8s Deployment), so each container runs one process that the orchestrator can see, restart and load-balance.

## Assumptions and decisions on unclear points

The challenge leaves some points open. These are the decisions we made:

1. **Generator bug.** Line 114 of `event_generator.py` logs `args.brokers`, but the CLI flag is `--broker`, so the script crashes with `AttributeError` before publishing (the producer itself uses the right attribute). Since the file must not be modified, `tools/run_generator.py` adds the missing attribute after the generator's own argument parsing and then calls its `main()` unchanged. The wrapper also accepts several files and runs them one after another, since June 2026 comes as six CSVs and loading them all at once would need about 10 GB of RAM.
2. **"Bike balance since stream start"** = arrivals (`trip_end`) − departures (`trip_start`) at the station. The events carry no dock-inventory snapshot, so this is **net flow**, not the number of bikes physically docked, and it can be negative.
3. **Trip stats** returns two averages: trips **departing from** the station and trips **arriving at** it. A duration is computed once both halves of a ride have been seen (the halves can arrive in either order). Round trips count in both.
4. **Invalid durations.** Durations ≤ 0 are excluded from trip stats as a defensive check (the June 2026 data has none; its minimum is 60 s). Such rides still count toward balance and busiest. Long rides (19 over 24 h) are kept as-is.
5. **"Since stream start"** = since the ingestion consumer group first read the topic from the earliest offset. Restarts and redeliveries don't double-count (idempotent processing). A full reset means emptying Redis and replaying the topic.
6. **Station IDs are opaque strings.** The data contains IDs like `SYS038`, `3184.07_OLD` and `Shop Morgan`, and 97 IDs end in `0`. They are never parsed as numbers, and the API accepts URL-encoded IDs.
7. **Unknown station** → `404`. A known station with no completed trips → averages are `null`.
8. **Ties for busiest** are broken deterministically: the lexicographically highest station ID wins (Redis `ZREVRANGE` order for equal scores).
9. **The generator isn't part of `docker compose up`.** The challenge lists the stack as Redpanda, ingestion, Redis and API, and shows the generator run from the host against `localhost:19092`.

## Delivery guarantees and failure modes

- **At-least-once + idempotent sink = effectively-once.** Kafka transactions only cover Kafka→Kafka, and Redis is outside them. Each event is applied by one atomic Lua script that first checks a per-ride dedup marker.
- **Consumer crash:** there is no local state, so no changelog to restore. The group rebalances (or the pod restarts), the uncommitted batch is redelivered, and duplicates are rejected. The cost is freshness only.
- **Redis crash:** AOF `everysec` can lose ≤ 1 s of writes whose offsets were already committed. Recovery: empty Redis and replay the topic. Production upgrade: store consumer offsets in Redis atomically with the state.
- **Malformed event:** logged and skipped. It never blocks a partition.
- **Redpanda down:** the API keeps serving the last known state.

## Limitations and what we'd change in production

- **Replication:** RF 1 locally (single broker). Production: RF 3 with `min.insync.replicas=2`.
- **Redis Cluster:** the multi-key Lua script assumes a single Redis node.
- **Edge concerns:** rate limiting, auth and TLS belong in an ingress or API gateway, not in the app.
- **Extending:** more per-station counters or hourly buckets fit the current Lua and Redis approach. Sliding windows with late-event rules, joins with other streams, or new output topics would justify Kafka Streams or Flink. That job would run as a separate consumer group that backfills from the retained topic, leaving the running pipeline untouched.
- _TODO: metrics and alerting._
