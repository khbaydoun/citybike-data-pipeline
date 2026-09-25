# citybike-data-pipeline

A real-time pipeline that tracks activity at Citibike NYC stations and answers questions about it with low latency.

```
event_generator.py → Redpanda → ingestion → Redis → HTTP API (FastAPI)
```

> Status: in progress. Sections marked _TODO_ are filled in as components land.

## How to run

_TODO (docker compose, generator, API examples, minikube via OpenTofu)._

## Configuration

Set these as environment variables or in a `.env` file next to `docker-compose.yml` (e.g. `PARTITIONS=12 docker compose up -d`).

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `TOPIC` | `citibike-events` | topic-init | Topic the generator publishes to |
| `PARTITIONS` | `6` | topic-init | Partition count, which caps consumer parallelism |

The topic is created once. To change `PARTITIONS` afterwards, reset with `docker compose down -v` (this deletes all data).

## Architecture

- **Redpanda** holds the raw event stream (topic `citibike-events`, 6 partitions, keyed by `station_id` by the provided generator).
- **ingestion** is a stateless Python consumer. It validates events, drops duplicates, pairs `trip_start`/`trip_end`, and updates per-station aggregates in Redis. Offsets are committed only after Redis confirms the writes.
- **Redis** is the serving layer and the only place state lives: one hash per station plus a sorted set for "busiest".
- **API** is a stateless FastAPI service. Every endpoint is an O(1)/O(log N) Redis lookup.

The data profile behind these choices is in [`docs/data-profile.md`](docs/data-profile.md), and the endpoint responses are in [`docs/api-contract.md`](docs/api-contract.md).

## Design decisions (why?)

- **Why a plain Python consumer, not Flink / Spark / Kafka Streams?** The real peak is about 13 events/s. The hard part is correctness (duplicates, out-of-order events, start/end pairing), not scale.
- **Why keep state in Redis and make consumers stateless?** Redis is the required serving layer anyway. Stateless consumers restart and scale freely, with no local store or changelog to restore, and both halves of a ride meet in Redis whichever consumer processes them.
- **Why this Redis model?** One hash per station answers last-activity, bike-balance and trip-stats with a single O(1) read (we pre-aggregate on write). "Busiest" compares all stations, so it uses a sorted set whose top is one lookup. Averages are stored as sum + count, so they can be updated in any order.
- **Why a `ride:{ride_id}` hash?** A duration needs both events of a ride. They're on different partitions (the generator keys by station) and arrive in either order, so the first one waits here for the second. It also serves as the dedup marker, and a 48 h TTL keeps memory bounded.
- **Why a Lua script per event?** Dedup, counter updates and pairing must be all-or-nothing. A crash or two concurrent consumers must never lose or double-count an event. Lua runs atomically inside Redis in one round-trip.
- **Why 6 partitions, created explicitly?** Partitions cap consumer parallelism, and 6 splits evenly across 1, 2, 3 or 6 consumers. Auto-creation is disabled, so the broker default (1 partition) can't sneak in.
- **Why no compacted topic, and infinite retention?** The key is `station_id`, so compaction would keep only the last event per station. Keeping the full history lets us rebuild Redis by replaying the topic.
- **Why Redis AOF `everysec` + `noeviction`?** Redis is our only copy of the state, not a cache. It must survive restarts (at most about 1 s lost, covered by replay) and never silently evict keys.
- **Why REST polling, not SSE or WebSocket?** The spec asks for queries, not subscriptions. Stateless request/response scales behind any load balancer.

## Assumptions and decisions on unclear points

The challenge leaves some points open. These are the decisions we made:

1. **Generator bug.** `event_generator.py` reads `args.brokers`, but the CLI flag is `--broker`, so publishing fails with `AttributeError`. Since the file must not be modified, we run it through a thin wrapper (`tools/run_generator.py`) that supplies the missing attribute. The generator's logic is unchanged.
2. **"Bike balance since stream start"** = arrivals (`trip_end`) − departures (`trip_start`) at the station. The events carry no dock-inventory snapshot, so this is **net flow**, not the number of bikes physically docked, and it can be negative.
3. **Trip stats** returns two averages: trips **departing from** the station and trips **arriving at** it. A duration is computed once both halves of a ride have been seen (the halves can arrive in either order). Round trips count in both.
4. **Invalid durations.** Durations ≤ 0 are excluded from trip stats as a defensive check (the June 2026 data has none; its minimum is 60 s). Such rides still count toward balance and busiest. Long rides (19 over 24 h) are kept as-is.
5. **"Since stream start"** = since the ingestion consumer group first read the topic from the earliest offset. Restarts and redeliveries don't double-count (idempotent processing). A full reset means emptying Redis and replaying the topic.
6. **Station IDs are opaque strings.** The data contains IDs like `SYS038`, `3184.07_OLD` and `Shop Morgan`, and 97 IDs end in `0`. They are never parsed as numbers, and the API accepts URL-encoded IDs.
7. **Unknown station** → `404`. A known station with no completed trips → averages are `null`.
8. **Ties for busiest** are broken deterministically (lexicographically by station ID, as Redis sorted sets order equal scores).
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
- _TODO: metrics and alerting._
