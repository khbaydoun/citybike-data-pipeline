# citybike-data-pipeline

A real-time pipeline that tracks activity at Citibike NYC stations and answers questions about it with low latency.

```
event_generator.py → Redpanda → ingestion → Redis → HTTP API (FastAPI)
```

## Project structure

```
├── docker-compose.yml        full local stack: Redpanda, topic-init, Redis, ingestion, API
├── event_generator.py        provided generator (unmodified)
├── tools/run_generator.py    wrapper: fixes the generator's args.brokers typo, publishes several files
├── ingestion/                stream processor (Redpanda → Redis)
│   ├── ingestion/main.py         consumer loop: poll → validate → apply → commit offsets
│   ├── ingestion/models.py       event schema and validation
│   ├── ingestion/store.py        batched Redis writes (one round-trip per batch)
│   ├── ingestion/apply_event.lua atomic per-event update: dedup, counters, last activity, pairing
│   └── tests/
├── api/                      FastAPI service (Redis → HTTP)
│   ├── api/main.py               routes, 404/503 handling, /health, /ready
│   ├── api/repository.py         Redis reads (the only place that knows key names)
│   ├── api/schemas.py            response models = the public contract
│   └── tests/
├── infra/terraform/          Kubernetes deployment on minikube (Terraform / OpenTofu)
├── helpers/
│   ├── inspect_data.py           data profiling (DuckDB)
│   └── acceptance_check.py       end-to-end check: live API vs. DuckDB over the same CSVs
├── docs/
│   ├── api-contract.md           endpoint responses and status codes
│   └── data-profile.md           data findings that drove the design
├── requirements-dev.txt      local tools (generator client, DuckDB, tests)
└── data/                     Citibike CSVs (git-ignored, not submitted)
```

## How to run

```bash
git clone https://github.com/khbaydoun/citybike-data-pipeline.git
cd citybike-data-pipeline
mkdir -p data        # not in git: put the downloaded Citibike CSVs here
```

**Prerequisites:** Docker Desktop (≥ 6 GB memory), Python 3.10+, and Citibike trip data in `data/` (see *Data* below). For Kubernetes also: minikube, Terraform ≥ 1.6 (or OpenTofu) and kubectl. On macOS: `brew install minikube kubectl hashicorp/tap/terraform`.

**Data.** Download trip data from https://citibikenyc.com/system-data and unzip it anywhere under `data/`. File names and sub-folders don't matter: yearly archives unzip into nested folders, and the commands below find every CSV with `find`.
- **Format:** the current Citibike columns (`ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, …`, used since 2021). Check with `head -1 <file>.csv`. Older files (`tripduration, starttime, …`) aren't supported by the provided generator.
- **Size:** one month (≈ 5M rides, ≈ 1 GB in Redis) is the recommended amount. Redis memory grows with the number of rides replayed within the ride-key TTL (48 h): for a fast replay of more than a month, lower it (`RIDE_TTL_S=3600`).


There are two ways to run the same pipeline, with the same images and settings: **Docker Compose** (below) or **Kubernetes via Terraform** (next subsection). Everything after startup (generator, API, acceptance check) is identical.

### Run with Docker Compose

```bash
# 1. Start the full stack (Redpanda, topic, Redis, ingestion, API)
docker compose up -d --build

# 2. Local tools (generator client, tests). Each service installs its own requirements.txt
#    inside its Docker image; locally you only need requirements-dev.txt.
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

# 3. Publish events. Download a month from https://citibikenyc.com/system-data into data/
.venv/bin/python tools/run_generator.py --file $(find data -name '*.csv' | sort) --broker localhost:19092
#    default pace is ~10 events/s (real time); add --burst 1000 --interval 0.01 for a fast replay

# 4. Query the API (interactive docs: http://localhost:8000/docs)
curl localhost:8000/stations/busiest
STATION=$(curl -s localhost:8000/stations/busiest | python3 -c 'import sys, json; print(json.load(sys.stdin)["station_id"])')
curl localhost:8000/stations/$STATION/last-activity
curl localhost:8000/stations/$STATION/bike-balance
curl localhost:8000/stations/$STATION/trip-stats

# 5. Verify: compare the API with values computed independently by DuckDB from the same CSVs
#    (waits for ingestion to catch up first)
.venv/bin/python helpers/acceptance_check.py --file $(find data -name '*.csv' | sort)   # the same files you published

# Watch it work
docker compose logs -f ingestion                                   # throughput, duplicates, latency every 10 s
docker compose exec redpanda rpk group describe citibike-ingestion  # partition ownership and lag

# Reset to a clean state (the topic keeps events forever; republishing without a reset would send duplicates)
docker compose down -v && docker compose up -d --build

# Unit / integration tests (need Redis from step 1)
cd ingestion && ../.venv/bin/python -m pytest -q && cd ../api && ../.venv/bin/python -m pytest -q
```

### Run on Kubernetes (minikube + Terraform)

```bash
docker compose down                                    # free Docker Desktop memory
docker compose build                                   # build the two service images
minikube start --driver=docker --cpus=4 --memory=5g
minikube image load citybike-ingestion:local citybike-api:local

cd infra/terraform && terraform init && terraform apply   # prints the next steps (output "next_steps")
kubectl -n citibike get pods                           # redpanda, redis, ingestion, 2x api Running; topic-init Completed

# Reach the cluster from the Mac (two terminals, keep running)
kubectl -n citibike port-forward svc/api 8000:8000
kubectl -n citibike port-forward svc/redpanda 19092:19092

# From here, the same commands as with compose: generator, curl, acceptance check
```

Checks that show the Kubernetes design at work (from `infra/terraform/`):

```bash
terraform plan                                      # "No changes": cluster matches the config
terraform apply -var ingestion_replicas=3           # scale consumers; rpk group describe shows 2/2/2
kubectl -n citibike scale deployment api --replicas=1 && terraform plan   # drift detected: 1 -> 2
kubectl -n citibike scale statefulset redis --replicas=0                  # Redis outage:
curl -i localhost:8000/ready                        #   503; api pods NotReady but not restarted
terraform apply                                     #   Redis back, data intact (persistent volume)
kubectl -n citibike delete pod redis-0              # self-healing: recreated with the same volume
```

Run compose **or** minikube, not both: they share Docker Desktop's memory and the host ports 8000/19092. To switch back: stop the port-forwards, `minikube stop`, `docker compose up -d`.

The `.tf` files also run unchanged with OpenTofu (`tofu init && tofu apply`). Tear down with `terraform destroy` or `minikube delete`.

## Testing and results

**Automated tests (25).** `ingestion/tests` covers validation and the Lua script's edge cases: duplicates, end before start, late older events, same-timestamp ties, non-positive durations, round trips, and station IDs kept as text. `api/tests` covers every endpoint, 404/503, and the Redis contract with ingestion, with test data written by the real ingestion code. Both run against a real Redis (db 15).

**End-to-end acceptance.** `helpers/acceptance_check.py` recomputes every station's expected answers with DuckDB, straight from the CSVs, and compares them with the live API for the busiest station, the top N and a random sample.

**Results: full June 2026 (6 files, one month)**, MacBook, Docker Desktop (8 GB), 1 consumer instance:

| Metric | Result |
|---|---|
| Events | 10,735,194 published, 10,735,194 applied, 0 duplicates, 0 invalid |
| Correctness | **212/212 checks passed** (busiest + all endpoints for 30 stations). The six files were replayed one after another, and each spans the whole month, so event time jumped back five times. The results are still exact, which confirms the logic is order-independent |
| Throughput | ~13.5k events/s over 13.4 min, **limited by the provided generator** (pure-Python client). Consumer lag stayed at a few hundred events and ended at 0 |
| Latency (generator → Redis) | **~220 ms** at real-time pace (bounded by the 0.2 s poll timeout). During the fast replay the measured value rose to seconds because messages queue inside the generator's own client, while consumer lag stayed at a few hundred events |
| Redis memory | 944 MB for 5.37M keys: 2,391 station hashes, 1 sorted set, and ~5.37M ride keys, which expire 48 h after their last write |
| Redelivery | Republishing the same rides: all rejected as duplicates, no counts changed |
| Restart | Redis restarted mid-run: state reloaded from AOF, ingestion retried and continued. API returned `503` meanwhile, `/health` stayed `200` |
| Scaling | 1 → 3 consumers: partitions split 2/2/2. 8 consumers: 6 active, 2 idle standbys |
| Kubernetes (minikube) | 10k-ride sample (19,990 events) published from the Mac through `port-forward`: **72/72 checks passed**, 0 duplicates, max latency 477 ms |
| Multiple months + nested folders | July 2024 (in its own sub-folder) + June 2026 (top level), published together via `find`: **72/72 checks** against both months combined. The months share 0 `ride_id`s, and republishing the already-loaded June sample was fully rejected as duplicates |

Busiest station for the month: **`6140.05` (W 21 St & 6 Ave)**, 36,210 events, bike balance +52, average trip 646.9 s departing / 649.3 s arriving.

## Configuration

Set these as environment variables or in a `.env` file next to `docker-compose.yml` (e.g. `PARTITIONS=12 docker compose up -d`).

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `TOPIC` | `citibike-events` | topic-init | Topic the generator publishes to |
| `PARTITIONS` | `6` | topic-init | Partition count, which caps consumer parallelism |
| `RIDE_TTL_S` (Terraform: `ride_ttl_s`) | `172800` (48 h) | ingestion | How long a ride's first half waits for its partner in Redis. It's the pairing window, and it drives Redis memory |

The topic is created once. To change `PARTITIONS` afterwards, reset with `docker compose down -v` (this deletes all data).

## Scaling the consumers

All ingestion instances share one consumer group (`GROUP_ID`, default `citibike-ingestion`). Kafka assigns each partition to exactly one instance, so adding instances splits the partitions among them. No other configuration is needed.

```bash
# Docker Compose
docker compose up -d --scale ingestion=3

# Kubernetes (minikube), through Terraform so the state doesn't drift
terraform -chdir=infra/terraform apply -var ingestion_replicas=3

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
- **Why Terraform for the Kubernetes deployment?** `plan` shows every change before it happens, state tracking makes `destroy` clean, and it handles ordering (the topic Job must complete before consumers start). Replicas and partitions are variables. In production the same tool would also create the cluster, managed Kafka and Redis, so everything is managed in one place.
- **Why StatefulSets for Redpanda and Redis, and Deployments for ingestion and API?** Only Redpanda and Redis have data on disk. They need a stable identity and their own persistent volume. The consumers and the API are stateless, so any pod can replace any other and they scale by changing `replicas`.
- **Why our own small Redpanda StatefulSet, not the official Helm chart?** The chart expects cert-manager and needs more memory than a laptop minikube has. A single-node StatefulSet with the same flags as compose is about 80 lines and fully explainable. The chart or Operator is the production path.
- **Why 2 API replicas but 1 consumer?** An API outage is visible to users, so 2 replicas keep it up during restarts and rolling updates (readiness pulls a broken pod out of the Service). A consumer outage only delays freshness, because events wait in Kafka, so 1 is enough at this traffic.
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
- **Cluster access:** `kubectl port-forward` is a developer tunnel. Production would expose the API through an Ingress or LoadBalancer, and producers would reach Kafka through proper external listeners with TLS.
- **Images:** loaded with `minikube image load`. Production would push versioned images to a registry.
- **Ingestion liveness:** the consumer has no HTTP port. A hung consumer is evicted by Kafka (`max.poll.interval.ms`), a crash is restarted by Kubernetes. A heartbeat-file liveness probe would be a production addition.
- **Observability:** logs only (throughput, duplicates, latency every 10 s). Production would export metrics (consumer lag, latency, 5xx rate) to Prometheus with alerts.
