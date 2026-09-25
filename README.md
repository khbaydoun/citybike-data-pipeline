# citybike-data-pipeline

A real-time pipeline that ingests Citibike NYC trip events and answers station questions with low latency.

```
event_generator.py → Redpanda → ingestion → Redis → HTTP API (FastAPI)
```

**At a glance**
- **Verified end to end** on a full month (10.7M events): 212/212 API values match an independent DuckDB computation.
- **Duplicate-safe and order-independent:** redelivered events are rejected, and late or out-of-order events give the same result.
- **~220 ms** from generator to Redis at real-time pace. Throughput ~13.5k events/s, limited by the provided generator.
- Runs with **Docker Compose** or on **minikube via Terraform**, with the same images and settings.

## Architecture

| Component | Role |
|---|---|
| **Redpanda** | Event stream. Topic `citibike-events`, 6 partitions, keyed by `station_id` (set by the provided generator) |
| **ingestion** | Stateless Python consumer. Validates events, drops duplicates, pairs each ride's `trip_start`/`trip_end`, updates per-station aggregates. Commits offsets only after Redis confirms |
| **Redis** | Serving layer and the only place state lives: one hash per station + a sorted set for "busiest" |
| **API** | Stateless FastAPI service. Every endpoint is one or two O(1)/O(log N) Redis reads |

Endpoint responses: [`docs/api-contract.md`](docs/api-contract.md). Data findings behind the design: [`docs/data-profile.md`](docs/data-profile.md).

## How to run

**Prerequisites:** Docker Desktop (≥ 6 GB memory) and Python 3.10+. For Kubernetes also minikube, Terraform ≥ 1.6 (or OpenTofu) and kubectl (macOS: `brew install minikube kubectl hashicorp/tap/terraform`).

```bash
git clone https://github.com/khbaydoun/citybike-data-pipeline.git
cd citybike-data-pipeline
mkdir -p data
```

**Data:** download a month from https://citibikenyc.com/system-data and unzip it anywhere under `data/` (any file names or sub-folders). Files must be in the current Citibike format (2021 onwards, first column `ride_id`). One month is the recommended size (see `RIDE_TTL_S` under [Configuration](#configuration)).

### Option A: Docker Compose

```bash
# 1. Start the stack: Redpanda, topic, Redis, ingestion, API
docker compose up -d --build

# 2. Local tools (generator client, DuckDB, tests)
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

# 3. Publish the events (see *Generator run time* below; add --burst 1000 --interval 0.01 for a fast replay)
.venv/bin/python tools/run_generator.py --file $(find data -name '*.csv' | sort) --broker localhost:19092

# 4. Query the API (interactive docs: http://localhost:8000/docs)
curl localhost:8000/stations/busiest
STATION=$(curl -s localhost:8000/stations/busiest | python3 -c 'import sys, json; print(json.load(sys.stdin)["station_id"])')
curl localhost:8000/stations/$STATION/last-activity
curl localhost:8000/stations/$STATION/bike-balance
curl localhost:8000/stations/$STATION/trip-stats

# 5. Verify against DuckDB, computed from the same files (waits for ingestion to catch up)
.venv/bin/python helpers/acceptance_check.py --file $(find data -name '*.csv' | sort)
```

**Generator run time** (measured, one CSV file ≈ 1M rides ≈ 2M events):

| Input | Default pace (~10 events/s, real time) | Fast replay (`--burst 1000 --interval 0.01`, ~13.5k events/s) |
|---|---|---|
| 10k-row sample (20k events) | ~35 min | ~5 s |
| One CSV file (~2M events) | ~2.5 days | ~2.5 min |
| One month (~10M events, 5–6 files) | ~12 days | ~13–15 min |

Each file also takes ~20–30 s to load and sort before publishing starts. Ctrl-C is safe: rerunning the same command re-publishes from the start, events already applied are rejected as duplicates, and the result is identical to an uninterrupted run (within the 48 h ride-key TTL).

Useful extras:

```bash
docker compose logs -f ingestion                                    # throughput, duplicates, latency every 10 s
docker compose exec redpanda rpk group describe citibike-ingestion  # partition ownership and lag
docker compose down -v && docker compose up -d --build              # reset (the topic keeps events forever)
cd ingestion && ../.venv/bin/python -m pytest -q && cd ../api && ../.venv/bin/python -m pytest -q   # tests
```

### Option B: Kubernetes (minikube + Terraform)

Run it instead of Option A, not alongside: both use Docker's memory and host ports 8000/19092. If Option A is running, stop it first with `docker compose down`.

```bash
minikube start --driver=docker --cpus=4 --memory=5g
minikube image build -t citybike-ingestion:local ./ingestion   # Kubernetes runs images but doesn't build them:
minikube image build -t citybike-api:local ./api               # build them directly inside the cluster

cd infra/terraform
terraform init                          # once: downloads the Kubernetes provider (version locked in .terraform.lock.hcl)
terraform plan                          # preview: 9 resources to create
terraform apply                         # creates everything and prints the next steps
kubectl -n citibike get pods            # redpanda, redis, ingestion, 2x api Running; topic-init Completed

# Reach the cluster from the host (two terminals, keep running)
kubectl -n citibike port-forward svc/api 8000:8000
kubectl -n citibike port-forward svc/redpanda 19092:19092
# Then steps 2-5 of Option A work unchanged (venv, publish, query, verify).
```

Checks that show the Kubernetes design at work (from `infra/terraform/`):

```bash
terraform plan                                      # "No changes": cluster matches the config
terraform apply -var ingestion_replicas=3           # scale consumers: partitions split 2/2/2
kubectl -n citibike scale statefulset redis --replicas=0                  # Redis outage:
curl -i localhost:8000/ready                        #   503, /health still 200, api pods NotReady but not restarted
terraform apply                                     #   Redis back, data intact (persistent volume)
kubectl -n citibike scale deployment api --replicas=1 && terraform plan   # drift detected: 1 -> 2
```

A port-forward is bound to one pod: if a check removes that pod (the last one can), restart the port-forward.

Works unchanged with OpenTofu (`tofu init && tofu apply`). Tear down with `terraform destroy` or `minikube delete`.

## Testing and results

- **25 automated tests** against a real Redis. Ingestion: validation, duplicates, end before start, late events, ties, non-positive durations, round trips, station IDs kept as text. API: every endpoint, 404/503, and the Redis contract with ingestion.
- **Acceptance check** (`helpers/acceptance_check.py`): recomputes each station's expected answers with DuckDB from the CSVs and compares them with the live API.

Results on a MacBook (Docker Desktop, 8 GB), one consumer:

| Test | Result |
|---|---|
| Full month (June 2026, 6 files) | 10,735,194 events applied, 0 duplicates, 0 invalid. **212/212 checks passed** over 30 stations. Each file spans the whole month, so event time jumped back five times: the logic is order-independent |
| Throughput | ~13.5k events/s (13.4 min), limited by the provided generator. Consumer lag stayed at a few hundred events |
| Latency | ~220 ms at real-time pace. During fast replays it rises to seconds, from queueing inside the generator's own client |
| Redis memory | 944 MB after the full month, mostly ride keys waiting out their 48 h TTL |
| Duplicates | Republishing the same rides: all rejected, no count changed |
| Redis restart | State reloaded from AOF. Ingestion retried and continued. API returned `503`, `/health` stayed `200` |
| Scaling | 3 consumers: partitions split 2/2/2. 8 consumers: 6 active, 2 idle standbys |
| Kubernetes | 10k-ride sample through `port-forward`: 72/72 checks, 477 ms max latency |
| Two months | July 2024 (sub-folder) + June 2026: 72/72 checks over both combined |

Busiest station in June 2026: **`6140.05` (W 21 St & 6 Ave)**, 36,210 events, balance +52, average trip 646.9 s departing / 649.3 s arriving.

## Design decisions (why?)

**Streaming**
- **Why a plain Python consumer, not Kafka Streams / Flink?** Three of the four endpoints are simple per-event updates. The only join (a ride's start with its end) is a Redis hash with a TTL. At ~13 events/s peak the challenge is correctness, not scale. The design still maps onto stream-processing concepts: the topic is a KStream, each station hash a KTable, the ride hash a windowed join, and Redis the state store. Kafka Streams would add a JVM, a repartition topic and stateful deployments, and we'd still have to write the results into Redis.
- **Why 6 partitions, created explicitly?** Partitions cap consumer parallelism, so they're sized for the most consumers we'd want. Throughput alone needs 1. 6 divides evenly across 1, 2, 3 or 6 consumers. Adding partitions later remaps keys, so the count is fixed up front, and auto-creation is off so the broker default (1) can't slip in.
- **Why no compacted topic, and infinite retention?** The key is `station_id`, so compaction would keep only the last event per station. The full history lets us rebuild Redis by replaying the topic.

**State and Redis**
- **Why state in Redis, stateless consumers?** Redis is the required serving layer anyway. Consumers restart and scale freely with nothing to restore, and both halves of a ride meet in Redis whichever consumer processes them.
- **Why this Redis model?** One hash per station answers last-activity, bike-balance and trip-stats in one O(1) read (pre-aggregated on write). "Busiest" compares all stations, so it's a sorted set. Averages are stored as sum + count, so they can be updated in any order.
- **Why a `ride:{ride_id}` hash?** A ride's start and end land on different partitions and can arrive in either order, so the first half waits there for the second. It doubles as the dedup marker, and its TTL bounds memory.
- **Why a Lua script per event?** Dedup, counters and pairing must be all-or-nothing, so a crash or two concurrent consumers can never lose or double-count an event. Lua runs atomically inside Redis in one round-trip.
- **Why AOF `everysec` + `noeviction`?** Redis holds the only copy of the state, not a cache. It must survive restarts (≤ 1 s lost, covered by replay) and never silently evict keys.

**API**
- **Why REST polling, not SSE or WebSocket?** The spec asks for queries, not subscriptions, and stateless request/response scales behind any load balancer.
- **Why separate `/health` and `/ready`?** Liveness never touches Redis, so a Redis outage doesn't make Kubernetes restart every API pod. Readiness does check Redis, so an affected pod just stops receiving traffic. A 1 s Redis timeout turns an outage into a fast `503`.
- **Why one uvicorn worker per container?** We scale with replicas, so each container is one process the orchestrator can see, restart and load-balance.

**Kubernetes**
- **Why Terraform?** `plan` previews every change, `destroy` is clean, and it handles ordering (the topic Job completes before consumers start). In production the same tool would also create the cluster and managed Kafka/Redis.
- **Why StatefulSets for Redpanda and Redis, Deployments for the rest?** Only Redpanda and Redis have data on disk and need a stable identity and volume. Ingestion and the API are stateless.
- **Why our own Redpanda StatefulSet, not the Helm chart?** The chart expects cert-manager and more memory than a laptop minikube has. About 80 lines with the same flags as compose is enough, and fully explainable.
- **Why 2 API replicas but 1 consumer?** An API outage is visible to users. A consumer outage only delays freshness, because events wait in Kafka.

## Assumptions on unclear points

1. **Generator bug.** Line 114 of `event_generator.py` logs `args.brokers`, but the flag is `--broker`, so it crashes before publishing. As the file must not be modified, `tools/run_generator.py` adds the missing attribute and calls the generator unchanged. It also accepts several files and publishes them one after another.
2. **Bike balance** = arrivals − departures since stream start. The events carry no dock inventory, so this is net flow, and it can be negative.
3. **Trip stats** returns two averages: trips departing from and trips arriving at the station. A trip counts once both its events have arrived. Round trips count in both.
4. **Durations ≤ 0** are excluded from trip stats as a defensive check (none in the data; minimum 60 s). Such rides still count for balance and busiest.
5. **"Since stream start"** = since the consumer group first read the topic from the earliest offset. Restarts don't double-count. A full reset = empty Redis and replay.
6. **Station IDs are opaque strings** (`SYS038`, `3184.07_OLD`, `Shop Morgan`, IDs ending in `0`). Never parsed as numbers. The API accepts URL-encoded IDs.
7. **Unknown station** → `404`. No completed trips → average is `null`.
8. **Ties for busiest:** the lexicographically highest station ID wins (Redis sorted-set order).
9. **The generator isn't in `docker compose up`.** The challenge lists the stack as Redpanda, ingestion, Redis and API, and runs the generator from the host.

## Delivery guarantees and failure modes

- **At-least-once + idempotent writes = effectively-once.** Kafka transactions don't cover Redis, so each event is applied by one atomic Lua script that first checks a dedup marker.
- **Consumer crash:** no local state to restore. The group rebalances, the uncommitted batch is redelivered, duplicates are rejected. Only freshness suffers.
- **Redis crash:** AOF `everysec` can lose ≤ 1 s of writes whose offsets were already committed. Recovery: empty Redis and replay the topic. Production upgrade: store consumer offsets in Redis atomically with the state.
- **Malformed event:** logged and skipped, never blocks a partition.
- **Redpanda down:** the API keeps serving the last known state.

## Scaling the consumers

All ingestion instances share one consumer group, and Kafka gives each partition to exactly one instance, so adding instances splits the work. Nothing else to configure.

```bash
docker compose up -d --scale ingestion=3                              # Compose
terraform -chdir=infra/terraform apply -var ingestion_replicas=3      # Kubernetes (via Terraform, no drift)
docker compose exec redpanda rpk group describe citibike-ingestion    # who owns which partition, and lag
```

- **Useful maximum = partitions (6).** Extra instances wait as hot standbys. To go beyond, add partitions first (`rpk topic add-partitions`).
- **Each scale event triggers a rebalance.** With cooperative-sticky assignment, only moving partitions pause. An instance that dies without leaving is detected after 45 s.
- **Autoscaling** would follow consumer lag, not CPU (e.g. KEDA). At this traffic one instance never lags.

## Configuration

Settings you can change without editing code. Compose reads environment variables (or a `.env` file). Terraform takes `-var name=value`.

| Compose | Terraform | Default | Meaning |
|---|---|---|---|
| `TOPIC` | `topic` | `citibike-events` | Topic name |
| `PARTITIONS` | `partitions` | `6` | Partition count, set when the topic is first created (to change it: `docker compose down -v` / `terraform destroy`) |
| `RIDE_TTL_S` | `ride_ttl_s` | `172800` (48 h) | How long a ride's first half waits for its partner. It drives Redis memory: for a fast replay of more than a month, lower it (`3600`) |
| `--scale ingestion=N` | `ingestion_replicas` | `1` | Consumer instances (useful maximum = partitions) |
| — | `api_replicas` | `2` | API pods behind the Service |

## Project structure

```
├── docker-compose.yml        local stack: Redpanda, topic-init, Redis, ingestion, API
├── event_generator.py        provided generator (unmodified)
├── tools/run_generator.py    generator wrapper (typo fix, several files)
├── ingestion/                stream processor: consumer loop, validation, batched Redis writes, Lua script, tests
├── api/                      FastAPI service: routes, Redis reads, response models, tests
├── infra/terraform/          Kubernetes deployment on minikube
├── helpers/                  data profiling and the end-to-end acceptance check (DuckDB)
├── docs/                     API contract and data profile
└── requirements-dev.txt      local tools
```

## Limitations and production changes

- **Replication:** 1 broker, RF 1 locally. Production: RF 3 with `min.insync.replicas=2`.
- **Redis Cluster:** the multi-key Lua script assumes a single Redis node.
- **Cluster access:** `port-forward` is a dev tunnel. Production: Ingress/LoadBalancer for the API, TLS external listeners for Kafka. Images are built inside minikube here; production would push versioned images to a registry.
- **Edge concerns:** rate limiting, auth and TLS belong in an ingress or API gateway.
- **Ingestion liveness:** no HTTP port. Kafka evicts a hung consumer and Kubernetes restarts a crashed one. A heartbeat-file probe would be the production addition.
- **Observability:** logs only. Production: Prometheus metrics (lag, latency, 5xx) and alerts.
- **Extending:** more counters or hourly buckets fit the Lua/Redis approach. Sliding windows, stream joins or output topics would justify Kafka Streams or Flink, run as a separate consumer group that backfills from the retained topic.
