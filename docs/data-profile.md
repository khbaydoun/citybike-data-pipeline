# Data profile: Citibike June 2026 (`202606-citibike-tripdata_{1..6}.csv`)

Produced by `helpers/inspect_data.py` (DuckDB, about 27 s over all files). Rerun with:

```bash
.venv/bin/python helpers/inspect_data.py
```

## Findings and what they mean for the pipeline

| # | Finding | Implication |
|---|---|---|
| 1 | 5,384,468 rows; 5,367,597 survive the generator's filter, giving **10,735,194 events** | Enough for a load test. Use a small sample for development. |
| 2 | 16,871 rows dropped by the generator (3,154 missing start station, 14,211 missing end station) | Out of our hands. The source drops them. Mention it in the README. |
| 3 | **0 duplicate `ride_id`s** in the source | Any duplicate we see in the stream is a **delivery duplicate** (producer retry or consumer replay), so `(ride_id, event_type)` is a safe dedup key. |
| 4 | Only `electric_bike` and `classic_bike`; only `member` / `casual` | `docked_bike` is valid per the spec but absent. Validate against the spec's enum, not against what the data happens to contain. |
| 5 | All timestamps have milliseconds; none unparseable | The generator emits `2026-06-02T14:51:58.403000+00:00`. Our parser must accept `+00:00` (and `Z`). |
| 6 | 586 rides start on 31 May; none end after 30 June | The file is organised by ride end, so those 586 rides are the first events in the stream. Harmless. |
| 7 | Durations: **min 60 s, 0 negative**, p50 586 s, p99 3,589 s, max 89,405 s (19 rides > 24 h) | Citibike pre-cleans trips under 60 s. The source contains no end-before-start, **but the stream still can**: start and end land on different partitions (keyed by station). We keep the negative-duration guard as a defensive check. |
| 8 | 2,391 distinct station IDs | The state is tiny: a few MB in Redis at most. |
| 9 | **Station IDs aren't all numeric**: `5303.06_`, `3184.07_OLD`, `SYS038`, `HB202`, `Shop Morgan` (contains a space) | IDs are opaque **strings** end to end. The API must accept URL-encoded path params (`Shop%20Morgan`). |
| 10 | 97 IDs end in `0` (e.g. `…​.10`) | Parsing IDs as floats would silently merge or corrupt stations. Never cast. |
| 11 | Each station ID maps to exactly one name | Store `station_name` alongside the station's state without conflict handling. |
| 12 | Busiest station holds only 0.34% of events; median 1,979 events per station | **No hot-partition risk** with `station_id` as the key. Load spreads evenly. |
| 13 | Real-world rate: **avg 4.1 events/s, peak about 12.8 events/s** (per minute) | Throughput is low. The generator default (10 events/s) is close to real time. One consumer instance handles this easily, so a stream-processing framework (Flink/Spark) would be over-engineering. A full-month replay at the default pace takes about 12 days, so load tests need `--burst`. |

## Takeaways for architecture

- **Correctness matters more than throughput here.** The hard parts are idempotency, pairing starts with ends across partitions, and order-safe "last activity". Scale isn't the problem.
- **Keep IDs as strings** everywhere: Kafka key, Redis keys, API path.
- **A single lightweight Python consumer** is justified by the numbers. We still use a consumer group with several partitions so it *can* scale out, and say so in the README.
