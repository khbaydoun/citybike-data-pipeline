# API contract

The HTTP API serves per-station aggregates computed by the ingestion service. It reads only from Redis and never touches Kafka. All examples use real values for station `6140.05` after the full June 2026 dataset (all six files) has been processed. These examples double as acceptance checks (see the end of this file).

## Conventions

- **Paths** exactly as given in the challenge. No `/v1` prefix, so the specified URLs work as-is. In production we'd version the API.
- **`station_id`** is an opaque string (e.g. `6140.05`, `SYS038`, `Shop Morgan`). It's never parsed as a number. Clients URL-encode it: `/stations/Shop%20Morgan/bike-balance`.
- **Timestamps** are ISO 8601 UTC with millisecond precision and a `Z` suffix: `2026-06-30T23:48:04.114Z`.
- **Units** appear in field names: `avg_duration_seconds`.
- **Every station response** includes `station_id` and `station_name`, so it's self-describing.
- **Errors** share one shape: `{"detail": "<message>"}`.
- **Freshness:** values reflect every event ingested so far. Updates are visible sub-second after ingestion. Responses carry `Cache-Control: no-store`.
- **Style:** plain REST request/response (clients poll). Server push (SSE) is a possible extension and is out of scope.

## Status codes

| Code | When |
|---|---|
| `200` | Success |
| `404` | The station has never appeared in the stream, or `busiest` is called before any event has been ingested |
| `422` | Malformed request (FastAPI validation) |
| `503` | Redis unreachable |

---

## `GET /stations/{station_id}/last-activity`

The most recent event at the station, by **event timestamp** (not arrival order).

```json
{
  "station_id": "6140.05",
  "station_name": "W 21 St & 6 Ave",
  "event_type": "trip_end",
  "timestamp": "2026-06-30T23:48:04.114Z"
}
```

- `event_type`: `trip_start` | `trip_end`.
- An event older than the stored one never replaces it, even if it arrives later.

## `GET /stations/{station_id}/bike-balance`

Net change in bikes at the station since stream start: `arrivals − departures`.

```json
{
  "station_id": "6140.05",
  "station_name": "W 21 St & 6 Ave",
  "bike_balance": 52,
  "arrivals": 18131,
  "departures": 18079
}
```

- `arrivals` = number of `trip_end` events at the station. `departures` = number of `trip_start` events.
- This is **net flow, not dock inventory**. The stream has no starting inventory, and truck rebalancing isn't in the data. It can be negative.

## `GET /stations/busiest`

The station with the highest total event count (`trip_start` + `trip_end`) since stream start.

```json
{
  "station_id": "6140.05",
  "station_name": "W 21 St & 6 Ave",
  "event_count": 36210
}
```

- **Ties:** broken deterministically: the lexicographically highest `station_id` wins (Redis `ZREVRANGE` order for equal scores).
- **No events ingested yet:** `404 {"detail": "No station activity yet"}`.

## `GET /stations/{station_id}/trip-stats`

Average duration of trips **departing from** and **arriving at** the station.

```json
{
  "station_id": "6140.05",
  "station_name": "W 21 St & 6 Ave",
  "departing": { "trip_count": 18079, "avg_duration_seconds": 646.9 },
  "arriving":  { "trip_count": 18131, "avg_duration_seconds": 649.3 }
}
```

- Duration = `trip_end.timestamp − trip_start.timestamp` of the same `ride_id`.
- A trip counts once **both** of its events have been ingested (they can arrive in either order). While the stream is running, `trip_count` can therefore lag slightly behind `departures`/`arrivals`.
- Durations ≤ 0 are excluded as a defensive check.
- Round trips (same start and end station) count in both `departing` and `arriving`.
- `avg_duration_seconds` is `null` when `trip_count` is `0`.
- Rounded to 1 decimal.

---

## Operational endpoints

| Endpoint | Purpose | Response |
|---|---|---|
| `GET /health` | Liveness: the process is up (K8s `livenessProbe`) | `200 {"status": "ok"}` |
| `GET /ready` | Readiness: Redis is reachable (K8s `readinessProbe`) | `200 {"status": "ready"}` or `503` |
| `GET /docs` | Auto-generated OpenAPI UI | HTML |

## How each endpoint reads Redis

| Endpoint | Redis call(s) | Complexity |
|---|---|---|
| last-activity | `HMGET station:{id} name last_ts_ms last_type` | O(1) |
| bike-balance | `HMGET station:{id} name arrivals departures` | O(1) |
| trip-stats | `HMGET station:{id} name dep_trips dep_sum_ms arr_trips arr_sum_ms` | O(1) |
| busiest | `ZREVRANGE stations:busiest 0 0 WITHSCORES`, then `HGET station:{id} name` | O(log N) |

## Acceptance values (full June 2026, all six files)

Computed independently with DuckDB over `data/*.csv` (the same rows the generator keeps). After ingesting all six files, the API must return exactly:

| Check | Expected |
|---|---|
| `GET /stations/busiest` | `6140.05`, `event_count` 36210 |
| `6140.05` last-activity | `trip_end` at `2026-06-30T23:48:04.114Z` |
| `6140.05` bike-balance | `52` (18131 − 18079) |
| `6140.05` trip-stats | departing 18079 / 646.9 s, arriving 18131 / 649.3 s |
