"""Edge-case tests for the apply-event Lua script, run against a real Redis (db 15)."""

import os

import pytest
import redis

from ingestion.models import Event
from ingestion.store import BUSIEST_KEY, Store

REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def r():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture
def store(r):
    return Store(r, ride_ttl_s=3600)


def ev(ride, etype, station, ts, name="S"):
    return Event(ride_id=ride, event_type=etype, station_id=station, station_name=name, timestamp=ts)


def test_start_then_end_pairs_trip(store, r):
    store.apply_batch([
        ev("R1", "trip_start", "A", "2026-06-01T08:00:00Z"),
        ev("R1", "trip_end", "B", "2026-06-01T08:10:00Z"),
    ])
    a, b = r.hgetall("station:A"), r.hgetall("station:B")
    assert a["departures"] == "1" and a["dep_trips"] == "1" and a["dep_sum_ms"] == "600000"
    assert b["arrivals"] == "1" and b["arr_trips"] == "1" and b["arr_sum_ms"] == "600000"


def test_end_before_start_pairs_the_same(store, r):
    store.apply_batch([ev("R1", "trip_end", "B", "2026-06-01T08:10:00Z")])
    assert "arr_trips" not in r.hgetall("station:B")  # waiting for its start
    store.apply_batch([ev("R1", "trip_start", "A", "2026-06-01T08:00:00Z")])
    assert r.hget("station:A", "dep_sum_ms") == "600000"
    assert r.hget("station:B", "arr_sum_ms") == "600000"


def test_duplicates_are_not_counted(store, r):
    start = ev("R1", "trip_start", "A", "2026-06-01T08:00:00Z")
    end = ev("R1", "trip_end", "B", "2026-06-01T08:10:00Z")
    assert store.apply_batch([start, end]) == 2
    assert store.apply_batch([start, end, start]) == 0  # redelivery
    assert r.hget("station:A", "departures") == "1"
    assert r.hget("station:A", "dep_trips") == "1"
    assert r.zscore(BUSIEST_KEY, "A") == 1


def test_last_activity_ignores_older_late_events(store, r):
    store.apply_batch([ev("R2", "trip_end", "A", "2026-06-01T09:00:00Z")])
    store.apply_batch([ev("R1", "trip_start", "A", "2026-06-01T08:00:00Z")])  # older, arrives late
    assert r.hget("station:A", "last_type") == "trip_end"
    assert r.hget("station:A", "last_ts_ms") == str(Event(
        ride_id="x", event_type="trip_end", station_id="A", timestamp="2026-06-01T09:00:00Z").ts_ms)


def test_last_activity_tie_is_order_independent(store, r):
    ts = "2026-06-01T09:00:00Z"
    store.apply_batch([ev("R1", "trip_end", "A", ts), ev("R2", "trip_start", "A", ts)])
    store.apply_batch([ev("R3", "trip_start", "B", ts), ev("R4", "trip_end", "B", ts)])
    assert r.hget("station:A", "last_type") == r.hget("station:B", "last_type") == "trip_end"


def test_non_positive_duration_excluded_from_trip_stats(store, r):
    store.apply_batch([
        ev("R1", "trip_start", "A", "2026-06-01T08:10:00Z"),
        ev("R1", "trip_end", "B", "2026-06-01T08:00:00Z"),
    ])
    assert r.hget("station:A", "departures") == "1"  # still counts for balance/busiest
    assert r.hget("station:A", "dep_trips") is None


def test_round_trip_counts_both_ways(store, r):
    store.apply_batch([
        ev("R1", "trip_start", "A", "2026-06-01T08:00:00Z"),
        ev("R1", "trip_end", "A", "2026-06-01T08:05:00Z"),
    ])
    a = r.hgetall("station:A")
    assert a["departures"] == a["arrivals"] == a["dep_trips"] == a["arr_trips"] == "1"
    assert r.zscore(BUSIEST_KEY, "A") == 2


def test_station_ids_stay_strings(store, r):
    store.apply_batch([ev("R1", "trip_start", "7962.10", "2026-06-01T08:00:00Z")])
    assert r.exists("station:7962.10") and not r.exists("station:7962.1")
