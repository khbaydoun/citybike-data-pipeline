"""API tests against a real Redis (db 15).

Data is written with the real ingestion Store + Lua script, so these tests also
check the Redis contract between the two services.
"""

import os
import sys
from pathlib import Path

import pytest
import redis
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingestion"))
from ingestion.models import Event  # noqa: E402
from ingestion.store import Store  # noqa: E402

REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")


def ev(ride, etype, station, ts, name=None):
    return Event(ride_id=ride, event_type=etype, station_id=station,
                 station_name=name or f"Name {station}", timestamp=ts)


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
def ingest(r):
    return Store(r, ride_ttl_s=3600).apply_batch


@pytest.fixture
def api(r, monkeypatch):
    monkeypatch.setenv("REDIS_URL", REDIS_URL)
    from api.main import app
    with TestClient(app) as client:  # runs the lifespan (creates the Redis client)
        yield client


def test_all_endpoints(api, ingest):
    ingest([
        ev("R1", "trip_start", "A", "2026-06-01T08:00:00Z"),
        ev("R1", "trip_end", "B", "2026-06-01T08:10:00Z"),   # 600 s
        ev("R2", "trip_start", "A", "2026-06-01T09:00:00Z"),
        ev("R2", "trip_end", "C", "2026-06-01T09:05:00.250Z"),  # 300.25 s
    ])

    assert api.get("/stations/A/last-activity").json() == {
        "station_id": "A", "station_name": "Name A",
        "event_type": "trip_start", "timestamp": "2026-06-01T09:00:00.000Z"}

    assert api.get("/stations/A/bike-balance").json() == {
        "station_id": "A", "station_name": "Name A",
        "bike_balance": -2, "arrivals": 0, "departures": 2}

    assert api.get("/stations/A/trip-stats").json() == {
        "station_id": "A", "station_name": "Name A",
        "departing": {"trip_count": 2, "avg_duration_seconds": 450.1},
        "arriving": {"trip_count": 0, "avg_duration_seconds": None}}

    assert api.get("/stations/busiest").json() == {
        "station_id": "A", "station_name": "Name A", "event_count": 2}


def test_unknown_station_is_404(api, ingest):
    for path in ("last-activity", "bike-balance", "trip-stats"):
        res = api.get(f"/stations/nope/{path}")
        assert res.status_code == 404
        assert res.json() == {"detail": "Station 'nope' not found"}


def test_busiest_before_any_event_is_404(api):
    res = api.get("/stations/busiest")
    assert res.status_code == 404 and res.json() == {"detail": "No station activity yet"}


def test_busiest_tie_highest_station_id_wins(api, ingest):
    ingest([ev("R1", "trip_start", "100.01", "2026-06-01T08:00:00Z"),
            ev("R2", "trip_start", "200.01", "2026-06-01T08:00:00Z")])
    assert api.get("/stations/busiest").json()["station_id"] == "200.01"


def test_station_id_with_space_is_url_decoded(api, ingest):
    ingest([ev("R1", "trip_start", "Shop Morgan", "2026-06-01T08:00:00Z")])
    res = api.get("/stations/Shop%20Morgan/bike-balance")
    assert res.status_code == 200 and res.json()["departures"] == 1


def test_trip_waiting_for_its_other_half_is_not_counted_yet(api, ingest):
    ingest([ev("R1", "trip_start", "A", "2026-06-01T08:00:00Z")])
    body = api.get("/stations/A/trip-stats").json()
    assert body["departing"] == {"trip_count": 0, "avg_duration_seconds": None}


def test_health_ready_and_no_store_header(api):
    assert api.get("/health").json() == {"status": "ok"}
    res = api.get("/ready")
    assert res.json() == {"status": "ready"}
    assert res.headers["cache-control"] == "no-store"


def test_redis_down_gives_503_but_health_stays_up(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://localhost:1/0")  # nothing listens here
    from api.main import app
    with TestClient(app) as client:
        assert client.get("/stations/A/bike-balance").status_code == 503
        assert client.get("/ready").json() == {"detail": "Data store unavailable"}
        assert client.get("/health").status_code == 200
