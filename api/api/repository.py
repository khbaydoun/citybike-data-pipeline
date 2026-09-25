"""Reads the aggregates written by the ingestion service.

Key and field names are the contract with ingestion (ingestion/ingestion/apply_event.lua):
  station:{id}      HASH  name, last_ts_ms, last_type, arrivals, departures,
                          dep_trips, dep_sum_ms, arr_trips, arr_sum_ms
  stations:busiest  ZSET  station_id -> total event count
Every method returns None when the station (or any data) doesn't exist yet.
"""

from datetime import datetime, timezone

from redis.asyncio import Redis

from .schemas import BikeBalance, BusiestStation, LastActivity, TripDirectionStats, TripStats

BUSIEST_KEY = "stations:busiest"


def _station_key(station_id: str) -> str:
    return f"station:{station_id}"


def _int(value: str | None) -> int:
    return int(value) if value is not None else 0


def _direction(trips: str | None, sum_ms: str | None) -> TripDirectionStats:
    count = _int(trips)
    avg = round(_int(sum_ms) / 1000 / count, 1) if count else None
    return TripDirectionStats(trip_count=count, avg_duration_seconds=avg)


class StationRepository:
    def __init__(self, client: Redis):
        self._r = client  # created with decode_responses=True

    async def ping(self) -> bool:
        return await self._r.ping()

    async def last_activity(self, station_id: str) -> LastActivity | None:
        name, ts_ms, event_type = await self._r.hmget(
            _station_key(station_id), "name", "last_ts_ms", "last_type")
        if ts_ms is None:
            return None
        return LastActivity(
            station_id=station_id, station_name=name, event_type=event_type,
            timestamp=datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc),
        )

    async def bike_balance(self, station_id: str) -> BikeBalance | None:
        name, ts_ms, arrivals, departures = await self._r.hmget(
            _station_key(station_id), "name", "last_ts_ms", "arrivals", "departures")
        if ts_ms is None:
            return None
        arr, dep = _int(arrivals), _int(departures)
        return BikeBalance(station_id=station_id, station_name=name,
                           bike_balance=arr - dep, arrivals=arr, departures=dep)

    async def trip_stats(self, station_id: str) -> TripStats | None:
        name, ts_ms, dep_trips, dep_sum, arr_trips, arr_sum = await self._r.hmget(
            _station_key(station_id), "name", "last_ts_ms",
            "dep_trips", "dep_sum_ms", "arr_trips", "arr_sum_ms")
        if ts_ms is None:
            return None
        return TripStats(station_id=station_id, station_name=name,
                         departing=_direction(dep_trips, dep_sum),
                         arriving=_direction(arr_trips, arr_sum))

    async def busiest(self) -> BusiestStation | None:
        top = await self._r.zrevrange(BUSIEST_KEY, 0, 0, withscores=True)
        if not top:
            return None
        station_id, score = top[0]
        name = await self._r.hget(_station_key(station_id), "name")
        return BusiestStation(station_id=station_id, station_name=name, event_count=int(score))
