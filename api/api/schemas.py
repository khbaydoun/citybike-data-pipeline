"""Response models. These define the public API contract (see docs/api-contract.md)."""

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, field_serializer


class StationRef(BaseModel):
    station_id: str
    station_name: str | None


class LastActivity(StationRef):
    event_type: Literal["trip_start", "trip_end"]
    timestamp: datetime

    @field_serializer("timestamp")
    def _iso_ms_utc(self, ts: datetime) -> str:
        # "2026-06-30T23:48:04.114Z": millisecond precision, as in the source data
        return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class BikeBalance(StationRef):
    bike_balance: int
    arrivals: int
    departures: int


class BusiestStation(StationRef):
    event_count: int


class TripDirectionStats(BaseModel):
    trip_count: int
    avg_duration_seconds: float | None


class TripStats(StationRef):
    departing: TripDirectionStats
    arriving: TripDirectionStats


class ErrorResponse(BaseModel):
    detail: str
