import pytest
from pydantic import ValidationError

from ingestion.models import Event, parse

BASE = {"ride_id": "R1", "event_type": "trip_start", "station_id": "6140.05",
        "station_name": "W 21 St & 6 Ave", "rideable_type": "electric_bike",
        "member_casual": "member"}


@pytest.mark.parametrize("ts", ["2026-06-01T08:39:03.412000+00:00", "2026-06-01T08:39:03.412Z"])
def test_accepts_both_utc_formats(ts):
    assert Event(**BASE, timestamp=ts).ts_ms == 1780303143412


@pytest.mark.parametrize("override", [
    {"event_type": "trip_pause"},
    {"station_id": ""},
    {"ride_id": ""},
    {"timestamp": "2026-06-01T08:39:03"},  # naive: no timezone
    {"timestamp": "not a date"},
])
def test_rejects_invalid(override):
    with pytest.raises(ValidationError):
        Event(**{**BASE, "timestamp": "2026-06-01T08:39:03Z", **override})


def test_unknown_rideable_type_is_accepted():
    assert Event(**{**BASE, "rideable_type": "scooter"}, timestamp="2026-06-01T08:39:03Z")


def test_parse_returns_none_for_garbage():
    assert parse(b"{not json") is None
    assert parse(b'{"ride_id": "R1"}') is None
