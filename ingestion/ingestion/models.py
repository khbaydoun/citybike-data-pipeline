import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

log = logging.getLogger("ingestion")

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MS = timedelta(milliseconds=1)


class Event(BaseModel):
    """A trip event as published by the generator.

    Strict on the fields the aggregates depend on, lenient on the rest
    (an unknown rideable_type shouldn't drop real station activity).
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    ride_id: str = Field(min_length=1)
    event_type: Literal["trip_start", "trip_end"]
    station_id: str = Field(min_length=1)
    station_name: str = ""
    rideable_type: str = ""
    member_casual: str = ""
    timestamp: AwareDatetime  # accepts both "...Z" and "...+00:00"

    @property
    def ts_ms(self) -> int:
        return (self.timestamp - _EPOCH) // _MS


def parse(raw: bytes | None) -> Event | None:
    """Parse and validate one message. Invalid messages are logged and return None."""
    try:
        return Event.model_validate(json.loads(raw))
    except (ValueError, TypeError, ValidationError) as e:
        log.warning("invalid event skipped: %s | %.200r", str(e).splitlines()[0], raw)
        return None
