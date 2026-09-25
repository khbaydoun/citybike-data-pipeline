"""Redis side of ingestion: key names and the atomic apply-event script."""

from pathlib import Path

import redis

from .models import Event

BUSIEST_KEY = "stations:busiest"


def station_key(station_id: str) -> str:
    return f"station:{station_id}"


def ride_key(ride_id: str) -> str:
    return f"ride:{ride_id}"


class Store:
    def __init__(self, client: redis.Redis, ride_ttl_s: int):
        self._client = client
        self._ride_ttl_s = ride_ttl_s
        self._apply = client.register_script(
            (Path(__file__).parent / "apply_event.lua").read_text()
        )

    def apply_batch(self, events: list[Event]) -> int:
        """Apply events in one pipelined round-trip. Returns how many were new (not duplicates)."""
        if not events:
            return 0
        pipe = self._client.pipeline(transaction=False)
        for e in events:
            self._apply(
                keys=[ride_key(e.ride_id), station_key(e.station_id), BUSIEST_KEY],
                args=[e.event_type, e.station_id, e.station_name, str(e.ts_ms), self._ride_ttl_s],
                client=pipe,
            )
        return sum(pipe.execute())
