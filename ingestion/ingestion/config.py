import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    kafka_brokers: str = field(default_factory=lambda: os.getenv("KAFKA_BROKERS", "localhost:19092"))
    topic: str = field(default_factory=lambda: os.getenv("TOPIC", "citibike-events"))
    group_id: str = field(default_factory=lambda: os.getenv("GROUP_ID", "citibike-ingestion"))
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    # Batch size vs. freshness: a batch is flushed when full or after the poll timeout.
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "500")))
    poll_timeout_s: float = field(default_factory=lambda: float(os.getenv("POLL_TIMEOUT_S", "0.2")))
    # Longest ride in the data is ~24.8h; 48h covers it with margin.
    ride_ttl_s: int = field(default_factory=lambda: int(os.getenv("RIDE_TTL_S", str(48 * 3600))))
    stats_interval_s: float = field(default_factory=lambda: float(os.getenv("STATS_INTERVAL_S", "10")))
