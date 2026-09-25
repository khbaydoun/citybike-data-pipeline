import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    # Fail fast: a Redis outage should become a quick 503, not a hanging request.
    redis_timeout_s: float = field(default_factory=lambda: float(os.getenv("REDIS_TIMEOUT_S", "1.0")))
