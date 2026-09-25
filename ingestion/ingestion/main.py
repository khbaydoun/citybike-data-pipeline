"""Consume trip events from Redpanda and apply them to Redis.

At-least-once: offsets are committed only after the Redis pipeline succeeds.
Redeliveries are harmless because the Lua script deduplicates per ride half.
"""

import logging
import signal
import time

import redis
from confluent_kafka import Consumer, KafkaError

from .config import Config
from .models import Event, parse
from .store import Store

log = logging.getLogger("ingestion")


class Stats:
    def __init__(self):
        self.reset()

    def reset(self):
        self.consumed = self.applied = self.duplicates = self.invalid = 0
        self.max_latency_ms = 0
        self.started = time.monotonic()


def _is_error(m) -> bool:
    if m.error() is None:
        return False
    if m.error().code() != KafkaError._PARTITION_EOF:
        log.error("kafka error: %s", m.error())
    return True


def apply_with_retry(store: Store, events: list[Event], running) -> int:
    """Retry until Redis accepts the batch. We never commit offsets for a batch that wasn't applied."""
    delay = 0.5
    while True:
        try:
            return store.apply_batch(events)
        except redis.RedisError as e:
            if not running():
                raise
            log.error("redis write failed (%s), retrying in %.1fs", e, delay)
            time.sleep(delay)
            delay = min(delay * 2, 10)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Config()

    consumer = Consumer({
        "bootstrap.servers": cfg.kafka_brokers,
        "group.id": cfg.group_id,
        "enable.auto.commit": False,          # commit manually, after Redis
        "auto.offset.reset": "earliest",      # "since stream start"
        "partition.assignment.strategy": "cooperative-sticky",  # incremental rebalances
    })
    consumer.subscribe([cfg.topic])
    store = Store(redis.Redis.from_url(cfg.redis_url), cfg.ride_ttl_s)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("consuming %s from %s as group %s", cfg.topic, cfg.kafka_brokers, cfg.group_id)
    stats = Stats()
    try:
        while running:
            msgs = [m for m in consumer.consume(num_messages=cfg.batch_size, timeout=cfg.poll_timeout_s)
                    if not _is_error(m)]
            if msgs:
                events = []
                for m in msgs:
                    event = parse(m.value())
                    if event is None:
                        stats.invalid += 1  # poison message: logged and skipped, never blocks the partition
                    else:
                        events.append(event)
                applied = apply_with_retry(store, events, lambda: running)
                consumer.commit(asynchronous=False)
                # Producer timestamp -> committed in Redis: end-to-end ingestion latency.
                oldest_ms = min(m.timestamp()[1] for m in msgs)
                stats.max_latency_ms = max(stats.max_latency_ms, time.time() * 1000 - oldest_ms)
                stats.consumed += len(msgs)
                stats.applied += applied
                stats.duplicates += len(events) - applied

            if time.monotonic() - stats.started >= cfg.stats_interval_s:
                if stats.consumed:
                    elapsed = time.monotonic() - stats.started
                    log.info(
                        "consumed=%d (%.0f/s) applied=%d duplicates=%d invalid=%d max_latency_ms=%.0f",
                        stats.consumed, stats.consumed / elapsed, stats.applied,
                        stats.duplicates, stats.invalid, stats.max_latency_ms,
                    )
                stats.reset()
    finally:
        log.info("shutting down")
        consumer.close()


if __name__ == "__main__":
    main()
