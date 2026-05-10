"""kafka-python adapter.

Producer uses `send().add_callback()` so dispatch is async via kafka-python's
internal sender thread. Consumer uses the iterator API.
"""

from __future__ import annotations

import time

from kafka import KafkaConsumer, KafkaProducer

from bench.config import ConsumerConfig, ProducerConfig

_COMPRESSION_MAP = {"none": None, "lz4": "lz4", "zstd": "zstd", "snappy": "snappy", "gzip": "gzip"}


class KafkaPythonProducer:
    name = "kafka-python"

    def __init__(self) -> None:
        self._producer: KafkaProducer | None = None

    def setup(self, bootstrap_servers: str, config: ProducerConfig) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks=config.acks,
            linger_ms=config.linger_ms,
            # kafka-python rejects very small batches; clamp without changing semantics
            # at linger_ms=0 (each send is its own batch in practice).
            batch_size=max(config.batch_size_bytes, 16384),
            compression_type=_COMPRESSION_MAP[config.compression],
            enable_idempotence=config.enable_idempotence,
            max_in_flight_requests_per_connection=config.max_in_flight_requests_per_connection,
            request_timeout_ms=config.request_timeout_ms,
            delivery_timeout_ms=config.delivery_timeout_ms,
            api_version_auto_timeout_ms=10_000,
            # Make every message its own batch for latency runs (linger_ms=0 already does this).
        )

    def produce_async(self, topic, key, value, produce_ts_ns, on_ack):
        assert self._producer is not None
        future = self._producer.send(topic, key=bytes(key), value=bytes(value))
        future.add_callback(lambda _md, _ts=produce_ts_ns: on_ack(time.monotonic_ns() - _ts, None))
        future.add_errback(lambda err, _ts=produce_ts_ns: on_ack(time.monotonic_ns() - _ts, err))

    def drain(self, timeout_seconds=30.0):
        assert self._producer is not None
        self._producer.flush(timeout=timeout_seconds)

    def close(self):
        if self._producer is not None:
            self._producer.close()
            self._producer = None


class KafkaPythonConsumer:
    name = "kafka-python"

    def __init__(self) -> None:
        self._consumer: KafkaConsumer | None = None

    def setup(
        self, bootstrap_servers: str, topic: str, config: ConsumerConfig, partitions: int
    ) -> None:
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=config.group_id,
            fetch_min_bytes=config.fetch_min_bytes,
            fetch_max_wait_ms=config.fetch_max_wait_ms,
            auto_offset_reset=config.auto_offset_reset,
            enable_auto_commit=config.enable_auto_commit,
            max_poll_records=config.max_poll_records,
        )

    def consume_until(self, deadline_ns, on_message, stop=None):
        assert self._consumer is not None
        while time.monotonic_ns() < deadline_ns:
            if stop is not None and stop.is_set():
                return
            batch = self._consumer.poll(timeout_ms=100)
            recv_ts = time.monotonic_ns()
            if not batch:
                continue
            for records in batch.values():
                for r in records:
                    on_message(r.key or b"", r.value or b"", recv_ts)

    def close(self):
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
