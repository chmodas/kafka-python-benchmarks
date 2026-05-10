"""confluent-kafka adapter.

The dedicated poll thread is critical for fair latency measurement: delivery
callbacks fire from inside `poll()`, so the gap between librdkafka receiving
the broker ack and our thread calling `poll()` is recorded as ack-latency.
A `poll(timeout=0.001)` loop keeps that gap bounded by ~1 ms scheduler jitter.
This floor is documented in the report.
"""

from __future__ import annotations

import logging
import threading
import time

from confluent_kafka import Consumer, Producer

from bench.config import ConsumerConfig, ProducerConfig

log = logging.getLogger(__name__)


def _producer_conf(bootstrap: str, c: ProducerConfig) -> dict:
    conf = {
        "bootstrap.servers": bootstrap,
        "acks": str(c.acks),
        "linger.ms": c.linger_ms,
        "batch.size": c.batch_size_bytes,
        "enable.idempotence": c.enable_idempotence,
        "max.in.flight.requests.per.connection": c.max_in_flight_requests_per_connection,
        "request.timeout.ms": c.request_timeout_ms,
        "delivery.timeout.ms": c.delivery_timeout_ms,
        "socket.nagle.disable": True,
    }
    if c.compression != "none":
        conf["compression.type"] = c.compression
    return conf


def _consumer_conf(bootstrap: str, c: ConsumerConfig) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": c.group_id,
        "fetch.min.bytes": c.fetch_min_bytes,
        "fetch.wait.max.ms": c.fetch_max_wait_ms,
        "auto.offset.reset": c.auto_offset_reset,
        "enable.auto.commit": c.enable_auto_commit,
        "socket.nagle.disable": True,
    }


class ConfluentProducer:
    name = "confluent"

    def __init__(self) -> None:
        self._producer: Producer | None = None
        self._poll_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.buffer_error_count = 0  # exposed for the runner to surface in summaries

    def setup(self, bootstrap_servers: str, config: ProducerConfig) -> None:
        self._producer = Producer(_producer_conf(bootstrap_servers, config))
        self._stop.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="confluent-poll", daemon=True
        )
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        assert self._producer is not None
        # 1 ms timeout = ~1 ms upper bound on delivery-callback dispatch jitter.
        while not self._stop.is_set():
            self._producer.poll(0.001)

    def produce_async(self, topic, key, value, produce_ts_ns, on_ack):
        assert self._producer is not None

        def _cb(err, _msg, _ts=produce_ts_ns):
            on_ack(time.monotonic_ns() - _ts, err if err is not None else None)

        # confluent-kafka can raise BufferError under back-pressure; let the runner's
        # in-flight semaphore prevent this, but if it slips through, give the poll
        # thread a moment and retry once. Each occurrence introduces ~10ms of
        # blocking that *will* be recorded as ack-latency, so log them – a
        # non-zero count means the local queue is the bottleneck, not the broker.
        try:
            self._producer.produce(topic, key=bytes(key), value=bytes(value), on_delivery=_cb)
        except BufferError:
            self.buffer_error_count += 1
            log.warning(
                "confluent BufferError (count=%d); local queue full, retrying after poll",
                self.buffer_error_count,
            )
            self._producer.poll(0.01)
            self._producer.produce(topic, key=bytes(key), value=bytes(value), on_delivery=_cb)

    def drain(self, timeout_seconds=30.0):
        assert self._producer is not None
        self._producer.flush(timeout_seconds)

    def close(self):
        self._stop.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=2.0)
            self._poll_thread = None
        # confluent-kafka has no explicit producer close; flushing + dropping the ref is enough.
        self._producer = None


class ConfluentConsumer:
    name = "confluent"

    def __init__(self) -> None:
        self._consumer: Consumer | None = None

    def setup(
        self, bootstrap_servers: str, topic: str, config: ConsumerConfig, partitions: int
    ) -> None:
        self._consumer = Consumer(_consumer_conf(bootstrap_servers, config))
        self._consumer.subscribe([topic])

    def consume_until(self, deadline_ns, on_message, stop=None):
        assert self._consumer is not None
        while time.monotonic_ns() < deadline_ns:
            if stop is not None and stop.is_set():
                return
            msg = self._consumer.poll(0.1)
            recv_ts = time.monotonic_ns()
            if msg is None or msg.error() is not None:
                continue
            on_message(msg.key() or b"", msg.value() or b"", recv_ts)

    def close(self):
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
