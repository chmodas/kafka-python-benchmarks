"""Smoke integration tests for each client adapter.

Skipped automatically when no broker is reachable. Run with:
    make up
    uv run pytest tests/test_clients_integration.py -v
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from bench import admin
from bench import workload as wl
from bench.clients import make_consumer, make_producer
from bench.config import ConsumerConfig, ProducerConfig

PRODUCER_CFG = ProducerConfig(
    acks=1,
    linger_ms=0,
    batch_size_bytes=1,
    compression="none",
    enable_idempotence=False,
    max_in_flight_requests_per_connection=8,
)
CONSUMER_CFG = ConsumerConfig(group_id=f"smoke-{uuid.uuid4().hex[:6]}")


@pytest.mark.parametrize("client_name", ["kafka-python", "confluent", "aiokafka"])
def test_round_trip_1k_messages(bootstrap: str, client_name: str) -> None:
    topic = f"smoke-{client_name}-{uuid.uuid4().hex[:8]}"
    admin.create_topic(bootstrap, topic, partitions=1)

    producer = make_producer(client_name)
    consumer = make_consumer(client_name)
    received: list[int] = []
    received_lock = threading.Lock()
    ack_count = 0
    ack_lock = threading.Lock()
    err_holder: list[BaseException] = []

    def on_ack(latency_ns: int, err: BaseException | None) -> None:
        nonlocal ack_count
        if err is not None:
            err_holder.append(err)
            return
        with ack_lock:
            ack_count += 1

    def on_msg(_key: bytes, value: bytes | bytearray, _recv_ts: int) -> None:
        try:
            _ts, seq = wl.parse(value)
        except Exception:
            return
        with received_lock:
            received.append(seq)

    consumer_done = threading.Event()
    consumer_stop = threading.Event()
    deadline_ns = time.monotonic_ns() + 30 * 1_000_000_000

    def consumer_run() -> None:
        try:
            consumer.setup(
                bootstrap_servers=bootstrap,
                topic=topic,
                config=CONSUMER_CFG,
                partitions=1,
            )
            consumer.consume_until(deadline_ns, on_msg, stop=consumer_stop)
        finally:
            try:
                consumer.close()
            except Exception:
                pass
            consumer_done.set()

    consumer_thread = threading.Thread(target=consumer_run, daemon=True)
    consumer_thread.start()

    try:
        producer.setup(bootstrap_servers=bootstrap, config=PRODUCER_CFG)
        sem = threading.Semaphore(8)

        def gated_on_ack(latency_ns: int, err: BaseException | None) -> None:
            sem.release()
            on_ack(latency_ns, err)

        TOTAL = 1_000
        gen = wl.generator(seed=0xFEED, size_bytes=256, count=TOTAL)
        for msg in gen:
            sem.acquire()
            ts = time.monotonic_ns()
            msg.stamp(ts)
            producer.produce_async(topic, msg.key, msg.value, ts, gated_on_ack)
        producer.drain(timeout_seconds=15)

        wait_until = time.monotonic() + 15
        while time.monotonic() < wait_until:
            with received_lock:
                if len(received) >= TOTAL:
                    break
            time.sleep(0.05)

    finally:
        producer.close()
        # Tell the consumer to exit promptly so it doesn't outlive this test
        # and pollute the next parametrised run with an orphan rebalance.
        consumer_stop.set()
        consumer_done.wait(timeout=10)
        admin.delete_topic(bootstrap, topic)

    assert not err_holder, f"unexpected errors: {err_holder[:3]}"
    assert ack_count == TOTAL, f"expected {TOTAL} acks, got {ack_count}"
    with received_lock:
        assert len(received) == TOTAL, f"expected {TOTAL} consumed, got {len(received)}"
        assert sorted(received) == list(range(TOTAL))
