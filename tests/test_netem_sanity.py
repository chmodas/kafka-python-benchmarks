"""Sanity check that --rtt 1ms actually adds delay.

Skipped unless a broker is reachable AND the BENCH_NETEM env var is set
(because applying tc requires NET_ADMIN on the broker container, and we
don't want this running in CI by accident).
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
import uuid
from pathlib import Path

import pytest

from bench import admin
from bench import workload as wl
from bench.clients import make_producer
from bench.config import ProducerConfig

NETEM_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "netem.sh"


def _measure_p50_us(bootstrap: str) -> float:
    topic = f"netem-{uuid.uuid4().hex[:8]}"
    admin.create_topic(bootstrap, topic, partitions=1)
    producer = make_producer("confluent")
    samples: list[int] = []
    sem = threading.Semaphore(1)

    def on_ack(latency_ns: int, err) -> None:
        sem.release()
        if err is None:
            samples.append(latency_ns)

    try:
        producer.setup(bootstrap_servers=bootstrap, config=ProducerConfig(acks=1, linger_ms=0))
        gen = wl.generator(seed=0, size_bytes=128, count=300)
        for msg in gen:
            sem.acquire()
            ts = time.monotonic_ns()
            msg.stamp(ts)
            producer.produce_async(topic, msg.key, msg.value, ts, on_ack)
        producer.drain(timeout_seconds=10)
    finally:
        producer.close()
        admin.delete_topic(bootstrap, topic)

    samples.sort()
    return samples[len(samples) // 2] / 1_000  # µs


@pytest.mark.skipif(
    not os.environ.get("BENCH_NETEM"),
    reason="set BENCH_NETEM=1 to opt into netem-modifying tests",
)
def test_netem_1ms_increases_p50(bootstrap: str) -> None:
    baseline = _measure_p50_us(bootstrap)
    subprocess.run(["bash", str(NETEM_SCRIPT), "apply", "1"], check=True)
    try:
        with_delay = _measure_p50_us(bootstrap)
    finally:
        subprocess.run(["bash", str(NETEM_SCRIPT), "clear"], check=True)
    # 1 ms RTT should add at least ~1.5 ms to a small-message ack p50.
    assert (
        with_delay > baseline + 1_000
    ), f"expected with_delay >> baseline; baseline={baseline:.0f}us with_delay={with_delay:.0f}us"
