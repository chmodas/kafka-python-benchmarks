from __future__ import annotations

import json
import time
from pathlib import Path

from bench import metrics


def test_histogram_percentiles_sane() -> None:
    h = metrics.new_histogram()
    for v in range(1, 1001):
        h.record_value(v * 1000)  # 1 µs to 1 ms in 1 µs steps
    p50 = h.get_value_at_percentile(50.0)
    p99 = h.get_value_at_percentile(99.0)
    assert 450_000 < p50 < 550_000
    assert 980_000 < p99 < 1_010_000


def test_latency_metrics_records_and_summarises() -> None:
    m = metrics.LatencyMetrics()
    for v in (1_000, 5_000, 10_000, 100_000):
        m.record_ack(v)
    assert m.acked == 4
    summary = m.summary()
    ack = summary["ack_latency_ns"]
    assert ack["count"] == 4
    # HDR bucketing rounds to the nearest representable value within
    # `lowest_discernible_value`-sized buckets; allow generous tolerance.
    assert 0 < ack["min"] <= 1_500
    assert 90_000 <= ack["max"] <= 110_000


def test_resource_sampler_starts_and_stops_clean(tmp_path: Path) -> None:
    sampler = metrics.ResourceSampler(interval_seconds=0.05)
    sampler.start()
    time.sleep(0.3)
    sampler.stop()
    assert sampler.samples, "expected at least one sample"
    summary = sampler.summary()
    assert summary["samples"] >= 1
    assert summary["rss_bytes_max"] > 0


def test_write_histogram_round_trip(tmp_path: Path) -> None:
    h = metrics.new_histogram()
    for v in range(1, 101):
        h.record_value(v * 10_000)
    path = tmp_path / "hist.json"
    metrics.write_histogram(h, path)
    payload = json.loads(path.read_text())
    assert payload["lowest"] == metrics.LOWEST_NS
    assert payload["highest"] == metrics.HIGHEST_NS
    assert payload["summary"]["count"] == 100


def test_histogram_clamps_to_ceiling() -> None:
    m = metrics.LatencyMetrics()
    m.record_ack(metrics.HIGHEST_NS * 100)
    summary = m.summary()
    assert summary["ack_latency_ns"]["count"] == 1
    # HDR reports the bucket's representative value, which can be slightly
    # above the recorded value due to 3-sig-fig precision; accept up to 1%.
    assert summary["ack_latency_ns"]["max"] <= metrics.HIGHEST_NS * 1.01


def test_resource_sampler_is_idempotent_to_double_stop() -> None:
    """stop() should be safe to call twice (the runner's finally calls it after
    a possible normal stop)."""
    sampler = metrics.ResourceSampler(interval_seconds=0.05)
    sampler.start()
    time.sleep(0.15)
    sampler.stop()
    sampler.stop()  # should not raise
