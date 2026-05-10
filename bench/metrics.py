"""Latency histograms and resource sampling.

HDR histogram is configured for nanosecond latency with sub-microsecond resolution
up to a 60-second ceiling. The resource sampler is a light background thread that
records user-CPU, system-CPU, and RSS at a fixed cadence.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import psutil
from hdrh.histogram import HdrHistogram

LOWEST_NS = 1_000  # 1 µs resolution floor
HIGHEST_NS = 60_000_000_000  # 60 s ceiling
SIGNIFICANT_FIGURES = 3


def new_histogram() -> HdrHistogram:
    return HdrHistogram(LOWEST_NS, HIGHEST_NS, SIGNIFICANT_FIGURES)


@dataclass
class LatencyMetrics:
    ack: HdrHistogram = field(default_factory=new_histogram)
    end_to_end: HdrHistogram = field(default_factory=new_histogram)
    produced: int = 0
    acked: int = 0
    consumed: int = 0
    errors: int = 0

    def record_ack(self, latency_ns: int) -> None:
        self.acked += 1
        if latency_ns > 0:
            self.ack.record_value(min(latency_ns, HIGHEST_NS))

    def record_e2e(self, latency_ns: int) -> None:
        self.consumed += 1
        if latency_ns > 0:
            self.end_to_end.record_value(min(latency_ns, HIGHEST_NS))

    def summary(self) -> dict:
        return {
            "produced": self.produced,
            "acked": self.acked,
            "consumed": self.consumed,
            "errors": self.errors,
            "ack_latency_ns": _percentiles(self.ack),
            "e2e_latency_ns": _percentiles(self.end_to_end),
        }


def _percentiles(h: HdrHistogram) -> dict:
    if h.get_total_count() == 0:
        return {"count": 0}
    return {
        "count": h.get_total_count(),
        "min": h.get_min_value(),
        "p50": h.get_value_at_percentile(50.0),
        "p95": h.get_value_at_percentile(95.0),
        "p99": h.get_value_at_percentile(99.0),
        "p999": h.get_value_at_percentile(99.9),
        "max": h.get_max_value(),
        "mean": h.get_mean_value(),
    }


def write_histogram(h: HdrHistogram, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(
            {
                "lowest": LOWEST_NS,
                "highest": HIGHEST_NS,
                "sig_figs": SIGNIFICANT_FIGURES,
                "encoded": h.encode().decode("ascii"),
                "summary": _percentiles(h),
            },
            f,
        )


@dataclass
class ResourceSample:
    timestamp_ns: int
    cpu_user_pct: float
    cpu_system_pct: float
    rss_bytes: int


class ResourceSampler:
    """Samples cpu_times() + memory at a fixed interval in a daemon thread."""

    def __init__(self, interval_seconds: float = 0.1) -> None:
        self.interval = interval_seconds
        self.proc = psutil.Process()
        self.proc.cpu_percent(interval=None)
        self.samples: list[ResourceSample] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_user = 0.0
        self._last_system = 0.0
        self._last_wall = time.monotonic()

    def start(self) -> None:
        cpu = self.proc.cpu_times()
        self._last_user = cpu.user
        self._last_system = cpu.system
        self._last_wall = time.monotonic()
        self._thread = threading.Thread(target=self._run, daemon=True, name="resource-sampler")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            now = time.monotonic()
            elapsed = now - self._last_wall
            if elapsed <= 0:
                continue
            cpu = self.proc.cpu_times()
            user_delta = cpu.user - self._last_user
            sys_delta = cpu.system - self._last_system
            self._last_user, self._last_system, self._last_wall = cpu.user, cpu.system, now
            try:
                rss = self.proc.memory_info().rss
            except psutil.Error:
                rss = 0
            self.samples.append(
                ResourceSample(
                    timestamp_ns=time.monotonic_ns(),
                    cpu_user_pct=100.0 * user_delta / elapsed,
                    cpu_system_pct=100.0 * sys_delta / elapsed,
                    rss_bytes=rss,
                )
            )

    def summary(self) -> dict:
        if not self.samples:
            return {"samples": 0}
        users = [s.cpu_user_pct for s in self.samples]
        systems = [s.cpu_system_pct for s in self.samples]
        rss = [s.rss_bytes for s in self.samples]
        return {
            "samples": len(self.samples),
            "cpu_user_pct_median": _median(users),
            "cpu_user_pct_max": max(users),
            "cpu_system_pct_median": _median(systems),
            "cpu_system_pct_max": max(systems),
            "rss_bytes_median": int(_median(rss)),
            "rss_bytes_max": max(rss),
        }


def _median(values: list[float] | list[int]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    mid = len(s) // 2
    return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2.0
