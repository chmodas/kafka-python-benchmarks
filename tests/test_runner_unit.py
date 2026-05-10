"""Unit tests for the runner's pacing and gating primitives.

Uses a fake ProducerClient that does no I/O. These cover the two highest-risk
behaviours from the code review: (a) the pacing semaphore must release on
synchronous error so an in_flight=1 run doesn't deadlock, and (b) the
measurement-window gate must discard pre-warm samples.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from bench.metrics import LatencyMetrics
from bench.runner import _produce_for_duration, _StartGate


class _FakeProducer:
    """Records calls and fires on_ack synchronously after a small delay."""

    def __init__(self, raise_on: int | None = None, delay_ns: int = 1_000) -> None:
        self.raise_on = raise_on
        self.delay_ns = delay_ns
        self.calls = 0
        self.acks: list[int] = []

    def produce_async(
        self,
        topic: str,
        key: bytes,
        value: bytes | bytearray,
        produce_ts_ns: int,
        on_ack: Callable[[int, BaseException | None], None],
    ) -> None:
        self.calls += 1
        if self.raise_on is not None and self.calls == self.raise_on:
            raise RuntimeError("simulated synchronous failure")
        # Fire ack on a tiny background timer so the producer thread can
        # acquire the next semaphore slot before the callback runs.
        latency = self.delay_ns
        self.acks.append(latency)
        threading.Timer(latency / 1e9, on_ack, args=(latency, None)).start()


def test_semaphore_does_not_deadlock_on_synchronous_error() -> None:
    """If produce_async raises synchronously, the pacing semaphore must release.

    With in_flight=1 a single failed produce would otherwise block the next
    iteration forever.
    """
    fake = _FakeProducer(raise_on=3)  # 3rd call raises
    metrics = LatencyMetrics()
    gate = _StartGate()
    gate.start_ns = time.monotonic_ns()
    _produce_for_duration(
        producer=fake,
        topic="t",
        seed=0,
        size=64,
        in_flight=1,
        duration_seconds=0.5,
        metrics=metrics,
        gate=gate,
    )
    # No deadlock => loop produced more than 3 messages (the 3rd raised, the rest succeeded).
    assert fake.calls > 3
    assert metrics.errors >= 1


def test_gate_closed_phase_records_no_acks() -> None:
    """Pre-warm/discard phase: gate is closed, ack metrics must remain empty."""
    fake = _FakeProducer()
    metrics = LatencyMetrics()
    gate = _StartGate()  # closed
    _produce_for_duration(
        producer=fake,
        topic="t",
        seed=0,
        size=64,
        in_flight=4,
        duration_seconds=0.3,
        metrics=metrics,
        gate=gate,
    )
    # All messages succeed via the fake but since gate is closed nothing was recorded.
    time.sleep(0.1)  # let timers fire
    assert fake.calls > 0
    assert metrics.acked == 0
    assert metrics.produced == 0
    assert metrics.ack.get_total_count() == 0


def test_gate_open_phase_records_acks() -> None:
    """Record phase: gate is open, ack metrics populated."""
    fake = _FakeProducer()
    metrics = LatencyMetrics()
    gate = _StartGate()
    gate.start_ns = time.monotonic_ns()
    _produce_for_duration(
        producer=fake,
        topic="t",
        seed=0,
        size=64,
        in_flight=4,
        duration_seconds=0.3,
        metrics=metrics,
        gate=gate,
    )
    # Wait for outstanding fake-ack timers.
    time.sleep(0.1)
    assert metrics.produced > 0
    assert metrics.acked > 0
    assert metrics.ack.get_total_count() == metrics.acked


def test_messages_produced_before_gate_open_are_not_recorded() -> None:
    """Mid-run gate-open: messages produced before the open are discarded.

    Simulates the discard-then-record transition: the same fake producer
    runs first with a closed gate, then with the gate opened halfway.
    """
    fake = _FakeProducer()
    metrics = LatencyMetrics()
    gate = _StartGate()  # closed initially
    # Produce some during closed phase.
    _produce_for_duration(
        producer=fake,
        topic="t",
        seed=0,
        size=64,
        in_flight=2,
        duration_seconds=0.2,
        metrics=metrics,
        gate=gate,
    )
    closed_calls = fake.calls
    # Open the gate and produce more.
    gate.start_ns = time.monotonic_ns()
    _produce_for_duration(
        producer=fake,
        topic="t",
        seed=1,
        size=64,
        in_flight=2,
        duration_seconds=0.2,
        metrics=metrics,
        gate=gate,
    )
    time.sleep(0.1)
    # Recorded acks must be at most calls made *after* the gate opened, never
    # the calls from the closed phase.
    open_calls = fake.calls - closed_calls
    assert (
        metrics.acked <= open_calls
    ), f"recorded acks ({metrics.acked}) exceeds open-phase calls ({open_calls})"
    assert metrics.produced <= open_calls
