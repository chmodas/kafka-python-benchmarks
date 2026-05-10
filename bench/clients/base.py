"""Adapter protocols.

The producer adapter is non-blocking: `produce_async` returns immediately, and the
client's own background machinery (sender thread, poll thread, asyncio loop)
delivers acks via `on_ack`. The runner enforces N-in-flight via a bounded
semaphore *outside* the client. This shape is deliberately fair to confluent-kafka,
which is designed around `produce()` + `poll()` rather than serial dispatch.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Protocol

from bench.config import ConsumerConfig, ProducerConfig

OnAck = Callable[[int, BaseException | None], None]
"""(ack_latency_ns, err) – err is None on success."""


class ProducerClient(Protocol):
    name: str

    def setup(self, bootstrap_servers: str, config: ProducerConfig) -> None: ...

    def produce_async(
        self,
        topic: str,
        key: bytes,
        value: bytes | bytearray,
        produce_ts_ns: int,
        on_ack: OnAck,
    ) -> None: ...

    def drain(self, timeout_seconds: float = 30.0) -> None: ...

    def close(self) -> None: ...


class ConsumerClient(Protocol):
    name: str

    def setup(
        self, bootstrap_servers: str, topic: str, config: ConsumerConfig, partitions: int
    ) -> None: ...

    def consume_until(
        self,
        deadline_ns: int,
        on_message: Callable[[bytes, bytes | bytearray, int], None],
        stop: threading.Event | None = None,
    ) -> None:
        """Call `on_message(key, value, recv_ts_ns)` for each record until deadline or stop."""
        ...

    def close(self) -> None: ...
