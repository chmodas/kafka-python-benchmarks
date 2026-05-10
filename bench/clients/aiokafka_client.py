"""aiokafka adapter.

aiokafka is async-native. To match the synchronous adapter facade used by the
runner, this module owns a dedicated thread running an asyncio loop. Each
`produce_async` schedules a coroutine on that loop via
`asyncio.run_coroutine_threadsafe`. The coroutine awaits `producer.send_and_wait`
and invokes `on_ack` when it completes.

The asyncio overhead inherent to this design is treated as part of "what aiokafka
costs you"; the report subtracts a baseline measured by `scripts/asyncio_baseline.py`
so readers can separate library cost from event-loop cost.
"""

from __future__ import annotations

import asyncio
import threading
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from bench.config import ConsumerConfig, ProducerConfig

_COMPRESSION_MAP = {"none": None, "lz4": "lz4", "zstd": "zstd", "snappy": "snappy", "gzip": "gzip"}


class _LoopThread:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run, name="aiokafka-loop", daemon=True)

    def start(self) -> None:
        self.thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._ready.set()
        self.loop.run_forever()

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5.0)
        # Drain any pending tasks before closing.
        try:
            pending = asyncio.all_tasks(self.loop)
            for t in pending:
                t.cancel()
        except RuntimeError:
            pass
        self.loop.close()


class AiokafkaProducer:
    name = "aiokafka"

    def __init__(self) -> None:
        self._loop: _LoopThread | None = None
        self._producer: AIOKafkaProducer | None = None

    def setup(self, bootstrap_servers: str, config: ProducerConfig) -> None:
        self._loop = _LoopThread()
        self._loop.start()

        # aiokafka 0.14+ calls get_running_loop() in its constructor, so the
        # AIOKafkaProducer must be constructed inside the loop thread. We also
        # have no `max_in_flight_requests_per_connection` knob; the runner's
        # external semaphore enforces in-flight depth across all clients.
        async def _construct_and_start():
            self._producer = AIOKafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks=config.acks,
                linger_ms=config.linger_ms,
                max_batch_size=max(config.batch_size_bytes, 16384),
                compression_type=_COMPRESSION_MAP[config.compression],
                enable_idempotence=config.enable_idempotence,
                request_timeout_ms=config.request_timeout_ms,
            )
            await self._producer.start()

        self._loop.submit(_construct_and_start()).result(timeout=30)

    def produce_async(self, topic, key, value, produce_ts_ns, on_ack):
        assert self._producer is not None and self._loop is not None
        # Cast to bytes once so the coroutine doesn't re-copy.
        k = bytes(key)
        v = bytes(value)
        prod = self._producer

        async def _send():
            try:
                await prod.send_and_wait(topic, value=v, key=k)
                on_ack(time.monotonic_ns() - produce_ts_ns, None)
            except BaseException as exc:
                on_ack(time.monotonic_ns() - produce_ts_ns, exc)

        self._loop.submit(_send())

    def drain(self, timeout_seconds=30.0):
        assert self._producer is not None and self._loop is not None
        self._loop.submit(self._producer.flush()).result(timeout=timeout_seconds)

    def close(self):
        if self._producer is not None and self._loop is not None:
            try:
                self._loop.submit(self._producer.stop()).result(timeout=10)
            except Exception:
                pass
        if self._loop is not None:
            self._loop.stop()
            self._loop = None
        self._producer = None


class AiokafkaConsumer:
    name = "aiokafka"

    def __init__(self) -> None:
        self._loop: _LoopThread | None = None
        self._consumer: AIOKafkaConsumer | None = None

    def setup(
        self, bootstrap_servers: str, topic: str, config: ConsumerConfig, partitions: int
    ) -> None:
        self._loop = _LoopThread()
        self._loop.start()

        async def _construct_and_start():
            self._consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=config.group_id,
                fetch_min_bytes=config.fetch_min_bytes,
                fetch_max_wait_ms=config.fetch_max_wait_ms,
                auto_offset_reset=config.auto_offset_reset,
                enable_auto_commit=config.enable_auto_commit,
                max_poll_records=config.max_poll_records,
            )
            await self._consumer.start()

        self._loop.submit(_construct_and_start()).result(timeout=30)

    def consume_until(self, deadline_ns, on_message, stop=None):
        assert self._consumer is not None and self._loop is not None
        cons = self._consumer

        async def _drain():
            while time.monotonic_ns() < deadline_ns:
                if stop is not None and stop.is_set():
                    return
                remaining_ms = max(1, (deadline_ns - time.monotonic_ns()) // 1_000_000)
                batch = await cons.getmany(timeout_ms=min(100, remaining_ms))
                recv_ts = time.monotonic_ns()
                if not batch:
                    continue
                for records in batch.values():
                    for r in records:
                        on_message(r.key or b"", r.value or b"", recv_ts)

        self._loop.submit(_drain()).result(timeout=(deadline_ns - time.monotonic_ns()) / 1e9 + 10)

    def close(self):
        if self._consumer is not None and self._loop is not None:
            try:
                self._loop.submit(self._consumer.stop()).result(timeout=10)
            except Exception:
                pass
        if self._loop is not None:
            self._loop.stop()
            self._loop = None
        self._consumer = None
