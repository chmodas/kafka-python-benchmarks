"""Benchmark runner.

One invocation = one (client × workload × repeat-count) sweep against an existing
broker. Writes results to `<out>/<client>/<workload>/repeat_<i>/`.

Phase model per repeat:

  1. Pre-warm  (PRE_WARM_SECONDS)        – broker handler/epoll/page-cache warmup.
  2. Discard   (10% of duration)         – measurement-window warmup; adapter
                                            connections are warm but JIT and
                                            steady-state effects need a moment.
  3. Record    (remaining 90% of duration) – the actual measurement window.

The consumer runs continuously across all three phases (so its consumer-group
join doesn't show up as cold-start in the record window). Both ack and e2e
metrics are gated by the produce-timestamp embedded in the message: messages
produced before the record window starts are not recorded.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from bench import admin, env
from bench import workload as wl
from bench.clients import make_consumer, make_producer
from bench.config import ConsumerConfig, Workload, by_name
from bench.metrics import LatencyMetrics, ResourceSampler, write_histogram

PRE_WARM_SECONDS = 10
DISCARD_FRACTION = 0.1
MIN_DISCARD_SECONDS = 1.0


class _StartGate:
    """Mutable holder for the measurement-window start timestamp.

    Set to a non-zero monotonic_ns once the record phase begins. Producer's
    on_ack and consumer's on_msg consult this to gate which samples count.
    """

    __slots__ = ("start_ns",)

    def __init__(self) -> None:
        self.start_ns: int = 0

    def is_open(self) -> bool:
        return self.start_ns > 0


def _produce_for_duration(
    producer,
    topic: str,
    seed: int,
    size: int,
    in_flight: int,
    duration_seconds: float,
    metrics: LatencyMetrics,
    gate: _StartGate,
    target_rate: float | None = None,
) -> None:
    """Run the producer at exactly N in-flight, optionally rate-limited.

    Records into `metrics` only for messages whose produce-timestamp is at or
    after `gate.start_ns`. Pass a fresh, unopened gate for discard/pre-warm
    phases and a gate already opened to `time.monotonic_ns()` for the record
    phase.
    """
    sem = threading.Semaphore(in_flight)

    def _make_on_ack(produce_ts: int) -> Callable[[int, BaseException | None], None]:
        def cb(latency_ns: int, err: BaseException | None) -> None:
            sem.release()
            if err is not None:
                metrics.errors += 1
                return
            if gate.is_open() and produce_ts >= gate.start_ns:
                metrics.record_ack(latency_ns)

        return cb

    deadline = time.monotonic() + duration_seconds
    next_send = time.monotonic()
    interval = (1.0 / target_rate) if target_rate else 0.0
    gen = wl.generator(seed=seed, size_bytes=size)

    for msg in gen:
        if time.monotonic() >= deadline:
            break
        if interval:
            now = time.monotonic()
            if now < next_send:
                time.sleep(next_send - now)
            next_send += interval
        sem.acquire()
        ts = time.monotonic_ns()
        msg.stamp(ts)
        if gate.is_open() and ts >= gate.start_ns:
            metrics.produced += 1
        try:
            # Adapters convert bytearray->bytes internally; that cost is part
            # of the per-client latency profile and is paid identically across
            # all three clients.
            producer.produce_async(topic, msg.key, msg.value, ts, _make_on_ack(ts))
        except BaseException:
            # Synchronous failure: the callback will not fire, so release the
            # semaphore here and count the error. Without this, an in_flight=1
            # configuration would deadlock on the next iteration.
            sem.release()
            metrics.errors += 1


def _consumer_thread(
    client_name: str,
    bootstrap: str,
    topic: str,
    consumer_config: ConsumerConfig,
    partitions: int,
    metrics: LatencyMetrics,
    gate: _StartGate,
    deadline_ns: int,
    stop: threading.Event,
) -> threading.Thread:
    """Run a consumer in a daemon thread that records e2e latency.

    e2e is recorded only when (a) the gate is open and (b) the message's
    embedded send-timestamp is at or after the gate's start_ns. The thread
    exits when either the deadline is reached or `stop` is set.
    """

    def _on_msg(_key: bytes, value: bytes | bytearray, recv_ts: int) -> None:
        try:
            send_ts, _seq = wl.parse(value)
        except Exception:
            return
        if not gate.is_open() or send_ts < gate.start_ns or send_ts <= 0:
            return
        metrics.record_e2e(recv_ts - send_ts)

    def _run() -> None:
        consumer = make_consumer(client_name)
        try:
            consumer.setup(
                bootstrap_servers=bootstrap,
                topic=topic,
                config=consumer_config,
                partitions=partitions,
            )
            consumer.consume_until(deadline_ns, _on_msg, stop=stop)
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    t = threading.Thread(target=_run, name=f"consumer-{client_name}", daemon=True)
    t.start()
    return t


def _run_one_repeat(
    client_name: str,
    bootstrap: str,
    workload: Workload,
    seed: int,
    out_dir: Path,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    topic = f"bench-{workload.name}-{uuid.uuid4().hex[:8]}"
    admin.create_topic(bootstrap, topic, partitions=workload.partitions)

    metrics = LatencyMetrics()
    sampler = ResourceSampler(interval_seconds=0.1)
    producer = make_producer(client_name)
    gate = _StartGate()

    # Per-repeat group_id avoids accumulating per-topic state in __consumer_offsets
    # for the same group and keeps rebalances clean.
    consumer_config = replace(
        workload.consumer,
        group_id=f"{workload.consumer.group_id}-{uuid.uuid4().hex[:8]}",
    )

    consumer_stop = threading.Event()
    consumer_thread: threading.Thread | None = None
    discard_seconds = max(MIN_DISCARD_SECONDS, workload.duration_seconds * DISCARD_FRACTION)
    record_seconds = max(1.0, workload.duration_seconds - discard_seconds)

    try:
        producer.setup(bootstrap_servers=bootstrap, config=workload.producer)

        if workload.kind in ("latency", "throughput"):
            consumer_deadline = time.monotonic_ns() + int(
                (PRE_WARM_SECONDS + discard_seconds + record_seconds + 10) * 1e9
            )
            consumer_thread = _consumer_thread(
                client_name=client_name,
                bootstrap=bootstrap,
                topic=topic,
                consumer_config=consumer_config,
                partitions=workload.partitions,
                metrics=metrics,
                gate=gate,
                deadline_ns=consumer_deadline,
                stop=consumer_stop,
            )

        # Phase 1: pre-warm. gate is closed so neither ack nor e2e are recorded.
        warm_metrics = LatencyMetrics()
        _produce_for_duration(
            producer=producer,
            topic=topic,
            seed=seed ^ 0xDEADBEEF,
            size=workload.message_size_bytes,
            in_flight=workload.in_flight,
            duration_seconds=PRE_WARM_SECONDS,
            metrics=warm_metrics,
            gate=gate,
        )
        producer.drain()

        # Phase 2: discard window. Same producer/consumer state, gate still closed.
        discard_metrics = LatencyMetrics()
        _produce_for_duration(
            producer=producer,
            topic=topic,
            seed=seed ^ 0xBADF00D,
            size=workload.message_size_bytes,
            in_flight=workload.in_flight,
            duration_seconds=discard_seconds,
            metrics=discard_metrics,
            gate=gate,
        )
        producer.drain()

        # Phase 3: record window. Open the gate and start the resource sampler.
        gate.start_ns = time.monotonic_ns()
        sampler.start()
        t0 = time.monotonic()
        _produce_for_duration(
            producer=producer,
            topic=topic,
            seed=seed,
            size=workload.message_size_bytes,
            in_flight=workload.in_flight,
            duration_seconds=record_seconds,
            metrics=metrics,
            gate=gate,
            target_rate=workload.target_msgs_per_sec,
        )
        producer.drain()
        elapsed = time.monotonic() - t0

        # Give the consumer a few seconds to drain trailing records before stopping it.
        if consumer_thread is not None:
            drain_deadline = time.monotonic() + 5.0
            while metrics.consumed < metrics.acked and time.monotonic() < drain_deadline:
                time.sleep(0.05)
            consumer_stop.set()
            consumer_thread.join(timeout=5.0)

        write_histogram(metrics.ack, out_dir / "ack_latency.hgrm.json")
        write_histogram(metrics.end_to_end, out_dir / "e2e_latency.hgrm.json")

        summary = {
            "client": client_name,
            "workload": workload.name,
            "kind": workload.kind,
            "seed": seed,
            "topic": topic,
            "elapsed_seconds": elapsed,
            "discard_seconds": discard_seconds,
            "record_seconds": record_seconds,
            "msgs_per_sec": metrics.acked / elapsed if elapsed > 0 else 0.0,
            "metrics": metrics.summary(),
            "resource": sampler.summary(),
            "consumer_group_id": consumer_config.group_id,
        }
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
        return summary
    finally:
        # Always stop the sampler so its daemon thread doesn't outlive the repeat
        # and pollute subsequent CPU readings with its own wakeups.
        try:
            sampler.stop()
        except Exception:
            pass
        consumer_stop.set()
        if consumer_thread is not None:
            consumer_thread.join(timeout=2.0)
        try:
            producer.close()
        except Exception:
            pass
        admin.delete_topic(bootstrap, topic)
        # Drop the consumer group so its metadata doesn't accumulate in
        # __consumer_offsets – orphan groups across many repeats can pressure
        # the broker's group coordinator and trigger controller flaps.
        admin.delete_consumer_group(bootstrap, consumer_config.group_id)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--client", required=True, choices=["kafka-python", "confluent", "aiokafka"])
    p.add_argument("--workload", required=True, help="workload name from bench.config")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--out", required=True, help="output directory for this sweep")
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument(
        "--repeat-offset",
        type=int,
        default=0,
        help="repeat index offset for output dir naming and seed derivation",
    )
    p.add_argument("--seed", type=int, default=0xC0FFEE)
    p.add_argument(
        "--rtt",
        default="loopback",
        choices=["loopback", "1ms", "5ms"],
        help="recorded for run_meta only; netem must be applied separately via scripts/netem.sh",
    )
    p.add_argument(
        "--duration",
        type=int,
        default=None,
        help="override workload duration (seconds), useful for smoke runs",
    )
    args = p.parse_args()

    workload = by_name(args.workload)
    if args.duration is not None:
        workload = replace(workload, duration_seconds=args.duration)

    out_root = Path(args.out) / args.client / args.workload
    out_root.mkdir(parents=True, exist_ok=True)

    meta = env.capture(
        extra={
            "rtt_mode": args.rtt,
            "client": args.client,
            "workload": args.workload,
            "repeats": args.repeats,
            "repeat_offset": args.repeat_offset,
            "seed": args.seed,
            "started_at": time.time(),
        }
    )
    (out_root / "run_meta.json").write_text(json.dumps(meta, indent=2))

    summaries = []
    for i in range(args.repeats):
        idx = args.repeat_offset + i
        repeat_dir = out_root / f"repeat_{idx:02d}"
        seed = args.seed + idx
        print(f"[{args.client}] {args.workload} repeat {idx} seed=0x{seed:x}")
        summary = _run_one_repeat(
            client_name=args.client,
            bootstrap=args.bootstrap,
            workload=workload,
            seed=seed,
            out_dir=repeat_dir,
        )
        summaries.append(summary)
        time.sleep(2.0)

    (out_root / "summaries.json").write_text(json.dumps(summaries, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
