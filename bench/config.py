"""Workload and benchmark configuration.

The configurations enumerated here are the parity reference: every client adapter
maps these settings to its native option names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Acks = Literal[0, 1, "all"]
Compression = Literal["none", "lz4", "zstd", "snappy", "gzip"]
BenchKind = Literal["latency", "throughput", "consumer-throughput", "fetch-latency", "rebalance"]


@dataclass(frozen=True)
class ProducerConfig:
    acks: Acks = 1
    linger_ms: int = 0
    batch_size_bytes: int = 1
    compression: Compression = "none"
    enable_idempotence: bool = False
    max_in_flight_requests_per_connection: int = 5
    request_timeout_ms: int = 30_000
    delivery_timeout_ms: int = 60_000


@dataclass(frozen=True)
class ConsumerConfig:
    group_id: str = "bench-consumer"
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500


@dataclass(frozen=True)
class Workload:
    name: str
    kind: BenchKind
    message_size_bytes: int
    partitions: int
    in_flight: int  # 1, 8, 64 for latency runs; ignored for throughput-saturated runs
    duration_seconds: int
    producer: ProducerConfig
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    target_msgs_per_sec: int | None = None  # None = unbounded; used to find ceiling
    notes: str = ""


# ---------------------------------------------------------------------------
# Matrix definitions
# ---------------------------------------------------------------------------

MESSAGE_SIZES = (64, 1024, 10240)
LATENCY_IN_FLIGHTS = (1, 8, 64)
LATENCY_ACKS: tuple[Acks, ...] = (0, 1)
LATENCY_COMPRESSIONS: tuple[Compression, ...] = ("none", "lz4")
LATENCY_IDEMPOTENCE = (False, True)


def _latency_workloads() -> list[Workload]:
    """Latency-benchmark matrix.

    Idempotent producers are configured with `acks=all` because both
    librdkafka and kafka-python enforce that internally for idempotence.
    On a single-broker cluster (ISR=1) `acks=all` is identical to `acks=1`
    in latency terms. To avoid matrix cells that look like they vary `acks`
    while actually pinning it to "all" via the idempotence flag, we only
    enumerate idempotent cells once (with the original ack as a documentation
    knob, not a behaviour knob): the workload name embeds the *effective* acks.
    """
    out: list[Workload] = []
    for size in MESSAGE_SIZES:
        for acks in LATENCY_ACKS:
            for comp in LATENCY_COMPRESSIONS:
                for idem in LATENCY_IDEMPOTENCE:
                    if idem and acks == 0:
                        # idempotence forces acks=all; only enumerate once per (size, comp).
                        continue
                    if idem and acks == 1:
                        # The idem=True cell would have effective acks=all regardless of input.
                        # Enumerate only once (at acks=1 entry) and label with effective acks.
                        pass
                    for in_flight in LATENCY_IN_FLIGHTS:
                        max_inflight = 5 if idem else max(in_flight, 5)
                        effective_acks: Acks = "all" if idem else acks
                        producer = ProducerConfig(
                            acks=effective_acks,
                            linger_ms=0,
                            batch_size_bytes=1,
                            compression=comp,
                            enable_idempotence=idem,
                            max_in_flight_requests_per_connection=max_inflight,
                        )
                        name = (
                            f"lat_sz{size}_acks{effective_acks}_{comp}_"
                            f"idem{int(idem)}_inflight{in_flight}"
                        )
                        out.append(
                            Workload(
                                name=name,
                                kind="latency",
                                message_size_bytes=size,
                                partitions=1,
                                in_flight=in_flight,
                                duration_seconds=60,
                                producer=producer,
                            )
                        )
    return out


def _throughput_workloads() -> list[Workload]:
    out: list[Workload] = []
    for size in (1024, 10240):
        for comp in ("none", "lz4", "zstd"):
            for partitions in (1, 4, 16):
                for idem in (False, True):
                    producer = ProducerConfig(
                        acks="all" if idem else 1,
                        linger_ms=5,
                        batch_size_bytes=64 * 1024,
                        compression=comp,
                        enable_idempotence=idem,
                        max_in_flight_requests_per_connection=5 if idem else 64,
                    )
                    name = f"thr_sz{size}_{comp}_p{partitions}_idem{int(idem)}"
                    out.append(
                        Workload(
                            name=name,
                            kind="throughput",
                            message_size_bytes=size,
                            partitions=partitions,
                            in_flight=64,
                            duration_seconds=60,
                            producer=producer,
                        )
                    )
    return out


def _consumer_workloads() -> list[Workload]:
    out: list[Workload] = []
    for size in (1024, 10240):
        out.append(
            Workload(
                name=f"cons_thr_sz{size}",
                kind="consumer-throughput",
                message_size_bytes=size,
                partitions=4,
                in_flight=64,
                duration_seconds=60,
                producer=ProducerConfig(
                    acks=1,
                    linger_ms=5,
                    batch_size_bytes=64 * 1024,
                    compression="none",
                ),
                consumer=ConsumerConfig(fetch_min_bytes=1, fetch_max_wait_ms=500),
                notes="Pre-populates topic, then measures consumer drain rate.",
            )
        )
        out.append(
            Workload(
                name=f"fetch_lat_sz{size}",
                kind="fetch-latency",
                message_size_bytes=size,
                partitions=1,
                in_flight=1,
                duration_seconds=30,
                producer=ProducerConfig(acks=1, linger_ms=0, batch_size_bytes=1),
                consumer=ConsumerConfig(fetch_min_bytes=1, fetch_max_wait_ms=500),
                notes="Measures poll() to first-message latency on otherwise idle topic.",
            )
        )
    return out


def _rebalance_workloads() -> list[Workload]:
    return [
        Workload(
            name="rebalance_2to1",
            kind="rebalance",
            message_size_bytes=1024,
            partitions=8,
            in_flight=1,
            duration_seconds=60,
            producer=ProducerConfig(acks=1, linger_ms=5, batch_size_bytes=64 * 1024),
            consumer=ConsumerConfig(),
            notes="2-consumer group; kill one mid-run; measure partition reassignment time.",
        )
    ]


def all_workloads() -> list[Workload]:
    return (
        _latency_workloads()
        + _throughput_workloads()
        + _consumer_workloads()
        + _rebalance_workloads()
    )


_BY_NAME: dict[str, Workload] | None = None


def by_name(name: str) -> Workload:
    global _BY_NAME
    if _BY_NAME is None:
        _BY_NAME = {w.name: w for w in all_workloads()}
    if name not in _BY_NAME:
        raise KeyError(f"unknown workload: {name}")
    return _BY_NAME[name]


def workloads_for(kind: BenchKind) -> list[Workload]:
    return [w for w in all_workloads() if w.kind == kind]
