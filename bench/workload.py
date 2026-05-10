"""Deterministic message generation with embedded send-timestamp.

Layout of every produced value:

    bytes 0..7   : monotonic_ns send-timestamp, big-endian uint64
    bytes 8..11  : sequence number, big-endian uint32
    bytes 12..   : deterministic filler derived from seed + seq

The producer overwrites bytes 0..7 immediately before calling the client's
produce method, so the timestamp reflects the entry into the client (not the
generator). Consumers parse both fields to compute end-to-end latency and to
verify ordering / loss.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from random import Random

HEADER_SIZE = 12  # 8-byte ts + 4-byte seq
_TS_FORMAT = ">Q"  # big-endian uint64
_SEQ_FORMAT = ">I"  # big-endian uint32


def _filler(seed: int, seq: int, length: int) -> bytes:
    if length <= 0:
        return b""
    rng = Random((seed * 2654435761) ^ seq)  # inexpensive deterministic mix
    return rng.randbytes(length)


@dataclass
class Message:
    key: bytes
    value: bytearray
    seq: int

    def stamp(self, monotonic_ns: int) -> None:
        struct.pack_into(_TS_FORMAT, self.value, 0, monotonic_ns)


def make_message(seed: int, seq: int, size_bytes: int) -> Message:
    if size_bytes < HEADER_SIZE:
        raise ValueError(f"size_bytes must be >= {HEADER_SIZE}, got {size_bytes}")
    body = bytearray(size_bytes)
    struct.pack_into(_TS_FORMAT, body, 0, 0)  # placeholder; producer stamps before send
    struct.pack_into(_SEQ_FORMAT, body, 8, seq)
    body[HEADER_SIZE:] = _filler(seed, seq, size_bytes - HEADER_SIZE)
    key = struct.pack(_SEQ_FORMAT, seq)
    return Message(key=key, value=body, seq=seq)


def parse(value: bytes | bytearray) -> tuple[int, int]:
    """Return (send_ts_ns, seq) from a produced message body."""
    ts = struct.unpack_from(_TS_FORMAT, value, 0)[0]
    seq = struct.unpack_from(_SEQ_FORMAT, value, 8)[0]
    return ts, seq


def generator(seed: int, size_bytes: int, count: int | None = None):
    """Yield Messages indefinitely (or up to `count`) with monotonically increasing seq."""
    seq = 0
    while count is None or seq < count:
        yield make_message(seed, seq, size_bytes)
        seq += 1
