from __future__ import annotations

import struct

import pytest

from bench import workload as wl


def test_message_layout_round_trip() -> None:
    msg = wl.make_message(seed=42, seq=7, size_bytes=128)
    msg.stamp(123_456_789)
    ts, seq = wl.parse(msg.value)
    assert ts == 123_456_789
    assert seq == 7
    assert len(msg.value) == 128
    assert msg.key == struct.pack(">I", 7)


def test_same_seed_produces_identical_filler() -> None:
    a = wl.make_message(seed=99, seq=3, size_bytes=256)
    b = wl.make_message(seed=99, seq=3, size_bytes=256)
    # Header bytes are zeroed pre-stamp; filler must match.
    assert a.value[wl.HEADER_SIZE :] == b.value[wl.HEADER_SIZE :]


def test_different_seeds_diverge() -> None:
    a = wl.make_message(seed=1, seq=3, size_bytes=256)
    b = wl.make_message(seed=2, seq=3, size_bytes=256)
    assert a.value[wl.HEADER_SIZE :] != b.value[wl.HEADER_SIZE :]


def test_size_below_header_rejected() -> None:
    with pytest.raises(ValueError):
        wl.make_message(seed=0, seq=0, size_bytes=8)


def test_generator_is_monotonic_and_terminates() -> None:
    gen = wl.generator(seed=0, size_bytes=64, count=5)
    seqs = [m.seq for m in gen]
    assert seqs == [0, 1, 2, 3, 4]
