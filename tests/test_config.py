from __future__ import annotations

import pytest

from bench import config


def test_all_workloads_have_unique_names() -> None:
    names = [w.name for w in config.all_workloads()]
    assert len(names) == len(set(names)), "duplicate workload names detected"


def test_latency_matrix_covers_expected_axes() -> None:
    latency = config.workloads_for("latency")
    sizes = {w.message_size_bytes for w in latency}
    in_flights = {w.in_flight for w in latency}
    compressions = {w.producer.compression for w in latency}
    assert sizes == set(config.MESSAGE_SIZES)
    assert in_flights == set(config.LATENCY_IN_FLIGHTS)
    assert compressions == set(config.LATENCY_COMPRESSIONS)


def test_idempotence_acks0_is_excluded() -> None:
    latency = config.workloads_for("latency")
    bad = [w for w in latency if w.producer.enable_idempotence and w.producer.acks == 0]
    assert not bad, "idempotence requires acks=all; acks=0 cell should be skipped"


def test_idempotence_uses_acks_all() -> None:
    for w in config.workloads_for("latency"):
        if w.producer.enable_idempotence:
            assert w.producer.acks == "all"


def test_workload_name_reflects_effective_acks() -> None:
    """If the cell has idempotence on, name must contain `acksall`, not the original ack value."""
    for w in config.workloads_for("latency"):
        if w.producer.enable_idempotence:
            assert "acksall" in w.name, w.name
        else:
            assert f"acks{w.producer.acks}" in w.name, w.name


def test_by_name_round_trip() -> None:
    sample = config.workloads_for("latency")[0]
    # all_workloads() rebuilds dataclasses each call; check equality, not identity.
    assert config.by_name(sample.name) == sample


def test_by_name_unknown() -> None:
    with pytest.raises(KeyError):
        config.by_name("does-not-exist")
