from __future__ import annotations

from bench.reporting import _iqr


def test_iqr_returns_zero_for_small_n() -> None:
    assert _iqr([]) == 0.0
    assert _iqr([1.0]) == 0.0
    assert _iqr([1.0, 2.0]) == 0.0
    assert _iqr([1.0, 2.0, 3.0]) == 0.0


def test_iqr_for_n5_is_positive_and_smaller_than_range() -> None:
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    iqr = _iqr(values)
    assert iqr > 0
    assert iqr < (max(values) - min(values))


def test_iqr_zero_for_constant_values() -> None:
    assert _iqr([5.0] * 10) == 0.0


def test_iqr_grows_with_spread() -> None:
    tight = _iqr([10.0, 11.0, 12.0, 13.0, 14.0])
    wide = _iqr([10.0, 50.0, 100.0, 500.0, 1000.0])
    assert wide > tight
