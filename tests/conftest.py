from __future__ import annotations

import os
import socket

import pytest


def _broker_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def bootstrap() -> str:
    addr = os.environ.get("BENCH_BOOTSTRAP", "localhost:9092")
    host, _, port = addr.partition(":")
    if not _broker_reachable(host, int(port)):
        pytest.skip(f"no broker reachable at {addr}; integration tests require `make up`")
    return addr
