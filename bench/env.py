"""Environment capture for run_meta.json."""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version


def _safe(cmd: list[str]) -> str:
    if not shutil.which(cmd[0]):
        return ""
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:
        return ""


def _pkg_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "missing"


def _librdkafka_info() -> dict:
    try:
        from confluent_kafka import libversion  # type: ignore

        v_str, v_int = libversion()
        return {"version": v_str, "version_int": v_int}
    except Exception as exc:
        return {"error": repr(exc)}


def _docker_image_digest(image: str) -> str:
    return _safe(["docker", "inspect", "--format", "{{index .RepoDigests 0}}", image])


def _sysctl(key: str) -> str:
    return _safe(["sysctl", "-n", key])


def capture(extra: dict | None = None) -> dict:
    info: dict = {
        "python": {
            "version": sys.version,
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "uname": " ".join(platform.uname()),
        },
        "cpu": _cpu_info(),
        "ulimit_nofile": _safe(["sh", "-c", "ulimit -n"]),
        "loadavg": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
        "sysctl": {
            "net.core.rmem_max": _sysctl("net.core.rmem_max"),
            "net.core.wmem_max": _sysctl("net.core.wmem_max"),
            "net.ipv4.tcp_no_metrics_save": _sysctl("net.ipv4.tcp_no_metrics_save"),
        },
        "docker": {
            "version": _safe(["docker", "version", "--format", "{{.Server.Version}}"]),
            "info": _safe(["sh", "-c", "docker info --format '{{json .}}' 2>/dev/null"]),
        },
        "broker": {
            "image": "apache/kafka:3.9.0",
            "image_digest": _docker_image_digest("apache/kafka:3.9.0"),
        },
        "libraries": {
            "kafka-python": _pkg_version("kafka-python"),
            "confluent-kafka": _pkg_version("confluent-kafka"),
            "aiokafka": _pkg_version("aiokafka"),
            "hdrhistogram": _pkg_version("hdrhistogram"),
            "psutil": _pkg_version("psutil"),
            "numpy": _pkg_version("numpy"),
            "matplotlib": _pkg_version("matplotlib"),
        },
        "librdkafka": _librdkafka_info(),
    }
    if extra:
        info.update(extra)
    return info


def _cpu_info() -> dict:
    import psutil

    freq = psutil.cpu_freq()
    return {
        "logical_count": psutil.cpu_count(logical=True),
        "physical_count": psutil.cpu_count(logical=False),
        "freq_mhz_current": getattr(freq, "current", None),
        "freq_mhz_max": getattr(freq, "max", None),
    }
