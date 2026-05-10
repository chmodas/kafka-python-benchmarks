"""Aggregate per-repeat summaries into CSV + plots + REPORT.md.

Walks `<results>/<client>/<workload>/repeat_*/summary.json`, computes
median/min/IQR per (client, workload) over repeats, writes:

- summary.csv (flat, one row per repeat)
- aggregated.csv (one row per (client, workload))
- plots/ (PNG bar charts and CDFs)
- REPORT.md (human-readable tables)
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from hdrh.histogram import HdrHistogram  # noqa: E402

from bench.metrics import HIGHEST_NS, LOWEST_NS, SIGNIFICANT_FIGURES  # noqa: E402

PERCENTILE_KEYS = ("min", "p50", "p95", "p99", "p999", "max")
NS_PER_US = 1_000


def _walk_repeats(root: Path) -> list[dict]:
    rows: list[dict] = []
    for client_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for workload_dir in sorted(p for p in client_dir.iterdir() if p.is_dir()):
            for repeat_dir in sorted(p for p in workload_dir.iterdir() if p.is_dir()):
                summary_path = repeat_dir / "summary.json"
                if not summary_path.exists():
                    continue
                with summary_path.open() as f:
                    rows.append(json.load(f))
    return rows


def _flatten_row(s: dict) -> dict:
    ack = s["metrics"]["ack_latency_ns"]
    e2e = s["metrics"]["e2e_latency_ns"]
    res = s["resource"]
    out = {
        "client": s["client"],
        "workload": s["workload"],
        "kind": s["kind"],
        "seed": s["seed"],
        "elapsed_seconds": s["elapsed_seconds"],
        "msgs_per_sec": s["msgs_per_sec"],
        "produced": s["metrics"]["produced"],
        "acked": s["metrics"]["acked"],
        "consumed": s["metrics"]["consumed"],
        "errors": s["metrics"]["errors"],
        "cpu_user_pct_median": res.get("cpu_user_pct_median"),
        "cpu_system_pct_median": res.get("cpu_system_pct_median"),
        "rss_bytes_max": res.get("rss_bytes_max"),
    }
    for stage, label in (("ack", ack), ("e2e", e2e)):
        for k in PERCENTILE_KEYS:
            out[f"{stage}_{k}_us"] = round(label[k] / NS_PER_US, 3) if k in label else None
    return out


def _write_summary_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    flat = [_flatten_row(r) for r in rows]
    fieldnames = list(flat[0].keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(flat)


def _aggregate(rows: list[dict]) -> list[dict]:
    by_pair: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        by_pair.setdefault((r["client"], r["workload"]), []).append(r)

    out: list[dict] = []
    for (client, workload), group in by_pair.items():
        ack_p99 = [r["metrics"]["ack_latency_ns"].get("p99", 0) for r in group]
        e2e_p99 = [r["metrics"]["e2e_latency_ns"].get("p99", 0) for r in group]
        ack_p50 = [r["metrics"]["ack_latency_ns"].get("p50", 0) for r in group]
        rates = [r["msgs_per_sec"] for r in group]
        out.append(
            {
                "client": client,
                "workload": workload,
                "kind": group[0]["kind"],
                "repeats": len(group),
                "ack_p50_us_median": round(statistics.median(ack_p50) / NS_PER_US, 3),
                "ack_p99_us_median": round(statistics.median(ack_p99) / NS_PER_US, 3),
                "ack_p99_us_min": round(min(ack_p99) / NS_PER_US, 3),
                "ack_p99_us_iqr": round(_iqr(ack_p99) / NS_PER_US, 3),
                "e2e_p99_us_median": round(statistics.median(e2e_p99) / NS_PER_US, 3),
                "msgs_per_sec_median": round(statistics.median(rates), 1),
                "msgs_per_sec_max": round(max(rates), 1),
            }
        )
    return out


def _iqr(values: list[float]) -> float:
    """Interquartile range using exclusive linear interpolation.

    For n < 4 the IQR is not statistically meaningful; we return 0.0 and the
    REPORT prints the median + min instead. With the plan's 5-repeat default
    n=5 puts us comfortably above the threshold.
    """
    if len(values) < 4:
        return 0.0
    qs = statistics.quantiles(values, n=4, method="exclusive")
    return qs[2] - qs[0]


def _write_aggregated_csv(agg: list[dict], path: Path) -> None:
    if not agg:
        return
    fieldnames = list(agg[0].keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(agg)


def _bar_p99_by_client(agg: list[dict], plot_dir: Path) -> None:
    plot_dir.mkdir(parents=True, exist_ok=True)
    by_workload: dict[str, dict[str, float]] = {}
    for row in agg:
        by_workload.setdefault(row["workload"], {})[row["client"]] = row["ack_p99_us_median"]
    for workload, client_map in by_workload.items():
        clients = sorted(client_map.keys())
        values = [client_map[c] for c in clients]
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(clients, values)
        ax.set_title(f"{workload} – ack p99 (median over repeats)")
        ax.set_ylabel("ack p99 (µs)")
        ax.tick_params(axis="x", rotation=15)
        for i, v in enumerate(values):
            ax.text(i, v, f"{v:.0f}", ha="center", va="bottom")
        fig.tight_layout()
        fig.savefig(plot_dir / f"{workload}_ack_p99.png", dpi=120)
        plt.close(fig)


def _decode_histogram(payload: dict) -> HdrHistogram:
    h = HdrHistogram(LOWEST_NS, HIGHEST_NS, SIGNIFICANT_FIGURES)
    h.decode_and_add(payload["encoded"].encode("ascii"))
    return h


def _cdf_plot(root: Path, plot_dir: Path) -> None:
    """One CDF panel per latency workload, comparing all clients in that workload."""
    plot_dir.mkdir(parents=True, exist_ok=True)
    by_workload: dict[str, dict[str, list[Path]]] = {}
    for client_dir in (p for p in root.iterdir() if p.is_dir()):
        for workload_dir in (p for p in client_dir.iterdir() if p.is_dir()):
            for repeat_dir in (p for p in workload_dir.iterdir() if p.is_dir()):
                hist_path = repeat_dir / "ack_latency.hgrm.json"
                if hist_path.exists():
                    by_workload.setdefault(workload_dir.name, {}).setdefault(
                        client_dir.name, []
                    ).append(hist_path)

    for workload, client_paths in by_workload.items():
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for client, paths in sorted(client_paths.items()):
            merged = HdrHistogram(LOWEST_NS, HIGHEST_NS, SIGNIFICANT_FIGURES)
            for path in paths:
                with path.open() as f:
                    payload = json.load(f)
                if payload["summary"].get("count", 0) == 0:
                    continue
                merged.decode_and_add(payload["encoded"].encode("ascii"))
            if merged.get_total_count() == 0:
                continue
            xs = []
            ys = []
            for pct in (50, 75, 90, 95, 99, 99.9, 99.99):
                xs.append(merged.get_value_at_percentile(pct) / NS_PER_US)
                ys.append(pct)
            ax.plot(xs, ys, marker="o", label=client)
        ax.set_xscale("log")
        ax.set_xlabel("ack latency (µs, log)")
        ax.set_ylabel("percentile")
        ax.set_title(f"{workload} – ack-latency CDF")
        ax.legend()
        ax.grid(True, which="both", alpha=0.3)
        fig.tight_layout()
        fig.savefig(plot_dir / f"{workload}_cdf.png", dpi=120)
        plt.close(fig)


def _write_report(agg: list[dict], rows: list[dict], path: Path) -> None:
    by_kind: dict[str, list[dict]] = {}
    for r in agg:
        by_kind.setdefault(r["kind"], []).append(r)

    out: list[str] = ["# Benchmark report\n"]
    if rows:
        out.append(f"Repeats per (client, workload): up to {max(r['repeats'] for r in agg)}\n")
    for kind, group in sorted(by_kind.items()):
        out.append(f"## {kind}\n")
        out.append(
            "| client | workload | ack p50 (µs) | ack p99 median (µs) | "
            "ack p99 min (µs) | ack p99 IQR (µs) | e2e p99 median (µs) | msgs/s median |"
        )
        out.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for r in sorted(group, key=lambda x: (x["workload"], x["client"])):
            out.append(
                f"| {r['client']} | {r['workload']} | "
                f"{r['ack_p50_us_median']} | {r['ack_p99_us_median']} | "
                f"{r['ack_p99_us_min']} | {r['ack_p99_us_iqr']} | "
                f"{r['e2e_p99_us_median']} | {r['msgs_per_sec_median']} |"
            )
        out.append("")

    path.write_text("\n".join(out))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("results_dir", help="path to a results/<run-id>/ directory")
    args = p.parse_args()

    root = Path(args.results_dir)
    if not root.is_dir():
        raise SystemExit(f"not a directory: {root}")

    rows = _walk_repeats(root)
    agg = _aggregate(rows)

    _write_summary_csv(rows, root / "summary.csv")
    _write_aggregated_csv(agg, root / "aggregated.csv")
    plot_dir = root / "plots"
    _bar_p99_by_client(agg, plot_dir)
    _cdf_plot(root, plot_dir)
    _write_report(agg, rows, root / "REPORT.md")
    print(f"wrote {len(rows)} rows, {len(agg)} aggregated entries to {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
