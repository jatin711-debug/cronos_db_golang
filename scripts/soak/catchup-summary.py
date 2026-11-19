#!/usr/bin/env python3
"""Summarizes how a soak's cluster did while a consumer group caught up.

    catchup-summary.py <label>=<workload.csv> [<label>=<workload.csv> ...]

Reads the workload reports of TestSoak runs (CRONOS_SOAK_OUT) that included
the "consumer group behind" disruption, and prints a Markdown table: the
publish rate before the disruption, and from the moment the slow group was
furthest behind until it had caught up, its peak lag, how long catching up
took, the publish rate and latency meanwhile, and the slow group's own pace.
"""
import csv
import statistics
import sys


def summarize(path):
    rows = list(csv.DictReader(open(path, newline="")))
    samples = []
    for prev, row in zip(rows, rows[1:]):
        dt = float(row["elapsed_s"]) - float(prev["elapsed_s"])
        if dt <= 0:
            continue
        samples.append({
            "t": float(row["elapsed_s"]),
            "phase": row["phase"],
            "rate": (int(row["accepted"]) - int(prev["accepted"])) / dt,
            "slow_rate": (int(row["delivered_slow"]) - int(prev["delivered_slow"])) / dt,
            "slow_lag": int(row["slow_lag"]),
            "p50": float(row["publish_p50_ms"]),
            "p99": float(row["publish_p99_ms"]),
            "refused": int(row["refused"]),
        })
    disruption = [s for s in samples if s["phase"] == "disruption"]
    if not disruption:
        return None
    start = disruption[0]["t"]
    steady = [s["rate"] for s in samples if s["phase"] in ("steady", "burst") and s["t"] < start]
    peak = max(disruption, key=lambda s: s["slow_lag"])
    after = [s for s in disruption if s["t"] >= peak["t"]]
    caught = next((s for s in after if s["slow_lag"] <= max(peak["slow_lag"] // 100, 500)), None)
    catching = [s for s in after if caught is None or s["t"] <= caught["t"]]
    return {
        "steady_rate": statistics.median(steady) if steady else 0,
        "peak_lag": peak["slow_lag"],
        "catchup_s": (caught["t"] - peak["t"]) if caught else None,
        "rate_median": statistics.median(s["rate"] for s in catching),
        "rate_min": min(s["rate"] for s in catching),
        "p50_median": statistics.median(s["p50"] for s in catching),
        "p99_median": statistics.median(s["p99"] for s in catching),
        "slow_rate_median": statistics.median(s["slow_rate"] for s in catching),
        "refused": max(s["refused"] for s in samples),
    }


def main(args):
    if not args:
        print(__doc__)
        return 2
    print("| run | steady publishes/s | slow group peak lag | catch-up | publishes/s while catching up (median / min) | publish p50 / p99 ms (median) | slow group events/s | refused |")
    print("|---|---|---|---|---|---|---|---|")
    for arg in args:
        label, _, path = arg.partition("=")
        try:
            s = summarize(path)
        except (OSError, KeyError, ValueError) as err:
            print(f"| {label} | could not read {path}: {err} | | | | | | |")
            continue
        if s is None:
            print(f"| {label} | no disruption in {path} | | | | | | |")
            continue
        catchup = f"{s['catchup_s'] / 60:.1f} min" if s["catchup_s"] is not None else "not within the run"
        print(f"| {label} | {s['steady_rate']:.0f} | {s['peak_lag']:,} | {catchup} | "
              f"{s['rate_median']:.0f} / {s['rate_min']:.0f} | {s['p50_median']:.1f} / {s['p99_median']:.1f} | "
              f"{s['slow_rate_median']:.0f} | {s['refused']:,} |")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
