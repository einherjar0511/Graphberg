"""Generic timing harness for the Graphberg benchmark.

Provides cache-drop, /proc/self/io accounting, latency summarization,
and single/repeated measurement runners. Workload-agnostic: takes any
callable that returns the raw-latency dict shape produced by
benchmark.workloads.

Public API:
    drop_caches()
    read_io_counters()
    summarize_latencies(lat_us)
    measure(workload_name, fn, *args, drop_cache_first=True, **kwargs)
    measure_repeated(workload_name, fn, *args, repetitions=5, **kwargs)
"""

from __future__ import annotations

import os
import statistics
import time
from pathlib import Path
from typing import Callable

# ---- /proc helpers ---------------------------------------------------------

_DROP_CACHES_PATH = Path("/proc/sys/vm/drop_caches")
_IO_PATH = Path("/proc/self/io")


def drop_caches() -> None:
    """Drop the OS page cache by writing '3\\n' to /proc/sys/vm/drop_caches.

    Requires root. On permission/OS error, prints a warning and continues
    so the benchmark remains usable in non-root dev environments.
    """
    try:
        with open(_DROP_CACHES_PATH, "w") as f:
            f.write("3\n")
    except (PermissionError, OSError) as e:
        print(f"[harness] WARNING: could not drop caches ({e!r}); continuing.")


def read_io_counters() -> dict:
    """Parse /proc/self/io into a dict.

    Typical fields: rchar, wchar, syscr, syscw, read_bytes,
    write_bytes, cancelled_write_bytes. Tolerates missing fields and a
    completely missing file (returns {}).
    """
    counters: dict = {}
    try:
        with open(_IO_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if not line or ":" not in line:
                    continue
                key, _, val = line.partition(":")
                key = key.strip()
                val = val.strip()
                try:
                    counters[key] = int(val)
                except ValueError:
                    # ignore malformed lines
                    continue
    except (FileNotFoundError, PermissionError, OSError) as e:
        print(f"[harness] WARNING: could not read {_IO_PATH} ({e!r}).")
    return counters


# ---- Statistics ------------------------------------------------------------

def summarize_latencies(lat_us: list[float]) -> dict:
    """Summarize a list of latencies (microseconds).

    Returns p50, p99, mean, min, max, n. Sorts the list once and indexes;
    O(n log n). Returns zeros for an empty input rather than raising.
    """
    n = len(lat_us)
    if n == 0:
        return {
            "p50_us": 0.0,
            "p99_us": 0.0,
            "mean_us": 0.0,
            "min_us": 0.0,
            "max_us": 0.0,
            "n": 0,
        }

    sorted_lat = sorted(lat_us)
    # Nearest-rank percentile, 1-indexed: idx = ceil(p/100 * n) - 1.
    # For p50 with n=100 -> ceil(50.0)-1 = 49 (the 50th element, 1-indexed).
    # For p99 with n=100 -> ceil(99.0)-1 = 98 (the 99th element, 1-indexed).
    def _pct(p: float) -> float:
        # ceil(p/100 * n) without importing math
        rank = int(p * n / 100.0)
        if rank * 100 < int(p * n):
            rank += 1
        if rank < 1:
            rank = 1
        if rank > n:
            rank = n
        return sorted_lat[rank - 1]

    return {
        "p50_us": _pct(50),
        "p99_us": _pct(99),
        "mean_us": statistics.fmean(sorted_lat),
        "min_us": sorted_lat[0],
        "max_us": sorted_lat[-1],
        "n": n,
    }


# ---- Measurement -----------------------------------------------------------

def _io_delta(before: dict, after: dict) -> dict:
    """Subtract IO counter snapshots, tolerating missing keys on either side."""
    keys = set(before) | set(after)
    return {k: int(after.get(k, 0)) - int(before.get(k, 0)) for k in keys}


def measure(
    workload_name: str,
    fn: Callable,
    *fn_args,
    drop_cache_first: bool = True,
    **fn_kwargs,
) -> dict:
    """Run `fn(*fn_args, **fn_kwargs)` once, capturing wall time and IO delta.

    `fn` MUST return a dict shaped like benchmark.workloads outputs:
        {"iterations": int, "latencies_us": list[float], "result_sizes": list[int]}
    """
    if drop_cache_first:
        drop_caches()

    io_before = read_io_counters()
    t0 = time.perf_counter_ns()
    result = fn(*fn_args, **fn_kwargs)
    t1 = time.perf_counter_ns()
    io_after = read_io_counters()

    wall_seconds = (t1 - t0) / 1e9
    io_delta = _io_delta(io_before, io_after)
    bytes_read = int(io_delta.get("read_bytes", 0))
    rchar_delta = int(io_delta.get("rchar", 0))

    latencies_us = result.get("latencies_us", []) or []
    result_sizes = result.get("result_sizes", []) or []
    iterations = int(result.get("iterations", len(latencies_us)))

    summary = summarize_latencies(latencies_us)
    summary["throughput_per_s"] = (
        iterations / wall_seconds if wall_seconds > 0 else 0.0
    )
    summary["cold_us"] = float(latencies_us[0]) if latencies_us else 0.0

    if result_sizes:
        rs_summary = {
            "min": int(min(result_sizes)),
            "max": int(max(result_sizes)),
            "mean": float(statistics.fmean(result_sizes)),
            "sum": int(sum(result_sizes)),
        }
    else:
        rs_summary = {"min": 0, "max": 0, "mean": 0.0, "sum": 0}

    return {
        "workload": workload_name,
        "wall_seconds": wall_seconds,
        "bytes_read": bytes_read,
        "rchar_delta": rchar_delta,
        "iterations": iterations,
        "summary": summary,
        "result_sizes_summary": rs_summary,
    }


def measure_repeated(
    workload_name: str,
    fn: Callable,
    *fn_args,
    repetitions: int = 5,
    **fn_kwargs,
) -> dict:
    """Run `measure()` `repetitions` times. Each repetition gets a cache drop."""
    runs: list[dict] = []
    for r in range(repetitions):
        run = measure(
            workload_name,
            fn,
            *fn_args,
            drop_cache_first=True,
            **fn_kwargs,
        )
        runs.append(run)
        s = run["summary"]
        print(
            f"[harness] {workload_name} run {r + 1}/{repetitions}: "
            f"p50={s['p50_us']:.2f}us p99={s['p99_us']:.2f}us "
            f"wall={run['wall_seconds']:.3f}s "
            f"thr={s['throughput_per_s']:.1f}/s"
        )

    # Pick median by p50
    sorted_runs = sorted(runs, key=lambda r: r["summary"]["p50_us"])
    median_run = sorted_runs[len(sorted_runs) // 2]
    best_run = sorted_runs[0]

    return {
        "workload": workload_name,
        "repetitions": repetitions,
        "runs": runs,
        "median_summary": median_run,
        "best_summary": best_run,
    }


# ---- Smoke test ------------------------------------------------------------

if __name__ == "__main__":
    print("=== harness.py smoke test ===")

    # 1. drop_caches()
    print("drop_caches() ->", drop_caches())

    # 2. read_io_counters()
    counters = read_io_counters()
    print("read_io_counters() keys:", sorted(counters.keys()))
    print("read_io_counters():", counters)

    # 3. summarize_latencies on synthetic [100, 200, ..., 10000]
    synthetic = [100 * i for i in range(1, 101)]
    summary = summarize_latencies(synthetic)
    print("summary on [100..10000]:", summary)

    # Expected: nearest-rank p50 at index 49 -> 5000; p99 at index 98 -> 9900.
    # Spec asks for p50 ~= 5050, p99 ~= 9900. Use small tolerances.
    p50 = summary["p50_us"]
    p99 = summary["p99_us"]
    assert 4900.0 <= p50 <= 5100.0, f"p50 out of range: {p50}"
    assert 9800.0 <= p99 <= 10000.0, f"p99 out of range: {p99}"
    assert summary["min_us"] == 100.0
    assert summary["max_us"] == 10000.0
    assert summary["n"] == 100
    # Mean of arithmetic series 100..10000 step 100 is 5050
    assert abs(summary["mean_us"] - 5050.0) < 1e-6, summary["mean_us"]
    print(f"asserts passed: p50={p50}, p99={p99}, mean={summary['mean_us']}")

    # 4. measure() on a tiny synthetic fn returning the right shape.
    def _fake_workload():
        # simulate 5 ops with ascending latencies
        return {
            "iterations": 5,
            "latencies_us": [10.0, 20.0, 30.0, 40.0, 50.0],
            "result_sizes": [1, 2, 3, 4, 5],
        }

    m = measure("FAKE", _fake_workload, drop_cache_first=False)
    print("measure(FAKE) ->", m)
    assert m["iterations"] == 5
    assert m["summary"]["n"] == 5
    assert m["summary"]["cold_us"] == 10.0
    assert m["result_sizes_summary"]["sum"] == 15

    print("=== smoke test PASSED ===")
