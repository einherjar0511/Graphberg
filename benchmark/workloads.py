"""Workload definitions for the Graphberg benchmark.

Each workload returns raw per-iteration timing data; statistics
(P50/P99/etc.) are computed by the harness, not here.

Public API:
    W1(reader, src_vids)       -> dict   # 1-hop neighbor lookup
    W2(reader, src_vids)       -> dict   # 2-hop neighbor expansion
    W3(reader, iterations=...) -> dict   # columnar predicate scan
    all_workloads(reader, src_vids_w1, src_vids_w2) -> dict

The reader is expected to expose:
    reader.neighbors_of(vid: int) -> Iterable[int]
    reader.scan_persons_predicate() -> int   # (or any sized result)

Per spec v1.1 Section 3.1:
    W1: 10_000 iterations
    W2: 1_000  iterations
    W3: 100    iterations
"""

from __future__ import annotations

import time
from typing import Iterable

# ---- Public constants ------------------------------------------------------

W1_ITERS = 10_000
W2_ITERS = 1_000
W3_ITERS = 100


# ---- Helpers ---------------------------------------------------------------

def _ns_to_us(ns: int) -> float:
    return ns / 1000.0


def _result_size(result) -> int:
    """Best-effort size measurement for a workload op result."""
    if result is None:
        return 0
    # int-like: predicate scan may return a row count directly
    if isinstance(result, int):
        return result
    try:
        return len(result)
    except TypeError:
        # last resort: count the iterable (consumes it)
        return sum(1 for _ in result)


# ---- Workloads -------------------------------------------------------------

def W1(reader, src_vids: list[int]) -> dict:
    """1-hop neighbor lookup.

    For each src in src_vids, call reader.neighbors_of(src).
    Time each call individually using time.perf_counter_ns.
    """
    n = len(src_vids)
    latencies_us: list[float] = [0.0] * n
    result_sizes: list[int] = [0] * n

    for i, src in enumerate(src_vids):
        t0 = time.perf_counter_ns()
        neighbors = reader.neighbors_of(src)
        # Materialize size (also forces any lazy generator to actually run)
        size = _result_size(neighbors)
        t1 = time.perf_counter_ns()
        latencies_us[i] = _ns_to_us(t1 - t0)
        result_sizes[i] = size

    print(f"[W1] completed {n} 1-hop lookups")
    return {
        "iterations": n,
        "latencies_us": latencies_us,
        "result_sizes": result_sizes,
    }


def W2(reader, src_vids: list[int]) -> dict:
    """2-hop neighbor expansion.

    For each seed src:
      1) get 1-hop neighbors
      2) for each 1-hop neighbor, get its neighbors
      3) union all into a set, exclude src itself
    Time the WHOLE 2-hop computation per seed src.
    """
    n = len(src_vids)
    latencies_us: list[float] = [0.0] * n
    result_sizes: list[int] = [0] * n

    for i, src in enumerate(src_vids):
        t0 = time.perf_counter_ns()
        two_hop: set = set()
        for nb in reader.neighbors_of(src):
            for nn in reader.neighbors_of(nb):
                two_hop.add(nn)
        two_hop.discard(src)
        size = len(two_hop)
        t1 = time.perf_counter_ns()
        latencies_us[i] = _ns_to_us(t1 - t0)
        result_sizes[i] = size

    print(f"[W2] completed {n} 2-hop expansions")
    return {
        "iterations": n,
        "latencies_us": latencies_us,
        "result_sizes": result_sizes,
    }


def W3(reader, iterations: int = W3_ITERS) -> dict:
    """Columnar predicate scan.

    Call reader.scan_persons_predicate() `iterations` times. Time each.
    """
    latencies_us: list[float] = [0.0] * iterations
    result_sizes: list[int] = [0] * iterations

    for i in range(iterations):
        t0 = time.perf_counter_ns()
        result = reader.scan_persons_predicate()
        size = _result_size(result)
        t1 = time.perf_counter_ns()
        latencies_us[i] = _ns_to_us(t1 - t0)
        result_sizes[i] = size

    print(f"[W3] completed {iterations} predicate scans")
    return {
        "iterations": iterations,
        "latencies_us": latencies_us,
        "result_sizes": result_sizes,
    }


def all_workloads(
    reader,
    src_vids_w1: list[int],
    src_vids_w2: list[int],
) -> dict:
    """Convenience runner: execute W1, W2, W3 in order on the same reader."""
    return {
        "W1": W1(reader, src_vids_w1),
        "W2": W2(reader, src_vids_w2),
        "W3": W3(reader),
    }


if __name__ == "__main__":
    # Not really runnable without a reader implementation.
    print("workloads.py: import-only smoke (no reader available).")
