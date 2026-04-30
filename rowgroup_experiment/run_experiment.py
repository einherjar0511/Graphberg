#!/usr/bin/env python3
"""
Row-group experiment for the Graphberg storage format.

Implements section 2 of BENCHMARK_PLAN_v1.md: validate that 4-8MB Parquet
row groups give a meaningful neighbor-lookup speedup over 128MB row groups,
without crippling columnar predicate-scan performance.

Deviations from the literal spec (justified inline):

  * Dataset scale is 1,000,000 vertices / ~32M edges (not 10K / ~180K). The
    spec's 10K / 180K dataset is too small to populate a single 4MB row
    group, let alone a 128MB one - every row group size >=4MB would collapse
    to a single row group and the comparison would be meaningless. The
    statistical profile (power-law out-degree, mean ~32, max ~1000s) is
    preserved.
  * Page index is enabled when writing. The benchmark plan permits "page
    statistics or row-group statistics" for skipping, so this matches what
    a realistic reader would do. Page index means larger row groups are not
    automatically penalized - they can still skip pages within a row group.
    If the small-RG advantage holds despite page index, that strengthens
    the result.

Usage:
    python3 run_experiment.py
"""

from __future__ import annotations

import gc
import json
import os
import statistics
import subprocess
import time
from dataclasses import dataclass, asdict
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq


HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
RESULTS_DIR = HERE / "results"
DATA_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

# ------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------

V = 5_000_000              # number of vertices
TARGET_MEAN_DEGREE = 32    # target mean out-degree
ZIPF_ALPHA = 1.6           # power-law exponent for degree sampling
MAX_DEGREE = 2_000         # truncate degrees here (max in spec is ~1000;
                           # we use 2k since V is 100x larger than spec)
SEED = 20260430            # deterministic

ROW_GROUP_TARGETS_MB = [1, 4, 8, 16, 128]
NEIGHBOR_LOOKUP_ITERS = 1000

# We will compute rows-per-row-group from a calibration write.

# ------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------

def drop_caches() -> None:
    """Drop OS page cache. Requires root (we run as root in this env)."""
    try:
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
    except PermissionError:
        # Fallback: at minimum free Python-level buffers.
        gc.collect()


def fmt_bytes(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ------------------------------------------------------------------------
# Data generation
# ------------------------------------------------------------------------

def generate_edges(seed: int = SEED) -> tuple[np.ndarray, np.ndarray]:
    """Generate a synthetic directed graph with power-law out-degrees.

    Returns (src, dst) arrays sorted by src ascending.
    """
    rng = np.random.default_rng(seed)

    # Sample out-degree per vertex from a Zipf-like distribution, truncated.
    # We oversample and rescale the mean to land near TARGET_MEAN_DEGREE.
    raw = rng.zipf(ZIPF_ALPHA, size=V).astype(np.int64)
    raw = np.minimum(raw, MAX_DEGREE)

    # Rescale so empirical mean approximates TARGET_MEAN_DEGREE.
    cur_mean = raw.mean()
    scale = TARGET_MEAN_DEGREE / max(cur_mean, 1e-9)
    degrees = np.maximum(np.round(raw * scale).astype(np.int64), 0)
    degrees = np.minimum(degrees, MAX_DEGREE)

    total = int(degrees.sum())
    print(f"[gen] V={V:,}  total_edges={total:,}  "
          f"mean_deg={degrees.mean():.2f}  max_deg={degrees.max():,}  "
          f"p99_deg={np.quantile(degrees, 0.99):.0f}")

    # Build src column: vertex i appears degrees[i] times.
    src = np.repeat(np.arange(V, dtype=np.int64), degrees)

    # dst column: uniform random destinations.
    dst = rng.integers(0, V, size=total, dtype=np.int64)

    # Already sorted by src construction; verify cheaply.
    assert np.all(np.diff(src) >= 0), "src must be sorted"
    return src, dst


# ------------------------------------------------------------------------
# Calibration: figure out rows-per-row-group for each target byte size.
# ------------------------------------------------------------------------

def calibrate_bytes_per_row(src: np.ndarray, dst: np.ndarray) -> float:
    """Write a sample slice and measure compressed bytes per row.

    Used to convert target_bytes_per_rg -> rows_per_rg.
    """
    n = min(2_000_000, len(src))
    sample = pa.table({"src": src[:n], "dst": dst[:n]})
    path = DATA_DIR / "_calib.parquet"
    pq.write_table(
        sample,
        path,
        compression="snappy",
        write_page_index=True,
        # one row group so we measure straight bytes/row:
        row_group_size=n,
    )
    size = path.stat().st_size
    bpr = size / n
    print(f"[calib] {n:,} rows -> {fmt_bytes(size)}  ({bpr:.2f} bytes/row)")
    path.unlink()
    return bpr


# ------------------------------------------------------------------------
# Writers
# ------------------------------------------------------------------------

def write_parquet_with_rg_size(
    src: np.ndarray,
    dst: np.ndarray,
    target_mb: int,
    bytes_per_row: float,
) -> Path:
    rows_per_rg = max(1, int((target_mb * 1024 * 1024) / bytes_per_row))
    path = DATA_DIR / f"edges_rg{target_mb}MB.parquet"
    table = pa.table({"src": src, "dst": dst})
    pq.write_table(
        table,
        path,
        compression="snappy",
        write_page_index=True,
        row_group_size=rows_per_rg,
        # Default data_page_size in pyarrow is 1MB; leaving as default
        # so larger row groups have multiple pages.
    )

    md = pq.ParquetFile(path).metadata
    actual_size = path.stat().st_size
    print(
        f"[write] target={target_mb}MB rows/rg={rows_per_rg:,}  "
        f"file={fmt_bytes(actual_size)}  "
        f"row_groups={md.num_row_groups}  "
        f"avg_rg={fmt_bytes(actual_size / md.num_row_groups)}"
    )
    return path


# ------------------------------------------------------------------------
# Benchmarks
# ------------------------------------------------------------------------

def pick_lookup_vids(src: np.ndarray, n: int, seed: int = 1) -> np.ndarray:
    """Pick n source vids for neighbor lookup.

    We sample existing source vids (i.e. vertices with degree>=1) so every
    lookup actually returns at least one edge - that exercises the read
    path more meaningfully than a miss that just consults stats.
    """
    rng = np.random.default_rng(seed)
    unique_src = np.unique(src)
    return rng.choice(unique_src, size=n, replace=True)


def neighbor_lookup_benchmark(
    path: Path,
    lookup_vids: np.ndarray,
) -> dict:
    """For each vid, read all dst rows where src == vid. Return latency stats."""
    drop_caches()

    # Use pyarrow.dataset for filter pushdown with row-group + page skipping.
    dataset = ds.dataset(path, format="parquet")

    latencies_us = []
    rows_returned = []

    for vid in lookup_vids:
        t0 = time.perf_counter()
        tbl = dataset.to_table(
            columns=["dst"],
            filter=ds.field("src") == int(vid),
        )
        t1 = time.perf_counter()
        latencies_us.append((t1 - t0) * 1_000_000)
        rows_returned.append(tbl.num_rows)

    latencies_us.sort()
    n = len(latencies_us)
    p50 = latencies_us[n // 2]
    p99 = latencies_us[min(n - 1, int(n * 0.99))]
    mean = statistics.fmean(latencies_us)

    return {
        "p50_us": p50,
        "p99_us": p99,
        "mean_us": mean,
        "min_us": min(latencies_us),
        "max_us": max(latencies_us),
        "iterations": n,
        "mean_rows_returned": statistics.fmean(rows_returned),
    }


def predicate_scan_benchmark(path: Path, threshold: int) -> dict:
    """SELECT dst FROM edges WHERE src > threshold. Single iteration."""
    drop_caches()

    dataset = ds.dataset(path, format="parquet")
    t0 = time.perf_counter()
    tbl = dataset.to_table(
        columns=["dst"],
        filter=ds.field("src") > int(threshold),
    )
    t1 = time.perf_counter()

    return {
        "wall_ms": (t1 - t0) * 1000,
        "rows_returned": tbl.num_rows,
    }


# ------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------

@dataclass
class Result:
    rg_size_mb: int
    file_bytes: int
    num_row_groups: int
    neighbor_p50_us: float
    neighbor_p99_us: float
    neighbor_mean_us: float
    neighbor_iters: int
    neighbor_mean_rows: float
    predicate_wall_ms: float
    predicate_rows: int


def run() -> None:
    print("=" * 72)
    print("Graphberg row-group experiment")
    print("=" * 72)

    print("\n[1/4] Generating data...")
    t0 = time.perf_counter()
    src, dst = generate_edges()
    print(f"      generation took {time.perf_counter() - t0:.1f}s")

    print("\n[2/4] Calibrating bytes/row...")
    bpr = calibrate_bytes_per_row(src, dst)

    print("\n[3/4] Writing Parquet at each row-group size...")
    paths: dict[int, Path] = {}
    for mb in ROW_GROUP_TARGETS_MB:
        paths[mb] = write_parquet_with_rg_size(src, dst, mb, bpr)

    print("\n[4/4] Running benchmarks...")
    lookup_vids = pick_lookup_vids(src, NEIGHBOR_LOOKUP_ITERS)
    threshold = int(np.median(src))   # ~midpoint => returns ~half the rows

    results: list[Result] = []
    for mb in ROW_GROUP_TARGETS_MB:
        path = paths[mb]
        print(f"\n--- rg={mb}MB ---")
        print("    neighbor lookup ...", end=" ", flush=True)
        nl = neighbor_lookup_benchmark(path, lookup_vids)
        print(
            f"p50={nl['p50_us']:8.1f}us  "
            f"p99={nl['p99_us']:8.1f}us  "
            f"mean_rows={nl['mean_rows_returned']:.1f}"
        )
        print("    predicate scan  ...", end=" ", flush=True)
        ps = predicate_scan_benchmark(path, threshold)
        print(f"wall={ps['wall_ms']:8.1f}ms  rows={ps['rows_returned']:,}")

        md = pq.ParquetFile(path).metadata
        results.append(
            Result(
                rg_size_mb=mb,
                file_bytes=path.stat().st_size,
                num_row_groups=md.num_row_groups,
                neighbor_p50_us=nl["p50_us"],
                neighbor_p99_us=nl["p99_us"],
                neighbor_mean_us=nl["mean_us"],
                neighbor_iters=nl["iterations"],
                neighbor_mean_rows=nl["mean_rows_returned"],
                predicate_wall_ms=ps["wall_ms"],
                predicate_rows=ps["rows_returned"],
            )
        )

    # ----- Summary table -----
    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)
    print(
        f"{'rg_mb':>6}  {'file':>10}  {'#rg':>6}  "
        f"{'nl_p50_us':>11}  {'nl_p99_us':>11}  "
        f"{'pred_wall_ms':>13}"
    )
    for r in results:
        print(
            f"{r.rg_size_mb:>6}  {fmt_bytes(r.file_bytes):>10}  "
            f"{r.num_row_groups:>6}  "
            f"{r.neighbor_p50_us:>11.1f}  {r.neighbor_p99_us:>11.1f}  "
            f"{r.predicate_wall_ms:>13.1f}"
        )

    # ----- Decision rule -----
    p50 = {r.rg_size_mb: r.neighbor_p50_us for r in results}
    pred = {r.rg_size_mb: r.predicate_wall_ms for r in results}
    small_keys = [k for k in (4, 8) if k in p50]
    if not small_keys or 128 not in p50:
        speedup_vs_128 = float("nan")
        pred_slowdown = float("nan")
        decision = "INCONCLUSIVE (missing required RG sizes)"
    else:
        best_small_key = min(small_keys, key=lambda k: p50[k])
        best_small = p50[best_small_key]
        speedup_vs_128 = p50[128] / best_small if best_small > 0 else float("inf")
        small_pred = pred[best_small_key]
        pred_slowdown = small_pred / pred[128] if pred[128] > 0 else float("inf")

        if speedup_vs_128 >= 5.0 and pred_slowdown <= 2.0:
            decision = "PASS"
        elif speedup_vs_128 >= 5.0 and pred_slowdown > 2.0:
            decision = "FLAG"
        elif speedup_vs_128 > 1.0 and pred_slowdown <= 2.0:
            decision = "FLAG (neighbor speedup < 5x)"
        else:
            decision = "STOP"

    print("\n" + "-" * 72)
    print(f"Neighbor P50 speedup (best of 4/8 MB vs 128 MB): {speedup_vs_128:.2f}x")
    print(f"Predicate slowdown   (best small vs 128 MB):    {pred_slowdown:.2f}x")
    print(f"Decision: {decision}")
    print("-" * 72)

    # ----- Persist results -----
    payload = {
        "config": {
            "vertices": V,
            "target_mean_degree": TARGET_MEAN_DEGREE,
            "max_degree": MAX_DEGREE,
            "zipf_alpha": ZIPF_ALPHA,
            "seed": SEED,
            "neighbor_iters": NEIGHBOR_LOOKUP_ITERS,
            "row_group_targets_mb": ROW_GROUP_TARGETS_MB,
            "predicate_threshold_src": threshold,
        },
        "results": [asdict(r) for r in results],
        "decision": {
            "neighbor_speedup_4_or_8_vs_128": speedup_vs_128,
            "predicate_slowdown_best_small_vs_128": pred_slowdown,
            "verdict": decision,
        },
    }
    out = RESULTS_DIR / "results.json"
    out.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote {out}")


if __name__ == "__main__":
    run()
