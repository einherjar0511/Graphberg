"""End-to-end orchestrator for the Graphberg benchmark.

Idempotent: if data already exists (sentinel file present), skip
generation/writing. Stages:

    1. generate + write   (per-layout, with .complete sentinels)
    2. correctness check  (30 random vids vs in-memory ground truth)
    3. run workloads      (harness.measure_repeated for every layout x workload)
    4. analyze            (delegates to analyze.py)

CLI:

    run_benchmark.py write       # only stage 1
    run_benchmark.py check       # stage 1 + 2
    run_benchmark.py bench       # stages 1 + 2 + 3 + 4 (default)
    run_benchmark.py analyze     # invoke analyze.py only
    run_benchmark.py --smoke ... # use V=50_000 + smoke workload sizes
"""

from __future__ import annotations

import gc
import json
import random
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow as pa

import data_gen
import writers
import readers
import workloads
import harness


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
RESULTS_DIR = HERE / "results"
RESULTS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

V = data_gen.V
SEED = data_gen.SEED
REPETITIONS = 5

# Per workload: how many src_vids to sample
N_W1 = workloads.W1_ITERS
N_W2 = workloads.W2_ITERS

LAYOUT_DIRS = {
    "plain_parquet": DATA_DIR / "plain_parquet",
    "iceberg":       DATA_DIR / "iceberg",
    "graphar":       DATA_DIR / "graphar",
    "graphberg":     DATA_DIR / "graphberg",
}

WRITERS = {
    "plain_parquet": writers.write_plain_parquet,
    "iceberg":       writers.write_iceberg,
    "graphar":       writers.write_graphar,
    "graphberg":     writers.write_graphberg,
}

READER_CLASSES = {
    "plain_parquet": readers.PlainParquetReader,
    "iceberg":       readers.IcebergReader,
    "graphar":       readers.GraphArReader,
    "graphberg":     readers.GraphbergReader,
}


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _dir_size_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def _format_gb(n_bytes: int) -> str:
    return f"{n_bytes / (1024 ** 3):.2f} GB"


def _json_default(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, Path):
        return str(o)
    raise TypeError(f"not JSON serializable: {type(o)!r}")


# ---------------------------------------------------------------------------
# Stage 1: generate + write
# ---------------------------------------------------------------------------

def stage_generate_and_write(skip_existing: bool = True) -> dict:
    """If DATA_DIR/<layout>/.complete sentinel exists, skip that layout.

    Otherwise: ensure persons+knows are generated (cache to memory once),
    invoke the writer, touch the sentinel.

    Return per-layout {"size_bytes": ..., "elapsed_s": ...}.
    """
    results: dict = {}

    persons: pa.Table | None = None
    knows: pa.Table | None = None

    for layout, out_dir in LAYOUT_DIRS.items():
        out_dir.mkdir(parents=True, exist_ok=True)
        sentinel = out_dir / ".complete"

        if skip_existing and sentinel.exists():
            size = _dir_size_bytes(out_dir)
            _log(
                f"skipping {layout} (sentinel present, size {_format_gb(size)})"
            )
            results[layout] = {
                "size_bytes": size,
                "elapsed_s": 0.0,
                "skipped": True,
            }
            continue

        # Generate the in-memory dataset lazily, only once.
        if persons is None or knows is None:
            _log(f"generating persons + knows (V={V}, seed={SEED}) ...")
            t_gen = time.perf_counter()
            persons, knows = data_gen.generate_dataset(v=V, seed=SEED)
            _log(
                f"generated in {time.perf_counter() - t_gen:.1f}s "
                f"(persons={persons.num_rows:,}, knows={knows.num_rows:,})"
            )

        _log(f"writing {layout} ...")
        t0 = time.perf_counter()
        WRITERS[layout](persons, knows, out_dir)
        elapsed = time.perf_counter() - t0
        sentinel.write_text("ok\n")
        size = _dir_size_bytes(out_dir)
        _log(
            f"writing {layout} ... done in {elapsed:.1f}s "
            f"(size {_format_gb(size)})"
        )

        results[layout] = {
            "size_bytes": size,
            "elapsed_s": elapsed,
            "skipped": False,
        }

    return results


# ---------------------------------------------------------------------------
# Stage 2: correctness check
# ---------------------------------------------------------------------------

def _ground_truth_lookup(knows: pa.Table):
    """Return a closure ``truth(vid) -> set[int]`` that does on-demand
    lookups via np.searchsorted on the sorted-by-src knows table.

    We deliberately do NOT materialise a full {src: set} dict — at
    V=5M / E=160M that pure-Python structure peaks well over 16 GB.
    """
    src = knows.column("src").to_numpy(zero_copy_only=False)
    dst = knows.column("dst").to_numpy(zero_copy_only=False)
    if src.size and not np.all(src[1:] >= src[:-1]):
        order = np.argsort(src, kind="stable")
        src = src[order]
        dst = dst[order]

    def truth(vid: int) -> set:
        lo = int(np.searchsorted(src, vid, side="left"))
        hi = int(np.searchsorted(src, vid, side="right"))
        if lo == hi:
            return set()
        return set(int(x) for x in dst[lo:hi])

    return truth


def stage_correctness_check(persons: pa.Table, knows: pa.Table) -> dict:
    """For 30 random vids, compare each reader's neighbors_of(vid) against
    ground truth from the in-memory knows table. Print PASS/FAIL per system.
    """
    _log("building on-demand ground-truth adjacency from in-memory knows ...")
    truth = _ground_truth_lookup(knows)

    n_persons = persons.num_rows
    rng = random.Random(SEED + 1)
    sample_vids = [rng.randrange(n_persons) for _ in range(30)]

    summary: dict = {}
    any_fail = False

    for layout, ReaderCls in READER_CLASSES.items():
        _log(f"correctness check: {layout} ...")
        try:
            reader = ReaderCls(LAYOUT_DIRS[layout])
        except Exception as exc:  # noqa: BLE001
            _log(f"correctness check: {layout} ... FAIL (open error: {exc!r})")
            summary[layout] = {
                "pass": False,
                "mismatches": [{"open_error": repr(exc)}],
            }
            any_fail = True
            continue

        mismatches: list = []
        for vid in sample_vids:
            expected = truth(int(vid))
            try:
                actual_iter = reader.neighbors_of(vid)
                actual = set(int(x) for x in actual_iter)
            except Exception as exc:  # noqa: BLE001
                mismatches.append({"vid": vid, "error": repr(exc)})
                continue
            if actual != expected:
                mismatches.append({
                    "vid": vid,
                    "missing": sorted(expected - actual)[:10],
                    "extra": sorted(actual - expected)[:10],
                    "expected_n": len(expected),
                    "actual_n": len(actual),
                })

        try:
            reader.close()
        except Exception:  # noqa: BLE001
            pass

        ok = (len(mismatches) == 0)
        summary[layout] = {"pass": ok, "mismatches": mismatches}
        status = "PASS" if ok else "FAIL"
        _log(
            f"correctness check: {layout} ... {status} "
            f"({len(mismatches)} mismatches across {len(sample_vids)} vids)"
        )
        if not ok:
            any_fail = True
            for m in mismatches[:5]:
                _log(f"   mismatch: {m}")

    summary["_all_pass"] = not any_fail
    return summary


# ---------------------------------------------------------------------------
# Stage 3: run workloads
# ---------------------------------------------------------------------------

def _summary_for_persistence(rep_result: dict) -> dict:
    """Flatten the structure harness.measure_repeated returns into the
    canonical per-cell shape that analyze.py understands.

    The median run's summary is promoted to the top level so analyze can
    pull P50/P99 directly. We also surface ``cold_us`` and a derived
    ``bytes_read_per_op`` so the kill criteria can be checked.
    """
    median_run = rep_result.get("median_summary") or {}
    median_summary = median_run.get("summary") or {}

    bytes_read = int(median_run.get("bytes_read", 0) or 0)
    iters = int(median_run.get("iterations", 0) or 0)
    bytes_read_per_op = (bytes_read / iters) if iters > 0 else 0.0

    return {
        "P50": median_summary.get("p50_us"),
        "P99": median_summary.get("p99_us"),
        "mean_us": median_summary.get("mean_us"),
        "min_us": median_summary.get("min_us"),
        "max_us": median_summary.get("max_us"),
        "n": median_summary.get("n"),
        "throughput_per_s": median_summary.get("throughput_per_s"),
        "cold_us": median_summary.get("cold_us"),
        "wall_seconds": median_run.get("wall_seconds"),
        "bytes_read": bytes_read,
        "bytes_read_per_op": bytes_read_per_op,
        "iterations": iters,
        "result_sizes_summary": median_run.get("result_sizes_summary"),
    }


def stage_run_workloads(persons: pa.Table, knows: pa.Table) -> dict:
    """For each layout x workload, run harness.measure_repeated.

    Persist results/raw.json with all repetitions.
    Persist results/summary.json with the median per system x workload.
    """
    # Build src_vid samples deterministically. Sample from actual src vids
    # in the knows table so every selected vid is guaranteed to have at
    # least one outgoing edge.
    _log("sampling W1 / W2 src vids from knows ...")
    src_vids_w1 = data_gen.generate_lookup_vids(knows, N_W1, seed=SEED + 11)
    src_vids_w2 = data_gen.generate_lookup_vids(knows, N_W2, seed=SEED + 12)

    workload_specs = [
        ("W1", workloads.W1, (src_vids_w1,), {}),
        ("W2", workloads.W2, (src_vids_w2,), {}),
        ("W3", workloads.W3, (), {}),
    ]

    raw: dict = {}
    summary: dict = {}

    # Compute mean neighbor count once so analyze.py can size the
    # adjacency-bytes baseline correctly.
    src_np = knows.column("src").to_numpy(zero_copy_only=False)
    if src_np.size:
        unique_src, counts = np.unique(src_np, return_counts=True)
        mean_neighbors = float(counts.mean())
    else:
        mean_neighbors = 0.0

    for layout, ReaderCls in READER_CLASSES.items():
        raw[layout] = {}
        summary[layout] = {}

        _log(f"opening reader for {layout} ...")
        try:
            reader = ReaderCls(LAYOUT_DIRS[layout])
        except Exception as exc:  # noqa: BLE001
            _log(f"failed to open {layout}: {exc!r} -- skipping its workloads")
            continue

        for wname, wfn, wargs, wkwargs in workload_specs:
            _log(
                f"benchmarking {layout} / {wname} "
                f"({REPETITIONS} reps) ..."
            )
            t0 = time.perf_counter()
            reps = harness.measure_repeated(
                wname,
                wfn,
                reader,
                *wargs,
                repetitions=REPETITIONS,
                **wkwargs,
            )
            elapsed = time.perf_counter() - t0
            raw[layout][wname] = reps

            cell = _summary_for_persistence(reps)
            cell["mean_neighbors"] = mean_neighbors
            summary[layout][wname] = cell

            _log(
                f"benchmarking {layout} / {wname} ... done in {elapsed:.1f}s "
                f"(P50={cell.get('P50')!r} us)"
            )

        try:
            reader.close()
        except Exception:  # noqa: BLE001
            pass

    raw_path = RESULTS_DIR / "raw.json"
    summary_path = RESULTS_DIR / "summary.json"
    raw_path.write_text(json.dumps(raw, indent=2, default=_json_default))
    summary_path.write_text(json.dumps(summary, indent=2, default=_json_default))
    _log(f"wrote {raw_path} and {summary_path}")

    return summary


# ---------------------------------------------------------------------------
# Smoke / helpers
# ---------------------------------------------------------------------------

def _apply_smoke() -> None:
    """Reduce dataset and workload sizes for fast end-to-end smoke tests."""
    global V, N_W1, N_W2
    data_gen.V = 50_000
    V = 50_000
    workloads.W1_ITERS = 200
    workloads.W2_ITERS = 50
    workloads.W3_ITERS = 10
    N_W1 = workloads.W1_ITERS
    N_W2 = workloads.W2_ITERS
    _log(f"smoke mode: V={V}, N_W1={N_W1}, N_W2={N_W2}")


def _ensure_data_in_memory() -> tuple[pa.Table, pa.Table]:
    """Generate persons/knows tables for in-memory use by later stages."""
    _log(
        f"generating persons + knows (V={V}, seed={SEED}) for in-memory use ..."
    )
    t0 = time.perf_counter()
    persons, knows = data_gen.generate_dataset(v=V, seed=SEED)
    _log(
        f"generated in {time.perf_counter() - t0:.1f}s "
        f"(persons={persons.num_rows:,}, knows={knows.num_rows:,})"
    )
    return persons, knows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(args: list[str]) -> int:
    """CLI:
       run_benchmark.py write       # only stage 1
       run_benchmark.py check       # stage 1 + 2
       run_benchmark.py bench       # all stages (default)
       run_benchmark.py analyze     # invoke analyze.py
    """
    smoke = "--smoke" in args
    args = [a for a in args if a != "--smoke"]
    if smoke:
        _apply_smoke()

    cmd = args[0] if args else "bench"

    if cmd == "analyze":
        analyze_path = HERE / "analyze.py"
        _log(f"invoking {analyze_path} ...")
        rc = subprocess.call([sys.executable, str(analyze_path)])
        return rc

    # Stage 1: generate + write
    write_results = stage_generate_and_write(skip_existing=True)
    _log(
        "stage_generate_and_write: "
        f"{json.dumps(write_results, default=_json_default)}"
    )
    if cmd == "write":
        return 0

    # Materialize persons/knows for the remaining stages.
    persons, knows = _ensure_data_in_memory()

    # Stage 2: correctness check
    check_results = stage_correctness_check(persons, knows)
    (RESULTS_DIR / "correctness.json").write_text(
        json.dumps(check_results, indent=2, default=_json_default)
    )
    if not check_results.get("_all_pass", False):
        _log("CORRECTNESS CHECK FAILED -- aborting before benchmarks")
        return 2

    if cmd == "check":
        return 0

    # Hold references only as long as needed; the workload stage still
    # uses persons/knows to sample vids, but afterwards we can drop them.
    gc.collect()

    # Stage 3: run workloads
    stage_run_workloads(persons, knows)

    # Free big tables before invoking analyze.
    del persons
    del knows
    gc.collect()

    # Stage 4: analyze
    analyze_path = HERE / "analyze.py"
    _log(f"invoking {analyze_path} ...")
    rc = subprocess.call([sys.executable, str(analyze_path)])
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
