"""Track B follow-up: run Graphberg's W1/W2/W3 through the
``pyarrow.dataset.to_table`` path and merge into the existing summary.

This is intended to run AFTER the main bench (run_benchmark.py bench)
has produced ``results/summary.json`` and ``results/raw.json``. It
appends a ``graphberg_dataset`` entry next to ``graphberg`` so the
write-up can show how much of Graphberg's win is "layout" vs. simply
"bypassing the dataset planner".

It avoids regenerating the full V=5M graph (~125s) by reading only the
``src`` column of the already-written edges Parquet to re-derive
deterministic lookup vids.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import data_gen
import readers
import workloads
import harness
import run_benchmark as rb


HERE = Path(__file__).resolve().parent
RESULTS_DIR = HERE / "results"


def main() -> int:
    summary_path = RESULTS_DIR / "summary.json"
    raw_path = RESULTS_DIR / "raw.json"
    if not summary_path.exists() or not raw_path.exists():
        print("ERROR: run_benchmark.py bench must complete first.", file=sys.stderr)
        return 2

    summary = json.loads(summary_path.read_text())
    raw = json.loads(raw_path.read_text())

    # Re-derive lookup vids deterministically from the already-written edge
    # data. Reading just the src column from plain_parquet's edges.parquet is
    # ~10s for 160M rows.
    edges_path = HERE / "data" / "plain_parquet" / "edges.parquet"
    print(f"[track-b] reading src column from {edges_path} ...")
    t0 = time.perf_counter()
    src_only = pq.read_table(edges_path, columns=["src"])
    print(f"[track-b] read src column in {time.perf_counter() - t0:.1f}s "
          f"({src_only.num_rows:,} rows)")

    src_vids_w1 = data_gen.generate_lookup_vids(
        src_only, workloads.W1_ITERS, seed=data_gen.SEED + 11
    )
    src_vids_w2 = data_gen.generate_lookup_vids(
        src_only, workloads.W2_ITERS, seed=data_gen.SEED + 12
    )
    src_np = src_only.column("src").to_numpy(zero_copy_only=False)
    _, counts = np.unique(src_np, return_counts=True)
    mean_neighbors = float(counts.mean())
    del src_only, src_np, counts

    layout = "graphberg_dataset"
    data_dir = HERE / "data" / "graphberg"
    raw[layout] = {}
    summary[layout] = {}

    workload_specs = [
        ("W1", workloads.W1, (src_vids_w1,), {}),
        ("W2", workloads.W2, (src_vids_w2,), {}),
        ("W3", workloads.W3, (), {}),
    ]

    print(f"[track-b] opening GraphbergDatasetReader on {data_dir} ...")
    reader = readers.GraphbergDatasetReader(data_dir)

    for wname, wfn, wargs, wkwargs in workload_specs:
        print(f"[track-b] benchmarking {layout} / {wname} "
              f"({rb.REPETITIONS} reps) ...")
        t0 = time.perf_counter()
        reps = harness.measure_repeated(
            wname,
            wfn,
            reader,
            *wargs,
            repetitions=rb.REPETITIONS,
            **wkwargs,
        )
        elapsed = time.perf_counter() - t0
        raw[layout][wname] = reps

        cell = rb._summary_for_persistence(reps)
        cell["mean_neighbors"] = mean_neighbors
        summary[layout][wname] = cell

        print(f"[track-b] benchmarking {layout} / {wname} ... done in "
              f"{elapsed:.1f}s (P50={cell.get('P50')!r} us)")

    reader.close()

    summary_path.write_text(json.dumps(summary, indent=2, default=rb._json_default))
    raw_path.write_text(json.dumps(raw, indent=2, default=rb._json_default))
    print(f"[track-b] merged into {summary_path} and {raw_path}")

    print("[track-b] re-running analyze.py ...")
    return subprocess.call([sys.executable, str(HERE / "analyze.py")])


if __name__ == "__main__":
    sys.exit(main())
