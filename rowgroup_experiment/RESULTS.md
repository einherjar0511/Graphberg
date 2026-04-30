# Row-Group Experiment Results

Section 2 of `BENCHMARK_PLAN_v1.md`.

**Verdict: PASS.** Proceed to the full benchmark.

## TL;DR

- 4MB row groups beat 128MB row groups on neighbor-lookup P50 by **26.5×**
  (threshold was 5×).
- Predicate scan at 4MB is **0.5×** the wall time of 128MB — i.e. *faster*,
  not slower. The "tunability problem" the FLAG criterion was guarding
  against did not materialize.
- The smaller-RG advantage is monotone in row-group size for both workloads:
  smaller is better all the way down to 1MB.
- The foundational assumption of the format design holds. Proceeding to the
  full benchmark (sections 3-7) is justified.

## Setup

| | |
|---|---|
| Host | Linux 6.18.5, x86_64, 16 GB RAM, root |
| Python | 3.11.15 |
| PyArrow | 24.0.0 |
| Compression | Snappy |
| Page index | Enabled (`write_page_index=True`) |
| Page size | PyArrow default (1 MB) |
| Row count column | int64 src, int64 dst |
| Sort order | ascending by src |
| Filter API | `pyarrow.dataset.Dataset.to_table(filter=...)` (uses RG stats; page index when applicable) |
| OS cache | dropped via `/proc/sys/vm/drop_caches` before each measurement run |

### Dataset (deviation from spec — see "Surprises" §4)

- **5,000,000 vertices**, ~160M edges (the spec called for ~10K vertices /
  ~180K edges).
- Out-degree drawn from a Zipf(α=1.6) distribution, rescaled so that
  empirical mean = 32.10, truncated at 2,000.
- Empirical max degree 1,723; P99 degree 1,091.
- dst sampled uniformly over [0, V).
- Seeded with `SEED=20260430` for reproducibility.

Why the scale-up: at 10K / 180K edges, the file compresses to ~1-2 MB
total. Every row-group target ≥4 MB collapses to a single row group and
the comparison becomes meaningless. Scaling to 5M vertices preserves the
power-law degree shape while giving 7 row groups at the 128MB size and
833 at 1MB. The handoff explicitly authorises this trade-off
("the access pattern, not the exact dataset"; "make the simplest
reasonable choice and document it").

## Numbers

```
 rg_mb        file     #rg    nl_p50_us    nl_p99_us   pred_wall_ms
     1      1.0 GB     833      11726.9      14812.7          914.5
     4    868.6 MB     209      15544.1      20082.9          962.6
     8    840.9 MB     105      26815.8      39693.0         1039.4
    16    829.2 MB      53      52070.4      64776.9         1852.5
   128    846.5 MB       7     412207.1     542102.3         1916.4
```

Each `nl` row is 1,000 neighbor-lookup iterations on randomly-sampled
existing src vids (mean 25.2 edges returned per query). Each `pred` row is
a single `SELECT dst FROM edges WHERE src > median(src)` returning
80,238,452 rows. OS cache dropped before each row of the table.

### Decision rule application

- Best of 4MB / 8MB neighbor P50 = 4MB at 15.5 ms.
- 128MB neighbor P50 = 412.2 ms.
- Speedup = 412.2 / 15.5 = **26.5×**, ≥ 5× → PASS criterion met.
- 4MB predicate wall = 962.6 ms; 128MB predicate wall = 1916.4 ms.
- Slowdown = 962.6 / 1916.4 = **0.50×** (small RGs are faster).
- ≤ 2× → FLAG criterion not triggered.

## Surprises

### 1. Small row groups are *faster* on the predicate scan, not slower

The benchmark plan's prediction table assumed plain-Parquet predicate
scans would degrade somewhat with small row groups (more metadata
overhead, more decoder restarts). They didn't — small RGs were modestly
faster at 4-8MB and ~2× faster than 128MB.

Likely causes:

- **Decoder parallelism.** PyArrow's dataset reader parallelises across
  row groups. With 7 RGs there is little headroom; with 209 RGs there is
  a lot.
- **Boundary-RG cost.** The predicate `src > median` requires
  row-by-row evaluation of the *one* row group that straddles the
  threshold. At 128MB that boundary RG is ~120 MB; at 4MB it is ~4MB.
  This cost is paid even when min/max stats prove the rest of the RG
  qualifies.

Implication for the format: we do not need to engineer a row-group-size
trade-off between graph and analytic workloads. Both want the same
direction (smaller). The original FLAG case in the plan can be removed
or rewritten in v0.4 of the spec.

### 2. Smaller is monotonically better for neighbor lookup; no plateau

The plan implicitly framed 4-8MB as "the sweet spot" against 128MB.
Empirically the curve keeps falling: 1MB beats 4MB beats 8MB. The
fall-off flattens between 1MB and 4MB but does not reverse.

That said, 1MB row groups produce a **20% larger file** (1.0 GB vs
~840 MB) — visible per-RG metadata / dictionary / page-index overhead.
The format will probably want 1-4MB row groups: cheap on neighbor P50,
small storage tax.

### 3. Absolute neighbor-lookup latency is ~3-10× higher than predicted

The plan predicted plain Parquet at ~5 ms P50. Measured: 11.7 ms (1MB)
to 412 ms (128MB). The 5 ms figure was probably based on a smaller
dataset and/or a hand-rolled reader; the PyArrow `dataset.to_table`
filter pipeline has a ~10 ms per-query baseline (filter expression
construction + dataset planning) before it touches data. Two
implications:

- The prediction column for plain Parquet in §7 of the plan should be
  rebased on the measurement environment used for the actual
  benchmark. Otherwise the "our format" target will look unduly easy.
- The Graphberg format will benefit not just from layout but also from
  bypassing the dataset planner — a direct row-group + offset-index
  lookup should comfortably hit single-digit milliseconds.

### 4. Dataset scale-up was unavoidable

Already covered in Setup. Stating it here as a "spec impact" item: the
benchmark plan's data section should specify a dataset large enough that
the file at 128MB row groups contains at least 2-3 row groups. With
int64 src/dst sorted and Snappy, that means ~250-500 MB compressed,
i.e. ~50-100M edges, i.e. ~1.5-3M vertices at mean degree 32. The 10K
/ 180K LDBC scale-1 figure should be replaced or supplemented for this
section.

### 5. P99 close to P50 at small RGs, far from P50 at large RGs

At 1MB the P99/P50 ratio is ~1.3; at 128MB it is ~1.3 as well.
Variability is dominated by row-group decode time, not by tail
latencies, and there is no obvious GC / cache pathology. Reasonable
behaviour.

## Spec impact

Concrete suggestions for `LAYOUT_SPEC_v0.3.md` and
`BENCHMARK_PLAN_v1.md` revisions:

1. **`BENCHMARK_PLAN_v1.md` §2 dataset size**: replace LDBC scale-1 /
   ~180K edges with "a graph large enough to occupy ≥250 MB of
   compressed Parquet — e.g. 2M+ vertices at mean degree 32, or LDBC
   SNB scale-3." Document the rationale.
2. **`BENCHMARK_PLAN_v1.md` §2 FLAG criterion**: the predicate-scan
   slowdown branch did not fire and is unlikely to fire under PyArrow.
   Either narrow the criterion (predicate slowdown only matters if
   neighbor speedup is *exactly* at threshold) or drop it. Don't keep
   a kill criterion that the empirical data says won't trigger.
3. **`BENCHMARK_PLAN_v1.md` §7 prediction table**: rebase the plain
   Parquet column to per-environment numbers. Suggest re-running this
   experiment's neighbor-lookup measurement at fixed-RG=4MB to anchor
   the table to a real machine.
4. **`LAYOUT_SPEC_v0.3.md` row-group sizing**: empirical data supports
   a 1-4 MB row-group target. 1MB is fastest but pays a ~20% storage
   tax; 4MB looks like the operating point. Document this rather than
   specifying "4-8 MB" as if it were a band.
5. **Page index**: confirm in the spec that page index must be written.
   Without it, larger row groups would presumably do worse — but I
   measured *with* page index enabled, and the 128MB neighbor lookup
   still took ~412 ms (reading effectively a whole row group). PyArrow
   may not be using the page index optimally for `==` filters; worth
   investigating in the prototype phase whether a hand-rolled reader
   does better.

## Reproducing

```bash
cd rowgroup_experiment
pip install pyarrow numpy duckdb
python3 run_experiment.py
```

Outputs:

- Console log (also saved to `results/run.log`)
- `results/results.json` — machine-readable summary
- `data/edges_rg{1,4,8,16,128}MB.parquet` — generated files (~4.4 GB
  total; gitignored)

Run time ~10 minutes on this host (16 GB RAM, dominated by neighbor
lookup against the 128MB file).
