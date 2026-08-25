# Graphberg — Gaps & Risks Remediation Plan

**Status:** Proposed · **Date:** 2026-08-25 · **Supersedes:** the lost local-only
gaps-and-risks deliverable (see "Provenance" below)

This plan reconstructs the project's known-gaps-and-risks register from all
surviving materials and lays out a phased program to close every item. It is
the working document for taking Graphberg from "promising row-group
experiment + untested benchmark scaffold" to "defensible benchmark verdict +
ratified layout spec v0.4".

---

## 0. Provenance and scope

The original gaps-and-risks document — like `BENCHMARK_PLAN_v1.md`/`v1.1`,
`LAYOUT_SPEC_v0.3.md`, and `HANDOFF_REPORT.md` — was a local-only deliverable
of earlier working sessions and was lost when those environments were
reclaimed. It exists nowhere in this repository, its branches, its git
history, or its issue tracker (all searched exhaustively).

The register below is therefore **reconstructed** from the surviving record:

- `rowgroup_experiment/RESULTS.md` + `results/results.json` + `run_experiment.py`
  (branch `claude/rowgroup-experiment-y78GV`) — the Section-2 experiment,
  verdict PASS (26.5× neighbor-lookup speedup at 4 MB vs 128 MB row groups).
- `benchmark/` (same branch) — the full 4-layout benchmark scaffold
  (`data_gen`, `writers`, `readers`, `workloads`, `harness`, `analyze`,
  `run_benchmark`, `run_dataset_track`), built against plan v1.1 but with
  **no recorded execution**.
- Branch commit history (`6c861d8` → `6d557d9`).

Every finding was extracted by two independent deep audits of those materials.
Reconstructed decisions that originally rested on lost documents are marked
**[RATIFY]** — they need explicit sign-off from the project owner because
their authoritative wording can no longer be checked.

**Severity key:** 🔴 high — threatens the verdict, correctness, or the format
design itself · 🟡 medium — biases results or blocks scaling · 🟢 low —
hygiene/debt.

---

## 1. Register summary

| ID | Title | Sev | Phase |
|----|-------|-----|-------|
| A1 | Governing plan/spec documents lost | 🔴 | 0 |
| A2 | Benchmark results structurally uncommittable; no execution record | 🔴 | 0 |
| A3 | Zero dependency pinning | 🟡 | 0 |
| A4 | No environment/provenance capture in results | 🟡 | 0 |
| A5 | Documentation drift (phantom run.log, unused duckdb, stale docstrings, hidden seed) | 🟢 | 0 |
| B1 | Row-group **straddle bug** silently truncates neighbor lists (all 4 readers + aux index) | 🔴 | 0 |
| B2 | Symmetric `within_pct`: being *faster* fails W3 thresholds | 🔴 | 0 |
| B3 | Smoke mode doesn't actually shrink W3 (default-arg binding bug) | 🟡 | 0 |
| B4 | Sentinels encode no parameters → smoke/full data contamination | 🟡 | 0 |
| B5 | `mean_neighbors` hard-coded 32.0 fallback; wrong mean definition | 🟡 | 0 |
| B6 | `-1` missing-stats sentinel breaks `searchsorted` ordering invariant | 🟢 | 0 |
| B7 | Metric-mixing in persisted summaries; `cold_us` not the true coldest | 🟢 | 0 |
| B8 | `generate_lookup_vids` duck-typing trap | 🟢 | 0 |
| C1 | Iceberg cell confounded by pyiceberg default row-group size (≥50× threshold mostly measures RG size) | 🔴 | 1 |
| C2 | GraphAr comparison is an in-house emulation, not the real library | 🔴 | 1 |
| C3 | Reader-open cost (Graphberg's full aux load) excluded from every metric | 🔴 | 1 |
| C4 | Warm/cold conflation; silent cache-drop fallback; `bytes_read` undercounts | 🔴 | 1 |
| C5 | Thin statistics: n=1 experiment runs, 5-rep verdicts with no variance | 🟡 | 1 |
| C6 | Track B (planner-vs-layout decomposition) ignored by analyze, unrecordable | 🟡 | 1 |
| C7 | Missing comparisons; missing data can never produce KILL; storage/write metrics discarded | 🟡 | 1 |
| C8 | Query sampling unrealistic: no misses, no degree weighting, hubs underrepresented | 🟡 | 2 |
| C9 | Narrow workload menu: no dst-column filters, single selectivity, W2 Python overhead | 🟡 | 2 |
| D1 | Mechanism unverified: page-index use for `==`, decoder-parallelism hypothesis | 🔴 | 2 |
| D2 | Single engine/version (PyArrow 24.0.0) anchors all conclusions | 🔴 | 2 |
| D3 | No object-storage regime measured (the deployment target of Iceberg) | 🔴 | 2 |
| D4 | Bare int64-pair schema; no edge-property payloads | 🔴 | 2 |
| D5 | Synthetic-graph realism: no low-degree mass, uniform dst, dense sorted vids | 🟡 | 2 |
| D6 | Snappy-only; zstd (common Iceberg default) changes every byte target | 🟡 | 2 |
| D7 | Row-group sizing decision open (1 vs 4 MB); storage-tax baseline ambiguous | 🟡 | 3 |
| D8 | Sortedness degradation and real-Iceberg-table effects unmeasured | 🟡 | 2 |
| E1 | Layout is write-once single-file: no append/delete/compaction/multi-file story | 🔴 | 3 |
| E2 | Aux sidecar unversioned, unbound to target file, non-atomic publication | 🔴 | 3 |
| E3 | Double-write requirement: 2× write amplification, determinism assumption, no streaming | 🟡 | 3 |
| E4 | Footer KV hints scale O(#row groups) — compounds with tiny-RG preference | 🟡 | 3 |
| E5 | Hints bind by column name; no field-id indirection; lax parsing shim | 🟡 | 3 |
| E6 | Hints invisible to engines; aux file pollutes `*.parquet` globs | 🟡 | 3 |
| E7 | `byte_offset` stored but never used and insufficient as designed | 🟡 | 3 |
| E8 | Aux index O(distinct src), fully RAM-materialized | 🟢 | 3 |
| E9 | Calibration: shared `/tmp` temp file, prefix-sample bias (~25 % RG-size miss) | 🟢 | 1 |
| F1 | Unbounded runtime; Iceberg cell can run for hours; crash discards everything | 🔴 | 1 |
| F2 | Memory pressure: ~5 GB held through timing on 16 GB host; prior OOM | 🔴 | 1 |
| F3 | No disk-space preflight (~9–10 GB footprint with leftovers) | 🟡 | 1 |
| F4 | Root-only cache drops with silent, unrecorded fallback | 🟡 | 1 |
| F5 | No tests/CI for the harness; smoke path demonstrably not exercised end-to-end | 🟡 | 0 |
| F6 | Track B mutates the only results copy in place, no backup | 🟢 | 1 |

---

## 2. Workstream detail

### WS-A · Provenance & governance

**A1 — Governing documents lost.** 🔴
Docstrings across the scaffold cite `BENCHMARK_PLAN_v1.1` §3.1/3.4/4.1/4.2 and
`LAYOUT_SPEC_v0.3.md`; neither was ever committed. The threshold table
hard-coded in `analyze.py` is now the *only* record of the success criteria;
the §7 prediction table, the FLAG criterion's original rationale ("tunability
problem"), the "4–8 MB band" spec wording, and the handoff quotes that
authorized the 500× dataset scale-up are all unrecoverable.
*Remediation:* reverse-engineer and commit `BENCHMARK_PLAN_v1.2.md` (from
`analyze.py` thresholds + workload constants + scaffold structure) and a
skeleton `LAYOUT_SPEC_v0.4.md` (from `writers.py`/`readers.py` +
`RESULTS.md` Setup). Mark every reconstructed decision **[RATIFY]**:
(a) the 5× PASS / 2× FLAG row-group decision rule as implemented;
(b) the V=5M/160M-edge dataset deviation; (c) narrowing/dropping the FLAG
criterion; (d) the ~5 ms plain-Parquet prediction rebase.
*Acceptance:* both documents committed; scaffold docstring references resolve;
ratification checklist answered.

**A2 — No committable results; no execution record.** 🔴
`benchmark/.gitignore` ignores `data/` and `results/` wholesale, so
`summary.json`, `raw.json`, `correctness.json`, and `verdict.json` are
*un-committable*; there is no `RESULTS.md` for the full benchmark and nothing
distinguishes "ran and lost" from "never ran". (Evidence says never ran: no
curated write-up exists, unlike `rowgroup_experiment/`.)
*Remediation:* results policy — every accepted run commits a curated
`benchmark/RESULTS.md` plus `results/summary.json` and `results/verdict.json`
(gitignore exceptions), with raw latencies archived.
*Acceptance:* gitignore updated; policy stated in the plan doc; first run
committed under it (Phase 1 exit).

**A3 — No dependency pinning.** 🟡
No requirements file anywhere; version-sensitive surfaces include the
dictionary `count_distinct` workaround, row-group chunking determinism that
the double-write depends on (E3), the ~10 ms planner baseline underlying the
"bypass the planner" rule, and pyiceberg write properties (C1).
*Remediation:* `benchmark/requirements.txt` with exact pins
(pyarrow==24.0.0 to match the experiment, pyiceberg, numpy, PyYAML) + a lock;
versions embedded in every results file (A4).

**A4 — No environment capture.** 🟡
`results.json` records no PyArrow/Python version, hostname, storage class,
CPU count, root/cache-drop status, git SHA, or timestamp; the claimed
`results/run.log` was never written by the script and does not exist.
*Remediation:* a provenance block (versions, host, cores, storage, seeds,
git SHA, drop-cache verification result) written into every results file, and
stdout teed to a committed log.

**A5 — Documentation drift.** 🟢
`RESULTS.md` references a `run.log` that was never produced and installs
`duckdb` which nothing imports; `run_experiment.py`'s docstring claims
V=1,000,000 while the code uses 5,000,000; query sampling uses a second,
undocumented seed (`seed=1`) beside the advertised `SEED=20260430`.
*Remediation:* correct the docs; document both seeds; either use duckdb
(see D2) or drop it from the install line.

### WS-B · Correctness bugs (fix before any run)

**B1 — Row-group straddle bug.** 🔴 *Highest-leverage single fix.*
`readers.py:_neighbors_from_pf` and `GraphbergReader.neighbors_of` locate
**one** row group via `searchsorted(src_max, vid)` and read only it; the aux
index likewise records only the row group of a vid's *first* occurrence. A
vid whose edge run straddles a row-group boundary silently loses the tail of
its neighbors — in **all four readers**, so ground-truth-free cross-layout
comparison also can't catch it, and the 30-vid correctness sample (~0.01 %
straddle incidence, hub-biased) almost certainly passes despite it.
*Remediation:* compute the full range
`[searchsorted(src_max, vid, 'left'), searchsorted(src_min, vid, 'right'))`,
read and concatenate all row groups in it; store `rg_first`/`rg_last` in the
aux index; add a unit test that *constructs* a boundary-straddling vid and a
correctness mode that verifies the highest-degree hubs explicitly.
*Acceptance:* straddle test red on old code, green on new; hub verification
in the correctness stage.

**B2 — Symmetric `within_pct` misfires on improvement.** 🔴
`analyze.py` evaluates "W3 within 15 % of Iceberg / 25 % of plain Parquet"
with `abs(gb − other)/other`, so Graphberg being 20 % *faster* FAILS the row
and degrades the verdict to GRAY-ZONE. The row-group experiment (small RGs
made scans faster) makes exactly this outcome plausible — the same criterion
class `RESULTS.md` already flagged.
*Remediation:* one-sided check ("no more than X % slower"); unit-test both
directions.

**B3 — Smoke mode doesn't shrink W3.** 🟡
`_apply_smoke` reassigns `workloads.W3_ITERS`, but `W3`'s default argument
was bound at definition time, so smoke still runs 100 full scans per rep —
also evidence the smoke path never ran end-to-end.
*Remediation:* pass `iterations=` explicitly from the orchestrator; assert in
smoke mode.

**B4 — Parameterless sentinels contaminate data dirs.** 🟡
`.complete` sentinels encode nothing about V/seed/writer version and smoke
(V=50 K) shares `DATA_DIR` with full runs — mixed-mode data persists silently;
editing `writers.py` never invalidates sentinels.
*Remediation:* sentinel carries a parameter hash (V, seed, writer version,
pyarrow version); mismatch → regenerate; separate `data-smoke/` directory.

**B5 — `mean_neighbors` fallback and definition.** 🟡
Kill criterion #5's baseline silently falls back to a hard-coded 32.0, and the
computed value is the mean over vids *with* edges, not overall mean degree.
*Remediation:* persist the measured value with its definition; treat a missing
value as an error, not a default.

**B6 — `-1` missing-stats sentinel.** 🟢 Breaks the ascending-order invariant
`searchsorted` requires. *Remediation:* fail hard on missing row-group stats.

**B7 — Metric mixing in summaries.** 🟢 The median-by-P50 repetition's *other*
metrics (bytes, cold, wall) are promoted wholesale; `cold_us` is not the true
coldest observation; `best_run` is computed and discarded.
*Remediation:* per-metric aggregation across reps; report cold from rep 1.

**B8 — `generate_lookup_vids` duck-typing.** 🟢 Callers pass the knows table
through a parameter named `persons`; a real persons table silently samples
edge-less vids. *Remediation:* rename the parameter, assert on the schema.

### WS-C · Benchmark fairness & methodology

**C1 — The Iceberg cell is confounded.** 🔴
Plain/GraphAr/Graphberg all get calibrated ~4 MiB row groups; the Iceberg
writer delegates to `pyiceberg.Table.append` with *its* defaults (~128 MB).
Since the row-group experiment showed 4 MB-vs-128 MB alone is worth ~26.5×,
the flagship "≥50× faster than Iceberg" threshold mostly measures row-group
size, and kill criterion #2 (Graphberg > 10× slower than Iceberg) becomes
unfireable. Meanwhile `IcebergReader` strips pyiceberg's per-query planning,
flattering Iceberg in the other direction. Neither distortion is quantified.
*Remediation:* run **two Iceberg cells** — (a) matched: write with
`write.parquet.row-group-size-bytes` ≈ the calibrated target; (b) default:
stock pyiceberg settings; plus a planner-inclusive read variant to bound the
planning overhead. Re-derive the ≥50×/≥20× thresholds against the matched
cell **[RATIFY]** and make criterion #2 reference it.
*Acceptance:* verdict reports both cells; thresholds re-anchored and ratified.

**C2 — GraphAr is an emulation.** 🔴
"GraphAr-style" chunked layout with hand-written yml, read by this repo's own
searchsorted reader — the real library (offset arrays, O(1) adjacency
location) is never used, yet the headline thresholds are "within 3×/4× of
GraphAr".
*Remediation:* integrate the real GraphAr writer/reader for at least a
calibration dataset and measure emulation-vs-real delta; until then, relabel
the comparison "graphar_emulated" everywhere including `verdict.json`.
*Acceptance:* either real-GraphAr numbers in the verdict or an explicit,
quantified emulation-delta caveat attached to every GraphAr threshold row.

**C3 — Reader-open cost excluded.** 🔴
Readers are constructed once, *before* measurement. `GraphbergReader.__init__`
parses the footer hint JSON and loads the **entire aux sidecar** (one row per
distinct src — up to ~5 M rows) into numpy. That is precisely where Graphberg
pays for its fast path, and it appears in no metric, threshold, or kill
criterion — cold criterion #4 measures first-op latency on an already-open
reader.
*Remediation:* time `__init__` per layout as a first-class `open_us` metric;
include open+first-op in the cold criterion; add a lazy/paged aux-load option
(ties to E8).

**C4 — Warm/cold conflation and unverified cache drops.** 🔴
Cache is dropped once per repetition, then W1 runs 10,000 iterations against
a page cache that holds the whole file (16 GB RAM vs ~1 GB files) — P50/P99
are warm numbers labeled cold. On `PermissionError` the harness falls back to
`gc.collect()` (which does not drop the OS cache) with only a printed
warning; nothing records which path ran, so a published verdict can't prove
its own methodology. `bytes_read` via `/proc/self/io` counts block-device
reads only, so per-op bytes amortize toward zero and kill criterion #5 is
defanged without root.
*Remediation:* verified drop (write + assert + cold-read canary + result
recorded in provenance, A4); separate **cold regime** (drop between queries,
smaller n) and **warm regime** measurements, each labeled; state which regime
each spec claim targets; report `rchar` alongside `read_bytes`.

**C5 — Thin statistics.** 🟡
Every number in the row-group experiment's PASS is n=1 (one process, one file
write, one pass; the predicate scan literally one iteration) — fine for a
26.5×-vs-5× margin, not for the 0.50× predicate ratio, the 1 MB-vs-4 MB
ranking, the 8 MB P99/P50=1.48 outlier, or the non-monotonic file sizes. The
full benchmark's 5 reps produce a verdict with no variance or sensitivity
margins.
*Remediation:* ≥5 reps with per-metric median + stdev everywhere; margins in
`verdict.json` (flag any threshold decided by <20 % margin); re-run the
row-group experiment once with repetitions and archived stdout to confirm
reproduction (also closes the lost-run-log provenance hole).

**C6 — Track B is orphaned.** 🟡
`run_dataset_track.py` answers the project's key attribution question — how
much of Graphberg's win is layout vs merely bypassing the dataset planner —
but `analyze.py` never consumes the `graphberg_dataset` entry, and the merged
output lives only in gitignored JSON.
*Remediation:* add the layout-vs-planner decomposition to `analyze.py` output
and the committed RESULTS.md; back up `summary.json` before in-place merge
(F6).

**C7 — Threshold-table blind spots.** 🟡
No W1/W2 comparison vs plain_parquet (the most honest baseline: same file,
minus hints); no W3-vs-GraphAr; P99 checked for one cell only; storage size
and write time are measured in stage 1, then logged to stdout and discarded;
missing data can never produce KILL — a Graphberg reader that fails to open
yields GRAY-ZONE, indistinguishable from a near-miss.
*Remediation:* extend the threshold table (W1/W2 vs plain_parquet, storage
overhead vs plain_parquet, write-time ratio) **[RATIFY]**; persist stage-1
size/time metrics; add an INVALID verdict when the candidate's metrics are
absent.

**C8 — Unrealistic query sampling.** 🟡
Lookup vids are uniform over vids-with-≥1-edge: no miss lookups (where stats
pruning and the aux index differ most between layouts), no degree weighting
(real traversals reach hubs ∝ in-degree; hubs up to 2,000 edges span multiple
small row groups), and mean-returned-rows 25.2 vs true mean degree 32.1
confirms the bias.
*Remediation:* add miss-lookup and edge-biased (or BFS-frontier) query sets;
report hub-only percentiles.

**C9 — Narrow workload menu.** 🟡
One always-hit equality lookup; one ~50 %-selectivity range predicate on the
*sort key*. No filters on the unsorted `dst` column (where small row groups'
weaker min/max stats should actually hurt), no selectivity sweep, and W2's
timed region includes layout-independent pure-Python set machinery over up to
~2000×2000 elements, compressing inter-layout ratios.
*Remediation:* add dst-column filters and a selectivity sweep (0.1 %, 1 %,
10 %, 50 %); vectorize W2's assembly or report its overhead separately.

### WS-D · External validity & mechanism

**D1 — Mechanism unverified.** 🔴
With page index enabled, the 128 MB neighbor lookup still took ~412 ms —
suggesting PyArrow doesn't exploit the page index for `==` filters. If a
hand-rolled reader can, the 26.5× headline partly measures a PyArrow
limitation, not a layout property. The predicate-speedup explanation
(decoder parallelism + boundary-RG cost) is likewise hypothesized, never
tested.
*Remediation:* prototype a direct row-group + page-index reader and re-measure
128 MB; re-run the predicate scan single-threaded (`pa.set_cpu_count(1)`) and
with a non-boundary predicate; write the causal story down before it hardens
into spec.
*Acceptance:* a committed mechanism note that either confirms the layout
effect or re-attributes part of it.

**D2 — Single engine, single version.** 🔴
Everything — the ~10 ms planner baseline, "small RGs are faster for scans",
page-index behavior — is a PyArrow-24.0.0 observation.
*Remediation:* replicate W1 and W3 with DuckDB (already in the doc's install
line) at 4 MB and 128 MB; optionally arrow-rs or Spark. Pin versions (A3).

**D3 — No object-storage regime.** 🔴
All measurements are local-SSD-warm-cache; Iceberg's real deployments read
from object stores where per-request latency inverts the economics of
many-small-reads. The row-group sizing decision (D7) cannot be finalized on
local-disk data alone.
*Remediation:* repeat the neighbor-lookup experiment against MinIO/S3 (or
injected per-read latency) at 1/4/16/128 MB row groups.

**D4 — Bare int64-pair schema.** 🔴
Real edges carry payload columns; wider rows mean fewer rows per row group,
weaker per-stats-check skipping, larger boundary decode costs, different
metadata ratios. The 26.5× and the 1–4 MB recommendation may not transfer to
property graphs.
*Remediation:* re-run with a realistic property payload (2–4 mixed-type
columns) as defined in spec v0.4.

**D5 — Synthetic-graph realism.** 🟡
The mean-rescaling in `generate_edges` shifts minimum degree up, eliminating
degree-1..k vertices — real power-law graphs are *dominated* by low-degree
vertices. Uniform-random dst gives zero community structure and a
near-incompressible dst column, inflating bytes/row and distorting the entire
size-latency curve including the "20 % storage tax at 1 MB". Dense, perfectly
sorted vids are the best case for every layout under test.
*Remediation:* fix the generator's low-degree mass; add LDBC SNB (scale ≥ 3
per the amended dataset rule) and one real graph (e.g. a SNAP dataset) to the
matrix.

**D6 — Snappy-only.** 🟡 Zstd (common Iceberg default) changes bytes/row and
therefore rows-per-RG at every byte target. *Remediation:* zstd variant of the
write + W1 measurement.

**D7 — Row-group sizing decision open.** 🟡
1 MB beats 4 MB on latency but costs ~20 % storage — with no decision
criterion; the labels are nominal (actual ≈ 1.29/4.36 MB — prefix-sampled
calibration bias, E9); the tax baseline is ambiguous because file size is
non-monotonic in RG size (the 128 MB file is *larger* than 8/16 MB —
suspected dictionary-encoding fallback, uninvestigated); and the 8 MB P99
outlier (P99/P50 = 1.48 vs ~1.3 elsewhere) is unexplained on n=1.
*Remediation:* after D3–D6 inputs, define the latency-vs-storage rule, test
2/3 MB intermediates with repetitions, report *actual* RG sizes, use the true
minimum file size as the tax baseline, and pin **one number** in spec v0.4
**[RATIFY]**.

**D8 — Sortedness & real-table effects.** 🟡
One pristine pre-sorted file; no Iceberg manifests/planning, multi-file
datasets, unsorted appends, delete files, or post-compaction fragmentation —
none of the conditions the format will actually meet, and no measurement of
how sort degradation erodes the win.
*Remediation:* benchmark through a real Iceberg table (ties to C1) and add a
partial-sortedness sensitivity run.

### WS-E · Graphberg layout design risks (spec v0.4 inputs)

**E1 — Write-once, single-file.** 🔴
Footer hints live in the file's own KV metadata; `GraphbergReader` hard-codes
exactly one `edges.parquet` + one aux. There is no multi-file, append,
delete, or compaction story — the layout as implemented cannot represent a
mutable table.
*Remediation:* spec v0.4 must either define multi-file semantics (hints per
data file, aux per file, cross-file planning — natural fit: Iceberg data-file
granularity) or explicitly scope v0 as an immutable-snapshot format with a
documented compaction path. **[RATIFY]**

**E2 — Aux sidecar unversioned and unbound.** 🔴
Aux metadata is only `kind=offset_index` + `target=edges.parquet` — no spec
version, no content hash or snapshot id binding it to the exact target file.
A rewritten edges.parquet beside a stale aux passes validation and silently
returns wrong row groups; two-file publication is non-atomic.
*Remediation:* add `graphberg.aux.version` and a target fingerprint
(file length + footer hash, or Iceberg snapshot id); reader rejects
mismatches; publication protocol: write aux first, then commit both via table
metadata atomically.

**E3 — Double-write requirement.** 🟡
Write → inspect → rewrite with hints: 2× write amplification, full table in
memory, no streaming ingest, and correctness depends on the second
`pq.write_table` reproducing identical row-group boundaries — a
pyarrow-version-dependent assumption the reader never cross-validates.
*Remediation:* single-pass writer that buffers per-row-group and accumulates
hints incrementally, or move hints out of the footer entirely (into the aux /
table metadata, see E4); at minimum, reader-side validation of hints against
actual row-group stats on open.

**E4 — Footer hint scaling.** 🟡
~60 bytes of JSON per row group, at the format's own preferred 1–4 MB row
groups, means a 1 TB table carries 16–60 MB of JSON in the Thrift footer —
parsed on every open, potentially rejected by engines with footer limits. The
design's two choices (tiny RGs, per-RG footer hints) compound against each
other.
*Remediation:* measure open cost vs RG count; likely resolution is moving
per-RG hints to the (versioned, E2) sidecar or Iceberg metadata and keeping
only a small header in the footer. **[RATIFY]**

**E5 — Name-based schema binding.** 🟡
Hints implicitly bind to a column literally named `src`; no field-id
indirection (Iceberg-style), no schema fingerprint, plus a
"tolerate a bare list" parsing shim that loosens the format contract.
*Remediation:* bind hints by field id; strict versioned parsing; hint
invalidation rule on schema evolution in spec v0.4.

**E6 — Engine invisibility & glob pollution.** 🟡
KV-metadata hints benefit only the custom reader — no Spark/Trino/DuckDB
path is even sketched — and `edges.parquet.aux.parquet` matches `*.parquet`
globs, so any engine pointed at the directory ingests index rows as edge
data.
*Remediation:* rename the sidecar (`.gaux` or an `_index/` subdir, outside
data globs); document the engine-integration story (Iceberg table properties
+ planning hook) as a spec v0.4 section.

**E7 — Dead `byte_offset`.** 🟡
The aux stores per-vid `data_page_offset` that no reader uses, and which is
insufficient anyway (no page lengths, no dictionary-page offset, no
column-chunk ranges for `dst`). The "offset sub-index" — presumably the point
of the lost v0.3 design — is untested as designed.
*Remediation:* decide: implement page-level addressing properly (and prove it
beats row-group-level, per D1's hand-rolled reader) or drop it from the spec.
**[RATIFY]**

**E8 — Aux size & residency.** 🟢
One row per distinct src, fully materialized in RAM at open (~5 M rows here;
multi-GB at billion-vertex scale), duplicating information the footer hints
already provide.
*Remediation:* paged/partitioned aux or rely on RG hints + page index; ties
to C3's `open_us` metric.

**E9 — Calibration flaws.** 🟢
Hard-coded shared `/tmp/_calib.parquet` (concurrent-writer collision) and
first-500K-rows sampling that misses actual RG size by ~25 % at the 1 MB
target.
*Remediation:* per-process temp file; random-sample calibration; report
actual achieved RG sizes as the x-axis everywhere.

### WS-F · Operational hardening

**F1 — Unbounded runtime, all-or-nothing persistence.** 🔴
W1 = 10 K ops × 5 reps × 4 layouts; with pyiceberg's default row groups the
Iceberg cell alone extrapolates to ~5+ hours (412 ms/lookup measured at
128 MB); results are written only after *all* layouts finish, so a crash in
layout 3 of 4 discards everything; generation (~125 s) runs at least twice
per invocation.
*Remediation:* per-cell checkpointing to disk immediately after each
layout×workload completes; per-cell time budget with explicit TIMED_OUT
marker; reuse the in-memory dataset across stages. (Fixing C1 also collapses
the worst cell.)

**F2 — Memory pressure.** 🔴
Generation peaks over several 1.28 GB temporaries; `persons`/`knows`
(~2.6 GB) stay alive through the whole workload stage while ground-truth
numpy copies add ~2.6 GB more — on a 16 GB host that already OOM'd once
(commit `53d3ca1`), and resident memory competes with the page cache being
measured.
*Remediation:* free tables before timing (spill ground truth to disk),
peak-RSS logging per stage, and a documented minimum-RAM requirement.

**F3 — Disk preflight.** 🟡 Four layouts ≈ 4–6 GB plus the double-write
transient plus ~4.4 GB of row-group-experiment leftovers; nothing checks free
space. *Remediation:* preflight check; cleanup step for stale experiment data.

**F4 — Root/cache verification.** 🟡 Covered by C4's verified-drop mechanism;
listed separately because it gates *where* the benchmark may run: non-root
environments must refuse to produce cold-labeled results rather than degrade
silently.

**F5 — No tests or CI.** 🟡
Only inline smoke blocks; readers, analyze, and the full pipeline have no
tests; the B3 bug proves the smoke path never ran end-to-end.
*Remediation:* unit tests for readers (including the B1 straddle case),
analyze verdict logic (including B2 both-directions), sentinel invalidation
(B4); a `--smoke` end-to-end run wired into CI on the benchmark branch.

**F6 — Track B in-place mutation.** 🟢 Back up `summary.json`/`raw.json`
before merging; covered under C6.

---

## 3. Phased execution plan

Each phase has an explicit exit gate; later phases depend on earlier ones.
Phases 0–1 are mechanical and can start immediately; Phases 2–3 contain the
open research questions; Phase 4 is contingent on a PROCEED verdict.

### Phase 0 — Governance & correctness (≈ 2–4 focused days)
Items: A1–A5, B1–B8, F5, plus the analyze-layer fixes (B2, C7's INVALID
verdict, C6's decomposition hook).
Work: reconstruct and commit `BENCHMARK_PLAN_v1.2.md` + skeleton
`LAYOUT_SPEC_v0.4.md` with the [RATIFY] checklist; fix all eight B-bugs; add
the test suite; pin dependencies; add provenance capture; fix gitignore +
results policy.
**Exit gate:** all tests green (straddle test demonstrably red on the old
reader); reconstructed plan committed; ratification checklist ready for the
owner.

### Phase 1 — Fair, instrumented, recorded benchmark run (≈ 2–3 days)
Items: C1–C7, E9, F1–F4, F6.
Work: matched + default Iceberg cells; GraphAr relabeling (real-library
integration may slip to Phase 2); `open_us` metric; verified cold/warm
regimes; checkpointed orchestrator with per-cell budgets; memory fixes;
then execute `--smoke` end-to-end, then the full run **as root**, then
Track B; commit curated `benchmark/RESULTS.md` + summary + verdict with full
provenance.
**Exit gate:** committed verdict with provenance, both Iceberg cells, Track B
layout-vs-planner decomposition, and variance margins. *(Feasibility: the
current remote environment qualifies — root with writable
`/proc/sys/vm/drop_caches`, 15 GB RAM, 4 cores, ~30 GB free disk, Python
3.11; pyarrow/pyiceberg must be installed per the new requirements.txt.)*

### Phase 2 — Mechanism & generalization studies (≈ 1–2 weeks, parallelizable)
Items: D1–D6, D8, C8–C9, C5's experiment re-run.
Work: hand-rolled page-index reader vs 128 MB; single-threaded predicate
re-run; DuckDB replication; object-storage (MinIO/simulated-latency) run;
property-payload run; zstd run; fixed-generator + LDBC + real-graph runs;
extended workload menu; row-group experiment re-run with repetitions.
Each study is independent — run in parallel and commit each as a short
RESULTS-style note.
**Exit gate:** mechanism note committed; the D-matrix (engine × storage ×
schema × compression × dataset) filled for at least W1 at 4 MB and 128 MB.

### Phase 3 — Decisions & LAYOUT_SPEC v0.4 (≈ 1 week)
Items: D7, E1–E8, plus all accumulated [RATIFY] decisions.
Work: pin the row-group size with a stated decision rule; version + fingerprint
the aux; choose footer-vs-sidecar hint placement with measured open costs;
field-id binding; multi-file/append semantics or explicit immutable-v0
scoping; byte_offset keep-or-drop; sidecar naming + engine-integration
section. Write `LAYOUT_SPEC_v0.4.md` fresh (v0.3 is unrecoverable),
importing every constraint recoverable from code + RESULTS.md.
**Exit gate:** spec v0.4 committed with every [RATIFY] item explicitly
decided and rationale recorded.

### Phase 4 — Integration path (contingent on PROCEED)
Prototype the layout at Iceberg data-file granularity per this repository's
conventions (Java, `core/` engine-agnostic, no Jackson annotations, hints via
custom parser classes, revapi-clean), including the Spark/DuckDB read-path
story from E6. Out of scope for this plan beyond noting that Phases 0–3
deliberately keep every artifact Python-side and non-invasive to the Iceberg
build.

---

## 4. Top 10 priorities by leverage

1. **B1** straddle bug — a silent correctness bug in every reader invalidates
   any benchmark run until fixed.
2. **C1** Iceberg row-group confound — the flagship number currently measures
   the wrong thing in Graphberg's favor.
3. **B2** symmetric `within_pct` — a plausible *good* outcome would wreck the
   verdict.
4. **A1/A2** reconstruct the plan + make results committable — without these,
   Phase 1's run is as losable as its predecessors.
5. **C4/F4** verified cold/warm regimes — the published verdict must be able
   to prove its own methodology.
6. **C3** open-cost metric — Graphberg's amortized-away aux load is its real
   cold cost.
7. **D1** mechanism verification — decides whether PASS reflects the layout
   or a PyArrow quirk, before the spec hardens.
8. **E2** aux versioning/fingerprint — the one layout flaw that produces
   silently wrong query results in production.
9. **F1/F2** checkpointing + memory — the full run has already OOM'd once and
   can lose hours to a late crash.
10. **D3** object-storage regime — the row-group sizing decision is
    unfinalizable without it.

---

## 5. Standing risks (accepted, monitored)

- **Reconstruction risk:** thresholds and deviations canonized here are
  reconstructions of lost documents; the [RATIFY] checklist is the mitigation,
  not a guarantee the original intent is recovered.
- **Emulation risk:** until real GraphAr lands (C2), every GraphAr-relative
  claim carries an unquantified emulation delta.
- **Environment risk:** ephemeral containers reclaim local state; the results
  policy (A2) and per-study commit discipline are the only durable memory this
  project has. Nothing of value may exist solely in a working directory at
  the end of a session.
