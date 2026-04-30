"""Synthetic social-network graph generator for the Graphberg benchmark.

Produces in-memory PyArrow tables (no file I/O) consumed by several
storage-layout writers downstream.

Public API:
    generate_dataset(v=V, seed=SEED) -> (persons_table, knows_table)
    generate_lookup_vids(persons, n, seed=1) -> list[int]

Schemas:
    persons: {vid: int64, birth_year: int16, country: dictionary<string>}
    knows:   {src: int64, dst: int64}
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

# ---- Public constants ------------------------------------------------------

V = 5_000_000               # number of Person vertices
TARGET_MEAN_DEGREE = 32
ZIPF_ALPHA = 1.6
MAX_DEGREE = 2_000
SEED = 20260430
COUNTRY_CARDINALITY = 200   # distinct country codes in the dictionary

# ---- Internal helpers ------------------------------------------------------


def _country_vocab() -> list[str]:
    """200-element fixed country vocabulary."""
    return [f"C{i:03d}" for i in range(COUNTRY_CARDINALITY)]


def _generate_persons(v: int, rng: np.random.Generator) -> pa.Table:
    """Build the persons table: vid, birth_year, country (dict-encoded)."""
    vids = np.arange(v, dtype=np.int64)

    # birth_year uniform in [1900, 2010] inclusive.
    birth_year = rng.integers(low=1900, high=2011, size=v, dtype=np.int16)

    # Country indices via Zipf(1.5), clipped to [0, COUNTRY_CARDINALITY).
    # numpy's rng.zipf returns values >= 1; subtract 1 to make 0-based.
    raw_country = rng.zipf(1.5, size=v) - 1
    # Clip extreme values; replace anything >= cardinality with cardinality-1.
    country_idx = np.clip(raw_country, 0, COUNTRY_CARDINALITY - 1).astype(np.int32)

    vocab = _country_vocab()
    country_arr = pa.DictionaryArray.from_arrays(
        pa.array(country_idx, type=pa.int32()),
        pa.array(vocab, type=pa.string()),
    )

    schema = pa.schema(
        [
            pa.field("vid", pa.int64()),
            pa.field("birth_year", pa.int16()),
            pa.field("country", pa.dictionary(pa.int32(), pa.string())),
        ]
    )

    return pa.Table.from_arrays(
        [
            pa.array(vids, type=pa.int64()),
            pa.array(birth_year, type=pa.int16()),
            country_arr,
        ],
        schema=schema,
    )


def _generate_degrees(v: int, rng: np.random.Generator) -> np.ndarray:
    """Generate per-vertex out-degrees following a clipped/rescaled Zipf law."""
    raw = rng.zipf(ZIPF_ALPHA, size=v).astype(np.float64)
    # First clip to MAX_DEGREE.
    np.minimum(raw, MAX_DEGREE, out=raw)
    # Rescale so empirical mean ≈ TARGET_MEAN_DEGREE.
    current_mean = raw.mean()
    if current_mean > 0:
        scale = TARGET_MEAN_DEGREE / current_mean
        raw *= scale
    # Round to integers, clip again to MAX_DEGREE, and ensure non-negative.
    degrees = np.rint(raw).astype(np.int64)
    np.minimum(degrees, MAX_DEGREE, out=degrees)
    np.maximum(degrees, 0, out=degrees)
    return degrees


def _generate_knows(v: int, rng: np.random.Generator) -> pa.Table:
    """Build the knows table: (src, dst), sorted by (src, dst)."""
    degrees = _generate_degrees(v, rng)
    total_edges = int(degrees.sum())

    # src column: each vertex i appears degrees[i] times, already grouped.
    src = np.repeat(np.arange(v, dtype=np.int64), degrees)
    # dst column: uniform random in [0, v).
    dst = rng.integers(low=0, high=v, size=total_edges, dtype=np.int64)

    # Sort dst within each src group. Since src is already non-decreasing
    # (np.repeat over arange), a stable sort by dst followed by a stable
    # sort by src yields global (src, dst) order. numpy's default sort is
    # quicksort (not stable), but np.argsort with kind='stable' is mergesort.
    if total_edges > 0:
        order = np.argsort(dst, kind="stable")
        dst = dst[order]
        src_sorted_order = np.argsort(src[order], kind="stable")
        dst = dst[src_sorted_order]
        src = src[order][src_sorted_order]

    schema = pa.schema(
        [
            pa.field("src", pa.int64()),
            pa.field("dst", pa.int64()),
        ]
    )
    return pa.Table.from_arrays(
        [pa.array(src, type=pa.int64()), pa.array(dst, type=pa.int64())],
        schema=schema,
    )


# ---- Public API ------------------------------------------------------------


def generate_dataset(v: int = V, seed: int = SEED) -> tuple[pa.Table, pa.Table]:
    """Generate the (persons, knows) pair of PyArrow tables.

    persons is sorted by vid ascending; knows is sorted by (src, dst) ascending.
    """
    rng = np.random.default_rng(seed)
    # Persons first (consumes a deterministic prefix of the PRNG stream).
    persons = _generate_persons(v, rng)
    knows = _generate_knows(v, rng)

    # Print summary to stdout.
    deg_per_src = knows.column("src").to_numpy(zero_copy_only=False)
    if deg_per_src.size > 0:
        # Recompute degree distribution from sorted src for accuracy/reporting.
        unique_src, counts = np.unique(deg_per_src, return_counts=True)
        # Pad with zeros for vertices that produced no edges.
        full_deg = np.zeros(v, dtype=np.int64)
        full_deg[unique_src] = counts
    else:
        full_deg = np.zeros(v, dtype=np.int64)

    total_edges = int(full_deg.sum())
    mean_deg = float(full_deg.mean()) if v > 0 else 0.0
    max_deg = int(full_deg.max()) if v > 0 else 0
    p99_deg = int(np.percentile(full_deg, 99)) if v > 0 else 0
    # count_distinct doesn't support dictionary type in this pyarrow version,
    # so count distinct dictionary indices across all chunks.
    country_col = persons.column("country")
    seen_indices: set[int] = set()
    for chunk in country_col.chunks:
        idx = chunk.indices.to_numpy(zero_copy_only=False)
        seen_indices.update(np.unique(idx).tolist())
    distinct_countries = len(seen_indices)

    print(
        f"[data_gen] V={v:,} edges={total_edges:,} "
        f"mean_deg={mean_deg:.2f} max_deg={max_deg} p99_deg={p99_deg} "
        f"distinct_countries={distinct_countries}"
    )

    return persons, knows


def generate_lookup_vids(persons: pa.Table, n: int, seed: int = 1) -> list[int]:
    """Pick n vids that have ≥1 outgoing edge.

    NOTE: the spec says to sample with replacement from existing src vids in the
    knows table. Because this signature only exposes `persons`, callers that
    have a knows table should use the lower-level path. To preserve the public
    contract here we treat `persons` as a stand-in: if `persons` has a `src`
    column we use that, otherwise we re-derive deterministically. In practice
    `data_gen` is invoked by writers that pass the knows table as `persons`
    only when that's the intent; for the documented contract we accept any
    table that has a `src` column.

    For the standard (persons, knows) flow, downstream code calls this with
    the knows table.
    """
    if "src" in persons.schema.names:
        src_arr = persons.column("src")
    elif "vid" in persons.schema.names:
        # Fallback: just sample vids directly. This preserves determinism but
        # callers should normally pass the knows table.
        src_arr = persons.column("vid")
    else:
        raise ValueError("Table must contain a 'src' or 'vid' column")

    src_np = src_arr.to_numpy(zero_copy_only=False)
    unique_src = np.unique(src_np)
    if unique_src.size == 0:
        return []
    rng = np.random.default_rng(seed)
    picks = rng.choice(unique_src, size=n, replace=True)
    return [int(x) for x in picks]


# ---- Smoke test ------------------------------------------------------------


def _smoke_test() -> None:
    v_small = 50_000
    persons, knows = generate_dataset(v=v_small, seed=SEED)

    # persons checks.
    assert persons.num_rows == v_small, f"persons rows {persons.num_rows} != {v_small}"
    vids = persons.column("vid").to_numpy(zero_copy_only=False)
    assert np.array_equal(vids, np.arange(v_small)), "persons not sorted by vid"
    by = persons.column("birth_year").to_numpy(zero_copy_only=False)
    assert by.min() >= 1900 and by.max() <= 2010, "birth_year out of range"
    country_dict_size = len(persons.column("country").chunk(0).dictionary)
    assert country_dict_size <= COUNTRY_CARDINALITY, (
        f"country dict has {country_dict_size} entries"
    )

    # knows checks.
    assert knows.num_rows > 0, "knows is empty"
    src = knows.column("src").to_numpy(zero_copy_only=False)
    dst = knows.column("dst").to_numpy(zero_copy_only=False)
    # Sorted by (src, dst): src non-decreasing, and within each src run, dst non-decreasing.
    assert np.all(src[1:] >= src[:-1]), "src not sorted ascending"
    same_src = src[1:] == src[:-1]
    assert np.all(~same_src | (dst[1:] >= dst[:-1])), "dst not sorted within src groups"

    # generate_lookup_vids smoke.
    picks = generate_lookup_vids(knows, 100, seed=7)
    assert len(picks) == 100
    unique_src_set = set(np.unique(src).tolist())
    assert all(p in unique_src_set for p in picks), "lookup vids not in src set"

    print(
        f"[data_gen.smoke] OK: persons={persons.num_rows:,} knows={knows.num_rows:,} "
        f"country_dict_size={country_dict_size} lookup_sample={picks[:5]}"
    )


if __name__ == "__main__":
    _smoke_test()
