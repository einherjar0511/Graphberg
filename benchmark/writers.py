"""Writer implementations for the Graphberg benchmark.

Four writer functions, one per storage layout, that take in-memory
PyArrow tables and persist them under
``/home/user/Graphberg/benchmark/data/<layout>/`` (or any caller-chosen
``out_dir``).

All writers share:
    * snappy compression
    * page index enabled
    * a target row-group size of 4 MB (rows-per-rg is computed from a
      one-row-group calibration write of a small slice, mirroring
      ``rowgroup_experiment/run_experiment.py::calibrate_bytes_per_row``).

Public API:
    write_plain_parquet(persons, knows, out_dir) -> dict
    write_iceberg(persons, knows, out_dir)       -> dict
    write_graphar(persons, knows, out_dir,
                  chunk_size=1<<20)              -> dict
    write_graphberg(persons, knows, out_dir)     -> dict

Each function returns
    {"files": [Path, ...],
     "total_bytes": int,
     "num_row_groups": dict[str, int]}
for verification.
"""

from __future__ import annotations

import json
import math
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

TARGET_RG_BYTES = 4 * 1024 * 1024   # 4 MiB target compressed row-group size


# ---------------------------------------------------------------------------
# Calibration helper
# ---------------------------------------------------------------------------


def _bytes_per_row(table: pa.Table) -> float:
    """Write a small slice as a single row-group and measure bytes/row.

    Mirrors ``calibrate_bytes_per_row`` from
    ``rowgroup_experiment/run_experiment.py``: takes up to 500_000 rows,
    writes them with ``compression="snappy"`` and ``write_page_index=True``,
    then divides file size by row count.
    """
    n = min(500_000, table.num_rows)
    if n <= 0:
        # Empty table: pick a benign default so callers don't divide by 0.
        return 32.0
    sample = table.slice(0, n)
    p = Path("/tmp/_calib.parquet")
    pq.write_table(
        sample,
        p,
        compression="snappy",
        write_page_index=True,
        row_group_size=n,
    )
    bpr = p.stat().st_size / n
    p.unlink()
    return bpr


def _rows_per_rg(table: pa.Table, target_bytes: int = TARGET_RG_BYTES) -> int:
    """Convert ``target_bytes`` -> rows-per-row-group via calibration."""
    bpr = _bytes_per_row(table)
    rpr = max(1, int(target_bytes / bpr))
    return rpr


def _row_group_count(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_row_groups


def _file_size(path: Path) -> int:
    return path.stat().st_size


# ---------------------------------------------------------------------------
# write_plain_parquet
# ---------------------------------------------------------------------------


def write_plain_parquet(
    persons: pa.Table, knows: pa.Table, out_dir: Path
) -> dict:
    """Naive layout: one Parquet per logical table.

    Files:
        out_dir/vertices.parquet
        out_dir/edges.parquet
    """
    out_dir = Path(out_dir)
    vertices_path = out_dir / "vertices.parquet"
    edges_path = out_dir / "edges.parquet"

    v_rpr = _rows_per_rg(persons)
    e_rpr = _rows_per_rg(knows)

    pq.write_table(
        persons,
        vertices_path,
        compression="snappy",
        write_page_index=True,
        row_group_size=v_rpr,
    )
    pq.write_table(
        knows,
        edges_path,
        compression="snappy",
        write_page_index=True,
        row_group_size=e_rpr,
    )

    files = [vertices_path, edges_path]
    return {
        "files": files,
        "total_bytes": sum(_file_size(p) for p in files),
        "num_row_groups": {
            "vertices.parquet": _row_group_count(vertices_path),
            "edges.parquet": _row_group_count(edges_path),
        },
    }


# ---------------------------------------------------------------------------
# write_iceberg
# ---------------------------------------------------------------------------


def write_iceberg(
    persons: pa.Table, knows: pa.Table, out_dir: Path
) -> dict:
    """Iceberg layout via pyiceberg's ``SqlCatalog`` (sqlite-backed).

    Catalog DB lives at ``out_dir/catalog.db``; warehouse at
    ``out_dir/warehouse``. Tables ``graphberg.Person`` and
    ``graphberg.KNOWS`` are created and appended to. KNOWS is appended
    in 10M-row chunks to keep peak memory bounded.
    """
    # Lazy import to keep top-of-file clean and allow other writers to
    # be used without pyiceberg installed.
    from pyiceberg.catalog.sql import SqlCatalog

    out_dir = Path(out_dir)
    warehouse_dir = out_dir / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    catalog_db = out_dir / "catalog.db"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": f"file://{warehouse_dir}",
        },
    )
    catalog.create_namespace_if_not_exists("graphberg")

    # ---- Person --------------------------------------------------------
    person_table = catalog.create_table(
        "graphberg.Person", schema=persons.schema
    )
    person_table.append(persons)

    # ---- KNOWS ---------------------------------------------------------
    knows_table = catalog.create_table(
        "graphberg.KNOWS", schema=knows.schema
    )
    chunk_rows = 10_000_000
    n = knows.num_rows
    if n == 0:
        knows_table.append(knows)
    else:
        for start in range(0, n, chunk_rows):
            length = min(chunk_rows, n - start)
            knows_table.append(knows.slice(start, length))

    # ---- Collect resulting files for verification ---------------------
    files: list[Path] = [catalog_db]
    num_row_groups: dict[str, int] = {}

    # Walk the warehouse and pick up every parquet data file. We use
    # pyiceberg's manifest to be precise rather than globbing.
    for label, tbl in (
        ("Person", person_table),
        ("KNOWS", knows_table),
    ):
        # plan_files() resolves the current snapshot's data files.
        for task in tbl.scan().plan_files():
            uri = task.file.file_path
            local = uri[len("file://"):] if uri.startswith("file://") else uri
            p = Path(local)
            files.append(p)
            num_row_groups[f"{label}:{p.name}"] = _row_group_count(p)

    total_bytes = 0
    for p in files:
        try:
            total_bytes += _file_size(p)
        except FileNotFoundError:
            # The catalog db is always present, but be defensive.
            pass

    return {
        "files": files,
        "total_bytes": total_bytes,
        "num_row_groups": num_row_groups,
    }


# ---------------------------------------------------------------------------
# write_graphar
# ---------------------------------------------------------------------------


def _graphar_field_descriptor(field: pa.Field) -> dict:
    """Turn a pa.Field into a {name, type} dict for the yml manifest."""
    t = field.type
    if pa.types.is_dictionary(t):
        type_str = f"dictionary<{t.index_type}, {t.value_type}>"
    else:
        type_str = str(t)
    return {"name": field.name, "type": type_str}


def write_graphar(
    persons: pa.Table,
    knows: pa.Table,
    out_dir: Path,
    chunk_size: int = 1 << 20,
) -> dict:
    """GraphAr-style chunked layout.

    Files:
        out_dir/Person.vertex.yml
        out_dir/Person/chunk-{i:04d}.parquet      (chunk_size rows each)
        out_dir/KNOWS.edge.yml
        out_dir/KNOWS/chunk-{i:04d}.parquet
            (chunk i contains edges where src // chunk_size == i)
    """
    out_dir = Path(out_dir)
    person_dir = out_dir / "Person"
    edge_dir = out_dir / "KNOWS"
    person_dir.mkdir(parents=True, exist_ok=True)
    edge_dir.mkdir(parents=True, exist_ok=True)

    files: list[Path] = []
    num_row_groups: dict[str, int] = {}

    # Calibrate once on each table; per-chunk slices reuse the same
    # rows-per-rg figure since cardinality is uniform across chunks.
    v_rpr = _rows_per_rg(persons)
    e_rpr = _rows_per_rg(knows) if knows.num_rows else 1

    # ---- Vertex chunks ------------------------------------------------
    v = persons.num_rows
    n_v_chunks = max(1, math.ceil(v / chunk_size)) if v else 0
    for i in range(n_v_chunks):
        start = i * chunk_size
        length = min(chunk_size, v - start)
        chunk = persons.slice(start, length)
        path = person_dir / f"chunk-{i:04d}.parquet"
        pq.write_table(
            chunk,
            path,
            compression="snappy",
            write_page_index=True,
            row_group_size=v_rpr,
        )
        files.append(path)
        num_row_groups[f"Person/{path.name}"] = _row_group_count(path)

    # ---- Edge chunks --------------------------------------------------
    # For each chunk i, select knows rows where src // chunk_size == i.
    # We exploit the fact that knows is sorted by src so each chunk is a
    # contiguous slice. We find slice boundaries with a single linear
    # pass over the sorted src column.
    n_e_chunks = n_v_chunks  # one bucket per src bucket
    if knows.num_rows:
        import numpy as np

        src_np = knows.column("src").to_numpy(zero_copy_only=False)
        # bucket index for each row; sorted because src is sorted.
        bucket = (src_np // chunk_size).astype(np.int64)
        # Find boundaries via searchsorted on the bucket array.
        # bucket is non-decreasing.
        boundaries = [0] * (n_e_chunks + 1)
        # start of bucket i = first index where bucket >= i
        for i in range(n_e_chunks + 1):
            boundaries[i] = int(np.searchsorted(bucket, i, side="left"))
        for i in range(n_e_chunks):
            start = boundaries[i]
            end = boundaries[i + 1]
            length = end - start
            chunk = knows.slice(start, length)
            path = edge_dir / f"chunk-{i:04d}.parquet"
            pq.write_table(
                chunk,
                path,
                compression="snappy",
                write_page_index=True,
                row_group_size=e_rpr,
            )
            files.append(path)
            num_row_groups[f"KNOWS/{path.name}"] = _row_group_count(path)
    else:
        # No edges: emit an empty chunk so readers find at least one file.
        path = edge_dir / "chunk-0000.parquet"
        pq.write_table(
            knows,
            path,
            compression="snappy",
            write_page_index=True,
            row_group_size=e_rpr,
        )
        files.append(path)
        num_row_groups[f"KNOWS/{path.name}"] = _row_group_count(path)

    # ---- yml manifests ------------------------------------------------
    person_yml = {
        "name": "Person",
        "prefix": "Person/",
        "chunk_size": chunk_size,
        "num_chunks": n_v_chunks,
        "schema": [
            _graphar_field_descriptor(persons.schema.field(i))
            for i in range(len(persons.schema))
        ],
    }
    person_yml_path = out_dir / "Person.vertex.yml"
    with person_yml_path.open("w") as fh:
        yaml.safe_dump(person_yml, fh, sort_keys=False)
    files.append(person_yml_path)

    edge_yml = {
        "name": "KNOWS",
        "prefix": "KNOWS/",
        "src_label": "Person",
        "dst_label": "Person",
        "chunk_size": chunk_size,
        "num_chunks": n_e_chunks if knows.num_rows else 1,
        "schema": [
            _graphar_field_descriptor(knows.schema.field(i))
            for i in range(len(knows.schema))
        ],
    }
    edge_yml_path = out_dir / "KNOWS.edge.yml"
    with edge_yml_path.open("w") as fh:
        yaml.safe_dump(edge_yml, fh, sort_keys=False)
    files.append(edge_yml_path)

    total_bytes = sum(_file_size(p) for p in files)
    return {
        "files": files,
        "total_bytes": total_bytes,
        "num_row_groups": num_row_groups,
    }


# ---------------------------------------------------------------------------
# write_graphberg
# ---------------------------------------------------------------------------


def write_graphberg(
    persons: pa.Table, knows: pa.Table, out_dir: Path
) -> dict:
    """Graphberg layout: vertices.parquet + edges.parquet (with footer
    hints) + edges.parquet.aux.parquet (offset sub-index).

    Footer hints are embedded by writing edges.parquet TWICE:
        1) write once with the calibrated row-group size to discover
           the actual row-group min/max boundaries from Parquet stats;
        2) build the JSON hint payload from those stats and rewrite the
           same file with the hint baked into the Arrow schema's
           ``key_value_metadata``.
    The aux file is then derived from the second-write metadata so the
    ``data_page_offset`` it stores matches the on-disk file the readers
    will open.
    """
    out_dir = Path(out_dir)
    vertices_path = out_dir / "vertices.parquet"
    edges_path = out_dir / "edges.parquet"
    aux_path = out_dir / "edges.parquet.aux.parquet"

    # ---- Vertices -----------------------------------------------------
    v_rpr = _rows_per_rg(persons)
    pq.write_table(
        persons,
        vertices_path,
        compression="snappy",
        write_page_index=True,
        row_group_size=v_rpr,
    )

    # ---- Edges: first pass to discover row-group boundaries ----------
    e_rpr = _rows_per_rg(knows) if knows.num_rows else 1
    pq.write_table(
        knows,
        edges_path,
        compression="snappy",
        write_page_index=True,
        row_group_size=e_rpr,
    )

    pf = pq.ParquetFile(edges_path)
    md = pf.metadata
    src_col_idx = pf.schema_arrow.get_field_index("src")
    rg_entries = []
    for rg in range(md.num_row_groups):
        stats = md.row_group(rg).column(src_col_idx).statistics
        if stats is not None and stats.has_min_max:
            src_min = int(stats.min)
            src_max = int(stats.max)
        else:
            src_min = -1
            src_max = -1
        rg_entries.append(
            {"row_group": rg, "src_min": src_min, "src_max": src_max}
        )
    hint_payload = {"version": 1, "row_groups": rg_entries}

    # ---- Edges: second pass with footer hints embedded ---------------
    enriched_metadata = {
        b"graphberg.v1.row_groups": json.dumps(hint_payload).encode("utf-8"),
    }
    enriched_schema = knows.schema.with_metadata(enriched_metadata)
    knows_with_md = knows.replace_schema_metadata(enriched_metadata)
    pq.write_table(
        knows_with_md,
        edges_path,
        compression="snappy",
        write_page_index=True,
        row_group_size=e_rpr,
    )
    # Sanity: schemas should have round-tripped.
    _ = enriched_schema  # silence unused-var lint without runtime cost

    # ---- Aux offset sub-index -----------------------------------------
    # Build {vid, byte_offset, row_group} with one row per distinct src
    # vid. byte_offset is the data_page_offset of the src column for
    # the row group containing the FIRST occurrence of that vid.
    pf = pq.ParquetFile(edges_path)  # re-open after rewrite
    md = pf.metadata
    src_col_idx = pf.schema_arrow.get_field_index("src")

    aux_vids: list[int] = []
    aux_byte_offsets: list[int] = []
    aux_row_groups: list[int] = []
    seen_vids: set[int] = set()

    if knows.num_rows:
        import numpy as np

        for rg in range(md.num_row_groups):
            data_page_offset = md.row_group(rg).column(src_col_idx).data_page_offset
            tbl = pf.read_row_group(rg, columns=["src"])
            src_np = tbl.column("src").to_numpy(zero_copy_only=False)
            # Distinct src vids in the row-group, preserving order; we
            # only care about each vid's first appearance globally.
            uniq = np.unique(src_np)
            for vid in uniq:
                v_int = int(vid)
                if v_int in seen_vids:
                    continue
                seen_vids.add(v_int)
                aux_vids.append(v_int)
                aux_byte_offsets.append(int(data_page_offset))
                aux_row_groups.append(int(rg))

    aux_table = pa.table(
        {
            "vid": pa.array(aux_vids, type=pa.int64()),
            "byte_offset": pa.array(aux_byte_offsets, type=pa.int64()),
            "row_group": pa.array(aux_row_groups, type=pa.int32()),
        }
    )
    aux_metadata = {
        b"graphberg.aux.kind": b"offset_index",
        b"graphberg.aux.target": b"edges.parquet",
    }
    aux_table = aux_table.replace_schema_metadata(aux_metadata)
    pq.write_table(
        aux_table,
        aux_path,
        compression="snappy",
        write_page_index=True,
    )

    files = [vertices_path, edges_path, aux_path]
    return {
        "files": files,
        "total_bytes": sum(_file_size(p) for p in files),
        "num_row_groups": {
            "vertices.parquet": _row_group_count(vertices_path),
            "edges.parquet": _row_group_count(edges_path),
            "edges.parquet.aux.parquet": _row_group_count(aux_path),
        },
    }


__all__ = [
    "write_plain_parquet",
    "write_iceberg",
    "write_graphar",
    "write_graphberg",
]


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------


def _smoke_test() -> None:
    """End-to-end: generate a small dataset and run all four writers."""
    import sys

    # Make data_gen importable when running this file directly.
    here = Path(__file__).resolve().parent
    sys.path.insert(0, str(here))
    from data_gen import generate_dataset  # type: ignore

    persons, knows = generate_dataset(v=50_000, seed=20260430)
    print(
        f"[writers.smoke] persons={persons.num_rows:,} "
        f"knows={knows.num_rows:,}"
    )

    smoke_root = Path("/tmp/graphberg_writers_smoke")
    if smoke_root.exists():
        shutil.rmtree(smoke_root)
    smoke_root.mkdir(parents=True)

    layouts = {
        "plain_parquet": write_plain_parquet,
        "iceberg": write_iceberg,
        "graphar": write_graphar,
        "graphberg": write_graphberg,
    }
    for name, fn in layouts.items():
        sub = smoke_root / name
        sub.mkdir(parents=True, exist_ok=True)
        result = fn(persons, knows, sub)
        assert result["files"], f"{name}: no files produced"
        for p in result["files"]:
            assert Path(p).exists(), f"{name}: missing file {p}"
        print(
            f"[writers.smoke][{name}] files={len(result['files'])} "
            f"total_bytes={result['total_bytes']:,} "
            f"row_groups={result['num_row_groups']}"
        )

        # Read one parquet file back and print row count + first 3 rows.
        first_pq: Path | None = None
        for p in result["files"]:
            sp = str(p)
            if sp.endswith(".parquet"):
                first_pq = Path(p)
                break
        if first_pq is not None:
            tbl = pq.read_table(first_pq)
            head = tbl.slice(0, min(3, tbl.num_rows)).to_pylist()
            print(
                f"[writers.smoke][{name}] read {first_pq.name}: "
                f"rows={tbl.num_rows:,} first3={head}"
            )

    # Clean up.
    shutil.rmtree(smoke_root)
    print("[writers.smoke] OK")


if __name__ == "__main__":
    _smoke_test()
