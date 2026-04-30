"""Reader implementations for the Graphberg benchmark.

Four readers, one per storage layout, all implementing :class:`BaseReader`:

* :class:`PlainParquetReader`  -- naive Parquet pair (vertices.parquet,
  edges.parquet) sorted by ``src``.
* :class:`IcebergReader`       -- pyiceberg-managed warehouse, opened directly
  via the underlying Parquet data files.
* :class:`GraphArReader`       -- GraphAr-style chunked layout with yml
  manifests.
* :class:`GraphbergReader`     -- proposed Graphberg layout: a single edges
  Parquet with embedded row-group hints in file-level key/value metadata
  plus a sidecar offset sub-index parquet.

The hot path :meth:`BaseReader.neighbors_of` MUST bypass
``pyarrow.dataset.to_table`` and any other dataset planner -- per v1.1 plan
section 3.4 those add ~10 ms of baseline overhead that swamps the
format-level signal we are measuring. Instead each reader uses
``pyarrow.parquet.ParquetFile`` with cached row-group statistics so each
neighbor lookup is a small number of array searches plus exactly one
``read_row_group`` call.

The W3 path :meth:`BaseReader.scan_persons_predicate` is intentionally
engine-aware -- it MAY use ``pyarrow.dataset`` or pyiceberg, since this
workload measures engine + format together rather than the pure format
overhead.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------


class BaseReader:
    """Common interface every reader implements."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = Path(data_root)

    def neighbors_of(self, src: int) -> List[int]:
        """Return ``dst`` vids for the given ``src``. Empty list if none.

        CRITICAL: must NOT use ``pyarrow.dataset.to_table`` or any other
        dataset-planner pathway -- use ``ParquetFile`` + ``read_row_group``
        + post-filter only.
        """
        raise NotImplementedError

    def scan_persons_predicate(self) -> int:
        """Return ``COUNT(*)`` of persons matching the W3 predicate
        ``birth_year > 1990 AND country = 'C001'``.

        Engine-aware paths (dataset / DuckDB / pyiceberg) are permitted
        here because the workload is measuring engine + format together.
        """
        raise NotImplementedError

    def close(self) -> None:
        """Release any cached file handles. Safe to call multiple times."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _column_index(schema: pa.Schema, name: str) -> int:
    """Return the column position for ``name`` in an Arrow / Parquet schema."""
    return schema.get_field_index(name)


def _collect_src_stats(
    pf: pq.ParquetFile, src_col_idx: int
) -> Tuple[np.ndarray, np.ndarray]:
    """Read per-row-group min/max for the ``src`` column.

    Returns two ``np.int64`` arrays of length ``num_row_groups``.
    """
    md = pf.metadata
    n = md.num_row_groups
    src_min = np.empty(n, dtype=np.int64)
    src_max = np.empty(n, dtype=np.int64)
    for i in range(n):
        stats = md.row_group(i).column(src_col_idx).statistics
        if stats is None or not stats.has_min_max:
            # Without stats we cannot prune -- mark as covering everything
            # so the searchsorted fallback still scans this row group.
            src_min[i] = np.iinfo(np.int64).min
            src_max[i] = np.iinfo(np.int64).max
        else:
            src_min[i] = int(stats.min)
            src_max[i] = int(stats.max)
    return src_min, src_max


def _filter_dst(table: pa.Table, src: int) -> List[int]:
    """Post-filter an Arrow table on ``src == target`` and return dst as a list."""
    mask = pc.equal(table["src"], pa.scalar(int(src), type=pa.int64()))
    filtered = table.filter(mask)
    return filtered.column("dst").to_pylist()


def _neighbors_from_pf(
    pf: pq.ParquetFile,
    src_min: np.ndarray,
    src_max: np.ndarray,
    src: int,
) -> List[int]:
    """Shared row-group-stats fast path used by every reader.

    Find the first row group whose ``max(src) >= src`` via searchsorted on
    the cached ``src_max`` array. If that row group's ``min(src) > src``
    the value cannot be present and we return an empty list without any
    IO. Otherwise read just the ``src`` and ``dst`` columns of that row
    group and post-filter.
    """
    if src_max.size == 0:
        return []
    rg = int(np.searchsorted(src_max, src, side="left"))
    if rg >= src_max.size:
        return []
    if src_min[rg] > src:
        return []
    table = pf.read_row_group(rg, columns=["src", "dst"])
    return _filter_dst(table, src)


# ---------------------------------------------------------------------------
# PlainParquetReader
# ---------------------------------------------------------------------------


class PlainParquetReader(BaseReader):
    """Reads the naive ``vertices.parquet`` + ``edges.parquet`` layout."""

    def __init__(self, data_root: Path) -> None:
        super().__init__(data_root)
        self._vertices_path = self.data_root / "vertices.parquet"
        self._edges_path = self.data_root / "edges.parquet"
        self._pf: Optional[pq.ParquetFile] = pq.ParquetFile(self._edges_path)
        src_idx = _column_index(self._pf.schema_arrow, "src")
        self._src_min, self._src_max = _collect_src_stats(self._pf, src_idx)

    def neighbors_of(self, src: int) -> List[int]:
        if self._pf is None:
            raise RuntimeError("PlainParquetReader is closed; reopen first")
        return _neighbors_from_pf(self._pf, self._src_min, self._src_max, src)

    def scan_persons_predicate(self) -> int:
        dataset = ds.dataset(str(self._vertices_path), format="parquet")
        table = dataset.to_table(
            filter=(ds.field("birth_year") > 1990)
            & (ds.field("country") == "C001"),
            columns=["vid"],
        )
        return table.num_rows

    def close(self) -> None:
        self._pf = None


# ---------------------------------------------------------------------------
# IcebergReader
# ---------------------------------------------------------------------------


class IcebergReader(BaseReader):
    """Reads an Iceberg warehouse via pyiceberg + direct Parquet IO.

    pyiceberg's ``Table.scan(...)`` performs internal planning analogous
    to ``dataset.to_table``; to stay consistent with the v1.1 section 3.4
    constraint we use the manifest only to discover data-file URIs and
    then open each underlying Parquet file directly.
    """

    def __init__(self, data_root: Path) -> None:
        super().__init__(data_root)
        # Lazy import so the module loads cleanly even if pyiceberg is
        # absent on a workstation that only needs the other readers.
        from pyiceberg.catalog.sql import SqlCatalog

        self._catalog = SqlCatalog(
            "default",
            uri=f"sqlite:///{self.data_root}/catalog.db",
            warehouse=f"file://{self.data_root}/warehouse",
        )
        self._knows_table = self._catalog.load_table("graphberg.KNOWS")
        self._person_table = self._catalog.load_table("graphberg.Person")

        # Use the manifest only to discover data files; do not let
        # pyiceberg drive the actual read.
        self._files: List[Tuple[pq.ParquetFile, np.ndarray, np.ndarray]] = []
        for task in self._knows_table.scan().plan_files():
            uri = task.file.file_path
            local = _iceberg_uri_to_path(uri)
            pf = pq.ParquetFile(local)
            src_idx = _column_index(pf.schema_arrow, "src")
            src_min, src_max = _collect_src_stats(pf, src_idx)
            self._files.append((pf, src_min, src_max))

    def neighbors_of(self, src: int) -> List[int]:
        if not self._files:
            return []
        results: List[int] = []
        for pf, src_min, src_max in self._files:
            if pf is None:
                raise RuntimeError("IcebergReader is closed; reopen first")
            partial = _neighbors_from_pf(pf, src_min, src_max, src)
            if partial:
                results.extend(partial)
        return results

    def scan_persons_predicate(self) -> int:
        scan = self._person_table.scan(
            row_filter="birth_year > 1990 AND country = 'C001'",
            selected_fields=("vid",),
        )
        return scan.to_arrow().num_rows

    def close(self) -> None:
        # Drop ParquetFile handles but keep min/max arrays so tests can
        # inspect the structure even after close. Reopening recreates
        # everything from scratch in __init__.
        self._files = []


def _iceberg_uri_to_path(uri: str) -> str:
    """Convert a ``file://`` URI from an Iceberg manifest into a local path."""
    if uri.startswith("file://"):
        return uri[len("file://") :]
    return uri


# ---------------------------------------------------------------------------
# GraphArReader
# ---------------------------------------------------------------------------


class GraphArReader(BaseReader):
    """Reads a GraphAr-style chunked layout.

    Edges are partitioned into fixed-size chunks of ``chunk_size`` source
    vertices: chunk *i* covers ``src in [i*chunk_size, (i+1)*chunk_size)``.
    The chunk a given ``src`` belongs to is therefore
    ``src // chunk_size`` -- O(1) chunk lookup, then row-group pruning
    inside the chunk.
    """

    def __init__(self, data_root: Path) -> None:
        super().__init__(data_root)
        # Lazy import to keep PyYAML out of the hard dependency surface
        # for users who only run the other readers.
        import yaml

        with (self.data_root / "Person.vertex.yml").open("r") as fh:
            person_yml = yaml.safe_load(fh)
        with (self.data_root / "KNOWS.edge.yml").open("r") as fh:
            edge_yml = yaml.safe_load(fh)

        self._person_yml = person_yml
        self._edge_yml = edge_yml
        self._chunk_size = int(edge_yml["chunk_size"])

        edge_dir = self.data_root / "KNOWS"
        # Sort lexicographically so chunk-NNNN files are in vid order.
        chunk_paths = sorted(edge_dir.glob("chunk-*.parquet"))
        self._edge_chunk_paths: List[Path] = chunk_paths

        self._edge_chunks: List[
            Optional[Tuple[pq.ParquetFile, np.ndarray, np.ndarray]]
        ] = []
        for path in chunk_paths:
            pf = pq.ParquetFile(path)
            src_idx = _column_index(pf.schema_arrow, "src")
            src_min, src_max = _collect_src_stats(pf, src_idx)
            self._edge_chunks.append((pf, src_min, src_max))

        self._person_dir = str(self.data_root / "Person")

    def neighbors_of(self, src: int) -> List[int]:
        if not self._edge_chunks:
            return []
        chunk_idx = src // self._chunk_size
        if chunk_idx < 0 or chunk_idx >= len(self._edge_chunks):
            return []
        entry = self._edge_chunks[chunk_idx]
        if entry is None:
            raise RuntimeError("GraphArReader is closed; reopen first")
        pf, src_min, src_max = entry
        return _neighbors_from_pf(pf, src_min, src_max, src)

    def scan_persons_predicate(self) -> int:
        dataset = ds.dataset(self._person_dir, format="parquet")
        table = dataset.to_table(
            filter=(ds.field("birth_year") > 1990)
            & (ds.field("country") == "C001"),
            columns=["vid"],
        )
        return table.num_rows

    def close(self) -> None:
        self._edge_chunks = [None for _ in self._edge_chunks]


# ---------------------------------------------------------------------------
# GraphbergReader
# ---------------------------------------------------------------------------


class GraphbergReader(BaseReader):
    """Reads the proposed Graphberg layout.

    The main ``edges.parquet`` carries per-row-group hints in the
    file-level key/value metadata under ``graphberg.v1.row_groups``. A
    sidecar ``edges.parquet.aux.parquet`` holds an explicit offset
    sub-index (vid -> row_group) for O(log V) hot lookups.
    """

    _ROW_GROUPS_KEY = b"graphberg.v1.row_groups"
    _AUX_KIND_KEY = b"graphberg.aux.kind"
    _AUX_KIND_VALUE = b"offset_index"

    def __init__(self, data_root: Path) -> None:
        super().__init__(data_root)
        self._edges_path = self.data_root / "edges.parquet"
        self._aux_path = self.data_root / "edges.parquet.aux.parquet"
        self._vertices_path = self.data_root / "vertices.parquet"

        self._pf: Optional[pq.ParquetFile] = pq.ParquetFile(self._edges_path)

        # Pull row-group hints out of file-level KV metadata first; fall
        # back to actual statistics if for some reason the hint is
        # missing (e.g. an older writer).
        kv = self._pf.metadata.metadata or {}
        if self._ROW_GROUPS_KEY in kv:
            payload = json.loads(kv[self._ROW_GROUPS_KEY].decode("utf-8"))
            # Writer emits {"version": 1, "row_groups": [...]}; tolerate a
            # bare list for forward/backward compatibility.
            entries = payload["row_groups"] if isinstance(payload, dict) else payload
            n = len(entries)
            src_min = np.empty(n, dtype=np.int64)
            src_max = np.empty(n, dtype=np.int64)
            for i, entry in enumerate(entries):
                src_min[i] = int(entry["src_min"])
                src_max[i] = int(entry["src_max"])
            self._src_min = src_min
            self._src_max = src_max
        else:
            src_idx = _column_index(self._pf.schema_arrow, "src")
            self._src_min, self._src_max = _collect_src_stats(
                self._pf, src_idx
            )

        # Load and validate the offset sub-index.
        aux_pf = pq.ParquetFile(self._aux_path)
        aux_kv = aux_pf.metadata.metadata or {}
        if aux_kv.get(self._AUX_KIND_KEY) != self._AUX_KIND_VALUE:
            raise ValueError(
                f"{self._aux_path} is missing graphberg.aux.kind=offset_index"
            )
        aux_table = aux_pf.read(columns=["vid", "byte_offset", "row_group"])
        self._aux_vid = aux_table.column("vid").to_numpy(zero_copy_only=False).astype(
            np.int64, copy=False
        )
        self._aux_byte_offset = aux_table.column("byte_offset").to_numpy(
            zero_copy_only=False
        ).astype(np.int64, copy=False)
        self._aux_row_group = aux_table.column("row_group").to_numpy(
            zero_copy_only=False
        ).astype(np.int32, copy=False)

    def neighbors_of(self, src: int) -> List[int]:
        if self._pf is None:
            raise RuntimeError("GraphbergReader is closed; reopen first")

        # Fast path: exact vid match in the sub-index gives us the row
        # group with no scan of footer hints.
        rg: Optional[int] = None
        if self._aux_vid.size:
            pos = int(np.searchsorted(self._aux_vid, src))
            if pos < self._aux_vid.size and self._aux_vid[pos] == src:
                rg = int(self._aux_row_group[pos])

        if rg is None:
            if self._src_max.size == 0:
                return []
            rg_candidate = int(np.searchsorted(self._src_max, src, side="left"))
            if rg_candidate >= self._src_max.size:
                return []
            if self._src_min[rg_candidate] > src:
                return []
            rg = rg_candidate

        table = self._pf.read_row_group(rg, columns=["src", "dst"])
        return _filter_dst(table, src)

    def scan_persons_predicate(self) -> int:
        dataset = ds.dataset(str(self._vertices_path), format="parquet")
        table = dataset.to_table(
            filter=(ds.field("birth_year") > 1990)
            & (ds.field("country") == "C001"),
            columns=["vid"],
        )
        return table.num_rows

    def close(self) -> None:
        self._pf = None


__all__ = [
    "BaseReader",
    "PlainParquetReader",
    "IcebergReader",
    "GraphArReader",
    "GraphbergReader",
]
