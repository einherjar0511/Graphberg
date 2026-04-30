"""Apply v1.1 sec 4.1 success thresholds and sec 4.2 kill criteria to
``results/summary.json``. Persists ``results/verdict.json``.

Public function:

    evaluate(summary: dict) -> {
        "threshold_table": [...],
        "kill_criteria":   [...],
        "verdict": "PROCEED" | "KILL" | "GRAY-ZONE",
        "rationale": "...",
    }

The threshold table comparisons (sec 4.1):

    | Workload | Comparison        | Metric | Threshold        | Stretch |
    |----------|-------------------|--------|------------------|---------|
    | W1       | vs GraphAr        | P50    | within 3x        | 1.5x    |
    | W1       | vs GraphAr        | P99    | within 5x        | -       |
    | W1       | vs plain Iceberg  | P50    | >=50x faster     | >=100x  |
    | W2       | vs GraphAr        | P50    | within 4x        | 2x      |
    | W2       | vs plain Iceberg  | P50    | >=20x faster     | >=50x   |
    | W3       | vs plain Iceberg  | P50    | within 15%       | 5%      |
    | W3       | vs plain Parquet  | P50    | within 25%       | 10%     |

Kill criteria (sec 4.2):

    1. W1 graphberg P50 > 10x GraphAr's P50
    2. W1 graphberg P50 > 10x plain Iceberg's P50
    3. W3 graphberg P50 > 2x plain Iceberg's P50  (i.e. <50% of Iceberg's speed)
    4. W1 graphberg cold latency > 200_000 us
    5. W1 graphberg bytes_read per op > 10x adjacency-list bytes
       (adjacency-list bytes = mean_neighbors * 16, two int64 per edge)

Verdict policy:

    * ANY kill criterion triggered -> "KILL"
    * else, ALL sec 4.1 comparisons PASS or STRETCH -> "PROCEED"
    * else -> "GRAY-ZONE"
"""

from __future__ import annotations

import json
from pathlib import Path


# ---------------------------------------------------------------------------
# Threshold table (sec 4.1)
# ---------------------------------------------------------------------------
# Each entry:
#   workload:    "W1" | "W2" | "W3"
#   other:       "graphar" | "iceberg" | "plain_parquet"
#   comparison:  human-readable label that appears in the output
#   metric:      "P50" | "P99"
#   kind:        "within_x"   -> graphberg / other <= threshold
#                "faster_x"   -> other / graphberg >= threshold
#                "within_pct" -> |graphberg - other| / other <= threshold/100
#   threshold:   numeric, must satisfy
#   stretch:     numeric or None, optional bonus tier
THRESHOLDS = [
    {"workload": "W1", "other": "graphar",       "comparison": "vs GraphAr",
     "metric": "P50", "kind": "within_x",   "threshold": 3.0,   "stretch": 1.5},
    {"workload": "W1", "other": "graphar",       "comparison": "vs GraphAr",
     "metric": "P99", "kind": "within_x",   "threshold": 5.0,   "stretch": None},
    {"workload": "W1", "other": "iceberg",       "comparison": "vs plain Iceberg",
     "metric": "P50", "kind": "faster_x",   "threshold": 50.0,  "stretch": 100.0},
    {"workload": "W2", "other": "graphar",       "comparison": "vs GraphAr",
     "metric": "P50", "kind": "within_x",   "threshold": 4.0,   "stretch": 2.0},
    {"workload": "W2", "other": "iceberg",       "comparison": "vs plain Iceberg",
     "metric": "P50", "kind": "faster_x",   "threshold": 20.0,  "stretch": 50.0},
    {"workload": "W3", "other": "iceberg",       "comparison": "vs plain Iceberg",
     "metric": "P50", "kind": "within_pct", "threshold": 15.0,  "stretch": 5.0},
    {"workload": "W3", "other": "plain_parquet", "comparison": "vs plain Parquet",
     "metric": "P50", "kind": "within_pct", "threshold": 25.0,  "stretch": 10.0},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cell(summary: dict, system: str, workload: str) -> dict:
    """Return the per-cell dict (or {})."""
    sys_d = summary.get(system) or {}
    return sys_d.get(workload) or {}


def _get_metric(summary: dict, system: str, workload: str, metric: str):
    """Return the per-cell metric value, tolerating several key flavours."""
    cell = _cell(summary, system, workload)
    # Canonical key produced by run_benchmark._summary_for_persistence.
    if metric in cell:
        return cell[metric]
    # Fallbacks in case a different harness wrote this summary file.
    fallback_keys = {
        "P50": ("p50_us", "p50", "latency_p50_us"),
        "P99": ("p99_us", "p99", "latency_p99_us"),
    }.get(metric, ())
    for k in fallback_keys:
        if k in cell:
            return cell[k]
    # Nested under "summary" or "latency"?
    for sub in ("summary", "latency"):
        s = cell.get(sub)
        if isinstance(s, dict):
            for k in (metric, *fallback_keys):
                if k in s:
                    return s[k]
    return None


def _get_extra(summary: dict, system: str, workload: str, *keys: str):
    """Return the first present per-cell key from ``keys``, or None."""
    cell = _cell(summary, system, workload)
    for k in keys:
        if k in cell and cell[k] is not None:
            return cell[k]
    return None


def _evaluate_row(row: dict, summary: dict) -> dict:
    """Evaluate one row of the threshold table."""
    workload = row["workload"]
    other = row["other"]
    metric = row["metric"]
    kind = row["kind"]
    threshold = row["threshold"]
    stretch = row["stretch"]

    gb_val = _get_metric(summary, "graphberg", workload, metric)
    other_val = _get_metric(summary, other, workload, metric)

    out = {
        "workload": workload,
        "comparison": row["comparison"],
        "metric": metric,
        "ratio": None,
        "threshold": threshold,
        "stretch": stretch,
        "result": "MISSING",
    }

    if gb_val is None or other_val is None:
        out["note"] = (
            f"missing values: graphberg={gb_val} {other}={other_val}"
        )
        return out

    if gb_val <= 0 or other_val <= 0:
        out["note"] = "non-positive metric value"
        return out

    if kind == "within_x":
        # Within Nx: graphberg_p50 / other_p50 <= N
        ratio = gb_val / other_val
        out["ratio"] = ratio
        if ratio <= threshold:
            out["result"] = (
                "STRETCH" if (stretch is not None and ratio <= stretch) else "PASS"
            )
        else:
            out["result"] = "FAIL"

    elif kind == "faster_x":
        # >=Nx faster: other_p50 / graphberg_p50 >= N
        ratio = other_val / gb_val
        out["ratio"] = ratio
        if ratio >= threshold:
            out["result"] = (
                "STRETCH" if (stretch is not None and ratio >= stretch) else "PASS"
            )
        else:
            out["result"] = "FAIL"

    elif kind == "within_pct":
        # Within X%: |graphberg_p50 - other_p50| / other_p50 <= X/100
        delta_pct = abs(gb_val - other_val) / other_val * 100.0
        out["ratio"] = delta_pct
        if delta_pct <= threshold:
            out["result"] = (
                "STRETCH" if (stretch is not None and delta_pct <= stretch) else "PASS"
            )
        else:
            out["result"] = "FAIL"

    else:
        out["note"] = f"unknown kind {kind}"

    return out


# ---------------------------------------------------------------------------
# Kill criteria (sec 4.2)
# ---------------------------------------------------------------------------

def _evaluate_kill_criteria(summary: dict) -> list:
    out: list = []

    gb_w1_p50 = _get_metric(summary, "graphberg", "W1", "P50")
    ga_w1_p50 = _get_metric(summary, "graphar",   "W1", "P50")
    ic_w1_p50 = _get_metric(summary, "iceberg",   "W1", "P50")
    gb_w3_p50 = _get_metric(summary, "graphberg", "W3", "P50")
    ic_w3_p50 = _get_metric(summary, "iceberg",   "W3", "P50")

    gb_w1_cold = _get_extra(summary, "graphberg", "W1", "cold_us", "cold_latency_us")
    gb_w1_bytes = _get_extra(
        summary, "graphberg", "W1", "bytes_read_per_op", "bytes_per_op"
    )
    mean_neighbors = _get_extra(
        summary, "graphberg", "W1", "mean_neighbors"
    )
    if mean_neighbors is None:
        # Default for the canonical V=5M / E~=160M dataset (~32 mean degree).
        mean_neighbors = 32.0

    # 1. W1 graphberg P50 > 10x GraphAr P50
    if gb_w1_p50 is not None and ga_w1_p50 is not None and ga_w1_p50 > 0:
        ratio = gb_w1_p50 / ga_w1_p50
        out.append({
            "name": "W1 graphberg P50 > 10x GraphAr",
            "triggered": ratio > 10.0,
            "actual": ratio,
            "threshold": 10.0,
        })
    else:
        out.append({
            "name": "W1 graphberg P50 > 10x GraphAr",
            "triggered": False,
            "actual": None,
            "threshold": 10.0,
            "note": "missing data",
        })

    # 2. W1 graphberg P50 > 10x plain Iceberg P50
    if gb_w1_p50 is not None and ic_w1_p50 is not None and ic_w1_p50 > 0:
        ratio = gb_w1_p50 / ic_w1_p50
        out.append({
            "name": "W1 graphberg P50 > 10x plain Iceberg",
            "triggered": ratio > 10.0,
            "actual": ratio,
            "threshold": 10.0,
        })
    else:
        out.append({
            "name": "W1 graphberg P50 > 10x plain Iceberg",
            "triggered": False,
            "actual": None,
            "threshold": 10.0,
            "note": "missing data",
        })

    # 3. W3 graphberg P50 > 2x plain Iceberg P50
    if gb_w3_p50 is not None and ic_w3_p50 is not None and ic_w3_p50 > 0:
        ratio = gb_w3_p50 / ic_w3_p50
        out.append({
            "name": "W3 graphberg P50 > 2x plain Iceberg",
            "triggered": ratio > 2.0,
            "actual": ratio,
            "threshold": 2.0,
        })
    else:
        out.append({
            "name": "W3 graphberg P50 > 2x plain Iceberg",
            "triggered": False,
            "actual": None,
            "threshold": 2.0,
            "note": "missing data",
        })

    # 4. W1 graphberg cold latency > 200_000 us
    out.append({
        "name": "W1 graphberg cold latency > 200_000 us",
        "triggered": (gb_w1_cold is not None and gb_w1_cold > 200_000),
        "actual": gb_w1_cold,
        "threshold": 200_000,
    })

    # 5. W1 graphberg bytes_read per op > 10x adjacency-list bytes
    adjacency_bytes = float(mean_neighbors) * 16.0  # two int64 per edge
    if gb_w1_bytes is not None and adjacency_bytes > 0:
        ratio = gb_w1_bytes / adjacency_bytes
        out.append({
            "name": "W1 graphberg bytes_read/op > 10x adjacency-list bytes",
            "triggered": ratio > 10.0,
            "actual": ratio,
            "threshold": 10.0,
            "adjacency_bytes": adjacency_bytes,
            "bytes_read_per_op": gb_w1_bytes,
        })
    else:
        out.append({
            "name": "W1 graphberg bytes_read/op > 10x adjacency-list bytes",
            "triggered": False,
            "actual": None,
            "threshold": 10.0,
            "adjacency_bytes": adjacency_bytes,
            "bytes_read_per_op": gb_w1_bytes,
            "note": "bytes_read_per_op not reported by harness",
        })

    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def evaluate(summary: dict) -> dict:
    """Apply thresholds and kill criteria. Return verdict dict."""
    threshold_table = [_evaluate_row(row, summary) for row in THRESHOLDS]
    kill = _evaluate_kill_criteria(summary)

    any_kill = any(k.get("triggered") for k in kill)
    all_pass = all(
        r.get("result") in ("PASS", "STRETCH") for r in threshold_table
    )

    if any_kill:
        verdict = "KILL"
        triggered_names = [k["name"] for k in kill if k.get("triggered")]
        rationale = "Kill criterion triggered: " + "; ".join(triggered_names)
    elif all_pass:
        verdict = "PROCEED"
        n_stretch = sum(
            1 for r in threshold_table if r.get("result") == "STRETCH"
        )
        rationale = (
            f"All {len(threshold_table)} sec 4.1 thresholds satisfied "
            f"({n_stretch} at stretch goal); no sec 4.2 kill criteria triggered."
        )
    else:
        verdict = "GRAY-ZONE"
        failed = [
            f"{r['workload']} {r['comparison']} {r['metric']}"
            for r in threshold_table
            if r.get("result") == "FAIL"
        ]
        missing = [
            f"{r['workload']} {r['comparison']} {r['metric']}"
            for r in threshold_table
            if r.get("result") == "MISSING"
        ]
        parts = []
        if failed:
            parts.append("failed: " + ", ".join(failed))
        if missing:
            parts.append("missing: " + ", ".join(missing))
        rationale = (
            "No kill criteria triggered, but not all sec 4.1 thresholds passed -- "
            + "; ".join(parts)
        )

    return {
        "threshold_table": threshold_table,
        "kill_criteria": kill,
        "verdict": verdict,
        "rationale": rationale,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    summary = json.loads(
        (Path(__file__).parent / "results" / "summary.json").read_text()
    )
    out = evaluate(summary)
    (Path(__file__).parent / "results" / "verdict.json").write_text(
        json.dumps(out, indent=2)
    )
    print(json.dumps(out, indent=2))
