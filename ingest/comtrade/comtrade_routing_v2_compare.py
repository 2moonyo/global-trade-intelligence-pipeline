from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "comtrade"
DEFAULT_OUTPUT_SUFFIX = "_v2"

BASELINE_OUTPUTS = [
    Path("dim_trade_routes.parquet"),
    Path("dimensions/dim_country_ports.parquet"),
    Path("dimensions/dim_port_basin.parquet"),
    Path("dimensions/bridge_country_route_applicability.parquet"),
    Path("dimensions/dim_chokepoint.parquet"),
    Path("dimensions/bridge_basin_graph_edges.parquet"),
    Path("dimensions/bridge_port_basin_chokepoints.parquet"),
    Path("dimensions/dim_transshipment_hub.parquet"),
    Path("dimensions/bridge_basin_default_hubs.parquet"),
]


def _suffix_path(path: Path, suffix: str) -> Path:
    if not suffix:
        return path
    return path.with_name(f"{path.stem}{suffix}{path.suffix}")


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, np.ndarray):
        return json.dumps([_normalize_scalar(item) for item in value.tolist()], sort_keys=False)
    if isinstance(value, (list, tuple)):
        return json.dumps([_normalize_scalar(item) for item in value], sort_keys=False)
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, dict):
        return json.dumps({str(k): _normalize_scalar(v) for k, v in value.items()}, sort_keys=True)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return value


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in normalized.columns:
        normalized[column] = normalized[column].map(_normalize_scalar)
    return normalized


def _sorted_frame(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_frame(df)
    if normalized.empty or not list(normalized.columns):
        return normalized.reset_index(drop=True)

    sort_view = normalized.copy()
    for column in sort_view.columns:
        sort_view[column] = sort_view[column].map(lambda value: "__NULL__" if value is None else str(value))

    order = sort_view.sort_values(list(sort_view.columns), na_position="last", kind="mergesort").index
    return normalized.loc[order].reset_index(drop=True)


def _frames_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    return _normalize_frame(left).equals(_normalize_frame(right))


def _first_mismatch(left: pd.DataFrame, right: pd.DataFrame) -> dict[str, Any] | None:
    left_norm = _normalize_frame(left)
    right_norm = _normalize_frame(right)
    max_rows = min(len(left_norm), len(right_norm))
    for row_idx in range(max_rows):
        left_row = left_norm.iloc[row_idx]
        right_row = right_norm.iloc[row_idx]
        if not left_row.equals(right_row):
            mismatch = {
                "row_index": int(row_idx),
                "left_row": left_row.to_dict(),
                "right_row": right_row.to_dict(),
            }
            differing_columns = [
                column
                for column in left_norm.columns
                if left_row.get(column) != right_row.get(column)
            ]
            mismatch["columns"] = differing_columns
            return mismatch
    if len(left_norm) != len(right_norm):
        return {
            "row_index": max_rows,
            "columns": ["__row_count__"],
            "left_row": None if max_rows >= len(left_norm) else left_norm.iloc[max_rows].to_dict(),
            "right_row": None if max_rows >= len(right_norm) else right_norm.iloc[max_rows].to_dict(),
        }
    return None


def compare_output_pair(baseline_path: Path, candidate_path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "baseline_path": str(baseline_path),
        "candidate_path": str(candidate_path),
        "baseline_exists": baseline_path.exists(),
        "candidate_exists": candidate_path.exists(),
        "status": "missing",
    }

    if not baseline_path.exists() or not candidate_path.exists():
        return summary

    baseline_df = pd.read_parquet(baseline_path)
    candidate_df = pd.read_parquet(candidate_path)

    summary["baseline_rows"] = int(len(baseline_df))
    summary["candidate_rows"] = int(len(candidate_df))
    summary["baseline_columns"] = list(baseline_df.columns)
    summary["candidate_columns"] = list(candidate_df.columns)
    summary["column_order_match"] = list(baseline_df.columns) == list(candidate_df.columns)

    if not summary["column_order_match"]:
        summary["status"] = "column_mismatch"
        return summary

    in_order_match = _frames_equal(baseline_df, candidate_df)
    canonical_match = _frames_equal(_sorted_frame(baseline_df), _sorted_frame(candidate_df))

    summary["exact_match_in_order"] = in_order_match
    summary["exact_match_canonical"] = canonical_match

    if in_order_match:
        summary["status"] = "match"
        return summary

    if canonical_match:
        summary["status"] = "row_order_diff_only"
        return summary

    summary["status"] = "content_mismatch"
    summary["first_mismatch"] = _first_mismatch(
        _sorted_frame(baseline_df),
        _sorted_frame(candidate_df),
    )
    return summary


def run(silver_root: Path, baseline_suffix: str, candidate_suffix: str) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for relative_path in BASELINE_OUTPUTS:
        baseline_path = silver_root / _suffix_path(relative_path, baseline_suffix)
        candidate_path = silver_root / _suffix_path(relative_path, candidate_suffix)
        results.append(compare_output_pair(baseline_path, candidate_path))

    status_counts: dict[str, int] = {}
    for result in results:
        status = str(result["status"])
        status_counts[status] = status_counts.get(status, 0) + 1

    overall_status = "match"
    if any(result["status"] == "content_mismatch" for result in results):
        overall_status = "content_mismatch"
    elif any(result["status"] == "column_mismatch" for result in results):
        overall_status = "column_mismatch"
    elif any(result["status"] == "row_order_diff_only" for result in results):
        overall_status = "row_order_diff_only"
    elif any(result["status"] == "missing" for result in results):
        overall_status = "missing"

    return {
        "silver_root": str(silver_root),
        "baseline_suffix": baseline_suffix,
        "candidate_suffix": candidate_suffix,
        "overall_status": overall_status,
        "status_counts": status_counts,
        "results": results,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare one Comtrade routing parquet output set against another suffixed candidate set."
    )
    parser.add_argument("--silver-root", default=str(DEFAULT_SILVER_ROOT))
    parser.add_argument(
        "--baseline-suffix",
        default="",
        help="Suffix for the baseline output set. Leave empty to compare from the original unsuffixed files.",
    )
    parser.add_argument("--candidate-suffix", default=DEFAULT_OUTPUT_SUFFIX)
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="Return a non-zero exit code when any output differs from baseline.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    summary = run(
        silver_root=Path(args.silver_root),
        baseline_suffix=args.baseline_suffix,
        candidate_suffix=args.candidate_suffix,
    )
    print(json.dumps(summary, indent=2))
    if args.fail_on_mismatch and summary["overall_status"] != "match":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
