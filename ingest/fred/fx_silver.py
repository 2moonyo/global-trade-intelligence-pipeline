from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready


DATASET_NAME = "fx"
PARTITION_FILENAME = "ecb_fx_eu_monthly.parquet"
LOGGER_NAME = "fx.silver"
LOG_DIR = PROJECT_ROOT / "logs" / "fx"
LOG_PATH = LOG_DIR / "fx_silver.log"
MANIFEST_PATH = LOG_DIR / "fx_silver_manifest.jsonl"


@dataclass(frozen=True)
class FxSilverConfig:
    bronze_root: Path = PROJECT_ROOT / "data" / "bronze" / "ecb_fx_eu"
    silver_root: Path = PROJECT_ROOT / "data" / "silver" / "fx"
    monthly_partition_root: Path = PROJECT_ROOT / "data" / "silver" / "fx" / "ecb_fx_eu_monthly"
    monthly_snapshot_path: Path = PROJECT_ROOT / "data" / "silver" / "fx" / "ecb_fx_eu_monthly.parquet"
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH
    partition_filename: str = PARTITION_FILENAME


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def load_bronze_csvs(bronze_root: Path, logger) -> tuple[pd.DataFrame, list[str]]:
    bronze_files = sorted(bronze_root.glob("Batch/ecb_fx_eu_*.csv"))
    if bronze_files:
        logger.info("Loading %s FX bronze batch CSV files from %s", len(bronze_files), bronze_root)
        frames = [pd.read_csv(path) for path in bronze_files]
        combined = pd.concat(frames, ignore_index=True)
        logger.info("Loaded %s FX bronze rows from batch CSV", len(combined))
        return combined, [str(path) for path in bronze_files]

    standardized_files = sorted(bronze_root.glob("dt=*/part-*.csv"))
    if not standardized_files:
        raise FileNotFoundError(
            f"No FX bronze files found under {bronze_root}. "
            "Run the FX extractor first so Batch or dt-partitioned bronze files exist."
        )

    logger.info("Loading %s FX standardized bronze CSV files from %s", len(standardized_files), bronze_root)
    frames = [pd.read_csv(path) for path in standardized_files]
    combined = pd.concat(frames, ignore_index=True)
    series_parts = combined["series_id"].astype("string").str.split(".", expand=True)
    if series_parts.shape[1] < 5:
        raise RuntimeError("Unexpected FX bronze series_id format; expected EXR.D.<quote>.<base>.SP00.A")

    normalized = pd.DataFrame(
        {
            "date": combined["dt"],
            "quote_ccy": series_parts[2],
            "base_ccy": series_parts[3],
            "rate": combined["value"],
            "load_ts": combined["ingest_ts"],
        }
    )
    logger.info("Loaded %s FX bronze rows from standardized dt partitions", len(normalized))
    return normalized, [str(path) for path in standardized_files]


def prepare_monthly(df: pd.DataFrame, logger) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.copy()
    source_row_count = int(len(working))

    working["fx_date"] = pd.to_datetime(working["date"], errors="coerce")
    working["quote_ccy"] = working["quote_ccy"].astype("string").str.strip().str.upper()
    working["base_ccy"] = working["base_ccy"].astype("string").str.strip().str.upper()
    working["rate"] = pd.to_numeric(working["rate"], errors="coerce")
    working["load_ts"] = pd.to_datetime(working["load_ts"], format="%Y%m%dT%H%M%SZ", errors="coerce", utc=True)

    working = working.dropna(subset=["fx_date", "quote_ccy", "base_ccy", "rate", "load_ts"]).copy()
    rows_after_required_field_filter = int(len(working))

    working["fx_date"] = working["fx_date"].dt.tz_localize(None)
    working["load_ts"] = working["load_ts"].dt.tz_convert("UTC").dt.tz_localize(None)
    working = working.sort_values(
        ["fx_date", "quote_ccy", "base_ccy", "load_ts"],
        ascending=[True, True, True, False],
    )
    daily_deduped = working.drop_duplicates(
        subset=["fx_date", "quote_ccy", "base_ccy"],
        keep="first",
    ).copy()

    daily_deduped["month_start_date"] = daily_deduped["fx_date"].dt.to_period("M").dt.to_timestamp()
    daily_deduped["year_month"] = daily_deduped["month_start_date"].dt.strftime("%Y-%m")
    daily_deduped["year"] = daily_deduped["month_start_date"].dt.year.astype(int)
    daily_deduped["month"] = daily_deduped["month_start_date"].dt.month.astype(int)

    monthly = (
        daily_deduped.groupby(
            ["year_month", "month_start_date", "year", "month", "base_ccy", "quote_ccy"],
            as_index=False,
        )
        .agg(
            fx_rate=("rate", "mean"),
            trading_day_count=("fx_date", "count"),
            source_row_count=("fx_date", "size"),
            latest_load_ts=("load_ts", "max"),
        )
    )
    monthly = monthly.sort_values(["base_ccy", "quote_ccy", "year_month"]).reset_index(drop=True)
    monthly["fx_mom_change"] = monthly.groupby(["base_ccy", "quote_ccy"])["fx_rate"].pct_change()
    monthly = monthly.rename(
        columns={
            "base_ccy": "base_currency_code",
            "quote_ccy": "quote_currency_code",
        }
    )
    monthly["dataset_name"] = "ecb_fx_eu"
    monthly["source_name"] = "ECB"

    summary = {
        "source_row_count": source_row_count,
        "rows_after_required_field_filter": rows_after_required_field_filter,
        "daily_rows_deduplicated": int(len(daily_deduped)),
        "rows_monthly": int(len(monthly)),
        "base_currencies": sorted(monthly["base_currency_code"].dropna().unique().tolist()),
        "quote_currencies": sorted(monthly["quote_currency_code"].dropna().unique().tolist()),
        "year_months": sorted(monthly["year_month"].dropna().unique().tolist()),
    }
    return monthly, summary


def filter_month_window(
    df: pd.DataFrame,
    *,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
) -> pd.DataFrame:
    filtered = df.copy()
    if selected_year_months:
        filtered = filtered[filtered["year_month"].isin(selected_year_months)].copy()
    if since_year_month:
        filtered = filtered[filtered["year_month"] >= since_year_month].copy()
    if until_year_month:
        filtered = filtered[filtered["year_month"] <= until_year_month].copy()
    return filtered.reset_index(drop=True)


def write_partitioned_monthly(df: pd.DataFrame, output_root: Path, partition_filename: str) -> list[str]:
    output_root.mkdir(parents=True, exist_ok=True)
    written_files: list[str] = []

    for (year_value, month_value), part in df.groupby(["year", "month"], sort=True):
        partition_dir = output_root / f"year={int(year_value):04d}" / f"month={int(month_value):02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / partition_filename
        part.sort_values(["base_currency_code", "quote_currency_code"]).to_parquet(output_path, index=False)
        written_files.append(str(output_path))

    return written_files


def run(
    *,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    config: FxSilverConfig | None = None,
) -> dict[str, Any]:
    config = config or FxSilverConfig()
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
        log_to_stdout=config.log_to_stdout,
    )
    run_id = build_run_id("fx_silver")
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "fx_silver",
        "dataset_name": DATASET_NAME,
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "requested_year_months": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "bronze_files_read": [],
        "source_row_count": None,
        "rows_after_required_field_filter": None,
        "daily_rows_deduplicated": None,
        "rows_monthly": None,
        "base_currencies": [],
        "quote_currencies": [],
        "year_months_written": [],
        "partitions_written": [],
        "monthly_snapshot_path": str(config.monthly_snapshot_path),
        "output_dir": str(config.monthly_partition_root),
        "duration_seconds": None,
        "error_summary": None,
        "log_path": str(config.log_path),
        "manifest_path": str(config.manifest_path),
    }

    try:
        logger.info("Starting FX silver build run_id=%s", run_id)
        bronze_df, bronze_files = load_bronze_csvs(config.bronze_root, logger)
        manifest_entry["bronze_files_read"] = bronze_files

        monthly_df, prep_summary = prepare_monthly(bronze_df, logger)
        manifest_entry.update(prep_summary)

        monthly_df = filter_month_window(
            monthly_df,
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        )
        if monthly_df.empty:
            raise RuntimeError("No FX monthly rows remain after applying the requested year-month filters.")

        written_files = write_partitioned_monthly(
            monthly_df,
            config.monthly_partition_root,
            config.partition_filename,
        )
        monthly_df.sort_values(["year_month", "base_currency_code", "quote_currency_code"]).to_parquet(
            config.monthly_snapshot_path,
            index=False,
        )

        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "completed",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
                "rows_monthly": int(len(monthly_df)),
                "base_currencies": sorted(monthly_df["base_currency_code"].dropna().unique().tolist()),
                "quote_currencies": sorted(monthly_df["quote_currency_code"].dropna().unique().tolist()),
                "year_months_written": sorted(monthly_df["year_month"].dropna().unique().tolist()),
                "partitions_written": written_files,
            }
        )
        append_manifest(config.manifest_path, manifest_entry)
        logger.info(
            "Finished FX silver build run_id=%s rows=%s partitions=%s",
            run_id,
            len(monthly_df),
            len(written_files),
        )
        return json_ready(
            {
                "run_id": run_id,
                "run_status": "completed",
                "rows": int(len(monthly_df)),
                "base_currencies": manifest_entry["base_currencies"],
                "quote_currencies": manifest_entry["quote_currencies"],
                "year_months_written": manifest_entry["year_months_written"],
                "partitions_written": written_files,
                "monthly_snapshot_path": str(config.monthly_snapshot_path),
                "output_dir": str(config.monthly_partition_root),
                "log_path": str(config.log_path),
                "manifest_path": str(config.manifest_path),
            }
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "failed",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
                "error_summary": str(exc),
            }
        )
        append_manifest(config.manifest_path, manifest_entry)
        logger.exception("FX silver build failed run_id=%s", run_id)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build canonical monthly FX silver parquet outputs.")
    parser.add_argument("--year-month", action="append", default=None, help="Restrict output to a specific YYYY-MM month.")
    parser.add_argument("--since-year-month", default=None, help="Restrict output to months from YYYY-MM onward.")
    parser.add_argument("--until-year-month", default=None, help="Restrict output to months through YYYY-MM.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None
    result = run(
        selected_year_months=selected_year_months,
        since_year_month=since_year_month,
        until_year_month=until_year_month,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
