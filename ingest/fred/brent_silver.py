from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready


LOGGER_NAME = "brent.silver"
LOG_DIR = PROJECT_ROOT / "logs" / "brent"
LOG_PATH = LOG_DIR / "brent_silver.log"
MANIFEST_PATH = LOG_DIR / "brent_silver_manifest.jsonl"
PARTITION_FILENAME_DAILY = "brent_daily.parquet"
PARTITION_FILENAME_MONTHLY = "brent_monthly.parquet"

BENCHMARK_MAP = {
    "DCOILBRENTEU": {
        "benchmark_code": "BRENT_EU",
        "benchmark_name": "Brent Crude Europe",
        "region": "europe",
    },
    "DCOILWTICO": {
        "benchmark_code": "WTI_US",
        "benchmark_name": "WTI Crude US",
        "region": "us",
    },
}


@dataclass(frozen=True)
class BrentSilverConfig:
    bronze_root: Path = PROJECT_ROOT / "data" / "bronze" / "brent"
    silver_root: Path = PROJECT_ROOT / "data" / "silver" / "brent"
    daily_partition_root: Path = PROJECT_ROOT / "data" / "silver" / "brent" / "brent_daily"
    monthly_partition_root: Path = PROJECT_ROOT / "data" / "silver" / "brent" / "brent_monthly"
    daily_snapshot_path: Path = PROJECT_ROOT / "data" / "silver" / "brent" / "brent_daily.parquet"
    monthly_snapshot_path: Path = PROJECT_ROOT / "data" / "silver" / "brent" / "brent_monthly.parquet"
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH


def load_bronze_csvs(bronze_root: Path, logger) -> tuple[pd.DataFrame, list[str]]:
    bronze_files = sorted(bronze_root.glob("year=*/month=*/day=*/brent_prices_*.csv"))
    if not bronze_files:
        raise FileNotFoundError(f"No Brent bronze CSV files found under {bronze_root}")

    logger.info("Loading %s Brent bronze CSV files from %s", len(bronze_files), bronze_root)
    frames = [pd.read_csv(path) for path in bronze_files]
    combined = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %s Brent bronze rows", len(combined))
    return combined, [str(path) for path in bronze_files]


def prepare_daily(df: pd.DataFrame, logger) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.copy()
    source_row_count = int(len(working))

    working["trade_date"] = pd.to_datetime(working["date"], errors="coerce")
    working["price_usd_per_bbl"] = pd.to_numeric(working["price_usd"], errors="coerce")
    working["load_ts"] = pd.to_datetime(working["load_ts"], format="%Y%m%dT%H%M%SZ", errors="coerce", utc=True)

    mapped = working["series_id"].map(BENCHMARK_MAP)
    working["benchmark_code"] = mapped.map(
        lambda value: value["benchmark_code"] if isinstance(value, dict) else pd.NA
    )
    working["benchmark_name"] = mapped.map(
        lambda value: value["benchmark_name"] if isinstance(value, dict) else pd.NA
    )
    working["region"] = mapped.map(lambda value: value["region"] if isinstance(value, dict) else pd.NA)
    working["source_series_id"] = working["series_id"].astype("string")

    unknown_series_ids = sorted(
        {
            str(value)
            for value in working.loc[working["benchmark_code"].isna(), "series_id"].dropna().astype(str).tolist()
        }
    )
    if unknown_series_ids:
        logger.warning("Skipping unknown Brent source series ids: %s", unknown_series_ids)

    working = working.dropna(
        subset=["trade_date", "price_usd_per_bbl", "load_ts", "benchmark_code", "benchmark_name", "region"]
    ).copy()

    working["trade_date"] = working["trade_date"].dt.tz_localize(None)
    working["load_ts"] = working["load_ts"].dt.tz_convert("UTC").dt.tz_localize(None)
    working["year"] = working["trade_date"].dt.year.astype(int)
    working["month"] = working["trade_date"].dt.month.astype(int)
    working["day"] = working["trade_date"].dt.day.astype(int)
    working["year_month"] = working["trade_date"].dt.to_period("M").astype(str)

    working = working.sort_values(
        ["benchmark_code", "trade_date", "load_ts"],
        ascending=[True, True, False],
    )
    deduped = working.drop_duplicates(
        subset=["benchmark_code", "trade_date"],
        keep="first",
    ).copy()
    deduped = deduped.sort_values(["benchmark_code", "trade_date"]).reset_index(drop=True)

    summary = {
        "source_row_count": source_row_count,
        "rows_after_required_field_filter": int(len(working)),
        "rows_deduplicated": int(len(deduped)),
        "unknown_series_ids": unknown_series_ids,
        "benchmarks": sorted(deduped["benchmark_code"].dropna().unique().tolist()),
        "year_months": sorted(deduped["year_month"].dropna().unique().tolist()),
    }

    daily_columns = [
        "trade_date",
        "year",
        "month",
        "day",
        "year_month",
        "benchmark_code",
        "benchmark_name",
        "region",
        "source_series_id",
        "price_usd_per_bbl",
        "load_ts",
    ]
    return deduped[daily_columns].copy(), summary


def build_monthly(daily_df: pd.DataFrame) -> pd.DataFrame:
    ordered = daily_df.sort_values(["benchmark_code", "trade_date"]).copy()

    monthly = (
        ordered.groupby(
            ["benchmark_code", "benchmark_name", "region", "source_series_id", "year_month"],
            as_index=False,
        )
        .agg(
            avg_price_usd_per_bbl=("price_usd_per_bbl", "mean"),
            min_price_usd_per_bbl=("price_usd_per_bbl", "min"),
            max_price_usd_per_bbl=("price_usd_per_bbl", "max"),
            trading_day_count=("trade_date", "count"),
            month_start_date=("trade_date", "min"),
        )
    )

    first_prices = (
        ordered.groupby(["benchmark_code", "year_month"], as_index=False)
        .first()[["benchmark_code", "year_month", "price_usd_per_bbl"]]
        .rename(columns={"price_usd_per_bbl": "month_start_price_usd_per_bbl"})
    )
    last_prices = (
        ordered.groupby(["benchmark_code", "year_month"], as_index=False)
        .last()[["benchmark_code", "year_month", "price_usd_per_bbl"]]
        .rename(columns={"price_usd_per_bbl": "month_end_price_usd_per_bbl"})
    )

    monthly = monthly.merge(first_prices, on=["benchmark_code", "year_month"], how="left")
    monthly = monthly.merge(last_prices, on=["benchmark_code", "year_month"], how="left")
    monthly["year"] = monthly["month_start_date"].dt.year.astype(int)
    monthly["month"] = monthly["month_start_date"].dt.month.astype(int)
    monthly = monthly.sort_values(["benchmark_code", "year_month"]).reset_index(drop=True)
    monthly["mom_abs_change_usd"] = monthly.groupby("benchmark_code")["avg_price_usd_per_bbl"].diff()
    monthly["mom_pct_change"] = monthly.groupby("benchmark_code")["avg_price_usd_per_bbl"].pct_change()

    monthly_columns = [
        "year_month",
        "month_start_date",
        "year",
        "month",
        "benchmark_code",
        "benchmark_name",
        "region",
        "source_series_id",
        "avg_price_usd_per_bbl",
        "min_price_usd_per_bbl",
        "max_price_usd_per_bbl",
        "month_start_price_usd_per_bbl",
        "month_end_price_usd_per_bbl",
        "mom_abs_change_usd",
        "mom_pct_change",
        "trading_day_count",
    ]
    return monthly[monthly_columns].copy()


def write_partitioned_parquet(
    *,
    df: pd.DataFrame,
    output_root: Path,
    partition_filename: str,
    date_column: str,
) -> list[str]:
    written_files: list[str] = []
    output_root.mkdir(parents=True, exist_ok=True)

    years_months = (
        df.assign(
            partition_year=pd.to_datetime(df[date_column]).dt.year.astype(int),
            partition_month=pd.to_datetime(df[date_column]).dt.month.astype(int),
        )
        .groupby(["partition_year", "partition_month"], sort=True)
    )

    for (year_value, month_value), part in years_months:
        partition_dir = output_root / f"year={year_value:04d}" / f"month={month_value:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / partition_filename
        cleaned = part.drop(columns=["partition_year", "partition_month"]).copy()
        cleaned.to_parquet(output_path, index=False)
        written_files.append(str(output_path))

    return written_files


def run(config: BrentSilverConfig | None = None) -> dict[str, Any]:
    config = config or BrentSilverConfig()
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
        log_to_stdout=config.log_to_stdout,
    )
    run_id = build_run_id("brent_silver")
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "brent_silver",
        "dataset_name": "brent",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "bronze_files_read": [],
        "source_row_count": None,
        "rows_after_required_field_filter": None,
        "rows_deduplicated": None,
        "unknown_series_ids": [],
        "benchmarks": [],
        "year_months": [],
        "daily_partitions_written": [],
        "monthly_partitions_written": [],
        "daily_snapshot_path": str(config.daily_snapshot_path),
        "monthly_snapshot_path": str(config.monthly_snapshot_path),
        "error_summary": None,
        "log_path": str(config.log_path),
        "manifest_path": str(config.manifest_path),
    }

    try:
        logger.info("Starting Brent silver build run_id=%s", run_id)
        bronze_df, bronze_files = load_bronze_csvs(config.bronze_root, logger)
        manifest_entry["bronze_files_read"] = bronze_files

        daily_df, daily_summary = prepare_daily(bronze_df, logger)
        monthly_df = build_monthly(daily_df)

        config.silver_root.mkdir(parents=True, exist_ok=True)
        config.daily_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        daily_df.to_parquet(config.daily_snapshot_path, index=False)
        monthly_df.to_parquet(config.monthly_snapshot_path, index=False)

        daily_partitions_written = write_partitioned_parquet(
            df=daily_df,
            output_root=config.daily_partition_root,
            partition_filename=PARTITION_FILENAME_DAILY,
            date_column="trade_date",
        )
        monthly_partitions_written = write_partitioned_parquet(
            df=monthly_df,
            output_root=config.monthly_partition_root,
            partition_filename=PARTITION_FILENAME_MONTHLY,
            date_column="month_start_date",
        )

        finished_at = datetime.now(timezone.utc)
        summary = {
            "run_id": run_id,
            "status": "completed",
            "source_row_count": daily_summary["source_row_count"],
            "rows_after_required_field_filter": daily_summary["rows_after_required_field_filter"],
            "rows_deduplicated": daily_summary["rows_deduplicated"],
            "daily_rows_written": int(len(daily_df)),
            "monthly_rows_written": int(len(monthly_df)),
            "benchmarks": daily_summary["benchmarks"],
            "year_months": daily_summary["year_months"],
            "daily_partitions_written": len(daily_partitions_written),
            "monthly_partitions_written": len(monthly_partitions_written),
            "daily_snapshot_path": str(config.daily_snapshot_path),
            "monthly_snapshot_path": str(config.monthly_snapshot_path),
            "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "log_path": str(config.log_path),
            "manifest_path": str(config.manifest_path),
        }

        manifest_entry.update(
            {
                "status": "completed",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": summary["duration_seconds"],
                "source_row_count": daily_summary["source_row_count"],
                "rows_after_required_field_filter": daily_summary["rows_after_required_field_filter"],
                "rows_deduplicated": daily_summary["rows_deduplicated"],
                "unknown_series_ids": daily_summary["unknown_series_ids"],
                "benchmarks": daily_summary["benchmarks"],
                "year_months": daily_summary["year_months"],
                "daily_partitions_written": daily_partitions_written,
                "monthly_partitions_written": monthly_partitions_written,
            }
        )
        append_manifest(config.manifest_path, manifest_entry)
        logger.info(
            "Finished Brent silver run_id=%s daily_rows=%s monthly_rows=%s",
            run_id,
            len(daily_df),
            len(monthly_df),
        )
        return json_ready(summary)
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
        logger.exception("Brent silver build failed run_id=%s", run_id)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Build partitioned Brent silver daily and monthly parquet outputs.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    args = parser.parse_args()
    summary = run(BrentSilverConfig(log_level=args.log_level))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
