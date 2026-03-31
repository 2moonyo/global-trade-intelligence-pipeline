from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready

PARTITION_FILENAME = "energy_vulnerability.parquet"
LOGGER_NAME = "worldbank_energy.silver"
LOG_DIR = PROJECT_ROOT / "logs" / "worldbank_energy"
LOG_PATH = LOG_DIR / "worldbank_energy_silver.log"
MANIFEST_PATH = LOG_DIR / "worldbank_energy_silver_manifest.jsonl"

INDICATOR_STANDARDIZATION: dict[str, dict[str, str]] = {
    "renew": {
        "indicator_id": "EG.FEC.RNEW.ZS",
        "indicator_name": "Renewable energy consumption (% of total final energy consumption)",
        "indicator_code": "renewables_share",
        "unit_hint": "percent",
    },
    "fossil": {
        "indicator_id": "EG.USE.COMM.FO.ZS",
        "indicator_name": "Fossil fuel energy consumption (% of total)",
        "indicator_code": "fossil_fuels_share",
        "unit_hint": "percent",
    },
    "imports": {
        "indicator_id": "EG.IMP.CONS.ZS",
        "indicator_name": "Energy imports (% of energy use)",
        "indicator_code": "dependency_on_imported_energy",
        "unit_hint": "percent",
    },
    "oil": {
        "indicator_id": "EG.ELC.PETR.ZS",
        "indicator_name": "Electricity production from oil sources (% of total)",
        "indicator_code": "oil_electricity_share",
        "unit_hint": "percent",
    },
    "gas": {
        "indicator_id": "EG.ELC.NGAS.ZS",
        "indicator_name": "Electricity production from natural gas sources (% of total)",
        "indicator_code": "gas_electricity_share",
        "unit_hint": "percent",
    },
    "coal": {
        "indicator_id": "EG.ELC.COAL.ZS",
        "indicator_name": "Electricity production from coal sources (% of total)",
        "indicator_code": "coal_electricity_share",
        "unit_hint": "percent",
    },
}

STRING_COLUMNS = [
    "dataset",
    "source",
    "ingest_ts",
    "indicator_alias",
    "indicator_id",
    "indicator_name",
    "metric_name",
    "unit_hint",
    "country_name",
    "country_id",
    "country_iso3",
    "wb_unit",
    "obs_status",
]

SILVER_COLUMNS = [
    "dt",
    "month_start_date",
    "year",
    "dataset",
    "source",
    "ingest_ts",
    "indicator_alias",
    "indicator_code",
    "indicator_id",
    "indicator_name",
    "metric_name",
    "unit_hint",
    "country_name",
    "country_id",
    "country_iso3",
    "value",
    "wb_unit",
    "obs_status",
    "decimal_places",
    "grain_key",
]


@dataclass(frozen=True)
class WorldBankEnergySilverConfig:
    project_root: Path = PROJECT_ROOT
    bronze_root: Path = PROJECT_ROOT / "data" / "bronze" / "worldbank_energy"
    silver_root: Path = PROJECT_ROOT / "data" / "silver" / "worldbank_energy"
    contract_output_dir: Path = PROJECT_ROOT / "data" / "silver" / "worldbank_energy" / "energy_vulnerability"
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH
    partition_filename: str = PARTITION_FILENAME


def load_bronze_jsonl(bronze_root: Path, logger) -> tuple[pd.DataFrame, list[str]]:
    bronze_files = sorted(bronze_root.glob("dt=*/part-*.jsonl"))
    if not bronze_files:
        raise FileNotFoundError(f"No bronze JSONL files found under {bronze_root}")

    logger.info("Loading %s World Bank energy bronze JSONL files from %s", len(bronze_files), bronze_root)
    frames = [pd.read_json(path, lines=True) for path in bronze_files]
    combined = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %s bronze rows", len(combined))
    return combined, [str(path) for path in bronze_files]


def _clean_string(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )


def prepare_silver(df: pd.DataFrame, logger) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.copy()

    for column in STRING_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
        working[column] = _clean_string(working[column])

    working["indicator_alias"] = working["indicator_alias"].str.lower()
    working["country_iso3"] = working["country_iso3"].str.upper()
    working["metric_name"] = working["metric_name"].str.lower()

    working["dt"] = pd.to_datetime(working["dt"], errors="coerce").dt.normalize()
    working["year"] = pd.to_numeric(working["year"], errors="coerce").astype("Int64")
    working["year"] = working["year"].fillna(working["dt"].dt.year.astype("Int64"))
    working["month_start_date"] = working["dt"].dt.date
    working["dt"] = working["dt"].dt.date
    working["value"] = pd.to_numeric(working["value"], errors="coerce")
    working["decimal_places"] = pd.to_numeric(working["decimal_places"], errors="coerce").astype("Int64")

    alias_frame = pd.DataFrame.from_dict(INDICATOR_STANDARDIZATION, orient="index")
    alias_frame.index.name = "indicator_alias"
    alias_frame = alias_frame.reset_index()
    working = working.merge(alias_frame, on="indicator_alias", how="left", suffixes=("", "_canonical"))

    working["indicator_id"] = working["indicator_id_canonical"].combine_first(working["indicator_id"])
    working["indicator_name"] = working["indicator_name_canonical"].combine_first(working["indicator_name"])
    working["unit_hint"] = working["unit_hint_canonical"].combine_first(working["unit_hint"])
    working["indicator_code"] = working["indicator_code"].combine_first(working["metric_name"])
    working["metric_name"] = working["indicator_code"].combine_first(working["metric_name"])

    working["dataset"] = working["dataset"].fillna("worldbank_energy")
    working["source"] = working["source"].fillna("WorldBank")
    working["grain_key"] = (
        working["country_iso3"].fillna("")
        + "|"
        + working["year"].astype("string").fillna("")
        + "|"
        + working["indicator_code"].fillna("")
    )

    unknown_aliases = sorted(
        {
            str(value)
            for value in working.loc[working["indicator_code"].isna(), "indicator_alias"].dropna().tolist()
        }
    )
    if unknown_aliases:
        logger.warning("Found indicator aliases without canonical mapping: %s", unknown_aliases)

    source_rows = len(working)
    working = working.dropna(subset=["dt", "year", "indicator_alias", "indicator_code", "country_iso3"]).copy()
    dropped_missing_grain = source_rows - len(working)

    working = working.sort_values(
        ["country_iso3", "year", "indicator_code", "ingest_ts"],
        ascending=[True, True, True, False],
        na_position="last",
    )
    deduped = working.drop_duplicates(
        subset=["country_iso3", "year", "indicator_code"],
        keep="first",
    ).copy()

    deduped["year"] = deduped["year"].astype(int)
    deduped = deduped.sort_values(["year", "indicator_code", "country_iso3"]).reset_index(drop=True)
    deduped = deduped[SILVER_COLUMNS].copy()

    summary = {
        "source_row_count": int(source_rows),
        "rows_after_required_field_filter": int(len(working)),
        "rows_deduplicated": int(len(deduped)),
        "dropped_missing_grain_rows": int(dropped_missing_grain),
        "unknown_aliases": unknown_aliases,
        "null_value_row_count": int(deduped["value"].isna().sum()),
        "country_count": int(deduped["country_iso3"].nunique()),
        "indicator_count": int(deduped["indicator_code"].nunique()),
        "years": [int(year) for year in sorted(deduped["year"].dropna().unique().tolist())],
    }
    return deduped, summary


def filter_year_window(
    df: pd.DataFrame,
    *,
    start_year: Optional[int],
    end_year: Optional[int],
) -> pd.DataFrame:
    filtered = df.copy()
    if start_year is not None:
        filtered = filtered[filtered["year"] >= start_year].copy()
    if end_year is not None:
        filtered = filtered[filtered["year"] <= end_year].copy()
    return filtered.reset_index(drop=True)


def write_partitioned_yearly(
    df: pd.DataFrame,
    output_dir: Path,
    partition_filename: str,
) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written_files: list[str] = []

    for year_value, part in df.groupby("year", sort=True):
        partition_dir = output_dir / f"year={int(year_value)}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        out_path = partition_dir / partition_filename
        part.sort_values(["indicator_code", "country_iso3"]).to_parquet(out_path, index=False)
        written_files.append(str(out_path))

    return written_files


def run(
    *,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    config: Optional[WorldBankEnergySilverConfig] = None,
) -> dict[str, Any]:
    config = config or WorldBankEnergySilverConfig()
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
        log_to_stdout=config.log_to_stdout,
    )
    run_id = build_run_id("worldbank_energy_silver")
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "worldbank_energy_silver",
        "dataset_name": "worldbank_energy",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "requested_start_year": start_year,
        "requested_end_year": end_year,
        "bronze_files_read": [],
        "source_row_count": None,
        "rows_after_required_field_filter": None,
        "rows_deduplicated": None,
        "dropped_missing_grain_rows": None,
        "null_value_row_count": None,
        "country_count": None,
        "indicator_count": None,
        "years_written": [],
        "partitions_written": [],
        "unknown_aliases": [],
        "output_dir": str(config.contract_output_dir),
        "duration_seconds": None,
        "error_summary": None,
        "log_path": str(config.log_path),
        "manifest_path": str(config.manifest_path),
    }

    try:
        logger.info("Starting World Bank energy silver build run_id=%s", run_id)
        logger.info("Bronze root: %s", config.bronze_root)
        logger.info("Silver output dir: %s", config.contract_output_dir)

        bronze_df, bronze_files = load_bronze_jsonl(config.bronze_root, logger)
        manifest_entry["bronze_files_read"] = bronze_files

        silver_df, prep_summary = prepare_silver(bronze_df, logger)
        manifest_entry.update(prep_summary)

        silver_df = filter_year_window(
            silver_df,
            start_year=start_year,
            end_year=end_year,
        )
        if silver_df.empty:
            raise RuntimeError("No World Bank energy rows remain after applying the requested year window.")

        written_files = write_partitioned_yearly(
            silver_df,
            config.contract_output_dir,
            config.partition_filename,
        )

        manifest_entry["status"] = "completed"
        manifest_entry["finished_at"] = datetime.now(timezone.utc).isoformat()
        manifest_entry["duration_seconds"] = round(
            (datetime.now(timezone.utc) - started_at).total_seconds(),
            3,
        )
        manifest_entry["years_written"] = [int(year) for year in sorted(silver_df["year"].unique().tolist())]
        manifest_entry["partitions_written"] = written_files
        manifest_entry["source_row_count"] = int(len(bronze_df))
        manifest_entry["rows_after_required_field_filter"] = int(prep_summary["rows_after_required_field_filter"])
        manifest_entry["rows_deduplicated"] = int(len(silver_df))
        manifest_entry["null_value_row_count"] = int(silver_df["value"].isna().sum())
        manifest_entry["country_count"] = int(silver_df["country_iso3"].nunique())
        manifest_entry["indicator_count"] = int(silver_df["indicator_code"].nunique())

        append_manifest(config.manifest_path, manifest_entry)
        logger.info(
            "Finished World Bank energy silver build run_id=%s rows=%s partitions=%s",
            run_id,
            len(silver_df),
            len(written_files),
        )

        return json_ready(
            {
                "run_id": run_id,
                "run_status": "completed",
                "rows": int(len(silver_df)),
                "country_count": int(silver_df["country_iso3"].nunique()),
                "indicator_count": int(silver_df["indicator_code"].nunique()),
                "years_written": manifest_entry["years_written"],
                "partitions_written": written_files,
                "output_dir": str(config.contract_output_dir),
                "log_path": str(config.log_path),
                "manifest_path": str(config.manifest_path),
            }
        )

    except Exception as exc:
        manifest_entry["status"] = "failed"
        manifest_entry["finished_at"] = datetime.now(timezone.utc).isoformat()
        manifest_entry["duration_seconds"] = round(
            (datetime.now(timezone.utc) - started_at).total_seconds(),
            3,
        )
        manifest_entry["error_summary"] = str(exc)
        append_manifest(config.manifest_path, manifest_entry)
        logger.exception("World Bank energy silver build failed run_id=%s", run_id)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build annual silver parquet outputs for World Bank energy.")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run(start_year=args.start_year, end_year=args.end_year)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
