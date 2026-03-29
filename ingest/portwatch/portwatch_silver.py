from __future__ import annotations

import argparse
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
import pandas as pd


DEFAULT_TARGET_CHOKEPOINTS = (
    "Suez Canal",
    "Strait of Hormuz",
    "Panama Canal",
    "Cape of Good Hope",
    "Bab el-Mandeb Strait",
)

LEGACY_PARTITION_FILENAME = "portwatch_chokepoint_stress_monthly.parquet"
CONTRACT_PARTITION_FILENAME = "portwatch_monthly.parquet"
LOGGER_NAME = "portwatch.silver"


def find_project_root(start: Path) -> Path:
    """Walk upward until pyproject.toml is found, then return that folder."""
    for candidate in [start, *start.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Could not find project root containing pyproject.toml")


def _parse_date(value: Optional[object]) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Unsupported date value: {value!r}")


@dataclass
class PortWatchSilverConfig:
    project_root: Path = field(default_factory=lambda: find_project_root(Path(__file__).resolve()))
    bronze_portwatch_dir: Optional[Path] = None
    silver_portwatch_dir: Optional[Path] = None
    legacy_monthly_dir: Optional[Path] = None
    contract_monthly_dir: Optional[Path] = None
    full_output_path: Optional[Path] = None
    scaffold_output_path: Optional[Path] = None
    dimensions_dir: Optional[Path] = None
    dim_chokepoint_output_path: Optional[Path] = None
    dim_month_output_path: Optional[Path] = None
    target_chokepoints: Sequence[str] = DEFAULT_TARGET_CHOKEPOINTS
    write_legacy_outputs: bool = True
    write_contract_outputs: bool = True
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_to_file: bool = False
    log_path: Optional[Path] = None
    manifest_path: Optional[Path] = None
    respect_requested_window: bool = False

    def __post_init__(self) -> None:
        self.project_root = Path(self.project_root)
        self.bronze_portwatch_dir = Path(
            self.bronze_portwatch_dir or self.project_root / "data" / "bronze" / "portwatch"
        )
        self.silver_portwatch_dir = Path(
            self.silver_portwatch_dir or self.project_root / "data" / "silver" / "portwatch"
        )
        self.legacy_monthly_dir = Path(
            self.legacy_monthly_dir
            or self.silver_portwatch_dir / "mart_portwatch_chokepoint_stress_monthly"
        )
        self.contract_monthly_dir = Path(
            self.contract_monthly_dir or self.silver_portwatch_dir / "portwatch_monthly"
        )
        self.full_output_path = Path(
            self.full_output_path
            or self.silver_portwatch_dir / "portwatch_chokepoint_stress_monthly_all.parquet"
        )
        self.scaffold_output_path = Path(
            self.scaffold_output_path
            or self.silver_portwatch_dir / "portwatch_month_chokepoint_scaffold.parquet"
        )
        self.dimensions_dir = Path(self.dimensions_dir or self.silver_portwatch_dir / "dimensions")
        self.dim_chokepoint_output_path = Path(
            self.dim_chokepoint_output_path
            or self.dimensions_dir / "dim_portwatch_chokepoint.parquet"
        )
        self.dim_month_output_path = Path(
            self.dim_month_output_path or self.dimensions_dir / "dim_month.parquet"
        )
        self.log_path = Path(
            self.log_path or self.project_root / "logs" / "portwatch" / "portwatch_silver.log"
        )
        self.manifest_path = Path(
            self.manifest_path
            or self.project_root / "logs" / "portwatch" / "portwatch_silver_manifest.jsonl"
        )
        self.target_chokepoints = tuple(self.target_chokepoints)


def configure_logging(config: PortWatchSilverConfig) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")

    if config.log_to_stdout:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if config.log_to_file:
        config.log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(config.log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def append_manifest(path: Path, entry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def load_bronze_daily(bronze_portwatch_dir: Path, logger: logging.Logger) -> pd.DataFrame:
    parquet_files = sorted(bronze_portwatch_dir.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {bronze_portwatch_dir}")

    logger.info("Loading %s bronze parquet files from %s", len(parquet_files), bronze_portwatch_dir)
    df = pd.concat((pd.read_parquet(path) for path in parquet_files), ignore_index=True)
    logger.info("Loaded %s bronze rows", len(df))
    logger.info("Bronze columns: %s", sorted(df.columns.tolist()))
    return df


def clean_bronze_daily(
    df: pd.DataFrame,
    *,
    target_chokepoints: Sequence[str],
    start_date: Optional[date],
    end_date: Optional[date],
    respect_requested_window: bool,
    logger: logging.Logger,
) -> pd.DataFrame:
    df = df.copy()

    if not np.issubdtype(df["date"].dtype, np.datetime64):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    numeric_cols = [
        "n_total",
        "capacity",
        "n_tanker",
        "n_container",
        "n_dry_bulk",
        "capacity_tanker",
        "capacity_container",
        "capacity_dry_bulk",
    ]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["date", "portname", "portid", "n_total", "capacity"]).copy()

    if respect_requested_window and (start_date or end_date):
        start_date = start_date or df["date"].min().date()
        end_date = end_date or df["date"].max().date()
        logger.info(
            "Applying requested date window to bronze rows: %s -> %s",
            start_date.isoformat(),
            end_date.isoformat(),
        )
        df = df[df["date"].dt.date.between(start_date, end_date)].copy()
    elif start_date or end_date:
        logger.info(
            "Requested date window recorded for manifest only. Rebuilding full history to preserve notebook semantics."
        )

    df["year_month"] = df["date"].dt.to_period("M").astype(str)
    df = df[df["portname"].isin(target_chokepoints)].copy()

    logger.info("Rows after cleaning/filtering: %s", len(df))
    logger.info("Chokepoints: %s", sorted(df["portname"].dropna().unique().tolist()))
    logger.info("Date range: %s -> %s", df["date"].min(), df["date"].max())
    return df


def build_monthly_fact(df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        df.groupby(["portname", "year_month"], as_index=False)
        .agg(
            avg_n_total=("n_total", "mean"),
            max_n_total=("n_total", "max"),
            avg_capacity=("capacity", "mean"),
            max_capacity=("capacity", "max"),
            avg_n_tanker=("n_tanker", "mean"),
            avg_n_container=("n_container", "mean"),
            avg_n_dry_bulk=("n_dry_bulk", "mean"),
            avg_capacity_tanker=("capacity_tanker", "mean"),
            avg_capacity_container=("capacity_container", "mean"),
            avg_capacity_dry_bulk=("capacity_dry_bulk", "mean"),
            days_observed=("date", "nunique"),
        )
        .rename(columns={"portname": "chokepoint_name"})
    )

    monthly["total_vessel_classes_avg"] = (
        monthly["avg_n_tanker"].fillna(0)
        + monthly["avg_n_container"].fillna(0)
        + monthly["avg_n_dry_bulk"].fillna(0)
    )
    monthly["tanker_share"] = np.where(
        monthly["total_vessel_classes_avg"] > 0,
        monthly["avg_n_tanker"] / monthly["total_vessel_classes_avg"],
        np.nan,
    )
    monthly["container_share"] = np.where(
        monthly["total_vessel_classes_avg"] > 0,
        monthly["avg_n_container"] / monthly["total_vessel_classes_avg"],
        np.nan,
    )
    monthly["dry_bulk_share"] = np.where(
        monthly["total_vessel_classes_avg"] > 0,
        monthly["avg_n_dry_bulk"] / monthly["total_vessel_classes_avg"],
        np.nan,
    )

    period_index = pd.PeriodIndex(monthly["year_month"], freq="M")
    monthly["year"] = period_index.year.astype(int)
    monthly["month"] = period_index.month.astype(int).astype(str).str.zfill(2)

    monthly_cols = [
        "year_month",
        "chokepoint_name",
        "avg_n_total",
        "max_n_total",
        "avg_capacity",
        "max_capacity",
        "avg_n_tanker",
        "avg_n_container",
        "avg_n_dry_bulk",
        "avg_capacity_tanker",
        "avg_capacity_container",
        "avg_capacity_dry_bulk",
        "days_observed",
        "tanker_share",
        "container_share",
        "dry_bulk_share",
        "year",
        "month",
    ]
    return monthly[monthly_cols].copy()


def build_dim_chokepoint(df: pd.DataFrame, monthly_legacy: pd.DataFrame) -> pd.DataFrame:
    dim_chokepoint = (
        df.groupby(["portid", "portname"], as_index=False)
        .agg(
            first_observed_date=("date", "min"),
            last_observed_date=("date", "max"),
            days_observed_total=("date", "nunique"),
            mean_n_total=("n_total", "mean"),
            mean_capacity=("capacity", "mean"),
        )
        .rename(columns={"portid": "chokepoint_id", "portname": "chokepoint_name"})
    )

    share_rollup = (
        monthly_legacy.groupby("chokepoint_name", as_index=False)
        .agg(
            avg_tanker_share=("tanker_share", "mean"),
            avg_container_share=("container_share", "mean"),
            avg_dry_bulk_share=("dry_bulk_share", "mean"),
        )
    )

    dim_chokepoint = dim_chokepoint.merge(share_rollup, on="chokepoint_name", how="left")
    dim_chokepoint["dominant_vessel_class"] = (
        dim_chokepoint[["avg_tanker_share", "avg_container_share", "avg_dry_bulk_share"]]
        .idxmax(axis=1)
        .str.replace("avg_", "", regex=False)
        .str.replace("_share", "", regex=False)
    )
    return dim_chokepoint


def build_dim_month(monthly_legacy: pd.DataFrame) -> pd.DataFrame:
    return (
        monthly_legacy[["year_month", "year", "month"]]
        .drop_duplicates()
        .assign(month_start=lambda frame: pd.PeriodIndex(frame["year_month"], freq="M").to_timestamp())
        .sort_values("year_month")
        .reset_index(drop=True)
    )


def build_contract_monthly(
    monthly_legacy: pd.DataFrame,
    dim_chokepoint: pd.DataFrame,
) -> pd.DataFrame:
    chokepoint_lookup = dim_chokepoint[["chokepoint_id", "chokepoint_name"]].drop_duplicates()
    monthly_contract = monthly_legacy.merge(chokepoint_lookup, on="chokepoint_name", how="left", validate="many_to_one")
    monthly_contract["month_start_date"] = pd.PeriodIndex(
        monthly_contract["year_month"], freq="M"
    ).to_timestamp()

    contract_cols = [
        "month_start_date",
        "year_month",
        "year",
        "month",
        "chokepoint_id",
        "chokepoint_name",
        "avg_n_total",
        "max_n_total",
        "avg_capacity",
        "max_capacity",
        "avg_n_tanker",
        "avg_n_container",
        "avg_n_dry_bulk",
        "avg_capacity_tanker",
        "avg_capacity_container",
        "avg_capacity_dry_bulk",
        "days_observed",
        "tanker_share",
        "container_share",
        "dry_bulk_share",
    ]
    return monthly_contract[contract_cols].copy()


def build_scaffold(
    monthly_legacy: pd.DataFrame,
    dim_month: pd.DataFrame,
    dim_chokepoint: pd.DataFrame,
) -> pd.DataFrame:
    scaffold = (
        dim_month[["year_month", "year", "month"]]
        .drop_duplicates()
        .merge(
            dim_chokepoint[["chokepoint_id", "chokepoint_name"]].drop_duplicates(),
            how="cross",
        )
    )

    fact_cols = [
        "year_month",
        "chokepoint_name",
        "avg_n_total",
        "max_n_total",
        "avg_capacity",
        "max_capacity",
        "avg_n_tanker",
        "avg_n_container",
        "avg_n_dry_bulk",
        "avg_capacity_tanker",
        "avg_capacity_container",
        "avg_capacity_dry_bulk",
        "days_observed",
    ]

    portwatch_scaffold = scaffold.merge(
        monthly_legacy[fact_cols],
        on=["year_month", "chokepoint_name"],
        how="left",
    )

    portwatch_scaffold["has_portwatch_data_flag"] = (
        portwatch_scaffold["days_observed"].fillna(0).gt(0).astype("int8")
    )
    portwatch_scaffold["coverage_gap_flag"] = (
        portwatch_scaffold["has_portwatch_data_flag"].eq(0).astype("int8")
    )

    month_start = pd.PeriodIndex(portwatch_scaffold["year_month"], freq="M").to_timestamp()
    portwatch_scaffold["month_start_date"] = month_start
    portwatch_scaffold["days_in_month"] = month_start.days_in_month.astype("int16")
    portwatch_scaffold["coverage_ratio"] = (
        portwatch_scaffold["days_observed"].fillna(0) / portwatch_scaffold["days_in_month"]
    )

    return portwatch_scaffold.sort_values(["year_month", "chokepoint_name"]).reset_index(drop=True)


def write_partitioned_monthly(
    monthly_df: pd.DataFrame,
    output_dir: Path,
    partition_filename: str,
) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written_files: list[str] = []
    for (year_value, month_value), part in monthly_df.groupby(["year", "month"], sort=True):
        part_dir = output_dir / f"year={int(year_value)}" / f"month={month_value}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_path = part_dir / partition_filename
        part.sort_values(["chokepoint_name", "year_month"]).to_parquet(out_path, index=False)
        written_files.append(str(out_path))
    return written_files


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    return value


def run(
    start_date: Optional[object] = None,
    end_date: Optional[object] = None,
    config: Optional[PortWatchSilverConfig | dict[str, Any]] = None,
) -> dict[str, Any]:
    start_date = _parse_date(start_date)
    end_date = _parse_date(end_date)

    if isinstance(config, dict):
        config = PortWatchSilverConfig(**config)
    elif config is None:
        config = PortWatchSilverConfig()

    logger = configure_logging(config)
    run_id = f"portwatch_silver_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "portwatch_silver",
        "dataset_name": "portwatch",
        "requested_start_date": start_date.isoformat() if start_date else None,
        "requested_end_date": end_date.isoformat() if end_date else None,
        "requested_chokepoints": list(config.target_chokepoints),
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "source_row_count": None,
        "output_row_count": None,
        "partitions_written": [],
        "error_summary": None,
        "write_legacy_outputs": config.write_legacy_outputs,
        "write_contract_outputs": config.write_contract_outputs,
        "respect_requested_window": config.respect_requested_window,
    }

    try:
        logger.info("Starting PortWatch silver build run_id=%s", run_id)
        logger.info("Project root: %s", config.project_root)
        logger.info("Bronze PortWatch root: %s", config.bronze_portwatch_dir)
        logger.info("Legacy monthly output dir: %s", config.legacy_monthly_dir)
        logger.info("Contract monthly output dir: %s", config.contract_monthly_dir)

        bronze_df = load_bronze_daily(config.bronze_portwatch_dir, logger)
        manifest_entry["source_row_count"] = len(bronze_df)

        daily_df = clean_bronze_daily(
            bronze_df,
            target_chokepoints=config.target_chokepoints,
            start_date=start_date,
            end_date=end_date,
            respect_requested_window=config.respect_requested_window,
            logger=logger,
        )
        if daily_df.empty:
            raise RuntimeError("No PortWatch rows remain after cleaning/filtering.")

        monthly_legacy = build_monthly_fact(daily_df)
        dim_chokepoint = build_dim_chokepoint(daily_df, monthly_legacy)
        dim_month = build_dim_month(monthly_legacy)
        monthly_contract = build_contract_monthly(monthly_legacy, dim_chokepoint)
        scaffold = build_scaffold(monthly_legacy, dim_month, dim_chokepoint)

        logger.info("Built monthly legacy rows: %s", len(monthly_legacy))
        logger.info("Built monthly contract rows: %s", len(monthly_contract))
        logger.info("Built chokepoint dimension rows: %s", len(dim_chokepoint))
        logger.info("Built month dimension rows: %s", len(dim_month))
        logger.info("Built scaffold rows: %s", len(scaffold))

        partitions_written: list[str] = []

        if config.write_legacy_outputs:
            legacy_written = write_partitioned_monthly(
                monthly_legacy,
                config.legacy_monthly_dir,
                LEGACY_PARTITION_FILENAME,
            )
            partitions_written.extend(legacy_written)
            config.full_output_path.parent.mkdir(parents=True, exist_ok=True)
            monthly_contract.sort_values(["year_month", "chokepoint_name"]).to_parquet(
                config.full_output_path, index=False
            )
            logger.info("Wrote %s legacy monthly partition files", len(legacy_written))
            logger.info("Combined raw-compatible PortWatch monthly table: %s", config.full_output_path)

        if config.write_contract_outputs:
            contract_written = write_partitioned_monthly(
                monthly_contract,
                config.contract_monthly_dir,
                CONTRACT_PARTITION_FILENAME,
            )
            partitions_written.extend(contract_written)
            logger.info("Wrote %s contract monthly partition files", len(contract_written))

        config.dimensions_dir.mkdir(parents=True, exist_ok=True)
        dim_chokepoint.to_parquet(config.dim_chokepoint_output_path, index=False)
        dim_month.to_parquet(config.dim_month_output_path, index=False)
        config.scaffold_output_path.parent.mkdir(parents=True, exist_ok=True)
        scaffold.to_parquet(config.scaffold_output_path, index=False)

        logger.info("Dimension table (chokepoint): %s", config.dim_chokepoint_output_path)
        logger.info("Dimension table (month): %s", config.dim_month_output_path)
        logger.info("Scaffold table: %s", config.scaffold_output_path)
        logger.info("Rows with data: %s", int(scaffold["has_portwatch_data_flag"].sum()))
        logger.info("Coverage gaps: %s", int(scaffold["coverage_gap_flag"].sum()))

        qa = (
            monthly_legacy.groupby("chokepoint_name", as_index=False)
            .agg(
                months=("year_month", "nunique"),
                avg_vessels=("avg_n_total", "mean"),
                avg_capacity=("avg_capacity", "mean"),
                avg_coverage_days=("days_observed", "mean"),
            )
            .sort_values("avg_capacity", ascending=False)
        )
        logger.info("QA summary:\n%s", qa.to_string(index=False))

        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "completed",
                "finished_at": finished_at.isoformat(),
                "output_row_count": len(monthly_contract),
                "partitions_written": partitions_written,
                "output_counts": {
                    "monthly_legacy": len(monthly_legacy),
                    "monthly_contract": len(monthly_contract),
                    "dim_chokepoint": len(dim_chokepoint),
                    "dim_month": len(dim_month),
                    "scaffold": len(scaffold),
                },
            }
        )
        append_manifest(config.manifest_path, _json_ready(manifest_entry))
        logger.info("Manifest appended to %s", config.manifest_path)
        logger.info("Finished PortWatch silver build run_id=%s", run_id)
        return _json_ready(manifest_entry)
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "failed",
                "finished_at": finished_at.isoformat(),
                "error_summary": str(exc),
            }
        )
        append_manifest(config.manifest_path, _json_ready(manifest_entry))
        logger.exception("PortWatch silver build failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build PortWatch silver outputs from bronze parquet.")
    parser.add_argument("--start-date", type=str, default=None, help="Requested start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", type=str, default=None, help="Requested end date in YYYY-MM-DD.")
    parser.add_argument("--respect-requested-window", action="store_true", help="Apply the requested date window to the source rows before building silver outputs.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--log-to-file", action="store_true", help="Also write logs to disk.")
    parser.add_argument("--log-path", default=None, help="Optional log file path.")
    parser.add_argument("--manifest-path", default=None, help="Optional JSONL manifest path.")
    parser.add_argument("--no-stdout-logs", action="store_true", help="Disable console logging.")
    parser.add_argument("--contract-only", action="store_true", help="Write only the contract monthly output, not the legacy monthly files.")
    parser.add_argument("--legacy-only", action="store_true", help="Write only the legacy monthly files, not the contract monthly output.")
    parser.add_argument(
        "--chokepoint",
        action="append",
        default=None,
        help="Optional chokepoint override. Repeat the flag to pass multiple values.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.contract_only and args.legacy_only:
        parser.error("Choose either --contract-only or --legacy-only, not both.")

    config = PortWatchSilverConfig(
        target_chokepoints=tuple(args.chokepoint) if args.chokepoint else DEFAULT_TARGET_CHOKEPOINTS,
        write_legacy_outputs=not args.contract_only,
        write_contract_outputs=not args.legacy_only,
        log_level=args.log_level,
        log_to_stdout=not args.no_stdout_logs,
        log_to_file=args.log_to_file,
        log_path=Path(args.log_path) if args.log_path else None,
        manifest_path=Path(args.manifest_path) if args.manifest_path else None,
        respect_requested_window=args.respect_requested_window,
    )
    run(start_date=args.start_date, end_date=args.end_date, config=config)


if __name__ == "__main__":
    main()
