from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready


LOGGER_NAME = "events.silver"
LOG_DIR = PROJECT_ROOT / "logs" / "events"
LOG_PATH = LOG_DIR / "events_silver.log"
MANIFEST_PATH = LOG_DIR / "events_silver_manifest.jsonl"
PARTITION_FILENAME = "part-000.parquet"

CORE_CHOKEPOINTS = [
    "Bab el-Mandeb Strait",
    "Cape of Good Hope",
    "Panama Canal",
    "Strait of Hormuz",
    "Suez Canal",
]

EVENT_BLUEPRINTS: dict[str, dict[str, Any]] = {
    "ECO_001": {
        "event_type": "economic_policy",
        "event_scope": "trade_policy_regulatory",
        "source_class": "policy_regulatory",
        "base_severity": 0.60,
    },
    "ECO_002": {
        "event_type": "economic_policy",
        "event_scope": "trade_policy_regulatory",
        "source_class": "policy_regulatory",
        "base_severity": 0.60,
    },
    "ECO_003": {
        "event_type": "economic_policy",
        "event_scope": "trade_policy_regulatory",
        "source_class": "policy_regulatory",
        "base_severity": 0.60,
    },
    "ECO_004": {
        "event_type": "economic_policy",
        "event_scope": "trade_policy_regulatory",
        "source_class": "policy_regulatory",
        "base_severity": 0.60,
    },
    "EVT_001": {
        "event_type": "systemic_global",
        "event_scope": "systemic_macro_disruption",
        "source_class": "macro_systemic",
        "base_severity": 1.00,
        "global_event_flag": True,
        "core_links": [{"locations": CORE_CHOKEPOINTS, "link_role": "systemic_spillover", "phase_scope": "all"}],
    },
    "EVT_002": {
        "event_type": "physical_chokepoint",
        "event_scope": "chokepoint_disruption",
        "source_class": "maritime_disruption",
        "base_severity": 0.75,
        "core_links": [
            {"locations": ["Suez Canal"], "link_role": "primary_disruption", "phase_scope": "all"},
            {"locations": ["Cape of Good Hope"], "link_role": "reroute_spillover", "phase_scope": "lag_only"},
        ],
    },
    "EVT_003": {
        "event_type": "geopolitical",
        "event_scope": "systemic_macro_disruption",
        "source_class": "macro_systemic",
        "base_severity": 0.85,
        "global_event_flag": True,
        "core_links": [{"locations": ["Strait of Hormuz"], "link_role": "systemic_spillover", "phase_scope": "all"}],
        "region_links": [
            {"locations": ["Black Sea", "Turkish Straits"], "link_role": "primary_disruption", "phase_scope": "all"},
        ],
    },
    "EVT_004": {
        "event_type": "physical_chokepoint",
        "event_scope": "chokepoint_disruption",
        "source_class": "maritime_disruption",
        "base_severity": 0.75,
        "core_links": [{"locations": ["Panama Canal"], "link_role": "primary_disruption", "phase_scope": "all"}],
    },
    "EVT_005": {
        "event_type": "geopolitical",
        "event_scope": "chokepoint_disruption",
        "source_class": "maritime_disruption",
        "base_severity": 0.85,
        "core_links": [
            {"locations": ["Bab el-Mandeb Strait"], "link_role": "primary_disruption", "phase_scope": "all"},
            {"locations": ["Cape of Good Hope"], "link_role": "reroute_spillover", "phase_scope": "active_and_lag"},
            {"locations": ["Suez Canal"], "link_role": "secondary_spillover", "phase_scope": "active_and_lag"},
        ],
    },
    "EVT_006": {
        "event_type": "geopolitical",
        "event_scope": "chokepoint_disruption",
        "source_class": "maritime_disruption",
        "base_severity": 0.85,
        "core_links": [{"locations": ["Strait of Hormuz"], "link_role": "primary_disruption", "phase_scope": "all"}],
    },
    "EVT_007": {
        "event_type": "geopolitical",
        "event_scope": "chokepoint_disruption",
        "source_class": "maritime_disruption",
        "base_severity": 0.85,
        "core_links": [{"locations": ["Strait of Hormuz"], "link_role": "primary_disruption", "phase_scope": "all"}],
    },
    "EVT_008": {
        "event_type": "geopolitical",
        "event_scope": "chokepoint_disruption",
        "source_class": "maritime_disruption",
        "base_severity": 0.85,
        "core_links": [{"locations": ["Strait of Hormuz"], "link_role": "primary_disruption", "phase_scope": "all"}],
        "region_links": [{"locations": ["Gulf of Oman"], "link_role": "secondary_spillover", "phase_scope": "all"}],
    },
    "PHY_001": {
        "event_type": "physical_chokepoint",
        "event_scope": "chokepoint_disruption",
        "source_class": "maritime_disruption",
        "base_severity": 0.75,
        "region_links": [
            {"locations": ["Port of Baltimore"], "link_role": "primary_disruption", "phase_scope": "all"},
            {"locations": ["US East Coast"], "link_role": "secondary_spillover", "phase_scope": "all"},
        ],
    },
}

BRONZE_TO_SILVER_COLUMNS = {
    "Event_ID": "event_id",
    "Event_Name": "event_name",
    "Event_Category": "event_category",
    "Start_Date": "start_date",
    "End_Date": "end_date",
    "Lead_Months": "lead_months",
    "Lag_Months": "lag_months",
    "Primary_Chokepoint": "primary_chokepoint",
    "Secondary_Region": "secondary_region",
    "Description_Effects": "description",
}

BRIDGE_COLUMNS = [
    "event_id",
    "event_name",
    "year_month",
    "chokepoint_name",
    "event_phase",
    "event_active_flag",
    "lead_flag",
    "lag_flag",
    "severity_weight",
    "global_event_flag",
    "event_type",
    "event_scope",
    "link_role",
]


@dataclass(frozen=True)
class EventsSilverConfig:
    bronze_path: Path = PROJECT_ROOT / "data" / "bronze" / "events.csv"
    silver_root: Path = PROJECT_ROOT / "data" / "silver" / "events"
    dim_event_csv_path: Path = PROJECT_ROOT / "data" / "silver" / "events" / "dim_event.csv"
    dim_event_parquet_path: Path = PROJECT_ROOT / "data" / "silver" / "events" / "dim_event.parquet"
    core_csv_path: Path = PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_chokepoint_core.csv"
    region_csv_path: Path = PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_maritime_region.csv"
    core_partition_root: Path = PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_chokepoint_core"
    region_partition_root: Path = PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_maritime_region"
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH


def _last_completed_month_end(reference_date: date) -> date:
    month_start = reference_date.replace(day=1)
    return month_start - timedelta(days=1)


def _clean_string(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})


def load_bronze_events(bronze_path: Path, logger) -> pd.DataFrame:
    if not bronze_path.exists():
        raise FileNotFoundError(f"Events bronze file not found: {bronze_path}")
    logger.info("Loading events bronze CSV from %s", bronze_path)
    return pd.read_csv(bronze_path, dtype="string")


def prepare_dim_event(df: pd.DataFrame, *, as_of_date: date, logger) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = df.rename(columns=BRONZE_TO_SILVER_COLUMNS).copy()
    missing_columns = [column for column in BRONZE_TO_SILVER_COLUMNS.values() if column not in working.columns]
    if missing_columns:
        raise ValueError(f"Events bronze file is missing required columns: {missing_columns}")

    for column in working.columns:
        working[column] = _clean_string(working[column])

    working["event_id"] = working["event_id"].str.upper()
    working["lead_months"] = pd.to_numeric(working["lead_months"], errors="coerce").fillna(0).astype(int)
    working["lag_months"] = pd.to_numeric(working["lag_months"], errors="coerce").fillna(0).astype(int)
    working["start_date"] = pd.to_datetime(working["start_date"], errors="coerce").dt.date

    ongoing_mask = working["end_date"].fillna("").str.lower().eq("ongoing")
    working.loc[ongoing_mask, "end_date"] = as_of_date.isoformat()
    working["end_date"] = pd.to_datetime(working["end_date"], errors="coerce").dt.date

    if working["event_id"].duplicated().any():
        duplicated = sorted(working.loc[working["event_id"].duplicated(), "event_id"].dropna().tolist())
        raise ValueError(f"Duplicate event ids found in bronze events file: {duplicated}")

    missing_blueprints = sorted(set(working["event_id"].dropna()) - set(EVENT_BLUEPRINTS))
    if missing_blueprints:
        logger.warning("Events without explicit silver blueprints will use conservative defaults: %s", missing_blueprints)

    def _blueprint_for(event_id: str) -> dict[str, Any]:
        return EVENT_BLUEPRINTS.get(
            event_id,
            {
                "event_type": "geopolitical",
                "event_scope": "unscoped",
                "source_class": "manual_curated",
                "base_severity": 0.60,
            },
        )

    working["event_type"] = working["event_id"].map(lambda value: _blueprint_for(value)["event_type"])
    working["event_scope"] = working["event_id"].map(lambda value: _blueprint_for(value)["event_scope"])
    working["source_class"] = working["event_id"].map(lambda value: _blueprint_for(value)["source_class"])
    working["base_severity"] = working["event_id"].map(lambda value: _blueprint_for(value)["base_severity"]).astype(float)

    prepared = working[
        [
            "event_id",
            "event_name",
            "event_type",
            "event_scope",
            "start_date",
            "end_date",
            "lead_months",
            "lag_months",
            "base_severity",
            "description",
            "source_class",
        ]
    ].copy()
    prepared = prepared.sort_values("event_id").reset_index(drop=True)

    summary = {
        "event_count": int(len(prepared)),
        "ongoing_event_count": int(ongoing_mask.sum()),
        "start_month_min": min(prepared["start_date"]).isoformat() if not prepared.empty else None,
        "end_month_max": max(prepared["end_date"]).isoformat() if not prepared.empty else None,
    }
    return prepared, summary


def _month_sequence(start_month: pd.Period, end_month: pd.Period) -> list[pd.Period]:
    periods: list[pd.Period] = []
    current = start_month
    while current <= end_month:
        periods.append(current)
        current += 1
    return periods


def _phase_weight(base_severity: float, phase: str) -> float:
    if phase == "lead":
        return round(base_severity * 0.70, 2)
    if phase == "lag":
        return round(base_severity * 0.50, 2)
    return round(base_severity, 2)


def _phase_rows(event_row: pd.Series) -> list[dict[str, Any]]:
    start_month = pd.Period(event_row["start_date"], freq="M")
    end_month = pd.Period(event_row["end_date"], freq="M")
    lead_count = abs(int(event_row["lead_months"]))
    lag_count = max(int(event_row["lag_months"]), 0)

    rows: list[dict[str, Any]] = []
    for offset in range(lead_count, 0, -1):
        month_value = start_month - offset
        rows.append(
            {
                "year_month": str(month_value),
                "event_phase": "lead",
                "event_active_flag": 0,
                "lead_flag": 1,
                "lag_flag": 0,
                "severity_weight": _phase_weight(float(event_row["base_severity"]), "lead"),
            }
        )

    for month_value in _month_sequence(start_month, end_month):
        rows.append(
            {
                "year_month": str(month_value),
                "event_phase": "active",
                "event_active_flag": 1,
                "lead_flag": 0,
                "lag_flag": 0,
                "severity_weight": _phase_weight(float(event_row["base_severity"]), "active"),
            }
        )

    for offset in range(1, lag_count + 1):
        month_value = end_month + offset
        rows.append(
            {
                "year_month": str(month_value),
                "event_phase": "lag",
                "event_active_flag": 0,
                "lead_flag": 0,
                "lag_flag": 1,
                "severity_weight": _phase_weight(float(event_row["base_severity"]), "lag"),
            }
        )
    return rows


def _phase_included(phase_scope: str, event_phase: str) -> bool:
    if phase_scope == "all":
        return True
    if phase_scope == "lag_only":
        return event_phase == "lag"
    if phase_scope == "active_and_lag":
        return event_phase in {"active", "lag"}
    if phase_scope == "active_only":
        return event_phase == "active"
    return False


def build_bridge_frames(dim_event_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    core_rows: list[dict[str, Any]] = []
    region_rows: list[dict[str, Any]] = []

    for _, event_row in dim_event_df.iterrows():
        blueprint = EVENT_BLUEPRINTS.get(event_row["event_id"], {})
        phase_rows = _phase_rows(event_row)
        global_event_flag = 1 if blueprint.get("global_event_flag", False) else 0

        for phase_row in phase_rows:
            for link in blueprint.get("core_links", []):
                if not _phase_included(link["phase_scope"], phase_row["event_phase"]):
                    continue
                for location_name in link["locations"]:
                    core_rows.append(
                        {
                            "event_id": event_row["event_id"],
                            "event_name": event_row["event_name"],
                            "year_month": phase_row["year_month"],
                            "chokepoint_name": location_name,
                            "event_phase": phase_row["event_phase"],
                            "event_active_flag": phase_row["event_active_flag"],
                            "lead_flag": phase_row["lead_flag"],
                            "lag_flag": phase_row["lag_flag"],
                            "severity_weight": phase_row["severity_weight"],
                            "global_event_flag": global_event_flag,
                            "event_type": event_row["event_type"],
                            "event_scope": event_row["event_scope"],
                            "link_role": link["link_role"],
                        }
                    )

            for link in blueprint.get("region_links", []):
                if not _phase_included(link["phase_scope"], phase_row["event_phase"]):
                    continue
                for location_name in link["locations"]:
                    region_rows.append(
                        {
                            "event_id": event_row["event_id"],
                            "event_name": event_row["event_name"],
                            "year_month": phase_row["year_month"],
                            "chokepoint_name": location_name,
                            "event_phase": phase_row["event_phase"],
                            "event_active_flag": phase_row["event_active_flag"],
                            "lead_flag": phase_row["lead_flag"],
                            "lag_flag": phase_row["lag_flag"],
                            "severity_weight": phase_row["severity_weight"],
                            "global_event_flag": global_event_flag,
                            "event_type": event_row["event_type"],
                            "event_scope": event_row["event_scope"],
                            "link_role": link["link_role"],
                        }
                    )

    core_df = pd.DataFrame(core_rows, columns=BRIDGE_COLUMNS).sort_values(
        ["event_id", "year_month", "chokepoint_name"]
    )
    region_df = pd.DataFrame(region_rows, columns=BRIDGE_COLUMNS).sort_values(
        ["event_id", "year_month", "chokepoint_name"]
    )

    summary = {
        "core_bridge_row_count": int(len(core_df)),
        "region_bridge_row_count": int(len(region_df)),
        "core_bridge_event_count": int(core_df["event_id"].nunique()) if not core_df.empty else 0,
        "region_bridge_event_count": int(region_df["event_id"].nunique()) if not region_df.empty else 0,
        "bridge_month_min": min(
            [frame["year_month"].min() for frame in (core_df, region_df) if not frame.empty],
            default=None,
        ),
        "bridge_month_max": max(
            [frame["year_month"].max() for frame in (core_df, region_df) if not frame.empty],
            default=None,
        ),
    }
    return core_df.reset_index(drop=True), region_df.reset_index(drop=True), summary


def _reset_partition_root(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def write_partitioned_bridge(df: pd.DataFrame, output_root: Path) -> list[str]:
    _reset_partition_root(output_root)
    written_files: list[str] = []
    if df.empty:
        return written_files

    for year_month, partition_df in df.groupby("year_month", sort=True):
        partition_dir = output_root / f"year_month={year_month}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / PARTITION_FILENAME
        partition_df.to_parquet(output_path, index=False)
        written_files.append(str(output_path))
    return written_files


def run(config: EventsSilverConfig | None = None, *, as_of_date: date | None = None) -> dict[str, Any]:
    config = config or EventsSilverConfig()
    resolved_as_of_date = as_of_date or _last_completed_month_end(datetime.now(timezone.utc).date())

    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
        log_to_stdout=config.log_to_stdout,
    )

    run_id = build_run_id("events_silver")
    started_at = datetime.now(timezone.utc)
    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "events_silver",
        "dataset_name": "events",
        "started_at": started_at,
        "status": "started",
        "bronze_path": str(config.bronze_path),
        "silver_root": str(config.silver_root),
        "as_of_date": resolved_as_of_date.isoformat(),
        "log_path": str(config.log_path),
    }

    try:
        bronze_df = load_bronze_events(config.bronze_path, logger)
        dim_event_df, dim_summary = prepare_dim_event(bronze_df, as_of_date=resolved_as_of_date, logger=logger)
        core_bridge_df, region_bridge_df, bridge_summary = build_bridge_frames(dim_event_df)

        config.silver_root.mkdir(parents=True, exist_ok=True)
        dim_event_df.to_csv(config.dim_event_csv_path, index=False)
        dim_event_df.to_parquet(config.dim_event_parquet_path, index=False)
        core_bridge_df.to_csv(config.core_csv_path, index=False)
        region_bridge_df.to_csv(config.region_csv_path, index=False)

        core_partition_files = write_partitioned_bridge(core_bridge_df, config.core_partition_root)
        region_partition_files = write_partitioned_bridge(region_bridge_df, config.region_partition_root)

        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "success",
                "finished_at": finished_at,
                "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
                "dim_summary": dim_summary,
                "bridge_summary": bridge_summary,
                "bronze_row_count": int(len(bronze_df)),
                "dim_event_csv_path": str(config.dim_event_csv_path),
                "dim_event_parquet_path": str(config.dim_event_parquet_path),
                "core_csv_path": str(config.core_csv_path),
                "region_csv_path": str(config.region_csv_path),
                "core_partition_file_count": len(core_partition_files),
                "region_partition_file_count": len(region_partition_files),
                "core_partition_files": core_partition_files,
                "region_partition_files": region_partition_files,
            }
        )
        logger.info(
            "Built events silver outputs run_id=%s events=%s core_rows=%s region_rows=%s",
            run_id,
            len(dim_event_df),
            len(core_bridge_df),
            len(region_bridge_df),
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "failed",
                "finished_at": finished_at,
                "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }
        )
        logger.exception("Events silver build failed run_id=%s", run_id)
        append_manifest(config.manifest_path, manifest_entry)
        raise

    append_manifest(config.manifest_path, manifest_entry)
    return manifest_entry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated events silver outputs from the bronze events list.")
    parser.add_argument(
        "--as-of-date",
        help="Override the effective date used for Ongoing events. Defaults to the last completed month end in UTC.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--no-stdout-log", action="store_true", help="Disable stdout logging.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    as_of_date = date.fromisoformat(args.as_of_date) if args.as_of_date else None
    config = EventsSilverConfig(log_level=args.log_level, log_to_stdout=not args.no_stdout_log)
    result = run(config, as_of_date=as_of_date)
    print(json.dumps(json_ready(result), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
