from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready

LOG_DIR = PROJECT_ROOT / "logs" / "portwatch"
LOG_PATH = LOG_DIR / "portwatch_extract.log"
MANIFEST_PATH = LOG_DIR / "portwatch_extract_manifest.jsonl"
LOGGER_NAME = "portwatch.extract"
DEFAULT_EXTRACT_CHOKEPOINTS = (
    "Suez Canal",
    "Bab el-Mandeb Strait",
    "Cape of Good Hope",
    "Panama Canal",
    "Strait of Hormuz",
    "Strait of Malacca",
)


@dataclass(frozen=True)
class PortWatchConfig:
    base_dir: Path = PROJECT_ROOT / "data"
    timeout_s: int = 60
    max_retries: int = 5
    retry_backoff_s: float = 1.6
    page_size: int = 2000
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH

    chokepoints_lookup_url: str = (
        "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
        "PortWatch_chokepoints_database/FeatureServer/0/query"
    )
    daily_chokepoints_url: str = (
        "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
        "Daily_Chokepoints_Data/FeatureServer/0/query"
    )


def _request_json(session: requests.Session, url: str, params: dict, cfg: PortWatchConfig) -> dict:
    last_err: Optional[Exception] = None
    for attempt in range(1, cfg.max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=cfg.timeout_s)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_err = exc
            sleep_s = cfg.retry_backoff_s ** attempt
            time.sleep(sleep_s)
    raise RuntimeError(f"Failed after {cfg.max_retries} retries: {last_err}")


def fetch_chokepoints_lookup(cfg: PortWatchConfig) -> pd.DataFrame:
    params = {"where": "1=1", "outFields": "*", "outSR": "4326", "f": "json"}
    with requests.Session() as session:
        payload = _request_json(session, cfg.chokepoints_lookup_url, params, cfg)
    rows = [feature["attributes"] for feature in payload.get("features", [])]
    return pd.DataFrame(rows)


def fetch_daily_for_date(
    chokepoint_ids: Iterable[str],
    one_day: date,
    cfg: PortWatchConfig,
) -> pd.DataFrame:
    chokepoint_ids = list(chokepoint_ids)
    if not chokepoint_ids:
        raise ValueError("Provide at least one chokepoint id (for example 'chokepoint1').")

    quoted = ",".join([f"'{value}'" for value in chokepoint_ids])
    day_start = datetime(one_day.year, one_day.month, one_day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    day_start_ms = int(day_start.timestamp() * 1000)
    day_end_ms = int(day_end.timestamp() * 1000)

    # Apply day-level filtering in the ArcGIS query itself to avoid repeatedly
    # paging across a mostly recent global record set and missing older history.
    where = f"portid IN ({quoted}) AND date >= {day_start_ms} AND date < {day_end_ms}"

    params_base = {
        "where": where,
        "outFields": "*",
        "outSR": "4326",
        "f": "json",
        "resultRecordCount": cfg.page_size,
    }

    all_rows: list[dict] = []
    offset = 0

    with requests.Session() as session:
        while True:
            params = dict(params_base)
            params["resultOffset"] = offset
            payload = _request_json(session, cfg.daily_chokepoints_url, params, cfg)

            features = payload.get("features", [])
            if not features:
                break

            rows = [feature["attributes"] for feature in features]
            all_rows.extend(rows)

            exceeded = payload.get("exceededTransferLimit", False)
            if not exceeded and len(rows) < cfg.page_size:
                break
            offset += cfg.page_size

    df = pd.DataFrame(all_rows)
    if df.empty:
        return pd.DataFrame(
            columns=["date", "portid", "portname", "year", "month", "day"]
        )

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True).dt.tz_convert(None)

    df = df[df["date"].dt.date == one_day].copy()
    df["year"] = one_day.year
    df["month"] = f"{one_day.month:02d}"
    df["day"] = f"{one_day.day:02d}"
    return df.reset_index(drop=True)


def write_bronze_day(df: pd.DataFrame, cfg: PortWatchConfig) -> Optional[Path]:
    if df.empty:
        return None

    year_value = int(df["year"].iloc[0])
    month_value = str(df["month"].iloc[0])
    day_value = str(df["day"].iloc[0])

    out_dir = cfg.base_dir / "bronze" / "portwatch" / f"year={year_value}" / f"month={month_value}" / f"day={day_value}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "portwatch_chokepoints_daily.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def _monthly_summary(day_metrics: list[dict[str, object]]) -> list[dict[str, object]]:
    monthly: dict[str, dict[str, object]] = {}
    for metric in day_metrics:
        year_month = str(metric["year_month"])
        row_count = int(metric["row_count"])
        entry = monthly.setdefault(
            year_month,
            {
                "year_month": year_month,
                "row_count": 0,
                "days_processed": 0,
                "days_with_rows": 0,
                "null_days": 0,
            },
        )
        entry["row_count"] = int(entry["row_count"]) + row_count
        entry["days_processed"] = int(entry["days_processed"]) + 1
        if row_count > 0:
            entry["days_with_rows"] = int(entry["days_with_rows"]) + 1
        else:
            entry["null_days"] = int(entry["null_days"]) + 1
    return [monthly[key] for key in sorted(monthly)]


def run_portwatch_bronze(
    start: date,
    end: date,
    chokepoint_names: Iterable[str],
    cfg: Optional[PortWatchConfig] = None,
) -> dict[str, object]:
    cfg = cfg or PortWatchConfig()
    run_id = build_run_id("portwatch_extract")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=cfg.log_path,
        log_level=cfg.log_level,
        log_to_stdout=cfg.log_to_stdout,
    )
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "portwatch_extract",
        "dataset_name": "portwatch",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "requested_start_date": start.isoformat(),
        "requested_end_date": end.isoformat(),
        "requested_chokepoints": list(chokepoint_names),
        "selected_chokepoints": [],
        "selected_regions": {
            "countries": [],
            "iso3": [],
            "continents": [],
        },
        "lookup_path": None,
        "processed_dates": [],
        "dates_with_rows": [],
        "null_dates": [],
        "day_metrics": [],
        "monthly_row_counts": [],
        "total_rows_extracted": 0,
        "files_written": [],
        "duration_seconds": None,
        "error_summary": None,
        "log_path": str(cfg.log_path),
        "manifest_path": str(cfg.manifest_path),
    }

    try:
        logger.info("Starting PortWatch extract run_id=%s", run_id)
        logger.info("Requested window %s -> %s", start.isoformat(), end.isoformat())

        metadata_dir = cfg.base_dir / "metadata" / "portwatch"
        metadata_dir.mkdir(parents=True, exist_ok=True)

        lookup = fetch_chokepoints_lookup(cfg)
        lookup_path = metadata_dir / "chokepoints_lookup.parquet"
        lookup.to_parquet(lookup_path, index=False)
        manifest_entry["lookup_path"] = str(lookup_path)
        logger.info("Saved chokepoint lookup: %s (%s rows)", lookup_path, len(lookup))

        selected_lookup = lookup[lookup["portname"].isin(list(chokepoint_names))].copy()
        if selected_lookup.empty:
            raise RuntimeError("None of your chokepoint_names matched lookup['portname'].")

        chokepoint_ids = selected_lookup["portid"].tolist()
        selected_cols = [
            column
            for column in ["portid", "portname", "country", "ISO3", "continent", "fullname"]
            if column in selected_lookup.columns
        ]
        selected_records = selected_lookup[selected_cols].to_dict(orient="records")
        manifest_entry["selected_chokepoints"] = selected_records
        manifest_entry["selected_regions"] = {
            "countries": sorted({str(value) for value in selected_lookup.get("country", pd.Series(dtype="object")).dropna().tolist()}),
            "iso3": sorted({str(value) for value in selected_lookup.get("ISO3", pd.Series(dtype="object")).dropna().tolist()}),
            "continents": sorted({str(value) for value in selected_lookup.get("continent", pd.Series(dtype="object")).dropna().tolist()}),
        }
        logger.info("Selected chokepoints: %s", {record.get("portname"): record.get("portid") for record in selected_records})

        day_metrics: list[dict[str, object]] = []
        processed_dates: list[str] = []
        dates_with_rows: list[str] = []
        null_dates: list[str] = []
        files_written: list[str] = []
        total_rows = 0

        day = start
        while day <= end:
            day_started = time.perf_counter()
            df_day = fetch_daily_for_date(chokepoint_ids, day, cfg)
            out_path = write_bronze_day(df_day, cfg)
            elapsed_s = round(time.perf_counter() - day_started, 3)
            row_count = int(len(df_day))
            year_month = day.strftime("%Y-%m")

            processed_dates.append(day.isoformat())
            if row_count > 0:
                dates_with_rows.append(day.isoformat())
                total_rows += row_count
            else:
                null_dates.append(day.isoformat())

            if out_path is not None:
                files_written.append(str(out_path))

            metric = {
                "date": day.isoformat(),
                "year_month": year_month,
                "row_count": row_count,
                "elapsed_seconds": elapsed_s,
                "output_path": str(out_path) if out_path else None,
                "portnames_present": sorted(df_day["portname"].dropna().unique().tolist()) if "portname" in df_day.columns else [],
            }
            day_metrics.append(metric)
            logger.info(
                "Day %s rows=%s null=%s elapsed_s=%.3f saved=%s",
                day.isoformat(),
                row_count,
                row_count == 0,
                elapsed_s,
                out_path,
            )
            day += timedelta(days=1)

        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "completed",
                "finished_at": finished_at.isoformat(),
                "processed_dates": processed_dates,
                "dates_with_rows": dates_with_rows,
                "null_dates": null_dates,
                "day_metrics": day_metrics,
                "monthly_row_counts": _monthly_summary(day_metrics),
                "total_rows_extracted": total_rows,
                "files_written": files_written,
                "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            }
        )
        append_manifest(cfg.manifest_path, manifest_entry)
        logger.info(
            "Finished PortWatch extract run_id=%s processed_days=%s days_with_rows=%s null_days=%s total_rows=%s",
            run_id,
            len(processed_dates),
            len(dates_with_rows),
            len(null_dates),
            total_rows,
        )
        return json_ready(manifest_entry)
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
        append_manifest(cfg.manifest_path, manifest_entry)
        logger.exception("PortWatch extract failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract PortWatch daily chokepoint data into bronze parquet partitions.")
    parser.add_argument("--start-date", default="2020-01-01", help="Start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="End date in YYYY-MM-DD.")
    parser.add_argument(
        "--chokepoint",
        action="append",
        default=None,
        help="Optional chokepoint name override. Repeat for multiple values.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--no-stdout-logs", action="store_true", help="Disable console logging.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    cfg = PortWatchConfig(
        log_level=args.log_level,
        log_to_stdout=not args.no_stdout_logs,
    )
    run_portwatch_bronze(
        start=date.fromisoformat(args.start_date),
        end=date.fromisoformat(args.end_date),
        chokepoint_names=tuple(args.chokepoint) if args.chokepoint else DEFAULT_EXTRACT_CHOKEPOINTS,
        cfg=cfg,
    )


if __name__ == "__main__":
    main()
