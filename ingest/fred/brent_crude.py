from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready


load_dotenv()

DATASET = "brent"
SOURCE = "FRED"
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
DEFAULT_START_DATE = date(2020, 1, 1)
DEFAULT_TIMEOUT_SECONDS = 60

LOG_DIR = PROJECT_ROOT / "logs" / "brent"
LOG_PATH = LOG_DIR / "brent_extract.log"
MANIFEST_PATH = LOG_DIR / "brent_extract_manifest.jsonl"
LOGGER_NAME = "brent.extract"

BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", PROJECT_ROOT / "data" / "bronze"))
METADATA_ROOT = Path(os.getenv("METADATA_ROOT", PROJECT_ROOT / "data" / "metadata")) / DATASET
FRED_API_KEY = os.getenv("FRED_API_KEY")

BRENT_SERIES: dict[str, dict[str, str]] = {
    "BRENT_EU": {
        "region": "europe",
        "series_id": "DCOILBRENTEU",
        "series_name": "Crude Oil Prices: Brent - Europe",
    },
    "WTI_US": {
        "region": "us",
        "series_id": "DCOILWTICO",
        "series_name": "Crude Oil Prices: West Texas Intermediate",
    },
}


@dataclass(frozen=True)
class BrentExtractConfig:
    bronze_root: Path = BRONZE_ROOT
    metadata_root: Path = METADATA_ROOT
    fred_api_key: str | None = FRED_API_KEY
    request_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = 5
    retry_backoff_seconds: float = 1.5
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _fred_url(*, series_id: str, start_date: date, end_date: date, frequency: str, api_key: str | None) -> str:
    if not api_key:
        raise RuntimeError(
            "Missing FRED_API_KEY. Set it in your shell or local .env file before running Brent extract."
        )

    url = (
        f"{BASE_URL}"
        f"?series_id={series_id}"
        f"&api_key={api_key}"
        f"&file_type=json"
        f"&observation_start={start_date.isoformat()}"
        f"&observation_end={end_date.isoformat()}"
    )
    if frequency != "d":
        url += f"&frequency={frequency}"
    return url


def _chunk_windows(*, start_date: date, end_date: date, frequency: str) -> list[tuple[date, date]]:
    if frequency != "d":
        return [(start_date, end_date)]

    windows: list[tuple[date, date]] = []
    current_year = start_date.year
    while current_year <= end_date.year:
        chunk_start = max(start_date, date(current_year, 1, 1))
        chunk_end = min(end_date, date(current_year, 12, 31))
        windows.append((chunk_start, chunk_end))
        current_year += 1
    return windows


def _fetch_series_payload(
    *,
    session: requests.Session,
    series_id: str,
    chunk_start_date: date,
    chunk_end_date: date,
    frequency: str,
    timeout_seconds: int,
    api_key: str | None,
    max_retries: int,
    retry_backoff_seconds: float,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                _fred_url(
                    series_id=series_id,
                    start_date=chunk_start_date,
                    end_date=chunk_end_date,
                    frequency=frequency,
                    api_key=api_key,
                ),
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code is not None and status_code < 500:
                raise
        except requests.RequestException as exc:
            last_error = exc

        if attempt >= max_retries:
            break
        time.sleep(retry_backoff_seconds ** attempt)

    raise RuntimeError(
        f"FRED request failed for series_id={series_id} window={chunk_start_date}..{chunk_end_date} "
        f"after {max_retries} attempts: {last_error}"
    )


def _extract_rows(
    *,
    payload: dict[str, Any],
    benchmark_code: str,
    region: str,
    series_id: str,
    ingest_ts: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ingest_ts_text = ingest_ts.strftime("%Y%m%dT%H%M%SZ")

    for observation in payload.get("observations", []):
        dt_text = observation.get("date")
        value_text = observation.get("value")
        if not dt_text or not value_text or value_text == ".":
            continue
        try:
            price_value = float(value_text)
        except (TypeError, ValueError):
            continue

        rows.append(
            {
                "date": dt_text,
                "benchmark_code": benchmark_code,
                "region": region,
                "series_id": series_id,
                "price_usd": price_value,
                "load_ts": ingest_ts_text,
            }
        )
    return rows


def _csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    buffer = StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=["date", "benchmark_code", "region", "series_id", "price_usd", "load_ts"],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _md5_hexdigest(payload: bytes) -> str:
    return hashlib.md5(payload).hexdigest()


def _write_if_changed(path: Path, payload: bytes) -> tuple[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_checksum = _md5_hexdigest(payload)
    existed_before = path.exists()
    if existed_before:
        existing_checksum = hashlib.md5(path.read_bytes()).hexdigest()
        if existing_checksum == new_checksum:
            return "skipped_unchanged", new_checksum

    path.write_bytes(payload)
    return ("updated" if existed_before else "written"), new_checksum


def write_partitioned_bronze(rows: list[dict[str, Any]], bronze_root: Path) -> tuple[list[str], dict[str, int], list[dict[str, str]]]:
    rows_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_date.setdefault(str(row["date"]), []).append(row)

    actions = {"written": 0, "updated": 0, "skipped_unchanged": 0}
    files_written: list[str] = []
    file_checksums: list[dict[str, str]] = []

    for dt_text, grouped_rows in sorted(rows_by_date.items()):
        dt_value = date.fromisoformat(dt_text)
        output_path = (
            bronze_root
            / DATASET
            / f"year={dt_value.year:04d}"
            / f"month={dt_value.month:02d}"
            / f"day={dt_value.day:02d}"
            / f"brent_prices_{dt_value.strftime('%Y%m%d')}.csv"
        )
        payload = _csv_bytes(sorted(grouped_rows, key=lambda row: str(row["benchmark_code"])))
        action, checksum = _write_if_changed(output_path, payload)
        actions[action] += 1
        files_written.append(str(output_path))
        file_checksums.append({"path": str(output_path), "md5_hex": checksum, "action": action})

    return files_written, actions, file_checksums


def write_batch_snapshot(rows: list[dict[str, Any]], bronze_root: Path, ingest_ts: datetime) -> tuple[str, str]:
    output_path = bronze_root / DATASET / "Batch" / f"brent_crude_{ingest_ts.strftime('%Y%m%dT%H%M%SZ')}.csv"
    payload = _csv_bytes(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload)
    return str(output_path), _md5_hexdigest(payload)


def write_metadata_bundle(
    *,
    config: BrentExtractConfig,
    run_id: str,
    payload: dict[str, Any],
) -> str:
    config.metadata_root.mkdir(parents=True, exist_ok=True)
    output_path = config.metadata_root / f"brent_extract_metadata_{run_id}.json"
    output_path.write_text(json.dumps(json_ready(payload), indent=2), encoding="utf-8")
    return str(output_path)


def run_extract(
    *,
    start_date: date,
    end_date: date,
    frequency: str,
    config: BrentExtractConfig | None = None,
) -> dict[str, Any]:
    config = config or BrentExtractConfig()
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
        log_to_stdout=config.log_to_stdout,
    )
    run_id = build_run_id("brent_extract")
    started_at = datetime.now(timezone.utc)
    ingest_ts = started_at

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "brent_extract",
        "dataset_name": DATASET,
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        "frequency": frequency,
        "requested_benchmarks": sorted(BRENT_SERIES.keys()),
        "series_results": {},
        "total_observations": 0,
        "files_written": [],
        "write_actions": {},
        "file_checksums": [],
        "batch_snapshot_path": None,
        "batch_snapshot_md5_hex": None,
        "metadata_path": None,
        "error_summary": None,
        "log_path": str(config.log_path),
        "manifest_path": str(config.manifest_path),
    }

    try:
        logger.info("Starting Brent extract run_id=%s", run_id)
        logger.info("Requested window=%s -> %s frequency=%s", start_date, end_date, frequency)

        all_rows: list[dict[str, Any]] = []
        series_results: dict[str, dict[str, Any]] = {}

        with requests.Session() as session:
            for benchmark_code, metadata in BRENT_SERIES.items():
                series_id = metadata["series_id"]
                logger.info("Fetching Brent benchmark=%s series_id=%s", benchmark_code, series_id)
                observations: list[dict[str, Any]] = []
                chunk_windows = _chunk_windows(
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency,
                )
                logger.info(
                    "Benchmark=%s will be requested in %s chunk(s)",
                    benchmark_code,
                    len(chunk_windows),
                )
                for chunk_start_date, chunk_end_date in chunk_windows:
                    logger.info(
                        "Fetching benchmark=%s chunk=%s..%s",
                        benchmark_code,
                        chunk_start_date.isoformat(),
                        chunk_end_date.isoformat(),
                    )
                    payload = _fetch_series_payload(
                        session=session,
                        series_id=series_id,
                        chunk_start_date=chunk_start_date,
                        chunk_end_date=chunk_end_date,
                        frequency=frequency,
                        timeout_seconds=config.request_timeout_seconds,
                        api_key=config.fred_api_key,
                        max_retries=config.max_retries,
                        retry_backoff_seconds=config.retry_backoff_seconds,
                    )
                    observations.extend(payload.get("observations", []))
                payload = {"observations": observations}
                rows = _extract_rows(
                    payload=payload,
                    benchmark_code=benchmark_code,
                    region=metadata["region"],
                    series_id=series_id,
                    ingest_ts=ingest_ts,
                )
                all_rows.extend(rows)
                series_results[benchmark_code] = {
                    "series_id": series_id,
                    "series_name": metadata["series_name"],
                    "region": metadata["region"],
                    "observation_count": len(rows),
                    "observation_start": rows[0]["date"] if rows else None,
                    "observation_end": rows[-1]["date"] if rows else None,
                }

        all_rows = sorted(all_rows, key=lambda row: (str(row["date"]), str(row["benchmark_code"])))
        if not all_rows:
            raise RuntimeError("Brent extract returned no observations for the requested window.")

        files_written, write_actions, file_checksums = write_partitioned_bronze(all_rows, config.bronze_root)
        batch_snapshot_path, batch_snapshot_md5 = write_batch_snapshot(all_rows, config.bronze_root, ingest_ts)
        metadata_path = write_metadata_bundle(
            config=config,
            run_id=run_id,
            payload={
                "run_id": run_id,
                "dataset_name": DATASET,
                "source": SOURCE,
                "started_at": started_at,
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "frequency": frequency,
                "series_results": series_results,
                "total_observations": len(all_rows),
                "sample_rows": all_rows[:10],
            },
        )

        finished_at = datetime.now(timezone.utc)
        summary = {
            "run_id": run_id,
            "status": "completed",
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "frequency": frequency,
            "total_observations": len(all_rows),
            "series_results": series_results,
            "write_actions": write_actions,
            "files_written": len(files_written),
            "batch_snapshot_path": batch_snapshot_path,
            "metadata_path": metadata_path,
            "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "log_path": str(config.log_path),
            "manifest_path": str(config.manifest_path),
        }

        manifest_entry.update(
            {
                "status": "completed",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": summary["duration_seconds"],
                "series_results": series_results,
                "total_observations": len(all_rows),
                "files_written": files_written,
                "write_actions": write_actions,
                "file_checksums": file_checksums,
                "batch_snapshot_path": batch_snapshot_path,
                "batch_snapshot_md5_hex": batch_snapshot_md5,
                "metadata_path": metadata_path,
            }
        )
        append_manifest(config.manifest_path, manifest_entry)
        logger.info(
            "Finished Brent extract run_id=%s total_observations=%s written=%s updated=%s skipped_unchanged=%s",
            run_id,
            len(all_rows),
            write_actions["written"],
            write_actions["updated"],
            write_actions["skipped_unchanged"],
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
        logger.exception("Brent extract failed run_id=%s", run_id)
        raise


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract Brent and WTI crude price observations from FRED into bronze partitions.")
    parser.add_argument("--start", default=DEFAULT_START_DATE.isoformat(), help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", default=date.today().isoformat(), help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--freq",
        default="d",
        choices=("d", "w", "m"),
        help="FRED observation frequency: d=daily, w=weekly, m=monthly.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = run_extract(
        start_date=_parse_date(args.start),
        end_date=_parse_date(args.end),
        frequency=args.freq,
        config=BrentExtractConfig(log_level=args.log_level),
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
