from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.bronze_io import write_bronze_by_dt
from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready

# -------------------- IO CONFIG --------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", PROJECT_ROOT))
BRONZE_ROOT = os.getenv("BRONZE_ROOT", str(PROJECT_ROOT / "data" / "bronze"))
DATASET = os.getenv("DATASET", "ecb_fx_eu")  
SOURCE = os.getenv("SOURCE", "ECB")  
# -------------------- CONFIG --------------------
BASE_URL = "https://data-api.ecb.europa.eu/service"
DATAFLOW_ID = "EXR"
LOGGER_NAME = "fx.extract"
LOG_DIR = PROJECT_ROOT / "logs" / "fx"
LOG_PATH = LOG_DIR / "fx_extract.log"
MANIFEST_PATH = LOG_DIR / "fx_extract_manifest.jsonl"


@dataclass(frozen=True)
class FxExtractConfig:
    bronze_root: str = BRONZE_ROOT
    dataset: str = DATASET
    source: str = SOURCE
    log_level: str = "INFO"
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH



# -------------------- URL BUILDER --------------------
def build_fx_url(
    quote_ccys: List[str],  # e.g. ["USD","GBP","JPY"]
    base_ccy: str,          # e.g. "EUR"
    freq: str,              # "D" daily
    rate_type: str,         # "SP00"
    variation: str,         # "A"
    start: str,             # "2000-01-01"
    end: str,               # "2025-10-28"
    fmt: str = "jsondata",
) -> str:
    """
    Build ECB EXR URL using the SDMX key order:
    {freq}.{quote}.{base}.{rate_type}.{variation}
    Multiple quote currencies joined with '+'.
    """
    quote_key = "+".join([q.upper() for q in quote_ccys])
    series_key = f"{freq}.{quote_key}.{base_ccy}.{rate_type}.{variation}"

    url = (
        f"{BASE_URL}/data/{DATAFLOW_ID}/{series_key}"
        f"?startPeriod={start}"
        f"&endPeriod={end}"
        f"&format={fmt}"
    )
    return url


def fetch_fx_series(url: str) -> Dict[str, Any]:
    """
    Request the ECB API and return parsed JSON.
    Raises if non-200 or invalid JSON.
    """
    resp = requests.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"ECB API HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        # Helpful debugging if the ECB sends XML instead
        raise RuntimeError(
            f"Did not get JSON. content-type={resp.headers.get('content-type')}, body[:200]={resp.text[:200]}"
        ) from e


def extract_rows_final(payload: Dict[str, Any]) -> List[Tuple[str, str, str, float]]:
    """
    Convert ECB EXR SDMX-JSON payload into tidy rows:
    (date, quote_ccy, base_ccy, rate)

    Assumes payload shape like:
    - payload["dataSets"][0]["series"][series_key]["observations"]
    - payload["structure"]["dimensions"]["series"] (list of series dimensions)
    - payload["structure"]["dimensions"]["observation"][0]["values"] (time axis)
    This matches the actual response we inspected.
    """

    # 1. Get the series-level dimensions (FREQ, CURRENCY, CURRENCY_DENOM, EXR_TYPE, EXR_SUFFIX)
    structure = payload["structure"]
    series_dims = structure["dimensions"]["series"]  # list, ordered
    obs_dims = structure["dimensions"]["observation"]  # list with TIME_PERIOD

    series_dims_meta = []
    for dim in series_dims:
        dim_id = dim["id"]  # e.g. "CURRENCY"
        codes = [v["id"] for v in dim["values"]]  # e.g. ["GBP","JPY","USD"]
        series_dims_meta.append({
            "id": dim_id,
            "codes": codes,
        })

    # Find which dimension index corresponds to quote and base currencies
    quote_dim_idx = None
    base_dim_idx = None
    for idx, meta in enumerate(series_dims_meta):
        if meta["id"] == "CURRENCY":
            quote_dim_idx = idx
        if meta["id"] == "CURRENCY_DENOM":
            base_dim_idx = idx

    if quote_dim_idx is None or base_dim_idx is None:
        raise RuntimeError("Could not find currency dimensions in structure.")

    # 2. Build the time axis list
    # obs_dims[0] should define TIME_PERIOD; .values is list of {id: "2025-09-29", ...}
    time_values = [v["id"] for v in obs_dims[0]["values"]]

    # 3. Iterate over each series in dataSets[0].series
    series_dict = payload["dataSets"][0]["series"]

    rows: List[Tuple[str, str, str, float]] = []

    for series_key, series_body in series_dict.items():
        # series_key looks like "0:0:0:0:0"
        parts = series_key.split(":")

        # decode quote_ccy and base_ccy using the dimension maps
        quote_ccy_code = series_dims_meta[quote_dim_idx]["codes"][int(parts[quote_dim_idx])]
        base_ccy_code = series_dims_meta[base_dim_idx]["codes"][int(parts[base_dim_idx])]

        observations = series_body["observations"]
        # observations is dict: obs_index -> [value, ...]
        for obs_index_str, obs_value_arr in observations.items():
            obs_index = int(obs_index_str)
            obs_date = time_values[obs_index]
            value = obs_value_arr[0]  # the first element is the actual rate

            if value is None:
                continue

            rows.append(
                (obs_date, quote_ccy_code, base_ccy_code, float(value))
            )

    return rows
# -------------------- WRITE CSV --------------------

def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)

def write_csv(rows: List[Tuple[str, str, str, float]], out_path: str):
    """
    Save rows as a CSV with header and timestamp in the bronze directory.
    Columns: date, quote_ccy, base_ccy, rate, load_ts
    """
    # Generate timestamp
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    # Build full path to dataset directory
    dataset_dir = os.path.join(BRONZE_ROOT, DATASET)
    batch_dir = os.path.join(dataset_dir, "Batch")
    ensure_dir(batch_dir)
    full_path = os.path.join(batch_dir, f"ecb_fx_eu_{stamp}.csv")

    with open(full_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "quote_ccy", "base_ccy", "rate", "load_ts"])
        for date_str, quote_ccy, base_ccy, rate in rows:
            w.writerow([date_str, quote_ccy, base_ccy, rate, stamp])
    
    return full_path


def _normalize_currency_list(value: str) -> List[str]:
    currencies = [token.strip().upper() for token in value.split(",") if token.strip()]
    seen: set[str] = set()
    ordered: List[str] = []
    for currency in currencies:
        if currency in seen:
            continue
        seen.add(currency)
        ordered.append(currency)
    return ordered


def run(
    *,
    start_full: str,
    end_full: str,
    base_ccy: str,
    quote_ccys: list[str],
    freq: str,
    config: FxExtractConfig | None = None,
) -> dict[str, Any]:
    config = config or FxExtractConfig()
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
        log_to_stdout=config.log_to_stdout,
    )
    run_id = build_run_id("fx_extract")
    started_at = datetime.now(timezone.utc)
    rate_type = "SP00"
    variation = "A"

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "fx_extract",
        "dataset_name": "fx",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "start": start_full,
        "end": end_full,
        "base_ccy": base_ccy,
        "quote_ccys": quote_ccys,
        "freq": freq,
        "request_urls": [],
        "rows_written": None,
        "bronze_files": [],
        "reference_csv": None,
        "error_summary": None,
        "log_path": str(config.log_path),
        "manifest_path": str(config.manifest_path),
    }

    try:
        request_specs = [
            {
                "label": "canonical_eur_base",
                "quote_ccys": quote_ccys,
                "base_ccy": base_ccy,
            },
        ]

        rows: List[Tuple[str, str, str, float]] = []
        for spec in request_specs:
            url = build_fx_url(
                quote_ccys=spec["quote_ccys"],
                base_ccy=spec["base_ccy"],
                freq=freq,
                rate_type=rate_type,
                variation=variation,
                start=start_full,
                end=end_full,
            )
            logger.info("Requesting FX payload label=%s url=%s", spec["label"], url)
            manifest_entry["request_urls"].append(url)
            payload = fetch_fx_series(url)
            rows.extend(extract_rows_final(payload))

        rows.sort(key=lambda x: (x[0], x[2], x[1]))

        bronze_rows = []
        for dt_str, quote_ccy, raw_base_ccy, value in rows:
            bronze_rows.append(
                {
                    "dt": dt_str,
                    "dataset": config.dataset,
                    "series_id": f"EXR.D.{quote_ccy}.{raw_base_ccy}.{rate_type}.{variation}",
                    "geo": "EA20",
                    "value": float(value),
                    "source": config.source,
                    "ingest_ts": None,
                }
            )

        bronze_files = write_bronze_by_dt(bronze_rows, dataset=config.dataset, root=config.bronze_root)
        reference_path = write_csv(rows, "fx_history.csv")

        finished_at = datetime.now(timezone.utc)
        summary = {
            "run_id": run_id,
            "dataset": config.dataset,
            "rows": len(bronze_rows),
            "bronze_files": bronze_files,
            "reference_csv": reference_path,
            "bronze_root": config.bronze_root,
            "log_path": str(config.log_path),
            "manifest_path": str(config.manifest_path),
            "status": "completed",
        }
        manifest_entry.update(
            {
                "status": "completed",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
                "rows_written": len(bronze_rows),
                "bronze_files": bronze_files,
                "reference_csv": reference_path,
            }
        )
        append_manifest(config.manifest_path, manifest_entry)
        logger.info(
            "Finished FX extract run_id=%s rows=%s bronze_files=%s",
            run_id,
            len(bronze_rows),
            len(bronze_files),
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
        logger.exception("FX extract failed run_id=%s", run_id)
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="2020-01-01", help="Start date for FX series (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=date.today().isoformat(), help="End date for FX series (YYYY-MM-DD)")
    parser.add_argument("--base_ccy", type=str, default="EUR", help="Primary base currency for canonical FX ingest from the ECB reference rates feed (default: EUR)")
    parser.add_argument("--quote_ccys", type=str, default="USD,INR,TRY,ZAR,BRL,IDR,EGP,RUB,CNY", help="Comma-separated quote currencies fetched against the primary base currency.")
    parser.add_argument("--freq", type=str, default="D", help="Frequency of FX series (default: D)")
    args = parser.parse_args()

    result = run(
        start_full=args.start,
        end_full=args.end,
        base_ccy=args.base_ccy.strip().upper(),
        quote_ccys=_normalize_currency_list(args.quote_ccys),
        freq=args.freq,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
