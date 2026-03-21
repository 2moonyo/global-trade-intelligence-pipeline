from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests


@dataclass(frozen=True)
class PortWatchConfig:
    base_dir: Path = Path("data")
    timeout_s: int = 60
    max_retries: int = 5
    retry_backoff_s: float = 1.6
    page_size: int = 2000  # ArcGIS paging

    # ArcGIS Feature Services (PortWatch is built on ArcGIS REST)
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
            r = session.get(url, params=params, timeout=cfg.timeout_s)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = cfg.retry_backoff_s ** attempt
            logging.warning("Request failed (%s/%s): %s. Sleeping %.1fs",
                            attempt, cfg.max_retries, str(e), sleep_s)
            time.sleep(sleep_s)
    raise RuntimeError(f"Failed after {cfg.max_retries} retries: {last_err}")


def fetch_chokepoints_lookup(cfg: PortWatchConfig) -> pd.DataFrame:
    params = {"where": "1=1", "outFields": "*", "outSR": "4326", "f": "json"}
    with requests.Session() as s:
        payload = _request_json(s, cfg.chokepoints_lookup_url, params, cfg)
    rows = [f["attributes"] for f in payload.get("features", [])]
    return pd.DataFrame(rows)


def fetch_daily_for_date(
    chokepoint_ids: Iterable[str],
    one_day: date,
    cfg: PortWatchConfig,
) -> pd.DataFrame:
    """
    Pull daily rows for a specific date (UTC) for given chokepoints.
    We filter client-side by date to avoid ArcGIS date-query quirks.
    """
    chokepoint_ids = list(chokepoint_ids)
    if not chokepoint_ids:
        raise ValueError("Provide at least one chokepoint id (e.g. 'chokepoint1').")

    quoted = ",".join([f"'{x}'" for x in chokepoint_ids])
    where = f"portid IN ({quoted})"

    params_base = {
        "where": where,
        "outFields": "*",
        "outSR": "4326",
        "f": "json",
        "resultRecordCount": cfg.page_size,
    }

    all_rows: list[dict] = []
    offset = 0

    with requests.Session() as s:
        while True:
            params = dict(params_base)
            params["resultOffset"] = offset
            payload = _request_json(s, cfg.daily_chokepoints_url, params, cfg)

            features = payload.get("features", [])
            if not features:
                break

            rows = [f["attributes"] for f in features]
            all_rows.extend(rows)

            exceeded = payload.get("exceededTransferLimit", False)
            if not exceeded and len(rows) < cfg.page_size:
                break
            offset += cfg.page_size

    df = pd.DataFrame(all_rows)

    # ArcGIS epoch-ms → datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True).dt.tz_convert(None)

    # Filter to the single target day
    df = df[df["date"].dt.date == one_day].copy()

    # Add partition columns (storage layout only)
    df["year"] = one_day.year
    df["month"] = f"{one_day.month:02d}"
    df["day"] = f"{one_day.day:02d}"

    return df.reset_index(drop=True)


def write_bronze_day(df: pd.DataFrame, cfg: PortWatchConfig) -> Optional[Path]:
    if df.empty:
        return None

    y = int(df["year"].iloc[0])
    m = str(df["month"].iloc[0])
    d = str(df["day"].iloc[0])

    out_dir = cfg.base_dir / "bronze" / "portwatch" / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "portwatch_chokepoints_daily.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def run_portwatch_bronze(
    start: date,
    end: date,
    chokepoint_names: Iterable[str],
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = PortWatchConfig()

    # 1) Lookup → metadata
    metadata_dir = cfg.base_dir / "metadata" / "portwatch"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    lookup = fetch_chokepoints_lookup(cfg)
    lookup_path = metadata_dir / "chokepoints_lookup.parquet"
    lookup.to_parquet(lookup_path, index=False)
    logging.info("Saved chokepoint lookup: %s (%s rows)", lookup_path, len(lookup))

    # 2) Resolve names → ids
    sel = lookup[lookup["portname"].isin(list(chokepoint_names))].copy()
    if sel.empty:
        raise RuntimeError("None of your chokepoint_names matched lookup['portname'].")

    chokepoint_ids = sel["portid"].tolist()
    logging.info("Selected chokepoints: %s", dict(zip(sel["portname"], sel["portid"])))

    # 3) Daily → Bronze partitions
    day = start
    while day <= end:
        df_day = fetch_daily_for_date(chokepoint_ids, day, cfg)
        out = write_bronze_day(df_day, cfg)
        logging.info("Day %s: rows=%s saved=%s", day.isoformat(), len(df_day), out)
        day += timedelta(days=1)


if __name__ == "__main__":
    run_portwatch_bronze(
        start=date(2020, 1, 1),
        end=date.today(),
        chokepoint_names=[
            "Suez Canal",
            "Bab el-Mandeb Strait",
            "Cape of Good Hope",
            "Panama Canal",
            "Strait of Hormuz",
            "Strait of Malacca"

        ],
    )