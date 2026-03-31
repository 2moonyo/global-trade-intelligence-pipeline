"""
Local extractor for selected World Bank energy indicators.

Writes:
1. Bronze-style partitioned JSONL by dt
2. A batch CSV snapshot for each run
3. PortWatch-style run log + JSONL manifest
4. A run-level metadata JSON bundle under data/metadata/worldbank_energy

Examples
--------
# Default run: database countries, all energy types, existing database years
python ingest/world_bank/worldbank_energy.py

# Specific countries with a custom year range
python ingest/world_bank/worldbank_energy.py extract --selector country-codes --members NLD,USA,CHN --energy-types oil,gas --start-year 2010 --end-year 2024

# World and regional aggregates
python ingest/world_bank/worldbank_energy.py extract --selector country-codes --members WLD,ECS,LCN --energy-types all

# Member countries from World Bank regions
python ingest/world_bank/worldbank_energy.py extract --selector region-members --members ECS,LCN --energy-types renewable,imports

# Only years not already present in the local database
python ingest/world_bank/worldbank_energy.py extract --year-mode missing-from-db
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready

load_dotenv()

DATASET = os.getenv("DATASET", "worldbank_energy")
SOURCE = os.getenv("SOURCE", "WorldBank")
BASE_URL = os.getenv("WORLD_BANK_BASE_URL", "https://api.worldbank.org/v2")

BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", PROJECT_ROOT / "data" / "bronze"))
METADATA_ROOT = Path(os.getenv("METADATA_ROOT", PROJECT_ROOT / "data" / "metadata"))
WORLD_BANK_METADATA_ROOT = METADATA_ROOT / "worldbank_energy"

LOG_DIR = PROJECT_ROOT / "logs" / "worldbank_energy"
LOG_PATH = LOG_DIR / "worldbank_energy_extract.log"
MANIFEST_PATH = LOG_DIR / "worldbank_energy_extract_manifest.jsonl"
LOGGER_NAME = "worldbank_energy.extract"

DEFAULT_START_YEAR = 1960
DEFAULT_USER_AGENT = "worldbank-energy-extractor/2.0"


@dataclass(frozen=True)
class SelectorSpec:
    requires_members: bool
    description: str


@dataclass(frozen=True)
class WorldBankEnergyConfig:
    base_url: str = BASE_URL
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "60"))
    request_sleep_secs: float = float(os.getenv("REQUEST_SLEEP_SECS", "0.15"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "5"))
    retry_backoff_base_secs: float = float(os.getenv("RETRY_BACKOFF_BASE_SECS", "2.0"))
    per_page: int = int(os.getenv("WORLD_BANK_PER_PAGE", "20000"))
    log_level: str = os.getenv("WORLD_BANK_LOG_LEVEL", "INFO")
    log_to_stdout: bool = True
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH
    bronze_root: Path = BRONZE_ROOT
    metadata_root: Path = WORLD_BANK_METADATA_ROOT
    duckdb_path: Path = PROJECT_ROOT / "warehouse" / "analytics.duckdb"
    dim_country_parquet_path: Path = PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_country.parquet"
    energy_batch_glob: str = str(PROJECT_ROOT / "data" / "bronze" / "worldbank_energy" / "Batch" / "*.csv")
    user_agent: str = DEFAULT_USER_AGENT


INDICATORS: dict[str, dict[str, str]] = {
    "renew": {
        "indicator_id": "EG.FEC.RNEW.ZS",
        "series_name": "Renewable energy consumption (% of total final energy consumption)",
        "metric_name": "renewables_share",
        "unit_hint": "percent",
    },
    "fossil": {
        "indicator_id": "EG.USE.COMM.FO.ZS",
        "series_name": "Fossil fuel energy consumption (% of total)",
        "metric_name": "fossil_fuels_share",
        "unit_hint": "percent",
    },
    "imports": {
        "indicator_id": "EG.IMP.CONS.ZS",
        "series_name": "Energy imports (% of energy use)",
        "metric_name": "dependency_on_imported_energy",
        "unit_hint": "percent",
    },
    "oil": {
        "indicator_id": "EG.ELC.PETR.ZS",
        "series_name": "Electricity production from oil sources (% of total)",
        "metric_name": "oil_electricity_share",
        "unit_hint": "percent",
    },
    "gas": {
        "indicator_id": "EG.ELC.NGAS.ZS",
        "series_name": "Electricity production from natural gas sources (% of total)",
        "metric_name": "gas_electricity_share",
        "unit_hint": "percent",
    },
    "coal": {
        "indicator_id": "EG.ELC.COAL.ZS",
        "series_name": "Electricity production from coal sources (% of total)",
        "metric_name": "coal_electricity_share",
        "unit_hint": "percent",
    },
}

ALL_ENERGY_TYPES_ALIAS = "all"
ENERGY_TYPE_SYNONYMS = {
    "all": "all",
    "renew": "renew",
    "renewable": "renew",
    "renewables": "renew",
    "fossil": "fossil",
    "imports": "imports",
    "import": "imports",
    "oil": "oil",
    "gas": "gas",
    "coal": "coal",
}

SELECTORS: dict[str, SelectorSpec] = {
    "db-countries": SelectorSpec(
        requires_members=False,
        description="All ISO3 countries available in the local database country dimension.",
    ),
    "all-countries": SelectorSpec(
        requires_members=False,
        description="World Bank country/all selection across all countries and aggregates.",
    ),
    "country-codes": SelectorSpec(
        requires_members=True,
        description="Specific World Bank country or aggregate codes, for example NLD,USA,WLD,ECS.",
    ),
    "region-members": SelectorSpec(
        requires_members=True,
        description="All member countries inside one or more World Bank regions, for example ECS,LCN,MEA.",
    ),
    "income-members": SelectorSpec(
        requires_members=True,
        description="All member countries inside one or more World Bank income groups, for example HIC,LMC.",
    ),
    "lending-members": SelectorSpec(
        requires_members=True,
        description="All member countries inside one or more World Bank lending groups, for example IBD,IDX.",
    ),
}

SELECTOR_ALIASES = {
    "db": "db-countries",
    "world": "all-countries",
    "country": "country-codes",
    "region": "region-members",
    "income": "income-members",
    "lending": "lending-members",
}

GROUP_SELECTOR_PREFIXES = {
    "region-members": "region",
    "income-members": "incomeLevel",
    "lending-members": "lendingType",
}


class PartitionedJsonlWriter:
    """Streams rows directly to dt partitions without keeping all rows in memory."""

    def __init__(self, root: Path, dataset: str, stamp: str):
        self.root = root
        self.dataset = dataset
        self.stamp = stamp
        self.filepaths_by_dt: dict[str, Path] = {}

    def write_row(self, row: dict[str, Any]) -> None:
        dt = str(row["dt"])
        if dt not in self.filepaths_by_dt:
            part_dir = self.root / self.dataset / f"dt={dt}"
            ensure_dir(part_dir)
            self.filepaths_by_dt[dt] = part_dir / f"part-{self.stamp}.jsonl"

        out_path = self.filepaths_by_dt[dt]
        with out_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def files_written(self) -> list[str]:
        return [str(path) for _, path in sorted(self.filepaths_by_dt.items())]


class BatchCsvWriter:
    """Streams normalized rows to one batch CSV for the run."""

    def __init__(self, root: Path, dataset: str, stamp: str):
        self.path = root / dataset / "Batch" / f"{dataset}_{stamp}.csv"
        ensure_dir(self.path.parent)
        self._fieldnames = [
            "dt",
            "year",
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
            "value",
            "wb_unit",
            "obs_status",
            "decimal_places",
        ]
        self._handle = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._handle, fieldnames=self._fieldnames)
        self._writer.writeheader()

    def write_row(self, row: dict[str, Any]) -> None:
        self._writer.writerow(row)

    def close(self) -> None:
        self._handle.close()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_csv_values(value: Optional[str]) -> list[str]:
    if not value:
        return []
    cleaned = [item.strip().upper() for item in value.split(",") if item.strip()]
    seen: set[str] = set()
    ordered: list[str] = []
    for item in cleaned:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def resolve_selector_name(selector: str) -> str:
    selector_key = selector.strip().lower()
    selector_key = SELECTOR_ALIASES.get(selector_key, selector_key)
    if selector_key not in SELECTORS:
        valid = sorted(set(SELECTORS) | set(SELECTOR_ALIASES))
        raise argparse.ArgumentTypeError(f"Unknown selector '{selector}'. Valid values: {valid}")
    return selector_key


def resolve_indicator_aliases(value: str) -> list[str]:
    raw_aliases = [item.strip().lower() for item in value.split(",") if item.strip()]
    if not raw_aliases:
        raw_aliases = [ALL_ENERGY_TYPES_ALIAS]

    resolved: list[str] = []
    for alias in raw_aliases:
        canonical = ENERGY_TYPE_SYNONYMS.get(alias)
        if canonical is None:
            valid = sorted(set(ENERGY_TYPE_SYNONYMS) | {ALL_ENERGY_TYPES_ALIAS})
            raise argparse.ArgumentTypeError(f"Unknown energy type '{alias}'. Valid values: {valid}")
        if canonical == ALL_ENERGY_TYPES_ALIAS:
            return list(INDICATORS.keys())
        if canonical not in resolved:
            resolved.append(canonical)
    return resolved


def normalise_country_record(raw: dict[str, Any]) -> Optional[dict[str, Any]]:
    code = str(raw.get("id") or raw.get("iso3") or raw.get("country_iso3") or "").strip().upper()
    if not code:
        return None

    region = raw.get("region")
    if isinstance(region, dict):
        region = region.get("value")

    admin_region = raw.get("adminregion")
    if isinstance(admin_region, dict):
        admin_region = admin_region.get("value")

    income_level = raw.get("incomeLevel")
    if isinstance(income_level, dict):
        income_level = income_level.get("value")

    return {
        "country_iso3": code,
        "country_name": raw.get("name") or raw.get("country_name"),
        "region": region,
        "admin_region": admin_region,
        "income_level": income_level,
        "lending_type": (raw.get("lendingType") or {}).get("value") if isinstance(raw.get("lendingType"), dict) else raw.get("lendingType"),
        "subregion": raw.get("subregion"),
        "continent": raw.get("continent"),
    }


def dedupe_countries(countries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in countries:
        record = normalise_country_record(row)
        if record is None:
            continue
        deduped[record["country_iso3"]] = record
    return [deduped[key] for key in sorted(deduped)]


def _retry_sleep_seconds(resp: Optional[requests.Response], cfg: WorldBankEnergyConfig, attempt: int) -> float:
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return max(1.0, float(retry_after))
            except Exception:
                pass
    return cfg.retry_backoff_base_secs * (2 ** (attempt - 1))


def request_json(
    session: requests.Session,
    url: str,
    cfg: WorldBankEnergyConfig,
    logger,
    job_label: str,
) -> Any:
    last_error: Optional[Exception] = None

    for attempt in range(1, cfg.max_retries + 1):
        try:
            response = session.get(url, timeout=cfg.request_timeout)

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == cfg.max_retries:
                    raise RuntimeError(
                        f"World Bank API HTTP {response.status_code} after {cfg.max_retries} attempts: {url}"
                    )
                wait_s = _retry_sleep_seconds(response, cfg, attempt)
                logger.warning(
                    "Retrying %s after HTTP %s on attempt %s/%s wait=%.1fs",
                    job_label,
                    response.status_code,
                    attempt,
                    cfg.max_retries,
                    wait_s,
                )
                time.sleep(wait_s)
                continue

            if response.status_code != 200:
                raise RuntimeError(f"World Bank API HTTP {response.status_code}: {response.text[:500]}")

            return response.json()

        except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            last_error = exc
            if attempt == cfg.max_retries:
                raise RuntimeError(
                    f"Network timeout or connection failure for {job_label} after {cfg.max_retries} attempts: {url}"
                ) from exc
            wait_s = _retry_sleep_seconds(None, cfg, attempt)
            logger.warning(
                "Retrying %s after network error on attempt %s/%s wait=%.1fs",
                job_label,
                attempt,
                cfg.max_retries,
                wait_s,
            )
            time.sleep(wait_s)

    raise RuntimeError(f"Failed to request World Bank payload for {job_label}: {url}") from last_error


def fetch_paginated_rows(
    session: requests.Session,
    url: str,
    cfg: WorldBankEnergyConfig,
    logger,
    job_label: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    all_rows: list[dict[str, Any]] = []
    first_meta: dict[str, Any] = {}
    current_url = url

    while True:
        payload = request_json(session=session, url=current_url, cfg=cfg, logger=logger, job_label=job_label)
        if not isinstance(payload, list) or len(payload) < 2:
            raise RuntimeError(f"Unexpected World Bank payload shape for {job_label}: {payload}")

        meta = payload[0] or {}
        rows = payload[1] or []

        if isinstance(meta, dict) and "message" in meta:
            raise RuntimeError(f"World Bank API returned an error for {job_label}: {meta['message']}")

        if not first_meta:
            first_meta = dict(meta)
        all_rows.extend(rows)

        current_page = int(meta.get("page", 1) or 1)
        total_pages = int(meta.get("pages", 1) or 1)
        if current_page >= total_pages:
            break

        next_page = current_page + 1
        separator = "&" if "?" in url else "?"
        current_url = f"{url}{separator}page={next_page}"
        time.sleep(cfg.request_sleep_secs)

    return first_meta, all_rows


def build_indicator_url(
    entity_path: str,
    indicator_id: str,
    start_year: int,
    end_year: int,
    cfg: WorldBankEnergyConfig,
) -> str:
    return (
        f"{cfg.base_url}/{entity_path}/indicator/{indicator_id}"
        f"?format=json&per_page={cfg.per_page}&date={start_year}:{end_year}"
    )


def normalise_row(raw: dict[str, Any], dataset_alias: str, load_ts: str) -> Optional[dict[str, Any]]:
    year_raw = raw.get("date")
    if year_raw in (None, ""):
        return None

    try:
        year_value = int(year_raw)
    except Exception:
        return None

    meta = INDICATORS[dataset_alias]
    value = raw.get("value")

    return {
        "dt": f"{year_value:04d}-01-01",
        "year": year_value,
        "dataset": DATASET,
        "source": SOURCE,
        "ingest_ts": load_ts,
        "indicator_alias": dataset_alias,
        "indicator_id": meta["indicator_id"],
        "indicator_name": meta["series_name"],
        "metric_name": meta["metric_name"],
        "unit_hint": meta["unit_hint"],
        "country_name": (raw.get("country") or {}).get("value"),
        "country_id": (raw.get("country") or {}).get("id"),
        "country_iso3": raw.get("countryiso3code") or None,
        "value": float(value) if value is not None else None,
        "wb_unit": raw.get("unit"),
        "obs_status": raw.get("obs_status"),
        "decimal_places": raw.get("decimal"),
    }


def build_wide_preview(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, int], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["country_iso3"]), str(row["country_name"]), int(row["year"]))
        if key not in grouped:
            grouped[key] = {
                "country_iso3": row["country_iso3"],
                "country_name": row["country_name"],
                "year": row["year"],
            }
        grouped[key][row["metric_name"]] = row["value"]
    return list(grouped.values())


def load_db_countries(cfg: WorldBankEnergyConfig) -> list[dict[str, Any]]:
    if cfg.duckdb_path.exists():
        try:
            with duckdb.connect(str(cfg.duckdb_path), read_only=True) as con:
                rows = con.execute(
                    """
                    select
                      upper(trim(iso3)) as country_iso3,
                      country_name,
                      region,
                      cast(null as varchar) as admin_region,
                      cast(null as varchar) as income_level,
                      cast(null as varchar) as lending_type,
                      subregion,
                      continent
                    from raw.dim_country
                    where iso3 is not null
                    order by 1
                    """
                ).fetchdf()
            return rows.to_dict(orient="records")
        except Exception:
            pass

    if cfg.dim_country_parquet_path.exists():
        with duckdb.connect(":memory:") as con:
            rows = con.execute(
                """
                select
                  upper(trim(iso3)) as country_iso3,
                  country_name,
                  region,
                  cast(null as varchar) as admin_region,
                  cast(null as varchar) as income_level,
                  cast(null as varchar) as lending_type,
                  subregion,
                  continent
                from read_parquet(?)
                where iso3 is not null
                order by 1
                """,
                [str(cfg.dim_country_parquet_path)],
            ).fetchdf()
        return rows.to_dict(orient="records")

    raise RuntimeError("Could not resolve local database countries from DuckDB or dim_country parquet.")


def load_existing_db_years(cfg: WorldBankEnergyConfig) -> list[int]:
    if cfg.duckdb_path.exists():
        try:
            with duckdb.connect(str(cfg.duckdb_path), read_only=True) as con:
                rows = con.execute(
                    """
                    select distinct cast(year as integer) as year
                    from raw.energy_vulnerability
                    where year is not null
                    order by 1
                    """
                ).fetchall()
            return [int(row[0]) for row in rows if row and row[0] is not None]
        except Exception:
            pass

    batch_dir = Path(cfg.energy_batch_glob).parent
    if batch_dir.exists():
        with duckdb.connect(":memory:") as con:
            rows = con.execute(
                """
                select distinct cast(year as integer) as year
                from read_csv_auto(?, header=true)
                where year is not null
                order by 1
                """,
                [cfg.energy_batch_glob],
            ).fetchall()
        return [int(row[0]) for row in rows if row and row[0] is not None]

    return []


def resolve_requested_window(
    start_year: Optional[int],
    end_year: Optional[int],
    year_mode: str,
    existing_db_years: list[int],
) -> tuple[int, int]:
    current_year = datetime.now().year
    if existing_db_years and year_mode in {"existing-db", "missing-from-db"}:
        resolved_start = start_year if start_year is not None else min(existing_db_years)
        resolved_end = end_year if end_year is not None else max(existing_db_years)
    else:
        resolved_start = start_year if start_year is not None else DEFAULT_START_YEAR
        resolved_end = end_year if end_year is not None else current_year

    if resolved_start > resolved_end:
        raise argparse.ArgumentTypeError("--start-year cannot be greater than --end-year")

    return resolved_start, resolved_end


def resolve_years_to_write(
    start_year: int,
    end_year: int,
    year_mode: str,
    existing_db_years: list[int],
) -> tuple[list[int], str]:
    requested_years = list(range(start_year, end_year + 1))
    existing_year_set = set(existing_db_years)

    if year_mode == "requested":
        return requested_years, year_mode

    if not existing_db_years:
        return requested_years, "requested_fallback_no_existing_db_years"

    if year_mode == "existing-db":
        return [year for year in requested_years if year in existing_year_set], year_mode

    if year_mode == "missing-from-db":
        return [year for year in requested_years if year not in existing_year_set], year_mode

    raise argparse.ArgumentTypeError(f"Unsupported year mode: {year_mode}")


def fetch_country_rows(session: requests.Session, entity_path: str, cfg: WorldBankEnergyConfig, logger, job_label: str) -> list[dict[str, Any]]:
    url = f"{cfg.base_url}/{entity_path}?format=json&per_page=400"
    _, rows = fetch_paginated_rows(session=session, url=url, cfg=cfg, logger=logger, job_label=job_label)
    return rows


def fetch_world_bank_country_catalog(
    session: requests.Session,
    cfg: WorldBankEnergyConfig,
    logger,
) -> list[dict[str, Any]]:
    rows = fetch_country_rows(
        session=session,
        entity_path="country",
        cfg=cfg,
        logger=logger,
        job_label="world_bank_country_catalog",
    )
    catalog = dedupe_countries(rows)
    return [row for row in catalog if str(row.get("region") or "").strip().lower() != "aggregates"]


def resolve_selection(
    selector: str,
    members: Optional[str],
    cfg: WorldBankEnergyConfig,
    logger,
    session: requests.Session,
) -> dict[str, Any]:
    selector_name = resolve_selector_name(selector)
    selector_spec = SELECTORS[selector_name]
    requested_members = parse_csv_values(members)

    if selector_spec.requires_members and not requested_members:
        raise argparse.ArgumentTypeError(f"--members is required for selector='{selector_name}'")

    if selector_name == "db-countries":
        local_countries = dedupe_countries(load_db_countries(cfg))
        world_bank_catalog = fetch_world_bank_country_catalog(
            session=session,
            cfg=cfg,
            logger=logger,
        )
        valid_world_bank_codes = {row["country_iso3"] for row in world_bank_catalog}
        countries = [row for row in local_countries if row["country_iso3"] in valid_world_bank_codes]
        excluded_countries = [row for row in local_countries if row["country_iso3"] not in valid_world_bank_codes]
        country_codes = [row["country_iso3"] for row in countries]
        if not country_codes:
            raise RuntimeError("No valid World Bank country codes were resolved from the local database country dimension.")
        entity_path = "country/" + ";".join(country_codes)
        return {
            "selector": selector_name,
            "requested_members": requested_members,
            "resolved_members": country_codes,
            "entity_path": entity_path,
            "resolved_countries": countries,
            "excluded_countries": excluded_countries,
            "selection_source": "local_database_dim_country",
        }

    if selector_name == "all-countries":
        countries = dedupe_countries(
            fetch_country_rows(
                session=session,
                entity_path="country/all",
                cfg=cfg,
                logger=logger,
                job_label="resolve_all_countries",
            )
        )
        return {
            "selector": selector_name,
            "requested_members": requested_members,
            "resolved_members": ["ALL"],
            "entity_path": "country/all",
            "resolved_countries": countries,
            "excluded_countries": [],
            "selection_source": "world_bank_country_all",
        }

    if selector_name == "country-codes":
        entity_path = "country/" + ";".join(requested_members)
        countries = dedupe_countries(
            fetch_country_rows(
                session=session,
                entity_path=entity_path,
                cfg=cfg,
                logger=logger,
                job_label="resolve_country_codes",
            )
        )
        return {
            "selector": selector_name,
            "requested_members": requested_members,
            "resolved_members": requested_members,
            "entity_path": entity_path,
            "resolved_countries": countries,
            "excluded_countries": [],
            "selection_source": "world_bank_country_codes",
        }

    group_prefix = GROUP_SELECTOR_PREFIXES[selector_name]
    countries: list[dict[str, Any]] = []
    for member_code in requested_members:
        entity_path = f"{group_prefix}/{member_code}/country"
        member_rows = fetch_country_rows(
            session=session,
            entity_path=entity_path,
            cfg=cfg,
            logger=logger,
            job_label=f"resolve_{selector_name}_{member_code}",
        )
        countries.extend(member_rows)

    deduped_countries = dedupe_countries(countries)
    country_codes = [row["country_iso3"] for row in deduped_countries]
    if not country_codes:
        raise RuntimeError(f"No countries were resolved for selector '{selector_name}' and members {requested_members}")

    return {
        "selector": selector_name,
        "requested_members": requested_members,
        "resolved_members": country_codes,
        "entity_path": "country/" + ";".join(country_codes),
        "resolved_countries": deduped_countries,
        "excluded_countries": [],
        "selection_source": f"world_bank_{group_prefix}",
    }


def fetch_indicator_api_metadata(
    session: requests.Session,
    indicator_id: str,
    cfg: WorldBankEnergyConfig,
    logger,
) -> dict[str, Any]:
    url = f"{cfg.base_url}/indicator/{indicator_id}?format=json"
    _, rows = fetch_paginated_rows(
        session=session,
        url=url,
        cfg=cfg,
        logger=logger,
        job_label=f"indicator_metadata_{indicator_id}",
    )
    if not rows:
        return {}
    return rows[0]


def write_metadata_bundle(
    *,
    run_id: str,
    cfg: WorldBankEnergyConfig,
    selector_summary: dict[str, Any],
    requested_start_year: int,
    requested_end_year: int,
    year_mode_requested: str,
    year_mode_effective: str,
    existing_db_years: list[int],
    years_to_write: list[int],
    aliases: list[str],
    indicator_metadata: dict[str, dict[str, Any]],
    request_urls: list[dict[str, str]],
) -> Path:
    ensure_dir(cfg.metadata_root)
    metadata_path = cfg.metadata_root / f"worldbank_energy_api_metadata_{run_id}.json"

    payload = {
        "run_id": run_id,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "source_system": {
            "name": "World Bank Indicators API v2",
            "base_url": cfg.base_url,
            "response_format": "json",
            "country_metadata_template": f"{cfg.base_url}/country/<codes>?format=json&per_page=400",
            "indicator_metadata_template": f"{cfg.base_url}/indicator/<indicator_id>?format=json",
            "indicator_data_template": (
                f"{cfg.base_url}/country/<codes>/indicator/<indicator_id>"
                f"?format=json&per_page={cfg.per_page}&date=<start_year>:<end_year>"
            ),
        },
        "how_to_extract": {
            "selector_reference": {
                key: {
                    "requires_members": spec.requires_members,
                    "description": spec.description,
                }
                for key, spec in SELECTORS.items()
            },
            "selection_used": {
                "selector": selector_summary["selector"],
                "selection_source": selector_summary["selection_source"],
                "requested_members": selector_summary["requested_members"],
                "resolved_members": selector_summary["resolved_members"],
                "entity_path": selector_summary["entity_path"],
            },
            "year_window": {
                "requested_start_year": requested_start_year,
                "requested_end_year": requested_end_year,
                "year_mode_requested": year_mode_requested,
                "year_mode_effective": year_mode_effective,
                "existing_db_years": existing_db_years,
                "years_to_write": years_to_write,
            },
            "energy_types": aliases,
        },
        "selected_countries": selector_summary["resolved_countries"],
        "excluded_countries": selector_summary.get("excluded_countries", []),
        "selected_indicators": [
            {
                "alias": alias,
                **INDICATORS[alias],
                "api_metadata": indicator_metadata.get(alias, {}),
            }
            for alias in aliases
        ],
        "request_urls": request_urls,
    }

    metadata_path.write_text(json.dumps(json_ready(payload), indent=2, ensure_ascii=False), encoding="utf-8")
    return metadata_path


def run_extract(
    *,
    energy_types: str,
    selector: str,
    members: Optional[str],
    start_year: Optional[int],
    end_year: Optional[int],
    year_mode: str,
    write_wide_preview: bool,
    cfg: Optional[WorldBankEnergyConfig] = None,
) -> dict[str, Any]:
    cfg = cfg or WorldBankEnergyConfig()
    run_id = build_run_id("worldbank_energy_extract")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=cfg.log_path,
        log_level=cfg.log_level,
        log_to_stdout=cfg.log_to_stdout,
    )

    started_at = datetime.now(timezone.utc)
    aliases = resolve_indicator_aliases(energy_types)
    existing_db_years = load_existing_db_years(cfg)
    requested_start_year, requested_end_year = resolve_requested_window(
        start_year=start_year,
        end_year=end_year,
        year_mode=year_mode,
        existing_db_years=existing_db_years,
    )
    years_to_write, effective_year_mode = resolve_years_to_write(
        start_year=requested_start_year,
        end_year=requested_end_year,
        year_mode=year_mode,
        existing_db_years=existing_db_years,
    )

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "worldbank_energy_extract",
        "dataset_name": DATASET,
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "requested_start_year": requested_start_year,
        "requested_end_year": requested_end_year,
        "requested_year_mode": year_mode,
        "effective_year_mode": effective_year_mode,
        "existing_db_years": existing_db_years,
        "years_to_write": years_to_write,
        "requested_selector": resolve_selector_name(selector),
        "requested_members": parse_csv_values(members),
        "requested_energy_types": aliases,
        "selected_energy_types": [],
        "country_count": 0,
        "countries_included": [],
        "selection_country_count": 0,
        "selection_countries": [],
        "selection_excluded_countries": [],
        "selection_source": None,
        "entity_path": None,
        "indicator_run_details": [],
        "year_row_counts": [],
        "total_rows_extracted": 0,
        "bronze_files_written": [],
        "batch_csv_path": None,
        "wide_preview_path": None,
        "wide_preview_rows": 0,
        "metadata_path": None,
        "duration_seconds": None,
        "error_summary": None,
        "log_path": str(cfg.log_path),
        "manifest_path": str(cfg.manifest_path),
    }

    load_ts = utc_now_stamp()
    run_stamp = utc_now_stamp()
    year_filter = set(years_to_write)
    request_urls: list[dict[str, str]] = []
    indicator_api_metadata: dict[str, dict[str, Any]] = {}
    preview_rows: list[dict[str, Any]] = []

    batch_writer = BatchCsvWriter(root=cfg.bronze_root, dataset=DATASET, stamp=run_stamp)
    partition_writer = PartitionedJsonlWriter(root=cfg.bronze_root, dataset=DATASET, stamp=run_stamp)

    try:
        logger.info("Starting World Bank energy extract run_id=%s", run_id)
        logger.info(
            "Requested selector=%s members=%s energy_types=%s years=%s:%s year_mode=%s",
            selector,
            members,
            aliases,
            requested_start_year,
            requested_end_year,
            year_mode,
        )

        with requests.Session() as session:
            session.headers.update({"User-Agent": cfg.user_agent})

            selection_summary = resolve_selection(
                selector=selector,
                members=members,
                cfg=cfg,
                logger=logger,
                session=session,
            )
            manifest_entry["selected_energy_types"] = aliases
            manifest_entry["selection_source"] = selection_summary["selection_source"]
            manifest_entry["entity_path"] = selection_summary["entity_path"]
            manifest_entry["selection_country_count"] = len(selection_summary["resolved_countries"])
            manifest_entry["selection_countries"] = selection_summary["resolved_countries"]
            manifest_entry["selection_excluded_countries"] = selection_summary.get("excluded_countries", [])

            logger.info(
                "Resolved selector=%s entity_path=%s country_count=%s excluded_count=%s",
                selection_summary["selector"],
                selection_summary["entity_path"],
                len(selection_summary["resolved_countries"]),
                len(selection_summary.get("excluded_countries", [])),
            )

            countries_seen: dict[str, str] = {}
            year_counter: Counter[int] = Counter()
            total_rows = 0

            if not years_to_write:
                logger.warning("No years resolved to write after applying year_mode=%s", year_mode)

            for alias in aliases:
                indicator_id = INDICATORS[alias]["indicator_id"]
                indicator_url = build_indicator_url(
                    entity_path=selection_summary["entity_path"],
                    indicator_id=indicator_id,
                    start_year=requested_start_year,
                    end_year=requested_end_year,
                    cfg=cfg,
                )
                request_urls.append(
                    {
                        "alias": alias,
                        "indicator_id": indicator_id,
                        "url": indicator_url,
                    }
                )

                logger.info("Fetching alias=%s indicator_id=%s", alias, indicator_id)
                page_meta, raw_rows = fetch_paginated_rows(
                    session=session,
                    url=indicator_url,
                    cfg=cfg,
                    logger=logger,
                    job_label=f"extract_{alias}",
                )

                indicator_api_metadata[alias] = fetch_indicator_api_metadata(
                    session=session,
                    indicator_id=indicator_id,
                    cfg=cfg,
                    logger=logger,
                )

                alias_rows = 0
                for raw in raw_rows:
                    row = normalise_row(raw=raw, dataset_alias=alias, load_ts=load_ts)
                    if row is None:
                        continue
                    if int(row["year"]) not in year_filter:
                        continue

                    partition_writer.write_row(row)
                    batch_writer.write_row(row)
                    total_rows += 1
                    alias_rows += 1
                    year_counter[int(row["year"])] += 1

                    country_iso3 = str(row.get("country_iso3") or "").upper()
                    country_name = str(row.get("country_name") or "")
                    if country_iso3:
                        countries_seen[country_iso3] = country_name

                    if write_wide_preview:
                        preview_rows.append(row)

                manifest_entry["indicator_run_details"].append(
                    {
                        "alias": alias,
                        "indicator_id": indicator_id,
                        "rows_written": alias_rows,
                        "api_total": page_meta.get("total"),
                        "api_pages": page_meta.get("pages"),
                        "request_url": indicator_url,
                    }
                )
                logger.info("Finished alias=%s rows_written=%s", alias, alias_rows)

            metadata_path = write_metadata_bundle(
                run_id=run_id,
                cfg=cfg,
                selector_summary=selection_summary,
                requested_start_year=requested_start_year,
                requested_end_year=requested_end_year,
                year_mode_requested=year_mode,
                year_mode_effective=effective_year_mode,
                existing_db_years=existing_db_years,
                years_to_write=years_to_write,
                aliases=aliases,
                indicator_metadata=indicator_api_metadata,
                request_urls=request_urls,
            )

            manifest_entry["country_count"] = len(countries_seen)
            manifest_entry["countries_included"] = [
                {"country_iso3": iso3, "country_name": countries_seen[iso3]}
                for iso3 in sorted(countries_seen)
            ]
            manifest_entry["year_row_counts"] = [
                {"year": year, "row_count": count}
                for year, count in sorted(year_counter.items())
            ]
            manifest_entry["total_rows_extracted"] = total_rows
            manifest_entry["metadata_path"] = str(metadata_path)
            manifest_entry["bronze_files_written"] = partition_writer.files_written()
            manifest_entry["batch_csv_path"] = str(batch_writer.path)

            output: dict[str, Any] = {
                "run_id": run_id,
                "run_status": "completed",
                "dataset_root": str(cfg.bronze_root / DATASET),
                "rows": total_rows,
                "energy_types": aliases,
                "selector": selection_summary["selector"],
                "members": selection_summary["requested_members"],
                "selection_country_count": len(selection_summary["resolved_countries"]),
                "country_count_with_data": len(countries_seen),
                "requested_start_year": requested_start_year,
                "requested_end_year": requested_end_year,
                "requested_year_mode": year_mode,
                "effective_year_mode": effective_year_mode,
                "years_to_write": years_to_write,
                "bronze_files_written": partition_writer.files_written(),
                "batch_csv": str(batch_writer.path),
                "metadata_path": str(metadata_path),
                "log_path": str(cfg.log_path),
                "manifest_path": str(cfg.manifest_path),
                "project_root": str(PROJECT_ROOT),
            }

            if write_wide_preview and preview_rows:
                preview_dir = cfg.bronze_root / DATASET / "Preview"
                ensure_dir(preview_dir)
                preview_path = preview_dir / f"worldbank_energy_wide_{utc_now_stamp()}.csv"
                wide_rows = build_wide_preview(preview_rows)
                fieldnames = sorted({key for row in wide_rows for key in row.keys()})
                with preview_path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(wide_rows)

                manifest_entry["wide_preview_path"] = str(preview_path)
                manifest_entry["wide_preview_rows"] = len(wide_rows)
                output["wide_preview_csv"] = str(preview_path)
                output["wide_preview_rows"] = len(wide_rows)

        manifest_entry["status"] = "completed"
        manifest_entry["finished_at"] = datetime.now(timezone.utc).isoformat()
        manifest_entry["duration_seconds"] = round(
            (datetime.now(timezone.utc) - started_at).total_seconds(), 3
        )
        append_manifest(cfg.manifest_path, manifest_entry)
        logger.info(
            "Finished World Bank energy extract run_id=%s total_rows=%s countries=%s",
            run_id,
            manifest_entry["total_rows_extracted"],
            manifest_entry["country_count"],
        )
        return json_ready(output)

    except Exception as exc:
        manifest_entry["status"] = "failed"
        manifest_entry["finished_at"] = datetime.now(timezone.utc).isoformat()
        manifest_entry["duration_seconds"] = round(
            (datetime.now(timezone.utc) - started_at).total_seconds(), 3
        )
        manifest_entry["error_summary"] = str(exc)
        manifest_entry["bronze_files_written"] = partition_writer.files_written()
        manifest_entry["batch_csv_path"] = str(batch_writer.path)
        append_manifest(cfg.manifest_path, manifest_entry)
        logger.exception("World Bank energy extract failed run_id=%s", run_id)
        raise

    finally:
        batch_writer.close()


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(json_ready(payload), indent=2, ensure_ascii=False))


def handle_list_indicators() -> None:
    print_json(
        {
            "energy_types": {
                alias: {
                    "indicator_id": cfg["indicator_id"],
                    "metric_name": cfg["metric_name"],
                    "series_name": cfg["series_name"],
                }
                for alias, cfg in INDICATORS.items()
            },
            "synonyms": ENERGY_TYPE_SYNONYMS,
        }
    )


def handle_list_selectors() -> None:
    print_json(
        {
            "selectors": {
                name: {
                    "requires_members": spec.requires_members,
                    "description": spec.description,
                }
                for name, spec in SELECTORS.items()
            },
            "selector_aliases": SELECTOR_ALIASES,
        }
    )


def handle_check_indicators(energy_types: str, cfg: Optional[WorldBankEnergyConfig] = None) -> None:
    cfg = cfg or WorldBankEnergyConfig()
    logger = configure_logger(
        logger_name=f"{LOGGER_NAME}.check",
        log_path=cfg.log_path,
        log_level=cfg.log_level,
        log_to_stdout=False,
    )
    aliases = resolve_indicator_aliases(energy_types)

    results: list[dict[str, Any]] = []
    with requests.Session() as session:
        session.headers.update({"User-Agent": cfg.user_agent})
        for alias in aliases:
            indicator_id = INDICATORS[alias]["indicator_id"]
            try:
                metadata = fetch_indicator_api_metadata(
                    session=session,
                    indicator_id=indicator_id,
                    cfg=cfg,
                    logger=logger,
                )
                is_valid = bool(metadata)
                error = None
            except Exception as exc:
                metadata = {}
                is_valid = False
                error = str(exc)

            results.append(
                {
                    "alias": alias,
                    "indicator_id": indicator_id,
                    "series_name": INDICATORS[alias]["series_name"],
                    "metric_name": INDICATORS[alias]["metric_name"],
                    "is_valid": is_valid,
                    "error": error,
                    "api_metadata": metadata,
                }
            )

    print_json({"results": results})


def handle_search_indicators(contains: str, limit: int, cfg: Optional[WorldBankEnergyConfig] = None) -> None:
    cfg = cfg or WorldBankEnergyConfig()
    logger = configure_logger(
        logger_name=f"{LOGGER_NAME}.search",
        log_path=cfg.log_path,
        log_level=cfg.log_level,
        log_to_stdout=False,
    )
    needle = contains.strip().lower()

    with requests.Session() as session:
        session.headers.update({"User-Agent": cfg.user_agent})
        url = f"{cfg.base_url}/sources/2/indicators?format=json&per_page=20000"
        _, rows = fetch_paginated_rows(
            session=session,
            url=url,
            cfg=cfg,
            logger=logger,
            job_label="search_indicators",
        )

    matches: list[dict[str, Any]] = []
    for row in rows:
        indicator_id = str(row.get("id") or "")
        name = str(row.get("name") or "")
        haystack = f"{indicator_id} {name}".lower()
        if needle in haystack:
            matches.append({"indicator_id": indicator_id, "name": name})
        if len(matches) >= max(1, limit):
            break

    print_json({"contains": contains, "match_count": len(matches), "matches": matches})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="World Bank energy indicators extractor.")
    subparsers = parser.add_subparsers(dest="command")

    extract_parser = subparsers.add_parser("extract", help="Extract World Bank energy indicators.")
    extract_parser.add_argument(
        "--energy-types",
        "--dataset",
        default="all",
        help="Energy types to extract, for example all, oil, gas, renewable, imports, or comma-separated values.",
    )
    extract_parser.add_argument(
        "--selector",
        default="db-countries",
        help="Selection mode. Recommended default is db-countries.",
    )
    extract_parser.add_argument(
        "--members",
        default=None,
        help="Comma-separated members for member-based selectors, for example NLD,USA or ECS,LCN.",
    )
    extract_parser.add_argument("--start-year", type=int, default=None)
    extract_parser.add_argument("--end-year", type=int, default=None)
    extract_parser.add_argument(
        "--year-mode",
        choices=["requested", "existing-db", "missing-from-db"],
        default="existing-db",
        help="Choose the requested range, only years already in the database, or only years missing from the database.",
    )
    extract_parser.add_argument(
        "--write-wide-preview",
        action="store_true",
        help="Also write a wide preview CSV grouped by country and year.",
    )

    subparsers.add_parser("list-indicators", help="List supported energy types.")
    subparsers.add_parser("list-selectors", help="List supported selector modes.")

    check_parser = subparsers.add_parser("check-indicators", help="Check configured indicator ids.")
    check_parser.add_argument("--energy-types", "--dataset", default="all")

    search_parser = subparsers.add_parser("search-indicators", help="Search World Bank indicator metadata.")
    search_parser.add_argument("--contains", default="energy")
    search_parser.add_argument("--limit", type=int, default=25)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    argv = list(sys.argv[1:] if argv is None else argv)
    command_names = {"extract", "list-indicators", "list-selectors", "check-indicators", "search-indicators"}
    if not argv or argv[0] not in command_names:
        argv = ["extract", *argv]

    args = parser.parse_args(argv)

    if args.command == "list-indicators":
        handle_list_indicators()
        return 0

    if args.command == "list-selectors":
        handle_list_selectors()
        return 0

    if args.command == "check-indicators":
        handle_check_indicators(energy_types=args.energy_types)
        return 0

    if args.command == "search-indicators":
        handle_search_indicators(contains=args.contains, limit=args.limit)
        return 0

    result = run_extract(
        energy_types=args.energy_types,
        selector=args.selector,
        members=args.members,
        start_year=args.start_year,
        end_year=args.end_year,
        year_mode=args.year_mode,
        write_wide_preview=args.write_wide_preview,
    )
    print_json(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
