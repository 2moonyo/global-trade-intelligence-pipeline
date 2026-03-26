"""
worldbank_energy_local.py

Local extractor for selected World Bank energy indicators.
Writes:
1. Bronze-style partitioned JSONL by dt
2. A batch CSV snapshot for each run

Design notes
------------
- Uses World Bank Indicators API v2 JSON output
- Supports country / region / income / lending / world selection modes
- Stores tidy long-form rows for downstream dbt modelling
- Keeps countryiso3code for downstream joins

Examples
--------
# All countries, renewable share only
python worldbank_energy_local.py extract --dataset renew --selector world

# All countries, all six indicators
python worldbank_energy_local.py extract --dataset all --selector world --start-year 1990

# Selected ISO3 countries
python worldbank_energy_local.py extract --dataset gas --selector country --members NLD,ESP,ROU,CHN,USA

# World Bank regions
python worldbank_energy_local.py extract --dataset all --selector region --members ECS,LCN,MEA

# Income groups
python worldbank_energy_local.py extract --dataset fossil --selector income --members HIC,UMC,LMC,LIC

# Show available indicators / aliases
python worldbank_energy_local.py list-indicators
"""

from __future__ import annotations

import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import click
import requests
from dotenv import load_dotenv


# -------------------------------------------------------------------
# ENV / PATHS
# -------------------------------------------------------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", PROJECT_ROOT / "data" / "bronze"))
DATASET = os.getenv("DATASET", "worldbank_energy")
SOURCE = os.getenv("SOURCE", "WorldBank")
BASE_URL = os.getenv("WORLD_BANK_BASE_URL", "https://api.worldbank.org/v2")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))
REQUEST_SLEEP_SECS = float(os.getenv("REQUEST_SLEEP_SECS", "0.15"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BACKOFF_BASE_SECS = float(os.getenv("RETRY_BACKOFF_BASE_SECS", "2.0"))


class ExtractionRegistry:
    """Append-only JSONL manifest for extraction jobs."""

    def __init__(self, path: Path):
        self.path = path
        ensure_dir(self.path.parent)
        self._latest_by_job_key: Dict[str, dict] = {}
        self._load_latest()

    def _load_latest(self) -> None:
        if not self.path.exists():
            return
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except Exception:
                continue
            job_key = entry.get("job_key")
            if job_key:
                self._latest_by_job_key[job_key] = entry

    def append(self, entry: dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        job_key = entry.get("job_key")
        if job_key:
            self._latest_by_job_key[job_key] = entry

    def latest_status(self, job_key: str) -> Optional[str]:
        latest = self._latest_by_job_key.get(job_key)
        if not latest:
            return None
        return latest.get("status")


class LastCallCheckpoint:
    """Single JSON snapshot of the latest outbound API call context."""

    def __init__(self, path: Path):
        self.path = path
        ensure_dir(self.path.parent)

    def write(self, payload: dict) -> None:
        record = dict(payload)
        record["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(record, indent=2), encoding="utf-8")


class PartitionedJsonlWriter:
    """
    Streams rows directly to dt partitions without keeping all rows in memory.
    """

    def __init__(self, root: Path, dataset: str, stamp: str):
        self.root = root
        self.dataset = dataset
        self.stamp = stamp
        self.filepaths_by_dt: Dict[str, Path] = {}

    def write_row(self, row: dict) -> None:
        dt = row["dt"]
        if dt not in self.filepaths_by_dt:
            part_dir = self.root / self.dataset / f"dt={dt}"
            ensure_dir(part_dir)
            self.filepaths_by_dt[dt] = part_dir / f"part-{self.stamp}.jsonl"

        out_path = self.filepaths_by_dt[dt]
        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def files_written(self) -> List[str]:
        return [str(p) for _, p in sorted(self.filepaths_by_dt.items())]


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
        self._fh = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=self._fieldnames)
        self._writer.writeheader()

    def write_row(self, row: dict) -> None:
        self._writer.writerow(row)

    def close(self) -> None:
        self._fh.close()


def check_indicator_validity(session: requests.Session, indicator_id: str) -> tuple[bool, Optional[str]]:
    """
    Validate indicator ID against World Bank indicator metadata endpoint.
    Returns (is_valid, error_message).
    """
    url = f"{BASE_URL}/indicator/{indicator_id}?format=json"
    resp = request_with_retry(session=session, url=url, job_key=f"validate_{indicator_id}")
    payload = resp.json()

    if isinstance(payload, list) and payload:
        first = payload[0]

        if isinstance(first, dict) and "message" in first:
            return False, json.dumps(first, ensure_ascii=False)

        if isinstance(first, dict) and "total" in first and int(first.get("total", 0)) >= 1:
            return True, None

    return False, f"Unexpected indicator metadata payload for {indicator_id}: {payload}"


def fetch_indicator_catalog(session: requests.Session, source_id: int = 2, per_page: int = 20000) -> List[dict]:
    url = f"{BASE_URL}/sources/{source_id}/indicators?format=json&per_page={per_page}"
    resp = request_with_retry(session=session, url=url, job_key=f"catalog_source_{source_id}")
    payload = resp.json()

    if not isinstance(payload, list) or len(payload) < 2:
        raise RuntimeError(f"Unexpected indicator catalog payload shape: {payload}")

    return payload[1] or []


def make_job_key(alias: str, selector: str, members: Optional[str], start_year: int, end_year: int) -> str:
    clean_members = (members or "all").replace(";", "-").replace(",", "-")
    clean_members = re.sub(r"[^A-Za-z0-9_-]+", "_", clean_members)
    return f"wb_{alias}_{selector}_{clean_members}_{start_year}_{end_year}"


# -------------------------------------------------------------------
# INDICATOR CONFIG
# -------------------------------------------------------------------
INDICATORS: Dict[str, Dict[str, str]] = {
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

ALL_DATASETS_ALIAS = "all"


# -------------------------------------------------------------------
# SELECTOR MAPPING
# -------------------------------------------------------------------
@dataclass(frozen=True)
class SelectorSpec:
    entity_path: str
    requires_members: bool
    description: str


SELECTORS: Dict[str, SelectorSpec] = {
    "world": SelectorSpec(
        entity_path="country/all",
        requires_members=False,
        description="All countries/economies returned by the World Bank endpoint",
    ),
    "country": SelectorSpec(
        entity_path="country/{members}",
        requires_members=True,
        description="Specific country codes, preferably ISO3 comma-separated, e.g. NLD,ESP,ROU",
    ),
    "region": SelectorSpec(
        entity_path="region/{members}/country",
        requires_members=True,
        description="World Bank region codes, e.g. ECS,LCN,MEA,SAS,EAS,SSF,NAC",
    ),
    "income": SelectorSpec(
        entity_path="incomeLevel/{members}/country",
        requires_members=True,
        description="Income group codes, e.g. HIC,UMC,LMC,LIC",
    ),
    "lending": SelectorSpec(
        entity_path="lendingType/{members}/country",
        requires_members=True,
        description="Lending type codes, e.g. IBD,IDX,IDB,LNX",
    ),
}


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_members(members: Optional[str]) -> Optional[str]:
    if not members:
        return None
    cleaned = [m.strip().upper() for m in members.split(",") if m.strip()]
    return ";".join(cleaned) if cleaned else None
    # World Bank API supports multiple values separated by semicolons in path segments


def build_entity_path(selector: str, members: Optional[str]) -> str:
    spec = SELECTORS[selector]

    if spec.requires_members and not members:
        raise click.UsageError(f"--members is required for selector='{selector}'")

    if selector == "world":
        return spec.entity_path

    parsed = parse_members(members)
    if not parsed:
        raise click.UsageError(f"No valid members parsed from: {members!r}")

    return spec.entity_path.format(members=parsed)


def resolve_indicator_aliases(dataset: str) -> List[str]:
    dataset = dataset.strip().lower()

    if dataset == ALL_DATASETS_ALIAS:
        return list(INDICATORS.keys())

    aliases = [d.strip().lower() for d in dataset.split(",") if d.strip()]
    invalid = [a for a in aliases if a not in INDICATORS]
    if invalid:
        raise click.UsageError(
            f"Unknown dataset alias(es): {invalid}. "
            f"Valid: {sorted(list(INDICATORS.keys()) + [ALL_DATASETS_ALIAS])}"
        )
    return aliases


def build_indicator_url(
    entity_path: str,
    indicator_id: str,
    start_year: Optional[int],
    end_year: Optional[int],
    per_page: int = 1000,
) -> str:
    params = {
        "format": "json",
        "per_page": per_page,
    }
    if start_year is not None and end_year is not None:
        params["date"] = f"{start_year}:{end_year}"
    elif start_year is not None:
        params["date"] = f"{start_year}:{datetime.now().year}"
    elif end_year is not None:
        params["date"] = f"1960:{end_year}"

    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_URL}/{entity_path}/indicator/{indicator_id}?{query}"


def _retry_sleep_seconds(resp: Optional[requests.Response], attempt: int) -> float:
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return max(1.0, float(retry_after))
            except Exception:
                pass
    return RETRY_BACKOFF_BASE_SECS * (2 ** (attempt - 1))


def request_with_retry(session: requests.Session, url: str, job_key: str) -> requests.Response:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)

            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == MAX_RETRIES:
                    raise RuntimeError(
                        f"World Bank API HTTP {resp.status_code} after {MAX_RETRIES} attempts: {url}"
                    )
                wait_s = _retry_sleep_seconds(resp, attempt)
                click.echo(
                    f"Retrying {job_key}: HTTP {resp.status_code} on attempt {attempt}/{MAX_RETRIES}, waiting {wait_s:.1f}s"
                )
                time.sleep(wait_s)
                continue

            if resp.status_code != 200:
                raise RuntimeError(f"World Bank API HTTP {resp.status_code}: {resp.text[:500]}")

            return resp

        except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Network timeout/connection failure for {job_key} after {MAX_RETRIES} attempts: {url}"
                ) from e
            wait_s = _retry_sleep_seconds(None, attempt)
            click.echo(
                f"Retrying {job_key}: network error on attempt {attempt}/{MAX_RETRIES}, waiting {wait_s:.1f}s"
            )
            time.sleep(wait_s)

    raise RuntimeError(f"Failed to request URL for {job_key}: {url}") from last_error


def iter_pages(url: str, session: requests.Session, job_key: str, checkpoint: LastCallCheckpoint) -> Iterable[List[dict]]:
    """
    World Bank indicator responses are JSON arrays:
      [metadata, observations]
    metadata contains page/pages/per_page/total
    """
    current_url = url
    page = 1

    while True:
        checkpoint.write(
            {
                "job_key": job_key,
                "current_url": current_url,
                "page": page,
            }
        )

        resp = request_with_retry(session=session, url=current_url, job_key=job_key)

        payload = resp.json()

        if not isinstance(payload, list) or len(payload) < 2:
            raise RuntimeError(f"Unexpected World Bank payload shape: {payload}")

        meta, rows = payload[0], payload[1]
        yield rows or []

        total_pages = int(meta.get("pages", 1))
        current_page = int(meta.get("page", page))

        if current_page >= total_pages:
            break

        next_page = current_page + 1
        separator = "&" if "?" in url else "?"
        current_url = f"{url}{separator}page={next_page}"
        page = next_page
        time.sleep(REQUEST_SLEEP_SECS)


def normalise_row(
    raw: dict,
    dataset_alias: str,
    load_ts: str,
) -> Optional[dict]:
    """
    Expected row shape includes fields like:
    - indicator: {id, value}
    - country: {id, value}
    - countryiso3code
    - date
    - value
    - unit
    - obs_status
    - decimal
    """
    year_str = raw.get("date")
    value = raw.get("value")

    if not year_str:
        return None

    # Keep nulls optional. Often useful for auditing coverage.
    try:
        dt = f"{int(year_str):04d}-01-01"
    except Exception:
        return None

    meta = INDICATORS[dataset_alias]

    return {
        "dt": dt,
        "year": int(year_str),
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


def build_wide_preview(rows: List[dict]) -> List[dict]:
    """
    Optional helper for a quick downstream-friendly preview:
      country_iso3, country_name, year, oil_share, gas_share, ...
    """
    grouped: Dict[tuple, dict] = {}

    for r in rows:
        key = (r["country_iso3"], r["country_name"], r["year"])
        if key not in grouped:
            grouped[key] = {
                "country_iso3": r["country_iso3"],
                "country_name": r["country_name"],
                "year": r["year"],
            }

        grouped[key][r["metric_name"]] = r["value"]

    return list(grouped.values())


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------
@click.group()
def cli():
    """World Bank energy indicators extractor."""
    pass


@cli.command("list-indicators")
def list_indicators():
    """List available dataset aliases."""
    payload = {
        "dataset_aliases": {
            alias: {
                "indicator_id": cfg["indicator_id"],
                "metric_name": cfg["metric_name"],
                "series_name": cfg["series_name"],
            }
            for alias, cfg in INDICATORS.items()
        },
        "special_alias": ALL_DATASETS_ALIAS,
    }
    click.echo(json.dumps(payload, indent=2))


@cli.command("list-selectors")
def list_selectors():
    """List supported selector modes."""
    payload = {
        name: {
            "entity_path": spec.entity_path,
            "requires_members": spec.requires_members,
            "description": spec.description,
        }
        for name, spec in SELECTORS.items()
    }
    click.echo(json.dumps(payload, indent=2))


@cli.command("check-indicators")
@click.option(
    "--dataset",
    default="all",
    show_default=True,
    help="Dataset aliases to validate, e.g. all or renew,imports,oil",
)
def check_indicators(dataset: str):
    """Validate configured indicator IDs against World Bank metadata endpoint."""
    aliases = resolve_indicator_aliases(dataset)
    session = requests.Session()
    session.headers.update({"User-Agent": "worldbank-energy-extractor/1.0"})

    results = []
    for alias in aliases:
        cfg = INDICATORS[alias]
        is_valid, reason = check_indicator_validity(session, cfg["indicator_id"])
        results.append(
            {
                "alias": alias,
                "indicator_id": cfg["indicator_id"],
                "series_name": cfg["series_name"],
                "metric_name": cfg["metric_name"],
                "is_valid": is_valid,
                "error": reason,
            }
        )

    click.echo(json.dumps({"results": results}, indent=2))


@cli.command("search-indicators")
@click.option(
    "--contains",
    default="energy",
    show_default=True,
    help="Case-insensitive substring to match in indicator name or id.",
)
@click.option("--limit", default=25, type=int, show_default=True)
def search_indicators(contains: str, limit: int):
    """Search WDI indicator metadata to help discover valid indicator IDs."""
    needle = contains.strip().lower()
    session = requests.Session()
    session.headers.update({"User-Agent": "worldbank-energy-extractor/1.0"})

    rows = fetch_indicator_catalog(session=session, source_id=2)
    matches = []
    for row in rows:
        indicator_id = row.get("id") or ""
        name = row.get("name") or ""
        haystack = f"{indicator_id} {name}".lower()
        if needle in haystack:
            matches.append(
                {
                    "indicator_id": indicator_id,
                    "name": name,
                }
            )
        if len(matches) >= max(1, limit):
            break

    click.echo(
        json.dumps(
            {
                "contains": contains,
                "match_count": len(matches),
                "matches": matches,
            },
            indent=2,
        )
    )


@cli.command("extract")
@click.option(
    "--dataset",
    default="all",
    show_default=True,
    help="Dataset alias: renew, fossil, imports, oil, gas, coal, or all. "
         "Can also pass comma-separated aliases.",
)
@click.option(
    "--selector",
    default="world",
    type=click.Choice(sorted(SELECTORS.keys())),
    show_default=True,
    help="Selection mode.",
)
@click.option(
    "--members",
    default=None,
    help="Comma-separated members for country/region/income/lending selectors.",
)
@click.option("--start-year", default=1960, type=int, show_default=True)
@click.option("--end-year", default=datetime.now().year, type=int, show_default=True)
@click.option(
    "--resume-from-manifest/--no-resume-from-manifest",
    default=True,
    show_default=True,
    help="Skip aliases already completed for the same job_key and rerun only failed or missing aliases.",
)
@click.option(
    "--retry-failed/--skip-failed",
    default=False,
    show_default=True,
    help="When resuming from manifest, retry aliases whose latest status is failed, or skip them.",
)
@click.option(
    "--fail-fast/--continue-on-error",
    default=False,
    show_default=True,
    help="Fail immediately on first alias error, or continue trying remaining aliases.",
)
@click.option(
    "--strict-exit/--soft-exit",
    default=False,
    show_default=True,
    help="Return non-zero exit code when any alias fails, or exit 0 while recording partial failures in manifest/output.",
)
@click.option(
    "--write-wide-preview/--no-write-wide-preview",
    default=False,
    show_default=True,
    help="Also write a wide preview CSV grouped by country/year.",
)
def extract(
    dataset: str,
    selector: str,
    members: Optional[str],
    start_year: int,
    end_year: int,
    resume_from_manifest: bool,
    retry_failed: bool,
    fail_fast: bool,
    strict_exit: bool,
    write_wide_preview: bool,
):
    """Extract World Bank energy indicators."""
    if start_year > end_year:
        raise click.UsageError("--start-year cannot be greater than --end-year")

    aliases = resolve_indicator_aliases(dataset)
    entity_path = build_entity_path(selector, members)
    load_ts = utc_now_stamp()
    run_stamp = utc_now_stamp()

    session = requests.Session()
    session.headers.update({"User-Agent": "worldbank-energy-extractor/1.0"})

    manifest_dir = BRONZE_ROOT / DATASET / "Manifest"
    ensure_dir(manifest_dir)
    registry = ExtractionRegistry(manifest_dir / "extraction_registry.jsonl")
    checkpoint = LastCallCheckpoint(manifest_dir / "last_call_checkpoint.json")

    partition_writer = PartitionedJsonlWriter(root=BRONZE_ROOT, dataset=DATASET, stamp=run_stamp)
    batch_writer = BatchCsvWriter(root=BRONZE_ROOT, dataset=DATASET, stamp=run_stamp)

    total_rows = 0
    preview_map: Dict[tuple, dict] = {}
    skipped_completed_aliases: List[str] = []
    skipped_failed_aliases: List[str] = []
    failed_aliases: List[str] = []
    unsupported_aliases: List[str] = []
    indicator_validity_cache: Dict[str, tuple[bool, Optional[str]]] = {}

    aliases_to_run: List[str] = []
    for alias in aliases:
        job_key = make_job_key(alias, selector, members, start_year, end_year)
        latest_status = registry.latest_status(job_key)
        if resume_from_manifest and latest_status in {"completed", "unsupported_indicator"}:
            skipped_completed_aliases.append(alias)
            continue
        if resume_from_manifest and latest_status == "failed" and not retry_failed:
            skipped_failed_aliases.append(alias)
            continue
        aliases_to_run.append(alias)

    try:
        for alias in aliases_to_run:
            indicator_id = INDICATORS[alias]["indicator_id"]

            if indicator_id not in indicator_validity_cache:
                indicator_validity_cache[indicator_id] = check_indicator_validity(session, indicator_id)

            indicator_is_valid, invalid_reason = indicator_validity_cache[indicator_id]
            if not indicator_is_valid:
                unsupported_aliases.append(alias)
                registry.append(
                    {
                        "event_type": "extract",
                        "job_key": make_job_key(alias, selector, members, start_year, end_year),
                        "status": "unsupported_indicator",
                        "rows_written": 0,
                        "selector": selector,
                        "members": members,
                        "indicator_alias": alias,
                        "indicator_id": indicator_id,
                        "date_range": f"{start_year}:{end_year}",
                        "started_at_utc": datetime.now(timezone.utc).isoformat(),
                        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                        "last_call_url": f"{BASE_URL}/indicator/{indicator_id}?format=json",
                        "error": invalid_reason,
                    }
                )
                click.echo(f"Skipping unsupported indicator alias '{alias}' ({indicator_id})")
                continue

            url = build_indicator_url(
                entity_path=entity_path,
                indicator_id=indicator_id,
                start_year=start_year,
                end_year=end_year,
            )

            job_key = make_job_key(alias, selector, members, start_year, end_year)
            click.echo(f"Requesting: {url}")

            start_ts = datetime.now(timezone.utc).isoformat()
            alias_rows = 0
            status = "completed"
            error_message = None

            try:
                for raw_rows in iter_pages(url=url, session=session, job_key=job_key, checkpoint=checkpoint):
                    for raw in raw_rows:
                        row = normalise_row(raw=raw, dataset_alias=alias, load_ts=load_ts)
                        if row is None:
                            continue

                        partition_writer.write_row(row)
                        batch_writer.write_row(row)

                        if write_wide_preview:
                            key = (row["country_iso3"], row["country_name"], row["year"])
                            if key not in preview_map:
                                preview_map[key] = {
                                    "country_iso3": row["country_iso3"],
                                    "country_name": row["country_name"],
                                    "year": row["year"],
                                }
                            preview_map[key][row["metric_name"]] = row["value"]

                        total_rows += 1
                        alias_rows += 1

                time.sleep(REQUEST_SLEEP_SECS)

            except Exception as e:
                status = "failed"
                error_message = str(e)
                failed_aliases.append(alias)
                if fail_fast:
                    raise

            finally:
                registry.append(
                    {
                        "event_type": "extract",
                        "job_key": job_key,
                        "status": status,
                        "rows_written": alias_rows,
                        "selector": selector,
                        "members": members,
                        "indicator_alias": alias,
                        "indicator_id": indicator_id,
                        "date_range": f"{start_year}:{end_year}",
                        "started_at_utc": start_ts,
                        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                        "last_call_url": url,
                        "error": error_message,
                    }
                )
    finally:
        batch_writer.close()

    output = {
        "dataset_root": str(BRONZE_ROOT / DATASET),
        "rows": total_rows,
        "datasets_requested": aliases,
        "datasets_attempted_this_run": aliases_to_run,
        "datasets_skipped_completed": skipped_completed_aliases,
        "datasets_skipped_failed": skipped_failed_aliases,
        "datasets_failed": failed_aliases,
        "datasets_unsupported": unsupported_aliases,
        "selector": selector,
        "members": members,
        "date_range": f"{start_year}:{end_year}",
        "bronze_files_written": partition_writer.files_written(),
        "batch_csv": str(batch_writer.path),
        "manifest_registry": str(manifest_dir / "extraction_registry.jsonl"),
        "last_call_checkpoint": str(manifest_dir / "last_call_checkpoint.json"),
        "project_root": str(PROJECT_ROOT),
        "bronze_root": str(BRONZE_ROOT),
    }

    if write_wide_preview:
        preview_rows = list(preview_map.values())
        preview_dir = BRONZE_ROOT / DATASET / "Preview"
        ensure_dir(preview_dir)
        preview_path = preview_dir / f"worldbank_energy_wide_{utc_now_stamp()}.csv"

        if preview_rows:
            fieldnames = sorted({k for row in preview_rows for k in row.keys()})
            with preview_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(preview_rows)

            output["wide_preview_csv"] = str(preview_path)
            output["wide_preview_rows"] = len(preview_rows)

    if failed_aliases and not fail_fast:
        output["run_status"] = "partial_failure"
        output["message"] = (
            "Some aliases failed. Re-run the same command with --resume-from-manifest to retry only failed/missing aliases."
        )
    elif unsupported_aliases:
        output["run_status"] = "completed_with_unsupported"
        output["message"] = "Some aliases use unsupported indicator IDs and were skipped."
    else:
        output["run_status"] = "completed"

    click.echo(json.dumps(output, indent=2))

    if failed_aliases and strict_exit:
        raise click.ClickException(
            "Extraction finished with failures for aliases: " + ",".join(failed_aliases)
        )


if __name__ == "__main__":
    cli()