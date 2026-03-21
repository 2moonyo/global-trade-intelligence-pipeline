import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import click
import pandas as pd
import requests
from dotenv import load_dotenv



load_dotenv()
logger = logging.getLogger("comtrade")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - comtrade - %(levelname)s - %(message)s")

def _append_jsonl(path: Path, entry: Dict) -> None:
    """Append a single JSON object to a JSONL file (one line per object)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")



# ---------------- Exceptions ----------------
class QuotaHit(Exception):
    """Raised when we hit either daily quota (403/409) or throttling (429)."""

    def __init__(self, wait_seconds: int, message: str = "", http_status: Optional[int] = None):
        super().__init__(message)
        self.wait_seconds = int(wait_seconds)
        self.message = message or ""
        self.http_status = http_status


# ---------------- Registry ----------------
class ExtractionRegistry:
    """
    JSONL registry. Each line is a single event.

    Event types used:
      - extract:       status=completed|failed (job_key keyed)
      - coverage_gap:  status=gap (job_key keyed, will NOT be treated as completed)
    """

    def __init__(self, registry_path: str):
        self.registry_path = Path(registry_path)
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.registry: Dict[str, Dict] = {}
        self._load()

    def _load(self) -> None:
        if not self.registry_path.exists():
            logger.info("No existing registry found. Starting fresh.")
            return
        for line in self.registry_path.read_text().splitlines():
            if not line.strip():
                continue
            entry = json.loads(line)
            jk = entry.get("job_key")
            if jk:
                self.registry[jk] = entry
        logger.info("Loaded %s registry entries", len(self.registry))

    def append_entry(self, entry: Dict) -> None:
        with open(self.registry_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        jk = entry.get("job_key")
        if jk:
            self.registry[jk] = entry

    def is_completed(self, job_key: str) -> bool:
        """
        Only extraction jobs with status=completed count as completed.
        coverage gaps (status=gap) will not block future extraction.
        """
        return self.registry.get(job_key, {}).get("status") == "completed"

    def job_key(
        self,
        reporter_code: int,
        period: str,
        commodity: str,
        flow: str,
        partner: int,
        type_code: str,
        freq: str,
        cl: str,
    ) -> str:
        return f"{reporter_code}_{period}_{commodity}_{flow}_p{partner}_{type_code}_{freq}_{cl}"

    def gap_key(self, reporter_code: int, year: str, commodity: str, flow: str, partner: int, type_code: str, cl: str) -> str:
        # Stable key for annual-missing gap that drives monthly recovery.
        return f"gap_{reporter_code}_{year}_{commodity}_{flow}_p{partner}_{type_code}_A_{cl}"

    def record_completed(self, *, job_key: str, payload: Dict, filepath: Path, meta: Dict) -> None:
        entry = {
            "event_type": "extract",
            "job_key": job_key,
            "status": "completed",
            "record_count": int(payload.get("_metadata", {}).get("record_count", 0)),
            "filepath": str(filepath),
            "filename": filepath.name,
            "file_size_bytes": filepath.stat().st_size if filepath.exists() else 0,
            "meta": meta,
            "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.append_entry(entry)

    def record_failed(self, *, job_key: str, error_type: str, http_status: Optional[int], message: str, meta: Dict) -> None:
        entry = {
            "event_type": "extract",
            "job_key": job_key,
            "status": "failed",
            "error_type": error_type,
            "http_status": http_status,
            "message": (message or "")[:2000],
            "meta": meta,
            "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.append_entry(entry)

    def record_gap(
        self,
        *,
        reporter_code: int,
        year: str,
        commodity: str,
        flow: str,
        partner: int,
        type_code: str,
        cl: str,
        reason: str,
        meta: Dict,
    ) -> None:
        job_key = self.gap_key(reporter_code, year, commodity, flow, partner, type_code, cl)
        entry = {
            "event_type": "coverage_gap",
            "job_key": job_key,
            "status": "gap",
            "reason": reason,
            "meta": meta,
            "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.append_entry(entry)

    def iter_gaps(self) -> List[Dict]:
        return [e for e in self.registry.values() if e.get("event_type") == "coverage_gap" and e.get("status") == "gap"]


# ---------------- Checkpoint ----------------
class Checkpoint:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except Exception:
                return {}
        return {}

    def write(self, payload: Dict) -> None:
        payload = dict(payload)
        payload["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(payload, indent=2))


# ---------------- Helpers ----------------
@dataclass(frozen=True)
class QueryShape:
    partner_code: int = 0  # "all" = partners for full global tracing 0 = "World"
    type_code: str = "C"
    freq_code: str = "A"  # A annual, M monthly
    cl_code: str = "HS"


def parse_years(years_csv: str) -> List[str]:
    years = [y.strip() for y in years_csv.split(",") if y.strip()]
    for y in years:
        if not re.fullmatch(r"\d{4}", y):
            raise click.ClickException(f"Invalid year '{y}'")
    return years


def parse_months(months: str) -> List[int]:
    """
    Parse months specification:
      - "1-12"
      - "10-12"
      - "1,2,3,12"
    """
    months = months.strip()
    if re.fullmatch(r"\d{1,2}-\d{1,2}", months):
        a, b = months.split("-")
        start, end = int(a), int(b)
        if start < 1 or end > 12 or start > end:
            raise click.ClickException(f"Invalid months range '{months}'")
        return list(range(start, end + 1))

    parts = [p.strip() for p in months.split(",") if p.strip()]
    out = []
    for p in parts:
        if not re.fullmatch(r"\d{1,2}", p):
            raise click.ClickException(f"Invalid month '{p}'")
        m = int(p)
        if m < 1 or m > 12:
            raise click.ClickException(f"Invalid month '{p}'")
        out.append(m)
    return sorted(set(out))

def load_json_record_count(json_path):
    """
    Load a bronze JSON file and determine its record count.

    Priority order:
        1. _metadata.record_count
        2. top-level 'count'
        3. len(data)
    """

    try:
        import json

        with open(json_path, "r") as f:
            payload = json.load(f)

        if not isinstance(payload, dict):
            return {}, 0

        # 1️⃣ metadata count
        md = payload.get("_metadata")
        if isinstance(md, dict) and "record_count" in md:
            try:
                return payload, int(md.get("record_count") or 0)
            except Exception:
                pass

        # 2️⃣ top-level count
        if "count" in payload:
            try:
                rc = int(payload["count"])
                payload.setdefault("_metadata", {})
                payload["_metadata"]["record_count"] = rc
                return payload, rc
            except Exception:
                pass

        # 3️⃣ fallback to len(data)
        rows = payload.get("data") or []
        rc = len(rows) if isinstance(rows, list) else 0

        payload.setdefault("_metadata", {})
        payload["_metadata"]["record_count"] = rc

        return payload, rc

    except Exception:
        return {}, 0

def month_periods_for_year(year: str, months: List[int]) -> List[str]:
    return [f"{year}{m:02d}" for m in months]


def reporters_default_top3_eu_plus(world: bool = True) -> List[str]:
    base = ["ESP", "FRA", "NLD", "BGR", "ROU"]
    if world:
        base += ["USA", "EUN", "CHN"]
    return base


def load_reporters(metadata_dir: str) -> pd.DataFrame:
    p = Path(metadata_dir) / "reporters.csv"
    if not p.exists():
        raise click.ClickException(f"Missing reporters.csv at {p}")
    return pd.read_csv(p)


def reporter_code_for_iso3(df: pd.DataFrame, iso3: str) -> Optional[int]:
    m = df[df["reporterCodeIsoAlpha3"] == iso3]
    if m.empty:
        return None
    return int(m.iloc[0]["reporterCode"])


def find_eu_bloc_iso3(df: pd.DataFrame) -> Optional[str]:
    for cand in ("EUN", "EUU", "EUR"):
        if not df[df["reporterCodeIsoAlpha3"] == cand].empty:
            return cand
    return None


def update_coverage_tally(chk: Checkpoint, reporter_code: int, year: str, commodity: str, flow: str) -> None:
    """
    Keep a running tally of annual gaps in the checkpoint.
    coverage_gaps[reporter_code][year] -> list of "cmd_flow"
    """
    data = chk.load()
    gaps = data.get("coverage_gaps", {})
    r = str(reporter_code)
    y = str(year)
    gaps.setdefault(r, {})
    gaps[r].setdefault(y, [])
    item = f"{commodity}_{flow}"
    if item not in gaps[r][y]:
        gaps[r][y].append(item)
    data["coverage_gaps"] = gaps
    chk.write(data)


# ---------------- Extractor ----------------
class ComtradeDataExtractor:
    BASE_URL = "https://comtradeapi.un.org/data/v1/get"

    def __init__(self, api_key: str, bronze_dir: str):
        self.api_key = api_key
        self.headers = {"Ocp-Apim-Subscription-Key": api_key}
        self.bronze_dir = Path(bronze_dir)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def build_url(self, shape: QueryShape) -> str:
        return f"{self.BASE_URL}/{shape.type_code}/{shape.freq_code}/{shape.cl_code}"

    def parse_wait_time(self, message: str) -> int:
        m = re.search(r"(\d+):(\d+):(\d+)", message or "")
        if not m:
            return 3600
        h, mm, s = map(int, m.groups())
        return h * 3600 + mm * 60 + s + 60

    def _raise_quota_or_throttle(self, resp: requests.Response) -> None:
        if resp.status_code in (403, 409):
            try:
                msg = resp.json().get("message", "") or resp.text
            except Exception:
                msg = resp.text
            raise QuotaHit(self.parse_wait_time(msg), msg, http_status=resp.status_code)

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = 60
            if retry_after:
                try:
                    wait_s = int(float(retry_after))
                except Exception:
                    wait_s = 60
            try:
                msg = resp.json().get("message", "") or resp.text
            except Exception:
                msg = resp.text
            raise QuotaHit(wait_s, f"HTTP 429 throttled. {msg}", http_status=resp.status_code)

    def partition_dir(self, period: str, reporter_code: int, shape: QueryShape) -> Path:
        """
        Backwards-compatible annual path:
          year=YYYY/reporter=CODE/

        Monthly path:
          year=YYYY/monthly/reporter=CODE/
        """
        if shape.freq_code == "A":
            year = period
            d = self.bronze_dir / f"year={year}" / f"reporter={reporter_code}"
        else:
            year = period[:4]
            d = self.bronze_dir / f"year={year}" / "monthly" / f"reporter={reporter_code}"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def find_existing(self, period: str, reporter_code: int, commodity: str, flow: str, shape: QueryShape) -> Optional[Path]:
        d = self.partition_dir(period, reporter_code, shape)
        old_prefix = f"commodity_{commodity}_flow_{flow}_"
        new_prefix = f"commodity_{commodity}_flow_{flow}_p{shape.partner_code}_{shape.type_code}_{shape.freq_code}_{shape.cl_code}_"
        matches = [p for p in d.glob("*.json") if p.name.startswith(old_prefix) or p.name.startswith(new_prefix)]
        return sorted(matches, key=lambda p: p.name, reverse=True)[0] if matches else None

    def _validate_period(self, period: str, shape: QueryShape) -> None:
        if shape.freq_code == "A":
            if not re.fullmatch(r"\d{4}", str(period)):
                raise ValueError(f"Annual requires period YYYY. Got '{period}'")
        elif shape.freq_code == "M":
            if not re.fullmatch(r"\d{6}", str(period)):
                raise ValueError(f"Monthly requires period YYYYMM. Got '{period}'")
        else:
            raise ValueError(f"Unsupported freq_code '{shape.freq_code}'")

    def extract_one(self, reporter_code: str, period: str, commodity: str, flow: str, shape: QueryShape, timeout: int = 60) -> Dict:
        # Note: reporter_code and period can now be comma-separated strings
        url = self.build_url(shape)
        params = {
            "reporterCode": reporter_code,
            "partnerCode": shape.partner_code,
            "period": period,
            "flowCode": flow,
            "cmdCode": commodity,
            "includeDesc": "True" # Helps for the "Gold" layer later
        }
        
        # Debugging URL: print this to manually test in browser if it fails
        logger.info(f"Fetching: {url}?{requests.compat.urlencode(params)}")
        
        resp = requests.get(url, params=params, headers=self.headers, timeout=timeout)
        self._raise_quota_or_throttle(resp)
        resp.raise_for_status()
        
        data = resp.json()
        
        # Handle the "No Data" scenario gracefully
        record_count = len(data.get("data", []) or [])
        data["_metadata"] = {
            "record_count": record_count,
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "request": dict(params),
            "url": resp.url,
            "shape": shape.__dict__,
        }
        return data

    def save_one(self, data: Dict, period: str, reporter_code: int, commodity: str, flow: str, shape: QueryShape) -> Path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        d = self.partition_dir(period, reporter_code, shape)
        fn = f"commodity_{commodity}_flow_{flow}_p{shape.partner_code}_{shape.type_code}_{shape.freq_code}_{shape.cl_code}_{ts}.json"
        path = d / fn
        path.write_text(json.dumps(data, indent=2))
        return path


# ---------------- CLI ----------------
@click.group()
def cli():
    pass


@cli.command("run-annual")
@click.option("--api-key-env", type=click.Choice(["COMTRADE_API_KEY_DATA", "COMTRADE_API_KEY_DATA_A", "COMTRADE_API_KEY_DATA_B"]), default="COMTRADE_API_KEY_DATA", show_default=True)
@click.option("--metadata-dir", default="data/metadata/comtrade", show_default=True)
@click.option("--bronze-dir", default="data/bronze/comtrade/test2", show_default=True)
@click.option("--registry-path", default="logs/test2/extraction_registry.jsonl", show_default=True)
@click.option("--checkpoint-path", default="logs/test2/comtrade_checkpoint.json", show_default=True)
@click.option("--include-eu-bloc/--no-include-eu-bloc", default=True, show_default=True)
@click.option("--reporters", default=None, help="Comma-separated reporter ISO3 codes (e.g. 'USA,CHN,DEU'). If not provided, uses default set: ESP,FRA,NLD,BGR,ROU,USA,EUN,CHN")
@click.option("--years", required=True, help="Comma-separated years, e.g. 2020,2021,2022,2023,2024,2025,2026")
@click.option("--commodities", required=True, help="HS codes, comma-separated, e.g. 10,12")
@click.option("--flows", default="M,X", show_default=True)
@click.option("--partner-code", default=0, show_default=True, type=int)
@click.option("--exit-on-quota/--sleep-on-quota", default=True, show_default=True)
@click.option("--rate-delay", default=5.0, show_default=True, type=float)
@click.option("--audit-log-path", default="logs/test2/audit_scan.jsonl", show_default=True, help="JSONL audit log written during --audit-only scans")
@click.option("--zero-count-report-path", default="logs/test2/coverage_zero_count.jsonl", show_default=True, help="JSONL report of bronze files with count==0 found during --audit-only")
@click.option("--audit-only", is_flag=True, default=False, help="Do not call API, only audit registry and bronze coverage")
def run_annual(api_key_env, metadata_dir, bronze_dir, registry_path, checkpoint_path, include_eu_bloc, reporters, years, commodities, flows, partner_code, exit_on_quota, rate_delay, audit_only, audit_log_path, zero_count_report_path):
    """
    Annual extraction. If a job returns 0 rows, record a coverage gap and tally it in checkpoint.
    """
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise click.ClickException(f"Missing env var {api_key_env}")

    reporters_df = load_reporters(metadata_dir)
    
    # Use custom reporters if provided, otherwise use defaults
    if reporters:
        reporters_list = [r.strip().upper() for r in reporters.split(",") if r.strip()]
        logger.info("Using custom reporters from CLI: %s", reporters_list)
    else:
        reporters_list = reporters_default_top3_eu_plus(world=True)
        if include_eu_bloc:
            eu_bloc = find_eu_bloc_iso3(reporters_df)
            if eu_bloc and eu_bloc not in reporters_list:
                reporters_list.insert(3, eu_bloc)
        logger.info("Using default reporters: %s", reporters_list)

    periods = parse_years(years)
    commodities_list = [c.strip() for c in commodities.split(",") if c.strip()]
    flows_list = [f.strip() for f in flows.split(",") if f.strip()]

    shape = QueryShape(partner_code=partner_code, freq_code="A")
    reg = ExtractionRegistry(registry_path)
    chk = Checkpoint(checkpoint_path)
    ex = ComtradeDataExtractor(api_key or "AUDIT_ONLY_NO_KEY", bronze_dir)

    audit_log = Path(audit_log_path)
    zero_count_report = Path(zero_count_report_path)

    audit_zero_count_hits = 0
    audit_missing_hits = 0

    plan: List[Tuple[str, str, int, str, str, str]] = []
    for iso3 in reporters_list:
        code = reporter_code_for_iso3(reporters_df, iso3)
        if code is None:
            logger.warning("Reporter not found in metadata: %s", iso3)
            continue
        for year in periods:
            for cmd in commodities_list:
                for flow in flows_list:
                    job_key = reg.job_key(code, year, cmd, flow, shape.partner_code, shape.type_code, shape.freq_code, shape.cl_code)
                    plan.append((job_key, iso3, code, year, cmd, flow))

    total = len(plan)
    logger.info("Planned annual jobs: %s (api=%s)", total, api_key_env)

    if audit_only:
        logger.info("AUDIT MODE ENABLED: reconciling bronze JSON counts for %s planned jobs", total)
        logger.info("AUDIT → writing scan log to %s", str(audit_log))
        logger.info("AUDIT → writing zero-count coverage report to %s", str(zero_count_report))

    if audit_only:
        logger.info("AUDIT MODE ENABLED: reconciling bronze JSON counts for %s planned jobs", total)

    for idx, (job_key, iso3, code, year, cmd, flow) in enumerate(plan, start=1):
        meta = {
            "reporter_iso3": iso3,
            "reporter_code": int(code),
            "period": str(year),
            "commodity": str(cmd),
            "flow": str(flow),
            "shape": shape.__dict__,
            "api_key_env": api_key_env,
        }
        chk.write({"position": {"index": idx, "total": total}, "last_attempted": meta})

        if (not audit_only) and reg.is_completed(job_key):
            continue

        # Bronze existence check (idempotent)
        existing = ex.find_existing(year, code, cmd, flow, shape)
        if existing:
            # Count-based bronze truth: prefer payload['_metadata']['record_count'], then top-level 'count', else len(data)
            try:
                payload = json.loads(existing.read_text())
                payload.setdefault("_metadata", {})

                if isinstance(payload.get("_metadata"), dict) and "record_count" in payload["_metadata"]:
                    record_count = int(payload["_metadata"].get("record_count") or 0)
                elif "count" in payload:
                    record_count = int(payload.get("count") or 0)
                    payload["_metadata"]["record_count"] = record_count
                else:
                    record_count = len(payload.get("data", []) or [])
                    payload["_metadata"]["record_count"] = record_count
            except Exception:
                payload = {"_metadata": {"record_count": 0}}
                record_count = 0

            if audit_only:
                logger.info(
                    "AUDIT → found bronze %s (count=%s) for %s %s cmd=%s flow=%s",
                    existing.name, record_count, iso3, year, cmd, flow
                )

            
            if audit_only:
                _append_jsonl(audit_log, {
                    "scanned_at_utc": datetime.now(timezone.utc).isoformat(),
                    "job_key": job_key,
                    "status": "bronze_found",
                    "record_count": int(record_count),
                    "filepath": str(existing),
                    "reporter_iso3": iso3,
                    "reporter_code": int(code),
                    "year": str(year),
                    "commodity": str(cmd),
                    "flow": str(flow),
                    "partner_code": int(shape.partner_code),
                    "freq_code": str(shape.freq_code),
                    "type_code": str(shape.type_code),
                    "cl_code": str(shape.cl_code),
                })

                if int(record_count) == 0:
                    audit_zero_count_hits += 1
                    _append_jsonl(zero_count_report, {
                        "reported_at_utc": datetime.now(timezone.utc).isoformat(),
                        "reason": "bronze_zero_count",
                        "job_key": job_key,
                        "record_count": 0,
                        "filepath": str(existing),
                        "filename": existing.name,
                        "reporter_iso3": iso3,
                        "reporter_code": int(code),
                        "year": str(year),
                        "commodity": str(cmd),
                        "flow": str(flow),
                        "partner_code": int(shape.partner_code),
                        "freq_code": str(shape.freq_code),
                        "type_code": str(shape.type_code),
                        "cl_code": str(shape.cl_code),
                    })
            reg.record_completed(job_key=job_key, payload=payload, filepath=existing, meta=meta | {"note": "recorded_from_existing_bronze"})

            # If existing annual bronze is empty, mark a gap so monthly can recover
            if int(payload.get("_metadata", {}).get("record_count", record_count)) == 0:
                reg.record_gap(
                    reporter_code=code,
                    year=str(year),
                    commodity=str(cmd),
                    flow=str(flow),
                    partner=shape.partner_code,
                    type_code=shape.type_code,
                    cl=shape.cl_code,
                    reason="annual_zero_rows_existing_bronze",
                    meta=meta,
                )
                update_coverage_tally(chk, code, str(year), str(cmd), str(flow))
            continue
        
        if audit_only:
            logger.info(
                "AUDIT ONLY → missing bronze for %s %s cmd=%s flow=%s",
                iso3, year, cmd, flow
            )

            
            _append_jsonl(audit_log, {
                "scanned_at_utc": datetime.now(timezone.utc).isoformat(),
                "job_key": job_key,
                "status": "bronze_missing",
                "record_count": None,
                "filepath": None,
                "reporter_iso3": iso3,
                "reporter_code": int(code),
                "year": str(year),
                "commodity": str(cmd),
                "flow": str(flow),
                "partner_code": int(shape.partner_code),
                "freq_code": str(shape.freq_code),
                "type_code": str(shape.type_code),
                "cl_code": str(shape.cl_code),
            })
            audit_missing_hits += 1

            reg.record_gap(
                reporter_code=code,
                year=str(year),
                commodity=str(cmd),
                flow=str(flow),
                partner=shape.partner_code,
                type_code=shape.type_code,
                cl=shape.cl_code,
                reason="audit_missing_bronze",
                meta=meta,
            )

            update_coverage_tally(chk, code, str(year), str(cmd), str(flow))
            continue
        logger.info("[%s/%s] %s | %s | cmd=%s | flow=%s", idx, total, iso3, year, cmd, flow)

        try:
            data = ex.extract_one(code, year, cmd, flow, shape)
            path = ex.save_one(data, year, code, cmd, flow, shape)
            reg.record_completed(job_key=job_key, payload=data, filepath=path, meta=meta)

            if int(data.get("_metadata", {}).get("record_count", 0)) == 0:
                reg.record_gap(
                    reporter_code=code,
                    year=str(year),
                    commodity=str(cmd),
                    flow=str(flow),
                    partner=shape.partner_code,
                    type_code=shape.type_code,
                    cl=shape.cl_code,
                    reason="annual_returned_zero_rows",
                    meta=meta,
                )
                update_coverage_tally(chk, code, str(year), str(cmd), str(flow))

            time.sleep(rate_delay)

        except QuotaHit as q:
            reg.record_failed(job_key=job_key, error_type="quota_or_throttle", http_status=q.http_status, message=q.message, meta=meta | {"wait_seconds": q.wait_seconds})
            if q.http_status == 429 or "HTTP 429" in (q.message or ""):
                logger.warning("429 throttle. Sleeping %ss then retrying same key.", q.wait_seconds)
                time.sleep(q.wait_seconds)
                continue

            logger.warning("Quota hit. Suggested wait %ss. %s", q.wait_seconds, q.message)
            if exit_on_quota:
                raise SystemExit(2)
            time.sleep(q.wait_seconds)

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = ""
            try:
                body = e.response.text if e.response is not None else ""
            except Exception:
                body = ""
            reg.record_failed(job_key=job_key, error_type="http_error", http_status=status, message=str(e) + " " + body, meta=meta)
            continue

        except Exception as e:
            reg.record_failed(job_key=job_key, error_type="exception", http_status=None, message=str(e), meta=meta)
            continue



    if audit_only:
        logger.info(
            "AUDIT SUMMARY: scanned=%s bronze_missing=%s bronze_zero_count=%s",
            total, audit_missing_hits, audit_zero_count_hits
        )
        logger.info("AUDIT SUMMARY: scan log written to %s", str(audit_log))
        logger.info("AUDIT SUMMARY: zero-count report written to %s", str(zero_count_report))


@cli.command("run-monthly-direct")
@click.option("--api-key-env", type=click.Choice(["COMTRADE_API_KEY_DATA", "COMTRADE_API_KEY_DATA_A", "COMTRADE_API_KEY_DATA_B"]), default="COMTRADE_API_KEY_DATA", show_default=True)
@click.option("--metadata-dir", default="data/metadata/comtrade", show_default=True)
@click.option("--bronze-dir", default="data/bronze/comtrade", show_default=True)
@click.option("--registry-path", default="logs/extraction_registry.jsonl", show_default=True)
@click.option("--checkpoint-path", default="logs/comtrade_checkpoint.json", show_default=True)
@click.option("--reporters", required=True, help="Comma-separated reporter ISO3 codes (e.g. 'USA,CHN,DEU')")
@click.option("--periods", required=True, help="Comma-separated YYYYMM periods, e.g. 202501,202506,202509,202512")
@click.option("--commodities", required=True, help="HS codes, comma-separated, e.g. 10,12,2709,2710")
@click.option("--flows", default="M,X", show_default=True)
@click.option("--partner-code", default=0, show_default=True, type=int)
@click.option("--exit-on-quota/--sleep-on-quota", default=True, show_default=True)
@click.option("--rate-delay", default=3.0, show_default=True, type=float)
def run_monthly_direct(api_key_env, metadata_dir, bronze_dir, registry_path, checkpoint_path, reporters, periods, commodities, flows, partner_code, exit_on_quota, rate_delay):
    """
    Direct monthly extraction for specific YYYYMM periods.
    """
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise click.ClickException(f"Missing env var {api_key_env}")

    reporters_df = load_reporters(metadata_dir)
    reporters_list = [r.strip().upper() for r in reporters.split(",") if r.strip()]
    periods_list = [p.strip() for p in periods.split(",") if p.strip()]
    commodities_list = [c.strip() for c in commodities.split(",") if c.strip()]
    flows_list = [f.strip() for f in flows.split(",") if f.strip()]

    # Validate periods are YYYYMM
    for p in periods_list:
        if not re.fullmatch(r"\d{6}", p):
            raise click.ClickException(f"Invalid period '{p}'. Expected YYYYMM format.")

    shape = QueryShape(partner_code=partner_code, freq_code="M")
    reg = ExtractionRegistry(registry_path)
    chk = Checkpoint(checkpoint_path)
    ex = ComtradeDataExtractor(api_key, bronze_dir)

    plan = []
    for iso3 in reporters_list:
        code = reporter_code_for_iso3(reporters_df, iso3)
        if code is None:
            logger.warning("Reporter not found in metadata: %s", iso3)
            continue
        for period in periods_list:
            for cmd in commodities_list:
                for flow in flows_list:
                    job_key = reg.job_key(code, period, cmd, flow, shape.partner_code, shape.type_code, shape.freq_code, shape.cl_code)
                    plan.append((job_key, iso3, code, period, cmd, flow))

    total = len(plan)
    logger.info("Planned monthly jobs: %s (api=%s)", total, api_key_env)

    for idx, (job_key, iso3, code, period, cmd, flow) in enumerate(plan, start=1):
        meta = {
            "reporter_iso3": iso3,
            "reporter_code": int(code),
            "period": str(period),
            "commodity": str(cmd),
            "flow": str(flow),
            "shape": shape.__dict__,
            "api_key_env": api_key_env,
        }
        chk.write({"position": {"index": idx, "total": total}, "last_attempted": meta})

        if reg.is_completed(job_key):
            continue

        existing = ex.find_existing(period, code, cmd, flow, shape)
        if existing:
            payload, rc = load_json_record_count(existing)
            reg.record_completed(job_key=job_key, payload=payload, filepath=existing, meta=meta | {"note": "recorded_from_existing_bronze"})
            continue

        logger.info("[%s/%s] %s | %s | cmd=%s | flow=%s", idx, total, iso3, period, cmd, flow)

        try:
            data = ex.extract_one(code, period, cmd, flow, shape)
            path = ex.save_one(data, period, code, cmd, flow, shape)
            reg.record_completed(job_key=job_key, payload=data, filepath=path, meta=meta)
            time.sleep(rate_delay)

        except QuotaHit as q:
            reg.record_failed(job_key=job_key, error_type="quota_or_throttle", http_status=q.http_status, message=q.message, meta=meta | {"wait_seconds": q.wait_seconds})
            if q.http_status == 429:
                logger.warning("429 throttle. Sleeping %ss then retrying.", q.wait_seconds)
                time.sleep(q.wait_seconds)
                continue
            logger.warning("Quota hit. Suggested wait %ss.", q.wait_seconds)
            if exit_on_quota:
                raise SystemExit(2)
            time.sleep(q.wait_seconds)

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = e.response.text if e.response else ""
            reg.record_failed(job_key=job_key, error_type="http_error", http_status=status, message=str(e) + " " + body, meta=meta)
            continue

        except Exception as e:
            reg.record_failed(job_key=job_key, error_type="exception", http_status=None, message=str(e), meta=meta)
            continue

@cli.command("run-monthly-gaps")
@click.option("--api-key-env", type=click.Choice(["COMTRADE_API_KEY_DATA", "COMTRADE_API_KEY_DATA_A", "COMTRADE_API_KEY_DATA_B"]), default="COMTRADE_API_KEY_DATA", show_default=True)
@click.option("--metadata-dir", default="data/metadata/comtrade", show_default=True)
@click.option("--bronze-dir", default="data/bronze/comtrade/test2", show_default=True)
@click.option("--registry-path", default="logs/test2/extraction_registry.jsonl", show_default=True)
@click.option("--checkpoint-path", default="logs/test2/comtrade_checkpoint.json", show_default=True)
@click.option("--months", default="10-12", show_default=True, help="Months to attempt per gap year, e.g. '10-12' or '1,2,3'")
@click.option("--exit-on-quota/--sleep-on-quota", default=True, show_default=True)
@click.option("--rate-delay", default=3.0, show_default=True, type=float)
@click.option("--max-gap-years", default=2, show_default=True, type=int, help="Safety valve: max distinct years to process in one run")
@click.option(
    "--coverage-source",
    type=click.Choice(["checkpoint", "zero-count-report"], case_sensitive=False),
    default="checkpoint",
    show_default=True,
    help="Where to read annual coverage gaps from. 'checkpoint' uses checkpoint.coverage_gaps. 'zero-count-report' uses the JSONL report created by audit-only runs."
)
@click.option(
    "--zero-count-report-path",
    default="logs/test2/coverage_zero_count.jsonl",
    show_default=True,
    help="Path to the zero-count JSONL report written by audit-only runs (used when --coverage-source=zero-count-report)."
)
def run_monthly_gaps(
    api_key_env,
    metadata_dir,
    bronze_dir,
    registry_path,
    checkpoint_path,
    months,
    exit_on_quota,
    rate_delay,
    max_gap_years,
    coverage_source,
    zero_count_report_path,
):
    """
    Monthly recovery ingestion driven by annual coverage gaps.

    Sources:
      - checkpoint: checkpoint["coverage_gaps"] (reporter -> year -> ["cmd_flow", ...])
      - zero-count-report: JSONL with entries for annual bronze where record_count == 0

    Writes monthly bronze to: year=YYYY/monthly/reporter=CODE/
    """
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise click.ClickException(f"Missing env var {api_key_env}")

    load_reporters(metadata_dir)  # validates metadata dir
    reg = ExtractionRegistry(registry_path)
    chk = Checkpoint(checkpoint_path)
    ex = ComtradeDataExtractor(api_key, bronze_dir)

    # -------- Build gaps list (reporter_code, year, cmd, flow) --------
    gaps: List[Tuple[int, str, str, str]] = []

    if coverage_source.lower() == "checkpoint":
        chk_data = chk.load()
        chk_gaps = chk_data.get("coverage_gaps", {})
        for r, years in chk_gaps.items():
            for y, items in (years or {}).items():
                for it in items or []:
                    try:
                        cmd, flow = it.split("_", 1)
                    except ValueError:
                        continue
                    gaps.append((int(r), str(y), str(cmd), str(flow)))

    else:
        report_path = Path(zero_count_report_path)
        if not report_path.exists():
            raise click.ClickException(
                f"Zero-count report not found at {report_path}. "
                f"Run: run-annual --audit-only --zero-count-report-path {report_path}"
            )

        for line in report_path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            # Only entries that indicate 0 count should be used for monthly recovery
            rc = rec.get("record_count", rec.get("count", None))
            try:
                rc = int(rc)
            except Exception:
                rc = None

            if rc != 0:
                continue

            reporter_code = rec.get("reporter_code")
            year = rec.get("year") or rec.get("period")
            cmd = rec.get("commodity")
            flow = rec.get("flow")

            if reporter_code is None or year is None or cmd is None or flow is None:
                continue

            gaps.append((int(reporter_code), str(year), str(cmd), str(flow)))

    if not gaps:
        logger.info("No coverage gaps found for source=%s. Nothing to do.", coverage_source)
        return

    # De-dup + deterministic ordering
    gaps = sorted(set(gaps), key=lambda x: (x[1], x[0], x[2], x[3]))

    # Safety valve on distinct years
    years_in_gaps = sorted(set(y for _, y, _, _ in gaps))
    if len(years_in_gaps) > max_gap_years:
        years_in_gaps = years_in_gaps[:max_gap_years]
        gaps = [g for g in gaps if g[1] in years_in_gaps]
        logger.warning("Safety valve: limiting to years=%s (use --max-gap-years to change)", years_in_gaps)

    months_list = parse_months(months)
    shape = QueryShape(partner_code=0, freq_code="M")  # monthly, world partner

    plan = []
    for reporter_code, year, cmd, flow in gaps:
        for period in month_periods_for_year(year, months_list):
            job_key = reg.job_key(
                reporter_code,
                period,
                cmd,
                flow,
                shape.partner_code,
                shape.type_code,
                shape.freq_code,
                shape.cl_code,
            )
            plan.append((job_key, reporter_code, period, cmd, flow))

    total = len(plan)
    logger.info(
        "Planned monthly gap jobs: %s (source=%s) (api=%s)",
        total,
        coverage_source,
        api_key_env,
    )

    for idx, (job_key, reporter_code, period, cmd, flow) in enumerate(plan, start=1):
        meta = {
            "reporter_code": int(reporter_code),
            "period": str(period),
            "commodity": str(cmd),
            "flow": str(flow),
            "shape": shape.__dict__,
            "api_key_env": api_key_env,
            "source": f"monthly_gap_recovery:{coverage_source}",
        }
        chk.write({"position": {"index": idx, "total": total}, "last_attempted": meta})

        # Idempotency: if already completed, skip
        if reg.is_completed(job_key):
            continue

        # Idempotency: if bronze exists, record and skip
        existing = ex.find_existing(period, reporter_code, cmd, flow, shape)
        if existing:
            payload, rc = load_json_record_count(existing)
            reg.record_completed(job_key=job_key, payload=payload, filepath=existing, meta=meta | {"note": "recorded_from_existing_bronze"})
            continue

        logger.info("[%s/%s] reporter=%s | period=%s | cmd=%s | flow=%s", idx, total, reporter_code, period, cmd, flow)

        try:
            data = ex.extract_one(reporter_code, period, cmd, flow, shape)
            path = ex.save_one(data, period, reporter_code, cmd, flow, shape)
            reg.record_completed(job_key=job_key, payload=data, filepath=path, meta=meta)
            time.sleep(rate_delay)

        except QuotaHit as q:
            reg.record_failed(
                job_key=job_key,
                error_type="quota_or_throttle",
                http_status=q.http_status,
                message=q.message,
                meta=meta | {"wait_seconds": q.wait_seconds},
            )

            if q.http_status == 429 or "HTTP 429" in (q.message or ""):
                logger.warning("429 throttle. Sleeping %ss then continuing.", q.wait_seconds)
                time.sleep(q.wait_seconds)
                continue

            logger.warning("Quota hit. Suggested wait %ss. %s", q.wait_seconds, q.message)
            if exit_on_quota:
                raise SystemExit(2)
            time.sleep(q.wait_seconds)

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = ""
            try:
                body = e.response.text if e.response is not None else ""
            except Exception:
                body = ""
            reg.record_failed(job_key=job_key, error_type="http_error", http_status=status, message=str(e) + " " + body, meta=meta)
            continue

        except Exception as e:
            reg.record_failed(job_key=job_key, error_type="exception", http_status=None, message=str(e), meta=meta)
            continue


@cli.command("coverage-report")
@click.option("--registry-path", default="logs/test2/extraction_registry.jsonl", show_default=True)
def coverage_report(registry_path: str):
    reg = ExtractionRegistry(registry_path)
    gaps = reg.iter_gaps()

    if not gaps:
        click.echo("No coverage gaps recorded.")
        return

    summary: Dict[str, Dict[str, int]] = {}
    for g in gaps:
        meta = g.get("meta", {}) or {}
        reporter = str(meta.get("reporter_iso3") or meta.get("reporter_code") or "unknown")
        year = str(meta.get("period") or meta.get("year") or "unknown")
        summary.setdefault(reporter, {})
        summary[reporter][year] = summary[reporter].get(year, 0) + 1

    click.echo("Coverage gaps (annual returned zero rows):")
    for reporter in sorted(summary.keys()):
        years = summary[reporter]
        years_str = ", ".join([f"{y}:{years[y]}" for y in sorted(years.keys())])
        click.echo(f"  {reporter} -> {years_str}")


@cli.command("run-event-batch")
@click.option("--api-key-env", default="COMTRADE_API_KEY_DATA")
@click.option("--event-date", required=True, help="YYYYMM of the event (e.g. 202103 for Ever Given)")
@click.option("--commodities", default="10,12,2709,2710")
@click.option("--reporters", default="97,100,156,251,528,642,724,842")
def run_event_batch(api_key_env, event_date, commodities, reporters):
    """
    Fetches a 7-month window (Event Month +/- 3 months) in optimized batches.
    """
    api_key = os.getenv(api_key_env)
    ex = ComtradeDataExtractor(api_key, "data/bronze/comtrade/events")
    shape = QueryShape(freq_code="M")

    # Generate Month Window
    base_dt = datetime.strptime(event_date, "%Y%m%d" if len(event_date)>6 else "%Y%m")
    # Simplistic window generation
    periods = []
    for i in range(-3, 4):
        # Calculation for months spanning across years
        m = base_dt.month + i
        y = base_dt.year + (m - 1) // 12
        m = (m - 1) % 12 + 1
        periods.append(f"{y}{m:02d}")

    # UN Comtrade Batch Limits: Max 5 periods, Max 5 reporters per call
    # We break our 7 months into two batches: [4 months] and [3 months]
    period_batches = [",".join(periods[:4]), ",".join(periods[4:])]
    
    # Break 8 reporters into two batches: [5 reporters] and [3 reporters]
    rep_list = reporters.split(",")
    reporter_batches = [",".join(rep_list[:5]), ",".join(rep_list[5:])]

    for rb in reporter_batches:
        for pb in period_batches:
            for flow in ["M", "X"]: # Imports and Exports
                logger.info(f"Requesting Batch: Reps={rb} | Periods={pb} | Flow={flow}")
                try:
                    data = ex.extract_one(rb, pb, commodities, flow, shape)
                    
                    # Save with a name indicating it's a batch
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    fn = f"event_{event_date}_batch_{ts}.json"
                    path = Path(f"data/bronze/comtrade/events/{fn}")
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(data, indent=2))
                    
                    logger.info(f"Saved {data['_metadata']['record_count']} records to {fn}")
                    time.sleep(10) # Respectful delay for Free Tier
                    
                except Exception as e:
                    logger.error(f"Batch failed: {e}")


@cli.command("run-monthly-history")
@click.option("--api-key-env", default="COMTRADE_API_KEY_DATA", show_default=True)
@click.option("--bronze-dir", default="data/bronze/comtrade/monthly_history", show_default=True)
@click.option("--registry-path", default="logs/monthly_history/extraction_registry.jsonl", show_default=True)
@click.option("--checkpoint-path", default="logs/monthly_history/comtrade_checkpoint.json", show_default=True)
@click.option("--reporters", default="97,100,156,251,528,642,724,842", help="Comma-separated reporter numeric codes")
@click.option("--years", required=True, default="2020,2021,2022,2023,2024,2025", help="Comma-separated years")
@click.option("--commodities", default="10,12,2709,2710", help="HS codes, comma-separated")
@click.option("--flows", default="M,X", show_default=True)
@click.option("--rate-delay", default=10.0, show_default=True, type=float)
@click.option("--exit-on-quota/--sleep-on-quota", default=True, show_default=True)
def run_monthly_history(api_key_env, bronze_dir, registry_path, checkpoint_path, reporters, years, commodities, flows, rate_delay, exit_on_quota):
    """
    Robust bulk extraction of historical monthly data. 
    Batches reporters (max 5) and periods (max 5) to minimize API calls.
    Uses dedicated history logs to prevent registry pollution.
    """
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise click.ClickException(f"Missing env var {api_key_env}")

    # 1. Setup Infrastructure
    reg = ExtractionRegistry(registry_path)
    chk = Checkpoint(checkpoint_path)
    ex = ComtradeDataExtractor(api_key, bronze_dir)
    shape = QueryShape(freq_code="M")

    # 2. Generate all Periods (e.g. 202001, 202002 ... 202512)
    years_list = parse_years(years)
    all_periods = []
    for y in years_list:
        all_periods.extend([f"{y}{str(m).zfill(2)}" for m in range(1, 13)])

    # 3. Create API-Compliant Batches (Max 5 items per parameter)
    rep_list = [r.strip() for r in reporters.split(",") if r.strip()]
    reporter_batches = [",".join(rep_list[i:i+5]) for i in range(0, len(rep_list), 5)]
    period_batches = [",".join(all_periods[i:i+5]) for i in range(0, len(all_periods), 5)]
    flow_list = [f.strip() for f in flows.split(",") if f.strip()]

    # 4. Build Execution Plan
    plan = []
    for rb in reporter_batches:
        for pb in period_batches:
            for flow in flow_list:
                # Create a unique job key for this exact batch
                safe_rb = rb.replace(",", "-")
                safe_pb = pb.replace(",", "-")
                job_key = f"batch_hist_{safe_rb}_periods_{safe_pb}_flow_{flow}"
                plan.append((job_key, rb, pb, flow))

    total = len(plan)
    logger.info(f"Planned monthly history batches: {total} (api={api_key_env})")

    # 5. Execute Plan with Retry & Idempotency
    for idx, (job_key, rb, pb, flow) in enumerate(plan, start=1):
        meta = {
            "reporters": rb,
            "periods": pb,
            "commodities": commodities,
            "flow": flow,
            "api_key_env": api_key_env
        }
        chk.write({"position": {"index": idx, "total": total}, "last_attempted": meta})

        if reg.is_completed(job_key):
            logger.info(f"[{idx}/{total}] Skipping (Already Completed) | Reps: {rb} | Periods: {pb} | Flow: {flow}")
            continue

        logger.info(f"[{idx}/{total}] Extracting | Reps: {rb} | Periods: {pb} | Flow: {flow}")

        try:
            # We use extract_one which we upgraded previously to handle string batches
            data = ex.extract_one(rb, pb, commodities, flow, shape)
            
            # Save file - we name it according to the batch start/end period
            p_start, p_end = pb.split(",")[0], pb.split(",")[-1]
            batch_start_year = p_start[:4]  # Extracts 'YYYY' from 'YYYYMM'
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fn = f"hist_batch_reps_{rb.replace(',','-')}_periods_{p_start}_to_{p_end}_flow_{flow}_{ts}.json"
            
            # Add the year=YYYY folder dynamically to the path
            save_path = Path(bronze_dir) / f"year={batch_start_year}" / fn
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_text(json.dumps(data, indent=2))

            reg.record_completed(job_key=job_key, payload=data, filepath=save_path, meta=meta)
            logger.info(f"  -> Saved {data['_metadata']['record_count']} records to {fn}")
            
            time.sleep(rate_delay)

        except QuotaHit as q:
            reg.record_failed(job_key=job_key, error_type="quota_or_throttle", http_status=q.http_status, message=q.message, meta=meta)
            if q.http_status == 429:
                logger.warning(f"429 Throttle. Sleeping {q.wait_seconds}s...")
                time.sleep(q.wait_seconds)
                continue
            
            logger.warning(f"Quota Hit! Suggested wait: {q.wait_seconds}s. Message: {q.message}")
            if exit_on_quota:
                raise SystemExit(2)
            time.sleep(q.wait_seconds)

        except Exception as e:
            logger.error(f"  -> Batch failed: {str(e)}")
            reg.record_failed(job_key=job_key, error_type="exception", http_status=None, message=str(e), meta=meta)
            time.sleep(rate_delay) # Don't hammer the API on error


@cli.command("coverage-heatmap")
@click.option("--registry-path", default="logs/extraction_registry.jsonl", show_default=True, help="Used for listing reporters/years if you want to include registry gaps.")
@click.option("--checkpoint-path", default="logs/comtrade_checkpoint.json", show_default=True, help="Used when --source=checkpoint.")
@click.option(
    "--source",
    type=click.Choice(["checkpoint", "zero-count-report", "registry-gaps"], case_sensitive=False),
    default="zero-count-report",
    show_default=True,
    help="Data source for the heatmap. checkpoint uses checkpoint.coverage_gaps. zero-count-report uses the audit JSONL. registry-gaps uses coverage_gap events in the registry."
)
@click.option(
    "--zero-count-report-path",
    default="logs/coverage_zero_count.jsonl",
    show_default=True,
    help="Path to the zero-count JSONL report written by audit-only runs (used when --source=zero-count-report)."
)
@click.option("--format", "out_format", type=click.Choice(["text", "csv"], case_sensitive=False), default="text", show_default=True)
def coverage_heatmap(registry_path: str, checkpoint_path: str, source: str, zero_count_report_path: str, out_format: str):
    """
    Print a coverage heatmap showing which annual slices are OK vs gaps.

    Heatmap cell meanings:
      - '✓' : annual slice exists and has non-zero count (best-effort)
      - '0' : annual slice exists but count == 0 (gap)
      - '·' : unknown/not present in chosen source
    """
    rows: List[Tuple[str, str, str, str, str]] = []  # reporter, year, cmd, flow, status

    src = source.lower()

    if src == "checkpoint":
        chk = Checkpoint(checkpoint_path)
        chk_data = chk.load()
        chk_gaps = chk_data.get("coverage_gaps", {})
        for r, years in (chk_gaps or {}).items():
            for y, items in (years or {}).items():
                for it in items or []:
                    try:
                        cmd, flow = it.split("_", 1)
                    except ValueError:
                        continue
                    rows.append((str(r), str(y), str(cmd), str(flow), "0"))

    elif src == "registry-gaps":
        reg = ExtractionRegistry(registry_path)
        gaps = reg.iter_gaps()
        for g in gaps:
            meta = g.get("meta", {}) or {}
            reporter_code = meta.get("reporter_code") or meta.get("reporterCode") or meta.get("reporter")
            year = meta.get("period") or meta.get("year")
            cmd = meta.get("commodity")
            flow = meta.get("flow")
            if reporter_code is None or year is None or cmd is None or flow is None:
                continue
            rows.append((str(reporter_code), str(year), str(cmd), str(flow), "0"))

    else:
        report_path = Path(zero_count_report_path)
        if not report_path.exists():
            raise click.ClickException(f"Zero-count report not found at {report_path}")
        for line in report_path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            rc = rec.get("record_count", rec.get("count", None))
            try:
                rc = int(rc)
            except Exception:
                rc = None
            if rc != 0:
                continue
            reporter_code = rec.get("reporter_code")
            year = rec.get("year") or rec.get("period")
            cmd = rec.get("commodity")
            flow = rec.get("flow")
            if reporter_code is None or year is None or cmd is None or flow is None:
                continue
            rows.append((str(reporter_code), str(year), str(cmd), str(flow), "0"))

    if not rows:
        click.echo("No gap rows found for the chosen source.")
        return

    # Build dimensions
    reporters = sorted(set(r for r, _, _, _, _ in rows), key=lambda x: int(x) if x.isdigit() else x)
    years = sorted(set(y for _, y, _, _, _ in rows))
    metrics = sorted(set(f"HS{cmd}_{flow}" for _, _, cmd, flow, _ in rows))

    # Index for quick lookup
    idx = {(r, y, cmd, flow): st for r, y, cmd, flow, st in rows}

    # Build table
    header = ["reporter_code", "year"] + metrics

    lines = []
    if out_format.lower() == "csv":
        lines.append(",".join(header))
        for r in reporters:
            for y in years:
                row = [r, y]
                for m in metrics:
                    cmd = m[2:].split("_", 1)[0]
                    flow = m.split("_", 1)[1]
                    cell = idx.get((r, y, cmd, flow), "·")
                    row.append(cell)
                lines.append(",".join(row))
        click.echo("\n".join(lines))
        return

    # text format
    # compute widths
    table = [header]
    for r in reporters:
        for y in years:
            row = [r, y]
            for m in metrics:
                cmd = m[2:].split("_", 1)[0]
                flow = m.split("_", 1)[1]
                cell = idx.get((r, y, cmd, flow), "·")
                row.append(cell)
            table.append(row)

    widths = [max(len(str(table[i][j])) for i in range(len(table))) for j in range(len(header))]
    def fmt_row(rw):
        return "  ".join(str(rw[i]).ljust(widths[i]) for i in range(len(rw)))

    click.echo(fmt_row(table[0]))
    click.echo(fmt_row(["-" * w for w in widths]))
    for rw in table[1:]:
        click.echo(fmt_row(rw))


if __name__ == "__main__":
    cli()