import os
import csv
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Iterable, Dict, Any, List


# Required Bronze columns, in order
'''
Standardised structure for the Bronze table schema across datasets.
'''
BRONZE_COLUMNS = [
    "dt", "dataset", "series_id", "geo", "value", "source", "ingest_ts"
]

def ensure_bronze_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Fill missing keys and coerce obvious types; leaves value coercion to caller."""
    out = {}
    for k in BRONZE_COLUMNS:
        out[k] = row.get(k, None)
    # coerce dt to YYYY-MM-DD
    if isinstance(out["dt"], (datetime, date)):
        out["dt"] = out["dt"].strftime("%Y-%m-%d")
    # ingest_ts default now (UTC ISO)
    if not out["ingest_ts"]:
        out["ingest_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # defaults
    if not out["geo"]:
        out["geo"] = "NA"
    return out

def bronze_path(root: str, dataset: str, dt_str: str) -> Path:
    # {root}/{dataset}/dt=YYYY-MM-DD/
    return Path(root) / dataset / f"dt={dt_str}"

def write_bronze_csv(rows: Iterable[Dict[str, Any]], *, dataset: str, dt_str: str, root: str) -> str:
    """
    Writes a part-* CSV under {root}/{dataset}/dt={dt}/
    Returns the file path written.
    """
    target_dir = bronze_path(root, dataset, dt_str)
    target_dir.mkdir(parents=True, exist_ok=True)
    part_name = f"part-{datetime.now(timezone.utc).strftime('%H%M%S%f')}.csv"
    out_path = target_dir / part_name

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BRONZE_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(ensure_bronze_row(r))

    return str(out_path)

def write_bronze_by_dt(rows: List[Dict[str, Any]], *, dataset: str, root: str) -> List[str]:
    """
    Bucket records by r['dt'] and call write_bronze_csv() once per dt.
    Returns the list of written file paths.
    """
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        dt_str = r["dt"]
        buckets.setdefault(dt_str, []).append(r)

    written: List[str] = []
    for dt_str, group in buckets.items():
        written.append(write_bronze_csv(group, dataset=dataset, dt_str=dt_str, root=root))
    return written
