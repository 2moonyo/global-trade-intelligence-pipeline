import requests
import json
from datetime import date, datetime, timezone
from typing import Dict, Any, List, Tuple, Iterable
import csv
import os
from ingest.common.bronze_io import write_bronze_by_dt
from pathlib import Path
from dotenv import load_dotenv
import argparse
from collections import defaultdict

# -------------------- IO CONFIG --------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
BRONZE_ROOT = os.getenv("BRONZE_ROOT", str(PROJECT_ROOT / "data" / "bronze"))
DATASET = os.getenv("DATASET", "brent")  
SOURCE = os.getenv("SOURCE", "FRED")  
# -------------------- CONFIG --------------------
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY = os.getenv("FRED_API_KEY")  # Required: Set in .env file

# FRED Series IDs for Brent Crude Oil
BRENT_SERIES = {
    "europe": "DCOILBRENTEU",  # Crude Oil Prices: Brent - Europe ($/Barrel)
    "us": "DCOILWTICO",        # WTI Crude (closest proxy for US Brent pricing)
    "world": "POILBREUSDM"      # Global price of Brent Crude ($/Barrel)
}



# -------------------- URL BUILDER --------------------
def build_fred_url(
    series_id: str,         # FRED series ID
    start: str,             # "2000-01-01"
    end: str,               # "2025-10-28"
    freq: str = "d",        # "d" for daily
) -> str:
    """
    Build FRED API URL for a specific series.
    Requires FRED_API_KEY to be set.
    """
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY environment variable not set")
    
    url = (
        f"{BASE_URL}"
        f"?series_id={series_id}"
        f"&api_key={FRED_API_KEY}"
        f"&file_type=json"
        f"&observation_start={start}"
        f"&observation_end={end}"
        f"&frequency={freq}"
    )
    return url


def fetch_fred_series(url: str) -> Dict[str, Any]:
    """
    Request the FRED API and return parsed JSON.
    Raises if non-200 or invalid JSON.
    """
    resp = requests.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"FRED API HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Did not get JSON. content-type={resp.headers.get('content-type')}, body[:200]={resp.text[:200]}"
        ) from e


def extract_brent_rows(payload: Dict[str, Any], series_id: str, region: str) -> List[Tuple[str, str, str, float]]:
    """
    Convert FRED API response into tidy rows:
    (date, region, series_id, price)
    
    FRED response structure:
    {
      "observations": [
        {"date": "2020-01-01", "value": "66.00"},
        {"date": "2020-01-02", "value": "66.25"},
        ...
      ]
    }
    """
    rows: List[Tuple[str, str, str, float]] = []
    
    observations = payload.get("observations", [])
    
    for obs in observations:
        date_str = obs.get("date")
        value_str = obs.get("value")
        
        # Skip missing or non-numeric values (FRED uses "." for missing)
        if not date_str or not value_str or value_str == ".":
            continue
        
        try:
            price = float(value_str)
            rows.append((date_str, region, series_id, price))
        except (ValueError, TypeError):
            continue
    
    return rows
# -------------------- WRITE CSV --------------------

def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)

def write_brent_by_date(rows: List[Tuple[str, str, str, float]], dataset: str, root: str) -> List[str]:
    """
    Save Brent crude rows organized by partition structure (same as portwatch).
    Each day gets a CSV file with all three regional prices.
    Structure: Bronze/brent/year=YYYY/month=MM/day=DD/brent_prices_YYYYMMDD.csv
    
    Columns: date, region, series_id, price_usd, load_ts
    """
    
    
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    # Group rows by date
    rows_by_date = defaultdict(list)
    for date_str, region, series_id, price in rows:
        rows_by_date[date_str].append((region, series_id, price))
    
    written_files = []
    
    for date_str, day_rows in sorted(rows_by_date.items()):
        # Parse date to extract year, month, day
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Build directory structure: Bronze/brent/year=YYYY/month=MM/day=DD/
        day_dir = os.path.join(root, dataset, f"year={year}", f"month={month}", f"day={day}")
        os.makedirs(day_dir, exist_ok=True)
        
        # Write CSV for this date
        file_name = f"brent_prices_{dt.strftime('%Y%m%d')}.csv"
        file_path = os.path.join(day_dir, file_name)
        
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "region", "series_id", "price_usd", "load_ts"])
            for region, series_id, price in day_rows:
                w.writerow([date_str, region, series_id, price, stamp])
        
        written_files.append(file_path)
    
    return written_files


def write_csv_legacy(rows: List[Tuple[str, str, str, float]], out_path: str):
    """
    Legacy function - Save rows as a CSV with header and timestamp.
    Columns: date, region, series_id, price_usd, load_ts
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    dataset_dir = os.path.join(BRONZE_ROOT, DATASET)
    batch_dir = os.path.join(dataset_dir, "Batch")
    ensure_dir(batch_dir)
    full_path = os.path.join(batch_dir, f"brent_crude_{stamp}.csv")

    with open(full_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "region", "series_id", "price_usd", "load_ts"])
        for date_str, region, series_id, price in rows:
            w.writerow([date_str, region, series_id, price, stamp])
    
    return full_path


if __name__ == "__main__":
    # ---- CONFIG SECTION ----
    # Fetch Brent crude oil prices from FRED API

    parser = argparse.ArgumentParser(description="Extract Brent crude oil prices from FRED API")
    parser.add_argument("--start", type=str, default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=date.today().isoformat(), help="End date (YYYY-MM-DD)")
    parser.add_argument("--freq", type=str, default="d", help="Frequency: d=daily, w=weekly, m=monthly (default: d)")
    args = parser.parse_args()
    
    start_date = args.start
    end_date = args.end
    freq = args.freq

    print(f"Fetching Brent crude prices from {start_date} to {end_date}")
    print(f"Writing to: {os.path.join(BRONZE_ROOT, DATASET)}")
    print()

    # Fetch data for all three Brent series
    all_rows = []
    
    for region, series_id in BRENT_SERIES.items():
        print(f"Fetching {region.upper()} series: {series_id}")
        url = build_fred_url(
            series_id=series_id,
            start=start_date,
            end=end_date,
            freq=freq
        )
        
        try:
            payload = fetch_fred_series(url)
            rows = extract_brent_rows(payload, series_id, region)
            all_rows.extend(rows)
            print(f"  Retrieved {len(rows)} observations for {region}")
        except Exception as e:
            print(f"  Error fetching {region}: {e}")
            continue
    
    # Sort all rows by date and region
    all_rows.sort(key=lambda x: (x[0], x[1]))

    if not all_rows:
        print("No data retrieved. Exiting.")
        exit(1)

    # ---- WRITE TO BRONZE WITH NEW STRUCTURE ----
    # Write to Year/Month/Day structure
    bronze_files = write_brent_by_date(all_rows, dataset=DATASET, root=BRONZE_ROOT)

    # Also write a legacy batch CSV for reference
    reference_path = write_csv_legacy(all_rows, "brent_history.csv")

    print()
    print(json.dumps({
        "dataset": DATASET,
        "source": SOURCE,
        "total_observations": len(all_rows),
        "files_written": len(bronze_files),
        "bronze_root": BRONZE_ROOT,
        "reference_csv": reference_path,
        "date_range": f"{start_date} to {end_date}",
        "sample_files": bronze_files[:5] if len(bronze_files) > 5 else bronze_files
    }, indent=2))
