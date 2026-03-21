"""
FRED API Metadata Extraction Tool

Extracts and stores metadata for FRED series to support data governance.
Metadata is stored in JSON format under data/metadata/fred/

Usage:
    python ingest/fred/fred_metadata_extract.py
    python ingest/fred/fred_metadata_extract.py --series DCOILBRENTEU,DCOILWTICO
"""

import requests
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import argparse
from typing import List, Dict, Any

# -------------------- CONFIG --------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
METADATA_ROOT = os.getenv("METADATA_ROOT", str(PROJECT_ROOT / "data" / "metadata"))
FRED_API_KEY = os.getenv("FRED_API_KEY")
BASE_URL = "https://api.stlouisfed.org/fred"

# Default Brent crude series
DEFAULT_SERIES = [
    "DCOILBRENTEU",  # Crude Oil Prices: Brent - Europe
    "DCOILWTICO",    # WTI Crude (US proxy)
    "POILBREUSDM"    # Global price of Brent Crude
]


def fetch_series_metadata(series_id: str) -> Dict[str, Any]:
    """
    Fetch metadata for a FRED series.
    
    Returns dict with:
    - id, title, units, frequency
    - observation_start, observation_end
    - last_updated, popularity
    - notes (source documentation)
    """
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY environment variable not set")
    
    url = f"{BASE_URL}/series"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"FRED API HTTP {resp.status_code}: {resp.text[:300]}")
    
    data = resp.json()
    if "seriess" in data and len(data["seriess"]) > 0:
        return data["seriess"][0]
    else:
        raise RuntimeError(f"No metadata returned for series {series_id}")


def fetch_series_tags(series_id: str) -> List[Dict[str, Any]]:
    """
    Fetch tags/categories for a FRED series.
    Provides additional context and classification.
    """
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY environment variable not set")
    
    url = f"{BASE_URL}/series/tags"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        return []
    
    data = resp.json()
    return data.get("tags", [])


def fetch_series_categories(series_id: str) -> List[Dict[str, Any]]:
    """
    Fetch category hierarchy for a FRED series.
    """
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY environment variable not set")
    
    url = f"{BASE_URL}/series/categories"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        return []
    
    data = resp.json()
    return data.get("categories", [])


def save_metadata(series_id: str, metadata: Dict[str, Any], 
                  tags: List[Dict[str, Any]], categories: List[Dict[str, Any]]) -> str:
    """
    Save series metadata to JSON file in metadata/fred/ directory.
    
    Returns path to saved file.
    """
    # Create metadata directory
    fred_metadata_dir = os.path.join(METADATA_ROOT, "fred")
    os.makedirs(fred_metadata_dir, exist_ok=True)
    
    # Build comprehensive metadata document
    doc = {
        "series_id": series_id,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata,
        "tags": tags,
        "categories": categories,
        "data_dictionary": {
            "units": metadata.get("units"),
            "units_short": metadata.get("units_short"),
            "measurement": "Price per unit volume",
            "denomination": "US Dollars",
            "volume_unit": "Barrel (42 US gallons)",
            "seasonal_adjustment": metadata.get("seasonal_adjustment"),
            "frequency": metadata.get("frequency")
        },
        "governance": {
            "source": "Federal Reserve Economic Data (FRED)",
            "source_url": "https://fred.stlouisfed.org/",
            "api_endpoint": f"{BASE_URL}/series/observations",
            "last_updated": metadata.get("last_updated"),
            "observation_period": {
                "start": metadata.get("observation_start"),
                "end": metadata.get("observation_end")
            },
            "update_frequency": metadata.get("frequency"),
            "data_license": "Public Domain / Federal Government Data",
            "citation": f"U.S. Energy Information Administration, {metadata.get('title')} [{series_id}], retrieved from FRED, Federal Reserve Bank of St. Louis"
        }
    }
    
    # Save to file
    file_path = os.path.join(fred_metadata_dir, f"{series_id}_metadata.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
    
    return file_path


def generate_summary_report(all_metadata: List[Dict[str, Any]]) -> str:
    """
    Generate a summary report of all extracted metadata.
    """
    fred_metadata_dir = os.path.join(METADATA_ROOT, "fred")
    os.makedirs(fred_metadata_dir, exist_ok=True)
    
    report_path = os.path.join(fred_metadata_dir, "README.md")
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# FRED Data Metadata\n\n")
        f.write(f"**Extracted:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n")
        f.write("## Series Overview\n\n")
        
        for meta in all_metadata:
            series_id = meta["series_id"]
            m = meta["metadata"]
            
            f.write(f"### {series_id}: {m.get('title')}\n\n")
            f.write(f"- **Units:** {m.get('units')}\n")
            f.write(f"- **Frequency:** {m.get('frequency')}\n")
            f.write(f"- **Coverage:** {m.get('observation_start')} to {m.get('observation_end')}\n")
            f.write(f"- **Last Updated:** {m.get('last_updated')}\n")
            f.write(f"- **Seasonal Adjustment:** {m.get('seasonal_adjustment')}\n")
            
            if m.get('notes'):
                f.write(f"- **Notes:** {m.get('notes')[:200]}...\n")
            
            f.write(f"- **Metadata File:** `{series_id}_metadata.json`\n\n")
        
        f.write("\n## Data Dictionary\n\n")
        f.write("All Brent crude oil prices are measured in:\n\n")
        f.write("- **Currency:** US Dollars ($)\n")
        f.write("- **Volume Unit:** Barrel (42 US gallons or ~159 liters)\n")
        f.write("- **Measurement:** Price per Barrel ($/Barrel)\n\n")
        
        f.write("## API Documentation\n\n")
        f.write("- **API Provider:** Federal Reserve Bank of St. Louis\n")
        f.write("- **API Documentation:** https://fred.stlouisfed.org/docs/api/fred/\n")
        f.write("- **Series Endpoint:** `https://api.stlouisfed.org/fred/series`\n")
        f.write("- **Observations Endpoint:** `https://api.stlouisfed.org/fred/series/observations`\n")
        f.write("- **Authentication:** API Key required (free registration)\n\n")
        
    
    return report_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract metadata from FRED API for data governance"
    )
    parser.add_argument(
        "--series",
        type=str,
        default=",".join(DEFAULT_SERIES),
        help="Comma-separated list of FRED series IDs"
    )
    args = parser.parse_args()
    
    series_list = [s.strip() for s in args.series.split(",")]
    
    print(f"Extracting metadata for {len(series_list)} series...")
    print(f"Metadata will be saved to: {os.path.join(METADATA_ROOT, 'fred')}\n")
    
    all_metadata = []
    
    for series_id in series_list:
        try:
            print(f"Fetching {series_id}...")
            
            # Fetch all metadata
            metadata = fetch_series_metadata(series_id)
            tags = fetch_series_tags(series_id)
            categories = fetch_series_categories(series_id)
            
            # Save to file
            file_path = save_metadata(series_id, metadata, tags, categories)
            
            print(f"  ✓ Saved: {file_path}")
            print(f"  - Title: {metadata.get('title')}")
            print(f"  - Units: {metadata.get('units')}")
            print(f"  - Coverage: {metadata.get('observation_start')} to {metadata.get('observation_end')}")
            print()
            
            all_metadata.append({
                "series_id": series_id,
                "metadata": metadata,
                "tags": tags,
                "categories": categories
            })
            
        except Exception as e:
            print(f"  ✗ Error: {e}\n")
            continue
    
    # Generate summary report
    if all_metadata:
        report_path = generate_summary_report(all_metadata)
        print(f"\n✓ Summary report generated: {report_path}")
        print(f"\nTotal metadata files created: {len(all_metadata)}")
    else:
        print("\nNo metadata extracted.")
