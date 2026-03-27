#!/usr/bin/env python3
"""
Collects daily NOAA Climate Data Online observations for CONUS.
Target: previous day's TMAX, TMIN, PRCP at all available stations.
Outputs: daily CSV + monthly aggregate JSON.
"""

import os
import json
import time
import argparse
from datetime import date, timedelta
from pathlib import Path

import httpx

# ── Config ────────────────────────────────────────────────────────────────────
API_BASE = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
TOKEN = os.environ.get("NOAA_CDO_TOKEN")
if not TOKEN:
    raise RuntimeError("NOAA_CDO_TOKEN environment variable is required")

HEADERS = {"token": TOKEN, "Accept": "application/json"}
CONUS_BBOX = "-125.0,24.0,-66.0,49.0"  # approximate CONUS bounding box
DATASETS = {"TMAX": "GHCND:tmax", "TMIN": "GHCND:tmin", "PRCP": "GHCND:prcp"}
FRESHNESS_DAYS = 7

# Rate limit: 5 req/s → 0.2 s minimum gap between requests
REQ_GAP = 0.25


def date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def previous_day() -> date:
    return date.today() - timedelta(days=1)


def fetch(url: str, params: dict, max_retries: int = 5) -> dict:
    """GET with exponential back-off on 429."""
    for attempt in range(max_retries):
        r = httpx.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            wait = (2 ** attempt) * REQ_GAP
            time.sleep(wait)
            continue
        r.raise_for_status()
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")


def collect_daily(target_date: date, stations: list[str] | None = None) -> dict:
    """Fetch TMAX/TMIN/PRCP for a single day."""
    end = date_str(target_date)
    start = end  # single-day window
    results = {}
    for name, dataset_id in DATASETS.items():
        params = {
            "datasetid": dataset_id,
            "startdate": start,
            "enddate": end,
            "units": "metric",
            "bbox": CONUS_BBOX,
            "limit": 1000,
        }
        if stations:
            params["stationid"] = ",".join(stations[:100])  # batched
        data = fetch(f"{API_BASE}/data", params)
        results[name] = data.get("results", [])
        time.sleep(REQ_GAP)
    return results


def collect_monthly(year: int, month: int) -> dict:
    """Fetch monthly aggregates for a single month."""
    start = f"{year}-{month:02d}-01"
    import calendar
    end   = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"  # CDO clips to actual last day
    results = {}
    for name, dataset_id in DATASETS.items():
        params = {
            "datasetid": dataset_id,
            "startdate": start,
            "enddate": end,
            "units": "metric",
            "bbox": CONUS_BBOX,
            "limit": 1000,
            "includemetadata": "false",
        }
        data = fetch(f"{API_BASE}/data", params)
        results[name] = data.get("results", [])
        time.sleep(REQ_GAP)
    return results


def main():
    parser = argparse.ArgumentParser(description="NOAA CDO daily collection")
    parser.add_argument("--date", default=date_str(previous_day()),
                        help="Target date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--output-dir", default="./output",
                        help="Directory for output files")
    args = parser.parse_args()

    target = date.fromisoformat(args.date)
    freshness_cutoff = date.today() - timedelta(days=FRESHNESS_DAYS)
    if target < freshness_cutoff:
        raise SystemExit(f"Date {target} exceeds {FRESHNESS_DAYS}-day freshness window — skip.")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Daily
    daily = collect_daily(target)
    daily_path = out_dir / f"daily_{args.date}.json"
    daily_path.write_text(json.dumps(daily, indent=2))
    print(f"Wrote {daily_path}")

    # Monthly
    monthly = collect_monthly(target.year, target.month)
    monthly_path = out_dir / f"monthly_{target.year}_{target.month:02d}.json"
    monthly_path.write_text(json.dumps(monthly, indent=2))
    print(f"Wrote {monthly_path}")


if __name__ == "__main__":
    main()
