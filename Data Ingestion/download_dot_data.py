"""
DOT BTS Reporting Carrier On-Time Performance - Step 1: Local download + inspection. 

Goal of this script (ONLY these three steps): 
  1. Construct the BTS monthly download URL programmatically from (year, month). 
  2. Download the ZIP file to a local folder and keep it on disk. 
  3. Extract the CSV file and print a quick inspection summary. 

Usage:
  python step1_download_and_inspect.py --year 2024 --month 1
  python step1_download_and_inspect.py --year 2024 --month 2
  python step1_download_and_inspect.py --year 2024 --month 3

No S3 upload. No metadata log. No Snowflake. Those come in later steps. 
"""

from __future__ import annotations

import argparse
import sys
import zipfile 
from pathlib import Path
from urllib.parse import urlparse

import requests

# ---------- Constants ----------

# Local working directory. Using a visible folder (not a tempdir) so we can inspect the downloaded file manually after running the script.
LOCAL_DATA_DIR = Path("./data/dot_flights/raw")

# Verified BTS URL pattern for the "Reporting Carrier" dataset (1987 - present). 
BTS_URL_TEMPLATE = ("https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip")

# --------- Helpers ------------
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Step 1: Download and inspect BTS one month of DOT BTS flight data.")
    parser.add_argument("--year", type=int, required=True, help="Year of the data to download (e.g., 2024).")
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar = "{1..12}", help="Month of the data to download (1-12, not zero-padded).")
    return parser.parse_args()


def build_download_url(year: int, month: int) -> str:
    """Return the BTS monthly download URL. Month is NOT zero-padded on purpose."""
    return BTS_URL_TEMPLATE.format(year=year, month=month)

def download_file(url: str, destination: Path, timeout_seconds: int = 300) -> None: 
    """Stream-download a file to disk. Streaming avoids loading everything into memory""" 
    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}")
    with requests.get(url, stream=True, timeout=timeout_seconds) as response:
        response.raise_for_status()  # Check for HTTP errors
        total_bytes = 0
        with destination.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024): # 1 MB chunks
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)
                    total_bytes += len(chunk)
    print(f"Downloaded: {total_bytes / (1024 * 1024):.2f} MiB -> {destination}")

def extract_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
    """Extract a ZIP file and return the list of extracted file paths."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_dir)
        extracted = [extract_dir / name for name in zf.namelist()]
        print(f"Extracted {len(extracted)} file(s):")
        # Assuming there's only one CSV file in the ZIP
        for f in extracted:
            size_mib = f.stat().st_size / (1024 * 1024)
            print(f" - {f.name} ({size_mib:.2f} MiB)")
    return extracted


def inspect_csv(csv_path: Path, preview_rows: int = 3) -> None:
    """Print a quick summary: row count, column count, column names, first few rows."""
    # row count via a simple line count (minus the header). Fast even for ~500k rows.
    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        row_count = sum(1 for _ in f) - 1  # Total lines minus header

    # Read the header line only, so we do not load the full file into memory.
    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        header = f.readline().rstrip("\n").split(",") # Simple split by comma.

    # Print the summary and first 3 rows.
    print()
    print(f"CSV Inspection: {csv_path.name}")
    print(f"  Row count (excluding header): {row_count:,}")
    print(f"  Column count: {len(header)}")
    print(f"  First 10 columns: {header[:10]}")
    print(f"  Last 5 columns:   {header[-5:]}")

    print(f"\nPreview of the first {preview_rows} data rows:")
    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
    # Skip header
        next(f)
        for _ in range(preview_rows):
            line = f.readline()
            if not line:
                break  # EOF
            truncated = line[:200].rstrip() # Truncate long lines so terminal output stays readable. 
            suffix = "..." if len(line) > 200 else ""
            print(f"  {truncated}{suffix}")


# --------- Main Script Logic ------------
def main() -> int:
    args = parse_args()
    year = args.year
    month = args.month

    print(f"=== Step 1: Download and inspect DOT data for {year}-{month:02d} ===\n")

    url = build_download_url(year, month)  

    # Derive the local filename from the URL (e.g. "..._2024_1.zip").
    zip_filename = Path(urlparse(url).path).name  

    # Store under a partitioned folder structure: year=YYYY/month=MM/
    zip_path = LOCAL_DATA_DIR / f"year={year}" / f"month={month:02d}" / zip_filename

    extract_dir = zip_path.parent / "extracted" # Extract in the same folder as the ZIP file

    # Idempotency: skip the download if the ZIP is already on disk.
    # This matters even at v1 - re-running the script should be free.

    if zip_path.exists():
        print(f"ZIP file already on disk, skipping download: {zip_path}")
    else:
        download_file(url, zip_path)   # Download the ZIP file to disk (if not already there). This is the only time we hit the network in this script.

    extracted_files = extract_zip(zip_path, extract_dir) # Extract the ZIP file (even if it was already on disk - we can re-extract safely). This is usually very fast since it's local disk I/O.

    # The ZIP contains the main CSV plus a small README.TXT. Pick the CSV(s). 
    csv_files = [p for p in extracted_files if p.suffix.lower() == ".csv"]
    if not csv_files:
        print(f"Error: No CSV files found in the ZIP.", file=sys.stderr)
        return 1
    
    for csv_file in csv_files:
        inspect_csv(csv_file)       
    
    print(f"\nStep 1 complete for {year}-{month:02d}. Next: run step2b_recompress_upload.py")  # Print a quick inspection summary of the CSV file(s). This is the main "output" of this script - we want to verify the data looks correct before we move on to the next steps.
    return 0

if __name__ == "__main__":
    sys.exit(main())
