"""
DOT BTS On-Time Performance - Step 2b: Recompress CSV as GZIP and upload to S3.

Goal of this script (ONLY these things):
  1. Find the CSV extracted in Step 1.
  2. Compress it as .csv.gz (GZIP format — Snowflake supports this, not ZIP).
  3. Upload the .csv.gz to S3 at: dot_flights/raw/flights/year=YYYY/month=MM/
  4. Update the ingestion log.

Usage:
  python step2b_recompress_upload.py --year 2024 --month 1
  python step2b_recompress_upload.py --year 2024 --month 2
  python step2b_recompress_upload.py --year 2024 --month 3

Why GZIP instead of ZIP?
  - Snowflake COPY INTO supports GZIP but NOT ZIP.
  - GZIP is a single-file stream — better for data pipelines.
  - Smaller than uncompressed CSV, saves S3 storage cost.

Prerequisites:
  - Step 1 has been run (extracted CSV exists on disk).
  - AWS credentials are configured.
  - pip install boto3
"""

from __future__ import annotations

import argparse
import gzip
from pathlib import Path
import sys
import boto3
import json
from datetime import datetime, timezone
import shutil
import hashlib


# ---------- Constants ----------
# Local path (must match step 1 output structure)
LOCAL_DATA_DIR = Path("./data/dot_flights/raw")

# S3 configuration
S3_BUCKET = "olist-data-lake-leeyao-oregon"  # e.g. my datalake in AWS S3 in West 2 Oregon
S3_PROJECT_PREFIX = "dot_flights"  # This is the "folder" in S3 where we will store the data and logs
AWS_REGION = "us-west-2"  # AWS region where your S3 bucket is located

# --------- Helpers ------------
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Recompress CSV as GZIP and upload to S3.")
    parser.add_argument("--year", type=int, required=True, help="Year of the data to upload (e.g., 2024).")
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="{1..12}", help="Month of the data to upload (1-12).")
    return parser.parse_args()

def find_extracted_csv(year: int, month: int) -> Path:
    """Locate the extracted CSV file from Step 1."""
    extracted_dir = LOCAL_DATA_DIR / f"year={year}" / f"month={month:02d}" / "extracted"
    csv_files = list(extracted_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No extracted CSV file found in {extracted_dir}. Please run Step 1 for {year}-{month:02d}.")
    return csv_files[0]

def compress_to_gzip(csv_path: Path, gz_path: Path) -> None:
    """Compress the CSV file to GZIP format."""
    print(f"Compressing {csv_path.name} -> {gz_path.name}")
    with csv_path.open("rb") as f_in: 
        with gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    csv_size = csv_path.stat().st_size / (1024 * 1024)
    gz_size = gz_path.stat().st_size / (1024 * 1024)
    ratio = (1 - gz_size / csv_size) * 100
    print(f"  CSV size: {csv_size:.2f} MiB")
    print(f"  GZIP size: {gz_size:.2f} MiB")
    print(f"  Compression ratio: {ratio:.1f}% smaller than original CSV")

def sha256_of_file(file_path: Path) -> str:
    """Calculate the SHA256 hash of a file. This is useful for verifying file integrity and avoiding duplicates."""
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        for chuck in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chuck)
    return hasher.hexdigest()
    
def build_s3_key(year: int, month: int, filename: str) -> str: 
        """Build the S3 key for the raw data file."""
        return (f"{S3_PROJECT_PREFIX}/raw/flights/year={year}/month={month:02d}/{filename}")
    
def check_s3_object_exists(s3_client, bucket: str, key: str) -> bool: 
        """Check if an object already exists in S3."""
        try: 
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except s3_client.exceptions.ClientError:
            return False
        
# --------- Main Logic ------------

def main() -> int:
    args = parse_args()
    year = args.year
    month = args.month
    print(f"Starting Step 2b: Recompress and upload for {year}-{month:02d} ===\n")

    # Step 1: Find the extracted CSV from Step 1
    csv_path = find_extracted_csv(year, month)
    print(f"Found extracted CSV: {csv_path}")
    
    # Step 2: Compress the CSV to GZIP format
    gz_filename = f"on_time_{year}_{month:02d}.csv.gz"
    gz_path = csv_path.parent / gz_filename

    if gz_path.exists():
        print(f"GZIP file already exists: {gz_path}. Skipping compression.")
    else:
        compress_to_gzip(csv_path, gz_path)

    # Step 3: Compute file hash (for auditing and deduplication purposes)
    file_hash = sha256_of_file(gz_path)
    print(f"  SHA256: {file_hash[:16]}...") # Print only the first 16 characters of the hash for readability

    # Step 4: Upload the GZIP file to S3
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_key = build_s3_key(year, month, gz_filename)

    if check_s3_object_exists(s3_client, S3_BUCKET, s3_key):
        print(f"\nFile already exists in S3 at s3://{S3_BUCKET}/{s3_key}. Skipping upload.")
    else:
        print(f"Uploading {gz_filename} -> s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(str(gz_path), S3_BUCKET, s3_key)
        print("Upload completed.")

    # Step 5: Verify the upload by checking the S3 object exists
    if not check_s3_object_exists(s3_client, S3_BUCKET, s3_key): 
        print("Error: Uploaded verification failed - object not found in S3", file = sys.stderr)
        return 1
    print(f"Verified: s3://{S3_BUCKET}/{s3_key} exists.")

    # Step 6: Write an ingestion log to S3
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log_key = (f"{S3_PROJECT_PREFIX}/metadata/ingestion_logs/ingestion_{year}_{month:02d}_gzip_{ts}.json")
    ingestion_log = {
        "pipeline_name": "dot_flights_ingestion_v1",
        "source_system": "DOT BTS Reporting Carrier On-Time Performance",
        "step": "recompress_and_upload_gzip",
        "year": year,
        "month": month,
        "s3_bucket": S3_BUCKET,
        "s3_key": s3_key,
        "aws_region": AWS_REGION,
        "filename": gz_filename,
        "file_size_bytes": gz_path.stat().st_size,
        "file_sha256": file_hash,
        "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "SUCCESS",
    }
    s3_client.put_object(
        Bucket = S3_BUCKET, 
        Key = log_key, 
        Body = json.dumps(ingestion_log, indent=2).encode("utf-8"),
        ContentType = "application/json",
    )
    print(f"Ingestion log uploaded to s3://{S3_BUCKET}/{log_key}")

    #Summary. 
    print()
    print(f"Step 2b complete for {year}-{month:02d}.")
    print(f"  GZIP file: s3://{S3_BUCKET}/{s3_key}")
    print(f"  Ingestion log: s3://{S3_BUCKET}/{log_key}")
    print("  Next step: COPY INTO Snowflake from the GZIP file in S3.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
