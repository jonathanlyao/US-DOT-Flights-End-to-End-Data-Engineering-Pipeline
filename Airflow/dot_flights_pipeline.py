"""
DOT Flights Pipeline DAG
 
Orchestrates the complete monthly data pipeline:
  1. Download one month of DOT BTS flight data
  2. Compress CSV to GZIP
  3. Upload GZIP to S3
  4. Load into Snowflake via COPY INTO
  5. Run dbt models (staging → core → mart)
  6. Run dbt tests
 
Schedule: Manual trigger (for now). Will be set to monthly later.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
 
import boto3
import requests
import snowflake.connector

# =============================================================
# Configuration
# =============================================================
S3_BUCKET = "olist-data-lake-leeyao-oregon"
S3_PROJECT_PREFIX = "dot_flights"
AWS_REGION = "us-west-2"
LOCAL_DATA_DIR = "/tmp/dot_flights/raw"
 
BTS_URL_TEMPLATE = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
 
 # Default args applied to all tasks in the DAG.
default_args = {
    "owner": "jonathan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
 
# =============================================================
# Task Functions
# =============================================================
 
def download_dot_data(**context):
    """Task 1: Download one month of DOT data to local disk."""
    year = context["params"]["year"]
    month = context["params"]["month"]
 
    url = BTS_URL_TEMPLATE.format(year=year, month=month)
    zip_filename = Path(urlparse(url).path).name
    local_dir = Path(LOCAL_DATA_DIR) / f"year={year}" / f"month={month:02d}"
    zip_path = local_dir / zip_filename
    extract_dir = local_dir / "extracted"
 
    # Idempotency: skip if already downloaded.
    if zip_path.exists():
        print(f"ZIP already exists, skipping download: {zip_path}")
    else:
        local_dir.mkdir(parents=True, exist_ok=True)
        print(f"Downloading: {url}")
        with requests.get(url, stream=True, timeout=300) as response:
            response.raise_for_status()
            with zip_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        print(f"Downloaded: {zip_path}")
 
    # Extract ZIP.
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    csv_files = list(extract_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV found in {extract_dir}")
 
    csv_path = str(csv_files[0])
    print(f"Extracted CSV: {csv_path}")
 
    # Pass the CSV path to the next task via XCom.
    context["ti"].xcom_push(key="csv_path", value=csv_path)
    context["ti"].xcom_push(key="zip_path", value=str(zip_path))
 
 
def compress_and_upload_s3(**context):
    """Task 2: Compress CSV to GZIP and upload to S3."""
    year = context["params"]["year"]
    month = context["params"]["month"]
    csv_path = Path(context["ti"].xcom_pull(key="csv_path"))
 
    # Compress to GZIP.
    gz_filename = f"on_time_{year}_{month:02d}.csv.gz"
    gz_path = csv_path.parent / gz_filename
 
    if not gz_path.exists():
        print(f"Compressing: {csv_path.name} -> {gz_filename}")
        with csv_path.open("rb") as f_in:
            with gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        print(f"GZIP already exists: {gz_path}")
 
    gz_size = gz_path.stat().st_size / (1024 * 1024)
    print(f"GZIP size: {gz_size:.2f} MiB")
 
    # Compute hash.
    hasher = hashlib.sha256()
    with gz_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    file_hash = hasher.hexdigest()
 
    # Upload to S3.
    s3_key = (
        f"{S3_PROJECT_PREFIX}/raw/flights/"
        f"year={year}/month={month:02d}/{gz_filename}"
    )
    s3_client = boto3.client("s3", region_name=AWS_REGION)
 
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"File already in S3, skipping: s3://{S3_BUCKET}/{s3_key}")
    except s3_client.exceptions.ClientError:
        print(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(str(gz_path), S3_BUCKET, s3_key)
        print("Upload complete.")
 
    # Write ingestion log.
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_key = (
        f"{S3_PROJECT_PREFIX}/metadata/ingestion_logs/"
        f"ingestion_{year}_{month:02d}_gzip_{ts}.json"
    )
    log_payload = {
        "pipeline_name": "dot_flights_airflow_v1",
        "year": year,
        "month": month,
        "s3_key": s3_key,
        "file_size_bytes": gz_path.stat().st_size,
        "file_sha256": file_hash,
        "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "SUCCESS",
    }
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=log_key,
        Body=json.dumps(log_payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Ingestion log: s3://{S3_BUCKET}/{log_key}")
 
    context["ti"].xcom_push(key="s3_key", value=s3_key)
 
 
def copy_into_snowflake(**context):
    """Task 3: Load the GZIP file from S3 into Snowflake RAW_FLIGHTS."""
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        insecure_mode=True,
    )
    cursor = conn.cursor()
 
    copy_sql = """
    COPY INTO DOT_FLIGHTS.RAW.RAW_FLIGHTS (
        "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate",
        "Reporting_Airline", "DOT_ID_Reporting_Airline", "IATA_CODE_Reporting_Airline",
        "Tail_Number", "Flight_Number_Reporting_Airline",
        "OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID", "Origin",
        "OriginCityName", "OriginState", "OriginStateFips", "OriginStateName", "OriginWac",
        "DestAirportID", "DestAirportSeqID", "DestCityMarketID", "Dest",
        "DestCityName", "DestState", "DestStateFips", "DestStateName", "DestWac",
        "CRSDepTime", "DepTime", "DepDelay", "DepDelayMinutes", "DepDel15",
        "DepartureDelayGroups", "DepTimeBlk", "TaxiOut", "WheelsOff", "WheelsOn", "TaxiIn",
        "CRSArrTime", "ArrTime", "ArrDelay", "ArrDelayMinutes", "ArrDel15",
        "ArrivalDelayGroups", "ArrTimeBlk",
        "Cancelled", "CancellationCode", "Diverted",
        "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Flights", "Distance", "DistanceGroup",
        "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
        "FirstDepTime", "TotalAddGTime", "LongestAddGTime",
        "DivAirportLandings", "DivReachedDest", "DivActualElapsedTime", "DivArrDelay", "DivDistance",
        "Div1Airport", "Div1AirportID", "Div1AirportSeqID", "Div1WheelsOn",
        "Div1TotalGTime", "Div1LongestGTime", "Div1WheelsOff", "Div1TailNum",
        "Div2Airport", "Div2AirportID", "Div2AirportSeqID", "Div2WheelsOn",
        "Div2TotalGTime", "Div2LongestGTime", "Div2WheelsOff", "Div2TailNum",
        "Div3Airport", "Div3AirportID", "Div3AirportSeqID", "Div3WheelsOn",
        "Div3TotalGTime", "Div3LongestGTime", "Div3WheelsOff", "Div3TailNum",
        "Div4Airport", "Div4AirportID", "Div4AirportSeqID", "Div4WheelsOn",
        "Div4TotalGTime", "Div4LongestGTime", "Div4WheelsOff", "Div4TailNum",
        "Div5Airport", "Div5AirportID", "Div5AirportSeqID", "Div5WheelsOn",
        "Div5TotalGTime", "Div5LongestGTime", "Div5WheelsOff", "Div5TailNum",
        "_TRAILING_EMPTY_COL",
        "_SOURCE_FILE", "_LOAD_TIMESTAMP"
    )
    FROM (
        SELECT
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
            $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
            $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
            $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
            $61, $62, $63, $64, $65, $66, $67, $68, $69, $70,
            $71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
            $81, $82, $83, $84, $85, $86, $87, $88, $89, $90,
            $91, $92, $93, $94, $95, $96, $97, $98, $99, $100,
            $101, $102, $103, $104, $105, $106, $107, $108, $109, $110,
            METADATA$FILENAME,
            CURRENT_TIMESTAMP()
        FROM @DOT_FLIGHTS.RAW.S3_DOT_FLIGHTS_STAGE
    )
    PATTERN = '.*on_time_.*\\.csv\\.gz'
    FILE_FORMAT = (
        TYPE = 'CSV'
        COMPRESSION = 'GZIP'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        NULL_IF = ('', 'NULL')
    )
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;
    """
 
    print("Running COPY INTO...")
    cursor.execute("USE WAREHOUSE DOT_FLIGHTS_WH;")
    cursor.execute(copy_sql)
    results = cursor.fetchall()
    for row in results:
        print(row)
 
    # Verify row count.
    cursor.execute("SELECT COUNT(*) FROM DOT_FLIGHTS.RAW.RAW_FLIGHTS;")
    count = cursor.fetchone()[0]
    print(f"Total rows in RAW_FLIGHTS: {count:,}")
 
    cursor.close()
    conn.close()
 
 
# =============================================================
# DAG Definition
# =============================================================
with DAG(
    dag_id="dot_flights_pipeline",
    default_args=default_args,
    description="Monthly DOT BTS flight data pipeline: S3 → Snowflake → dbt",
    # No schedule for now — manual trigger only.
    # Change to "0 6 5 * *" later (5th of each month at 6 AM UTC).
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dot_flights", "pipeline"],
    # Parameters that can be set when manually triggering the DAG.
    params={
        "year": 2024,
        "month": 1,
    },
) as dag:
 
    # Task 1: Download DOT data
    task_download = PythonOperator(
        task_id="download_dot_data",
        python_callable=download_dot_data,
    )
 
    # Task 2: Compress and upload to S3
    task_upload_s3 = PythonOperator(
        task_id="compress_and_upload_s3",
        python_callable=compress_and_upload_s3,
    )
 
    # Task 3: COPY INTO Snowflake
    task_copy_snowflake = PythonOperator(
        task_id="copy_into_snowflake",
        python_callable=copy_into_snowflake,
    )
 
    # Task 4: dbt run (staging → core → mart)
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_dot_flights && dbt run --profiles-dir /home/airflow/.dbt",
    )
 
    # Task 5: dbt test
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_dot_flights && dbt test --profiles-dir /home/airflow/.dbt",
    )
 
    # =============================================================
    # Task Dependencies — the pipeline flow
    # =============================================================
    task_download >> task_upload_s3 >> task_copy_snowflake >> task_dbt_run >> task_dbt_test
