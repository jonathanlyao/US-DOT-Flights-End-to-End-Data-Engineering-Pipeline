# US DOT Flights — End-to-End Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat&logo=python&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-Data_Lake-FF9900?style=flat&logo=amazons3&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?style=flat&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformation-FF694B?style=flat&logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat&logo=docker&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Modeling-4479A1?style=flat&logo=postgresql&logoColor=white)

A production-grade data engineering pipeline ingesting real US Department of Transportation (DOT) Bureau of Transportation Statistics (BTS) flight data into a cloud data warehouse, modeled with dbt, and orchestrated with Apache Airflow.

---

## Architecture Overview

```
BTS Official Source
       │
       ▼
  Python Ingest
  (Parameterized by year/month)
       │
       ▼
  AWS S3 Data Lake
  (Partitioned: year=2024/month=1,2,3)
       │
       ▼
  Snowflake Raw Layer
  (COPY INTO via Storage Integration)
       │
       ▼
  dbt Transformation
  ┌────────────────────────────────┐
  │  Staging → Core → Mart         │
  │  (4-layer architecture)        │
  └────────────────────────────────┘
       │
       ▼
  Apache Airflow
  (Docker-containerized orchestration)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | US DOT BTS (real government flight data) |
| Ingestion | Python (parameterized, idempotent) |
| Data Lake | AWS S3 (partitioned by year/month) |
| Data Warehouse | Snowflake (Storage Integration + IAM trust chain) |
| Data Modeling | dbt Core (4-layer: staging → core → mart) |
| Orchestration | Apache Airflow 2.10.4 (Docker Compose) |
| Development | VS Code, PowerShell |

---

## Data Model

### Layer Architecture

**Staging** (`stg_flights`)
- Type casting, field normalization, time field transformation (`TO_TIME` + `LPAD`)
- Materialized as Views

**Core — Star Schema** (Materialized as Tables)
- `fact_flights` — flight-level measurements (delay minutes, distance, cancellation status)
  - Surrogate key: `MD5(carrier_code || flight_number || flight_date || origin || dest || tail_number)`
- `dim_carrier` — airline carrier metadata
- `dim_date` — date spine with calendar attributes (dynamic bounds, not hardcoded)
- `dim_airport` — airport and regional classification (US Census Bureau 4-region standard)

**Mart** (Materialized as Tables)
- `mart_carrier_performance_monthly` — monthly on-time rate, delay metrics, cancellation rate per carrier
- `mart_delay_attribution_monthly` — delay cause breakdown (carrier, weather, NAS, security, late aircraft) by month

### dbt Testing

17 tests across all models covering:
- `not_null` on all primary and foreign keys
- `unique` on all surrogate keys
- `accepted_values` on categorical fields (cancellation codes, delay flags)
- `relationships` across fact and dimension tables

---

## Pipeline DAG

The Airflow DAG `dot_flights_pipeline` orchestrates 5 tasks in sequence:

```
download_dot_data
      │
compress_and_upload_s3
      │
copy_into_snowflake
      │
dbt_run
      │
dbt_test
```

**Key design decisions:**
- **Parameterized**: DAG accepts `year` and `month` as runtime parameters, enabling backfill of any historical month
- **Idempotent**: All stages skip gracefully if data already exists (download check, S3 object check, Snowflake `FORCE = FALSE`)
- **Containerized**: Full Airflow stack (webserver, scheduler, metadata DB) runs via Docker Compose with zero local dependency conflicts

---

## Project Structure

```
US-DOT-Flights-End-to-End-Data-Engineering-Pipeline/
├── Airflow/
│   ├── dags/
│   │   └── dot_flights_pipeline.py        # Airflow DAG
│   ├── docker-compose.yml                 # Airflow Docker Compose stack
│   ├── Dockerfile                         # Custom Airflow image
│   └── requirements.txt                   # Airflow runtime dependencies
│
├── Data Ingestion/
│   ├── step1_download_and_inspect.py       # Download and inspect BTS monthly ZIP/CSV
│   ├── step2_upload_to_s3.py               # Upload original ZIP to S3 with ingestion log
│   └── step2b_recompress_upload.py         # Recompress CSV to GZIP and upload to S3
│
├── Snowflake SQL/
│   ├── 01_database_schema_warehouse.sql    # Create database, schema, and warehouse
│   ├── 02_storage_integration_stage.sql    # Configure S3 integration and external stage
│   ├── 03_create_raw_flights.sql           # Create RAW_FLIGHTS table
│   └── 04_copy_into_raw_flights.sql        # Load GZIP files into Snowflake raw layer
│
├── dbt/
│   ├── dbt_project.yml                     # dbt project configuration
│   ├── profiles.yml.example                # Example dbt profile without credentials
│   └── models/
│       ├── staging/
│       │   └── stg_flights.sql
│       ├── core/
│       │   ├── fact_flights.sql
│       │   ├── dim_carrier.sql
│       │   ├── dim_date.sql
│       │   └── dim_airport.sql
│       └── mart/
│           ├── mart_carrier_performance_monthly.sql
│           └── mart_delay_attribution_monthly.sql
│
├── Models/
│   └── screenshots_or_model_notes/          # Optional model documentation or screenshots
│
├── .gitignore
└── README.md
```

---

## Setup & Reproduction

### Prerequisites

- Python 3.9+
- Docker Desktop
- AWS account (S3 bucket + IAM user with S3 write access)
- Snowflake account (XSMALL warehouse, auto-suspend enabled)
- dbt Core (`pip install dbt-snowflake`)

### 1. Clone & Configure

```bash
git clone https://github.com/your-username/dot-flights-pipeline.git
cd dot-flights-pipeline
cp .env.example .env
# Fill in: AWS credentials, Snowflake credentials, S3 bucket name
```

### 2. Snowflake Setup

```sql
-- Create storage integration (IAM trust chain between Snowflake and S3)
CREATE STORAGE INTEGRATION dot_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/dot-flights/');

-- Create external stage
CREATE STAGE dot_flights_stage
  STORAGE_INTEGRATION = dot_s3_integration
  URL = 's3://your-bucket/dot-flights/';
```

### 3. Launch Airflow

```bash
docker-compose up -d
# UI available at http://localhost:8080
# Default credentials: airflow / airflow
```
<img width="1843" height="846" alt="image" src="https://github.com/user-attachments/assets/1b0cba99-95f2-4946-9277-6a7fe4ee1193" />

### 4. Trigger Pipeline

In the Airflow UI, trigger `dot_flights_pipeline` with parameters:

```json
{
  "year": 2024,
  "month": 1
}
```

Or via CLI:

```bash
docker exec -it airflow-scheduler airflow dags trigger dot_flights_pipeline \
  --conf '{"year": 2024, "month": 1}'
```

### 5. Run dbt Independently (Optional)

```bash
dbt run --select staging
dbt run --select core
dbt run --select mart
dbt test
```

---

## FinOps

This project runs within Snowflake's $400 free trial credit without modification:

- Warehouse size: `XSMALL`
- Auto-suspend: 60 seconds
- All development queries run via `dbt show --limit 5` and `dbt compile` before full `dbt run`

---

## Key Engineering Decisions

**Why surrogate keys via MD5?**
DOT data has no natural primary key. The combination of `carrier_code + flight_number + flight_date + origin + dest + tail_number` guarantees uniqueness. MD5 compresses this into a fixed 32-character hash for efficient JOINs.

**Why dynamic date spine bounds?**
Hardcoding `MIN_DATE` and `MAX_DATE` in `dim_date` creates maintenance debt. The model derives bounds dynamically from `fact_flights`, so adding new months requires no model changes.

**Why US Census Bureau 4-region classification?**
Custom regional groupings are non-standard and create confusion for downstream consumers. The Census Bureau 4-region standard (Northeast, Midwest, South, West) is universally understood and reproducible.

**Why idempotency throughout?**
All pipeline stages are designed to be safely re-run. This eliminates the risk of data duplication on retry and makes backfilling historical months safe and straightforward.

---

## Data Source

**US Bureau of Transportation Statistics (BTS) — On-Time Performance**
- URL: https://www.transtats.bts.gov/DL_SelectFields.aspx
- Coverage: All US domestic commercial flights reported by major carriers
- Format: Monthly CSV files, ~6–8 million rows/year

---

## Author

Jonathan Lee | [LinkedIn](https://linkedin.com/in/your-profile) | [Portfolio](https://your-portfolio.com)

Built as part of a data engineering portfolio series demonstrating end-to-end pipeline design with real government datasets.
