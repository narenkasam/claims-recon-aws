# Daily Claims Reconciliation Pipeline (AWS ECS)

This project implements a **daily reconciliation pipeline** that compares two CSV files
(OLD vs NEW) delivered to S3, computes reconciliation metrics and mismatch details, and
persists results to **Postgres** and **S3**.  
The pipeline runs as a **containerized ECS Fargate task**.

---

## What the Pipeline Does

On each run:

1. Lists an **inbound S3 prefix**
2. Validates input files:
   - **0 files** → log `NO_FILES`
   - **≠ 2 files** → log `INVALID_FILE_COUNT`
   - **2 files** → proceed
3. Identifies:
   - OLD file (does not contain `NEWSYSTEM`)
   - NEW file (contains `NEWSYSTEM`)
4. Downloads both CSVs
5. Loads data into Postgres **staging tables**
6. Computes reconciliation metrics:
   - Row counts (old vs new)
   - Matched keys
   - Missing / extra rows
   - Column-level mismatch counts
7. Generates a **row-level mismatch sample** (JSON payload: `old~new`)
8. Writes outputs:
   - Metrics tables in Postgres
   - CSV artifacts to S3
9. Moves processed files to a **processed S3 prefix**
10. Logs run metadata and status

---

## S3 Folder Structure
# Daily Claims Reconciliation Pipeline (AWS ECS)

This project implements a **daily reconciliation pipeline** that compares two CSV files
(OLD vs NEW) delivered to S3, computes reconciliation metrics and mismatch details, and
persists results to **Postgres** and **S3**.  
The pipeline runs as a **containerized ECS Fargate task**.

---

## What the Pipeline Does

On each run:

1. Lists an **inbound S3 prefix**
2. Validates input files:
   - **0 files** → log `NO_FILES`
   - **≠ 2 files** → log `INVALID_FILE_COUNT`
   - **2 files** → proceed
3. Identifies:
   - OLD file (does not contain `NEWSYSTEM`)
   - NEW file (contains `NEWSYSTEM`)
4. Downloads both CSVs
5. Loads data into Postgres **staging tables**
6. Computes reconciliation metrics:
   - Row counts (old vs new)
   - Matched keys
   - Missing / extra rows
   - Column-level mismatch counts
7. Generates a **row-level mismatch sample** (JSON payload: `old~new`)
8. Writes outputs:
   - Metrics tables in Postgres
   - CSV artifacts to S3
9. Moves processed files to a **processed S3 prefix**
10. Logs run metadata and status

---

## S3 Folder Structure

# Daily Claims Reconciliation Pipeline (AWS ECS)

This project implements a **daily reconciliation pipeline** that compares two CSV files
(OLD vs NEW) delivered to S3, computes reconciliation metrics and mismatch details, and
persists results to **Postgres** and **S3**.  
The pipeline runs as a **containerized ECS Fargate task**.

---

## What the Pipeline Does

On each run:

1. Lists an **inbound S3 prefix**
2. Validates input files:
   - **0 files** → log `NO_FILES`
   - **≠ 2 files** → log `INVALID_FILE_COUNT`
   - **2 files** → proceed
3. Identifies:
   - OLD file (does not contain `NEWSYSTEM`)
   - NEW file (contains `NEWSYSTEM`)
4. Downloads both CSVs
5. Loads data into Postgres **staging tables**
6. Computes reconciliation metrics:
   - Row counts (old vs new)
   - Matched keys
   - Missing / extra rows
   - Column-level mismatch counts
7. Generates a **row-level mismatch sample** (JSON payload: `old~new`)
8. Writes outputs:
   - Metrics tables in Postgres
   - CSV artifacts to S3
9. Moves processed files to a **processed S3 prefix**
10. Logs run metadata and status

---

## S3 Folder Structure

s3://<inbound-bucket>/claims/inbound/
├── <old_file>.csv
├── <new_file>_NEWSYSTEM.csv

s3://<inbound-bucket>/claims/processed/
s3://<out-bucket>/claims/out/


---

## Postgres Schemas & Tables

### Schemas
- `stage` – raw daily loads
- `metrics` – reconciliation results

### Tables
- `metrics.pipeline_run_log`  
  Run status, file names, timestamps, error messages

- `metrics.recon_summary`  
  One row per run with row counts and high-level metrics

- `metrics.mismatch_by_column`  
  Per-column mismatch counts

- `metrics.mismatch_detail_sample`  
  Row-level mismatches stored as JSON (`column : old~new`)

---

## Technology Stack

- **AWS ECS Fargate** – compute
- **Amazon ECR** – container registry
- **Amazon S3** – inbound, processed, output storage
- **Amazon RDS (Postgres)** – metrics persistence
- **Python (psycopg3, boto3)** – orchestration and logic
- **Docker** – packaging and deployment

---

## ECS Task Configuration (Key Points)

- Runs `run.py` inside a Docker container
- Receives all configuration via **environment variables**
- Uses an **ECS task role** (no static AWS credentials)
- Command arguments specify:
  - inbound S3 prefix
  - processed S3 prefix
  - output S3 prefix
  - dataset name
  - key columns
  - mismatch sample limit

Example (conceptual):

---

## Postgres Schemas & Tables

### Schemas
- `stage` – raw daily loads
- `metrics` – reconciliation results

### Tables
- `metrics.pipeline_run_log`  
  Run status, file names, timestamps, error messages

- `metrics.recon_summary`  
  One row per run with row counts and high-level metrics

- `metrics.mismatch_by_column`  
  Per-column mismatch counts

- `metrics.mismatch_detail_sample`  
  Row-level mismatches stored as JSON (`column : old~new`)

---

## Technology Stack

- **AWS ECS Fargate** – compute
- **Amazon ECR** – container registry
- **Amazon S3** – inbound, processed, output storage
- **Amazon RDS (Postgres)** – metrics persistence
- **Python (psycopg3, boto3)** – orchestration and logic
- **Docker** – packaging and deployment

---

## ECS Task Configuration (Key Points)

- Runs `run.py` inside a Docker container
- Receives all configuration via **environment variables**
- Uses an **ECS task role** (no static AWS credentials)
- Command arguments specify:
  - inbound S3 prefix
  - processed S3 prefix
  - output S3 prefix
  - dataset name
  - key columns
  - mismatch sample limit

Example (conceptual):

python run.py
--inbound-s3 s3://.../claims/inbound/
--processed-s3 s3://.../claims/processed/
--out-s3 s3://.../claims/out/
--dataset daily_claims
--keys DESYNPUF_ID
--detail-limit 5000


---

--Docker Build
aws ecr get-login-password --region us-east-1 |
docker login --username AWS --password-stdin 172172673968.dkr.ecr.us-east-1.amazonaws.com

aws sts get-caller-identity

# 1) build (use the same ECR repo + tag you already use)
docker build -t claims-recon-pipeline:devlatestl2 .

# 2) tag to ECR
docker tag claims-recon-pipeline:devlatestl2 172172673968.dkr.ecr.us-east-1.amazonaws.com/claims-recon-pipeline:v9

# 3) push
docker push 172172673968.dkr.ecr.us-east-1.amazonaws.com/claims-recon-pipeline:v9

## Operational Notes

- **Exactly two files** are expected per run
- CSV schemas must match between OLD and NEW
- All staging columns are loaded as `TEXT` to avoid schema drift
- Detailed mismatches are stored as **JSONB** for flexibility
- File lineage is tracked in `pipeline_run_log`

---

## How to Validate a Run

In Postgres:

```sql
SELECT * 
FROM metrics.pipeline_run_log
ORDER BY run_ts DESC
LIMIT 5;
SELECT *
FROM metrics.recon_summary
ORDER BY run_date DESC
LIMIT 5;
SELECT *
FROM metrics.mismatch_detail_sample
ORDER BY created_ts DESC
LIMIT 20;
SELECT
  d.run_date,
  d.key_text,
  kv.key   AS column_name,
  kv.value AS old_new_value
FROM metrics.mismatch_detail_sample d
CROSS JOIN LATERAL jsonb_each_text(d.payload) kv;









