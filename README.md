# MedLaunch — AWS Athena Pipeline

Event-driven pipeline: **S3 (raw JSON)** → **Lambda** → **EC2 runner** → **Athena** → **S3 (CSV)**, with CloudWatch logging.

## Repo structure

├─ lambda/
│ └─ start_ec2_on_upload.py # Lambda that starts EC2 on s3:ObjectCreated for raw/*.json
├─ ec2/
│ └─ athena_pipeline.py # EC2 script: create DB/table, run Stage-3 SQL, copy CSV, terminate
├─ sql/
│ └─ stage3_state_counts.sql # Stage-3 query (Athena)
├─ docs/
│ ├─ architecture.mmd # Mermaid diagram
│ └─ architecture.png # (optional PNG export)
├─ samples/
│ └─ Sample_Challenge.json # example input
└─ README.md

markdown
Copy
Edit


## Architecture

- **Trigger:** Upload `raw/*.json` to the S3 bucket.
- **Lambda:** Starts a pre-provisioned EC2 “runner” (if stopped) and waits until it’s healthy.
- **EC2 runner:** Executes `ec2/athena_pipeline.py`:
  - Ensures `healthcare_db` and external table `facility_data` over `s3://<bucket>/raw/`.
  - Runs Stage-3 query (see `sql/stage3_state_counts.sql`).
  - Copies `QueryExecutionId.csv` from `athena-results/` to `output/prod/state_counts_<UTC_TS>.csv`.
  - Terminates itself.
- **Athena:** Workgroup `primary`, catalog `AwsDataCatalog`, output `s3://<bucket>/athena-results/`.

Mermaid diagram (source in `docs/architecture.mmd`):

```mermaid
flowchart LR
  R["S3 raw/*.json"] --> L["Lambda"] --> EC2["EC2 Runner"]
  EC2 --> A["Athena"] --> P["S3 output/prod/*.csv"]
