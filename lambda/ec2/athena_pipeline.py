# ec2/athena_pipeline.py
# Run on EC2: creates/ensures Athena DB+Table (over S3 raw/),
# runs Stage-3 query, copies CSV to output/prod/, terminates self.

import os
import time
import datetime as dt
import boto3
from botocore.exceptions import ClientError

# ---- Config (override via EC2 env vars if you like) ----
REGION          = os.getenv("REGION", "us-east-1")
BUCKET          = os.getenv("BUCKET") or "YOUR_S3_BUCKET"          # e.g. devang-healthcare-data
RAW_PREFIX      = os.getenv("RAW_PREFIX", "raw/")
ATHENA_OUTPUT   = os.getenv("ATHENA_OUTPUT") or f"s3://{BUCKET}/athena-results/"
PROD_PREFIX     = os.getenv("PROD_PREFIX", "output/prod/")
DATABASE        = os.getenv("DATABASE", "healthcare_db")
TABLE           = os.getenv("TABLE", "facility_data")
WORKGROUP       = os.getenv("WORKGROUP", "primary")                # use your Athena workgroup if different
CATALOG         = os.getenv("CATALOG", "AwsDataCatalog")

# ---- SQL definitions ----
CREATE_DB_SQL = f"CREATE DATABASE IF NOT EXISTS {DATABASE};"

# External table over raw JSON. Adjust columns if your JSON changes.
CREATE_TABLE_SQL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
  facility_id string,
  facility_name string,
  location struct<
    address:string,
    city:string,
    state:string,
    zip:string>,
  employee_count int,
  services array<string>,
  labs array<struct<lab_name:string, certifications:array<string>>>,
  accreditations array<struct<accreditation_body:string, accreditation_id:string, valid_until:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION 's3://{BUCKET}/{RAW_PREFIX}'
TBLPROPERTIES ('has_encrypted_data'='false');
"""

# Example “Stage 3” aggregate: facilities per state
STAGE3_SQL = f"""
SELECT
  location.state AS state,
  COUNT(1)        AS facilities
FROM {DATABASE}.{TABLE}
GROUP BY location.state
ORDER BY facilities DESC;
"""

# ---- Clients ----
session = boto3.session.Session(region_name=REGION)
athena  = session.client("athena")
s3      = session.client("s3")
ec2     = session.client("ec2")

def wait_for_query(qid: str, timeout_sec: int = 300):
    start = time.time()
    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                reason = res["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                raise RuntimeError(f"Athena query {qid} {state}: {reason}")
            return res
        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Athena query {qid} timed out")
        time.sleep(2)

def run_query(sql: str, database: str | None = None) -> str:
    kwargs = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT},
        "WorkGroup": WORKGROUP,
        "QueryExecutionContext": {"Catalog": CATALOG},
    }
    if database:
        kwargs["QueryExecutionContext"]["Database"] = database

    q = athena.start_query_execution(**kwargs)
    qid = q["QueryExecutionId"]
    wait_for_query(qid)
    return qid

def copy_athena_csv_to_prod(qid: str, dest_key: str):
    # Athena writes CSV to s3://.../athena-results/<query-id>.csv
    src_key = f"athena-results/{qid}.csv".replace(f"s3://{BUCKET}/", "")
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": src_key},
        Key=dest_key,
    )

def terminate_self():
    try:
        # Discover this instance ID from metadata
        import urllib.request
        iid = urllib.request.urlopen(
            "http://169.254.169.254/latest/meta-data/instance-id", timeout=2
        ).read().decode()
        ec2.terminate_instances(InstanceIds=[iid])
    except Exception as e:
        print(f"[WARN] Could not terminate instance automatically: {e}")

def main():
    print(f"[INFO] Starting Athena pipeline in {REGION}")
    print(f"[INFO] Bucket={BUCKET}, RAW=s3://{BUCKET}/{RAW_PREFIX}, Output={PROD_PREFIX}")

    if BUCKET == "YOUR_S3_BUCKET":
        raise RuntimeError("Set BUCKET env var (or hardcode your bucket name) before running.")

    print("[STEP] Ensuring DB exists…")
    run_query(CREATE_DB_SQL)  # DB creation can be executed without database context

    print("[STEP] Ensuring external table exists…")
    run_query(CREATE_TABLE_SQL, database=DATABASE)

    print("[STEP] Running Stage-3 query…")
    qid = run_query(STAGE3_SQL, database=DATABASE)
    print(f"[OK] Query ID: {qid}")

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest_key = f"{PROD_PREFIX}state_counts_{ts}.csv"

    print("[STEP] Copying results to prod…")
    copy_athena_csv_to_prod(qid, dest_key)
    print(f"[OK] Prod CSV: s3://{BUCKET}/{dest_key}")

    print("[STEP] Terminating instance…")
    terminate_self()

if __name__ == "__main__":
    main()
