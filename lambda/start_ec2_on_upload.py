# lambda/start_ec2_on_upload.py
import os
import json
import boto3
from botocore.exceptions import ClientError

REGION = os.getenv("REGION", "us-east-1")
INSTANCE_ID = os.getenv("INSTANCE_ID")  # set in Lambda env vars

ec2 = boto3.client("ec2", region_name=REGION)

def _should_process(event) -> bool:
    """Process only S3 Put of *.json in the raw/ prefix."""
    try:
        for rec in event.get("Records", []):
            if rec.get("eventSource") == "aws:s3":
                key = rec["s3"]["object"]["key"]
                if key.startswith("raw/") and key.lower().endswith(".json"):
                    return True
    except Exception:
        pass
    return False

def lambda_handler(event, context):
    if not INSTANCE_ID:
        raise RuntimeError("Missing env var INSTANCE_ID (your EC2 runner id)")

    if not _should_process(event):
        # S3 trigger will filter already, but being safe is cheap
        return {"ok": True, "skipped": True}

    try:
        # Check state
        desc = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
        state = desc["Reservations"][0]["Instances"][0]["State"]["Name"]

        # Start if stopped
        if state in ("stopped", "stopping"):
            ec2.start_instances(InstanceIds=[INSTANCE_ID])

        # Wait until healthy (so the user-data/cron can run)
        waiter = ec2.get_waiter("instance_status_ok")
        waiter.wait(InstanceIds=[INSTANCE_ID], WaiterConfig={"Delay": 10, "MaxAttempts": 30})

        return {"ok": True, "instance": INSTANCE_ID}
    except ClientError as e:
        print(f"EC2 error: {e}")
        raise
