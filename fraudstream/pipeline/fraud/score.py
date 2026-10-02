from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

from fraudstream.pipeline.fraud.rules import score_transactions


load_dotenv()


def get_s3_client():
    profile = os.getenv("AWS_PROFILE", "default")
    region = os.getenv("AWS_REGION", "us-east-1")

    session = boto3.Session(
        profile_name=profile,
        region_name=region,
    )

    return session.client("s3")


def get_bucket() -> str:
    bucket = os.getenv("S3_BUCKET")

    if not bucket:
        raise RuntimeError("S3_BUCKET is not configured in .env")

    return bucket


def list_silver_files(
    s3,
    bucket: str,
    topic: str,
) -> list[str]:

    prefix = f"silver/{topic}/"

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    return sorted(
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    )


def read_parquet_from_s3(
    s3,
    bucket: str,
    key: str,
) -> pd.DataFrame:

    response = s3.get_object(
        Bucket=bucket,
        Key=key,
    )

    data = response["Body"].read()

    return pd.read_parquet(
        BytesIO(data)
    )


def read_topic(
    s3,
    bucket: str,
    topic: str,
) -> pd.DataFrame:

    files = list_silver_files(
        s3=s3,
        bucket=bucket,
        topic=topic,
    )

    if not files:
        return pd.DataFrame()

    frames = []

    for key in files:
        df = read_parquet_from_s3(
            s3=s3,
            bucket=bucket,
            key=key,
        )

        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
    )


def write_parquet_to_s3(
    s3,
    bucket: str,
    key: str,
    df: pd.DataFrame,
) -> None:

    if df.empty:
        return

    payload = df.to_parquet(
        index=False,
        engine="pyarrow",
    )

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
    )


def run_fraud_scoring() -> dict:

    s3 = get_s3_client()
    bucket = get_bucket()

    transactions = read_topic(
        s3=s3,
        bucket=bucket,
        topic="fraud_transactions",
    )

    logins = read_topic(
        s3=s3,
        bucket=bucket,
        topic="fraud_logins",
    )

    devices = read_topic(
        s3=s3,
        bucket=bucket,
        topic="fraud_devices",
    )

    if transactions.empty:
        print("No Silver transactions found.")
        return {
            "transactions": 0,
            "scored_transactions": 0,
        }

    scored = score_transactions(
        transactions=transactions,
        logins=logins,
        devices=devices,
    )

    if scored.empty:
        print("Fraud scoring returned no records.")
        return {
            "transactions": len(transactions),
            "scored_transactions": 0,
        }

    processing_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    run_id = uuid.uuid4().hex[:12]

    output_key = (
        "silver/scored_transactions/"
        f"processing_date={processing_date}/"
        f"part-scored-{run_id}.parquet"
    )

    write_parquet_to_s3(
        s3=s3,
        bucket=bucket,
        key=output_key,
        df=scored,
    )

    high_risk_count = int(
        (scored["risk_band"] == "HIGH").sum()
    )

    print(
        f"Fraud scoring completed | "
        f"transactions={len(transactions)} | "
        f"high_risk={high_risk_count}"
    )

    print(
        f"Scored transactions written → "
        f"s3://{bucket}/{output_key}"
    )

    return {
        "transactions": len(transactions),
        "scored_transactions": len(scored),
        "high_risk_transactions": high_risk_count,
    }


if __name__ == "__main__":
    run_fraud_scoring()