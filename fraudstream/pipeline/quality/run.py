from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv

from fraudstream.pipeline.quality.checks import fraud_checks


load_dotenv()


TOPICS = [
    "fraud_transactions",
    "fraud_logins",
    "fraud_devices",
    "fraud_chargebacks",
]


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

    from io import BytesIO

    return pd.read_parquet(
        BytesIO(data)
    )


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

    contents = response.get("Contents", [])

    return sorted(
        obj["Key"]
        for obj in contents
        if obj["Key"].endswith(".parquet")
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


def run_quality_for_topic(
    s3,
    bucket: str,
    topic: str,
) -> dict:

    silver_files = list_silver_files(
        s3=s3,
        bucket=bucket,
        topic=topic,
    )

    if not silver_files:
        print(f"No Silver records found for topic: {topic}")

        return {
            "topic": topic,
            "records": 0,
            "failed_records": 0,
        }

    frames = []

    for key in silver_files:
        df = read_parquet_from_s3(
            s3=s3,
            bucket=bucket,
            key=key,
        )

        if not df.empty:
            frames.append(df)

    if not frames:
        print(f"Silver files are empty for topic: {topic}")

        return {
            "topic": topic,
            "records": 0,
            "failed_records": 0,
        }

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    runner = fraud_checks(
        topic=topic,
        df=df,
    )

    scorecard = runner.scorecard()
    quarantine = runner.quarantine()

    processing_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    run_id = uuid.uuid4().hex[:12]

    dq_key = (
        f"silver/_dq_results/"
        f"{topic}/"
        f"processing_date={processing_date}/"
        f"quality-{run_id}.parquet"
    )

    write_parquet_to_s3(
        s3=s3,
        bucket=bucket,
        key=dq_key,
        df=scorecard,
    )

    quarantine_key = (
        f"quarantine/dq_failures/"
        f"{topic}/"
        f"processing_date={processing_date}/"
        f"quarantine-{run_id}.parquet"
    )

    if not quarantine.empty:
        write_parquet_to_s3(
            s3=s3,
            bucket=bucket,
            key=quarantine_key,
            df=quarantine,
        )

    print(
        f"Quality completed: {topic} | "
        f"records={len(df)} | "
        f"quarantine={len(quarantine)}"
    )

    return {
        "topic": topic,
        "records": len(df),
        "failed_records": len(quarantine),
        "dq_checks": len(scorecard),
    }


def run_quality() -> dict:

    s3 = get_s3_client()
    bucket = get_bucket()

    results = {}

    for topic in TOPICS:
        results[topic] = run_quality_for_topic(
            s3=s3,
            bucket=bucket,
            topic=topic,
        )

    print("\nQuality pipeline completed.")
    print(results)

    return results


if __name__ == "__main__":
    run_quality()