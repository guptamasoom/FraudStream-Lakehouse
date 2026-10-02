from __future__ import annotations

import io
import json
import os
from datetime import date
from uuid import uuid4

import boto3
import pandas as pd

from fraudstream.topics import TOPIC_NAMES


def get_s3_client():
    aws_profile = os.getenv("AWS_PROFILE")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if aws_profile:
        session = boto3.Session(
            profile_name=aws_profile,
            region_name=aws_region,
        )
    else:
        session = boto3.Session(
            region_name=aws_region,
        )

    return session.client("s3")


def read_jsonl_from_s3(
    s3_client,
    bucket: str,
    key: str,
) -> list[dict]:

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    content = response["Body"].read().decode("utf-8")

    rows = []

    for line in content.splitlines():

        line = line.strip()

        if not line:
            continue

        try:
            rows.append(json.loads(line))

        except json.JSONDecodeError:
            rows.append(
                {
                    "_malformed_record": line,
                }
            )

    return rows


def list_landing_files(
    s3_client,
    bucket: str,
    topic: str,
) -> list[str]:

    prefix = os.getenv(
        "S3_LANDING_PREFIX",
        "landing/",
    ).rstrip("/")

    topic_prefix = f"{prefix}/{topic}/"

    paginator = s3_client.get_paginator("list_objects_v2")

    files = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=topic_prefix,
    ):
        for obj in page.get("Contents", []):

            key = obj["Key"]

            if key.endswith(".jsonl"):
                files.append(key)

    return sorted(files)


def write_parquet_to_s3(
    s3_client,
    bucket: str,
    key: str,
    df: pd.DataFrame,
) -> None:

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False,
    )

    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )


def ingest_topic_to_bronze(
    topic: str,
    processing_date: str | None = None,
) -> pd.DataFrame:

    processing_date = (
        processing_date
        or date.today().isoformat()
    )

    bucket = os.getenv("S3_BUCKET")

    if not bucket:
        raise RuntimeError(
            "S3_BUCKET is not configured."
        )

    s3_client = get_s3_client()

    landing_files = list_landing_files(
        s3_client=s3_client,
        bucket=bucket,
        topic=topic,
    )

    batch_id = str(uuid4())

    rows: list[dict] = []

    for file_key in landing_files:

        payloads = read_jsonl_from_s3(
            s3_client=s3_client,
            bucket=bucket,
            key=file_key,
        )

        for payload in payloads:

            rows.append(
                {
                    "raw_json": json.dumps(
                        payload,
                        default=str,
                    ),
                    "event_id": payload.get(
                        "event_id"
                    ),
                    "event_type": payload.get(
                        "event_type"
                    ),
                    "ingestion_timestamp": pd.Timestamp.now(
                        tz="UTC"
                    ).isoformat(),
                    "source_topic": topic,
                    "source_file": file_key,
                    "batch_id": batch_id,
                    "processing_date": processing_date,
                    "_malformed_record": payload.get(
                        "_malformed_record"
                    ),
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        print(
            f"No landing records found for topic: {topic}"
        )
        return df

    bronze_prefix = os.getenv(
        "S3_BRONZE_PREFIX",
        "bronze/",
    ).rstrip("/")

    bronze_key = (
        f"{bronze_prefix}/"
        f"{topic}/"
        f"processing_date={processing_date}/"
        f"part-{batch_id}.parquet"
    )

    write_parquet_to_s3(
        s3_client=s3_client,
        bucket=bucket,
        key=bronze_key,
        df=df,
    )

    print(
        f"Bronze written: "
        f"{len(df)} records → "
        f"s3://{bucket}/{bronze_key}"
    )

    return df


def ingest_all(
    topics: list[str] | None = None,
) -> dict[str, int]:

    valid_topics = set(
        TOPIC_NAMES.values()
    )

    topics = topics or list(valid_topics)

    results = {}

    for topic in topics:

        if topic not in valid_topics:
            raise ValueError(
                f"Invalid FraudStream topic: {topic}"
            )

        df = ingest_topic_to_bronze(
            topic=topic,
        )

        results[topic] = len(df)

    total = sum(results.values())

    print(
        f"Bronze ingestion completed. "
        f"Total records: {total}"
    )

    return results


if __name__ == "__main__":

    from dotenv import load_dotenv

    load_dotenv()

    ingest_all()