from __future__ import annotations

import io
import json
import os
from datetime import date

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


def get_s3_bucket() -> str:
    bucket = os.getenv("S3_BUCKET")

    if not bucket:
        raise RuntimeError(
            "S3_BUCKET is not configured."
        )

    return bucket


def list_s3_parquet_files(
    s3_client,
    bucket: str,
    prefix: str,
) -> list[str]:

    paginator = s3_client.get_paginator(
        "list_objects_v2"
    )

    files = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".parquet"):
                files.append(key)

    return sorted(files)


def read_parquet_from_s3(
    s3_client,
    bucket: str,
    key: str,
) -> pd.DataFrame:

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    data = response["Body"].read()

    return pd.read_parquet(
        io.BytesIO(data)
    )


def read_s3_dataset(
    s3_client,
    bucket: str,
    prefix: str,
) -> pd.DataFrame:

    files = list_s3_parquet_files(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
    )

    if not files:
        return pd.DataFrame()

    frames = []

    for key in files:
        frames.append(
            read_parquet_from_s3(
                s3_client=s3_client,
                bucket=bucket,
                key=key,
            )
        )

    return pd.concat(
        frames,
        ignore_index=True,
    )


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


def parse_bronze(
    df: pd.DataFrame,
) -> pd.DataFrame:

    rows = []

    for _, row in df.iterrows():

        try:
            payload = json.loads(
                row.get("raw_json") or "{}"
            )

        except json.JSONDecodeError:

            payload = {
                "_malformed_record": row.get(
                    "raw_json"
                )
            }

        payload.update(
            {
                "ingestion_timestamp": row.get(
                    "ingestion_timestamp"
                ),
                "source_topic": row.get(
                    "source_topic"
                ),
                "source_file": row.get(
                    "source_file"
                ),
                "batch_id": row.get(
                    "batch_id"
                ),
                "processing_date": row.get(
                    "processing_date"
                ),
            }
        )

        rows.append(payload)

    return pd.DataFrame(rows)


def normalize(
    df: pd.DataFrame,
) -> pd.DataFrame:

    if df.empty:
        return df

    out = df.copy()

    if "event_timestamp" in out:

        out["event_timestamp"] = pd.to_datetime(
            out["event_timestamp"],
            errors="coerce",
            utc=True,
        )

        out["event_date"] = (
            out["event_timestamp"]
            .dt.date
            .astype("string")
        )

        out["event_hour"] = (
            out["event_timestamp"]
            .dt.hour
        )

    for column in [
        "amount",
        "chargeback_amount",
    ]:

        if column in out:

            out[column] = pd.to_numeric(
                out[column],
                errors="coerce",
            )

    for column in [
        "is_card_present",
        "is_known_fraud",
        "mfa_passed",
        "is_new_device",
    ]:

        if column in out:

            out[column] = (
                out[column]
                .fillna(False)
                .astype(bool)
            )

    if (
        "event_id" in out
        and "event_timestamp" in out
    ):

        out = (
            out.sort_values(
                "event_timestamp"
            )
            .drop_duplicates(
                "event_id",
                keep="last",
            )
        )

    return out


def transform_topic(
    s3_client,
    bucket: str,
    topic: str,
    processing_date: str | None = None,
) -> pd.DataFrame:

    processing_date = (
        processing_date
        or date.today().isoformat()
    )

    bronze_prefix = os.getenv(
        "S3_BRONZE_PREFIX",
        "bronze/",
    ).rstrip("/")

    bronze_topic_prefix = (
        f"{bronze_prefix}/"
        f"{topic}/"
    )

    bronze_df = read_s3_dataset(
        s3_client=s3_client,
        bucket=bucket,
        prefix=bronze_topic_prefix,
    )

    if bronze_df.empty:

        print(
            f"No Bronze records found for topic: "
            f"{topic}"
        )

        return pd.DataFrame()

    parsed = parse_bronze(
        bronze_df
    )

    normalized = normalize(
        parsed
    )

    silver_prefix = os.getenv(
        "S3_SILVER_PREFIX",
        "silver/",
    ).rstrip("/")

    silver_key = (
        f"{silver_prefix}/"
        f"{topic}/"
        f"processing_date={processing_date}/"
        f"part-{topic}.parquet"
    )

    write_parquet_to_s3(
        s3_client=s3_client,
        bucket=bucket,
        key=silver_key,
        df=normalized,
    )

    print(
        f"Silver written: "
        f"{len(normalized)} records → "
        f"s3://{bucket}/{silver_key}"
    )

    return normalized


def transform_all(
    topics: list[str] | None = None,
) -> dict[str, int]:

    s3_client = get_s3_client()

    bucket = get_s3_bucket()

    valid_topics = set(
        TOPIC_NAMES.values()
    )

    topics = topics or list(valid_topics)

    counts = {}

    for topic in topics:

        if topic not in valid_topics:

            raise ValueError(
                f"Invalid FraudStream topic: {topic}"
            )

        silver_df = transform_topic(
            s3_client=s3_client,
            bucket=bucket,
            topic=topic,
        )

        counts[topic] = len(
            silver_df
        )

    print(
        f"Silver transformation completed. "
        f"Counts: {counts}"
    )

    return counts


if __name__ == "__main__":

    from dotenv import load_dotenv

    load_dotenv()

    transform_all()