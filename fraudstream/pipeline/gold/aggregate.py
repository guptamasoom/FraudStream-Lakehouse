from __future__ import annotations

import io
import os

import boto3
import pandas as pd


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


def _load(
    s3_client,
    bucket: str,
    silver_prefix: str,
    table: str,
) -> pd.DataFrame:

    prefix = (
        f"{silver_prefix.rstrip('/')}/"
        f"{table}/"
    )

    return read_s3_dataset(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
    )


def build_daily_fraud_summary(
    tx: pd.DataFrame,
) -> pd.DataFrame:

    if tx.empty:
        return pd.DataFrame(
            columns=[
                "event_date",
                "transactions",
                "total_amount",
                "high_risk_transactions",
                "fraud_rate",
            ]
        )

    tx = tx.copy()

    tx["amount"] = pd.to_numeric(
        tx["amount"],
        errors="coerce",
    ).fillna(0)

    grouped = (
        tx.groupby(
            "event_date",
            dropna=False,
        )
        .agg(
            transactions=(
                "transaction_id",
                "nunique",
            ),
            total_amount=(
                "amount",
                "sum",
            ),
            high_risk_transactions=(
                "risk_band",
                lambda s: s.eq("HIGH").sum(),
            ),
            avg_risk_score=(
                "risk_score",
                "mean",
            ),
        )
        .reset_index()
    )

    grouped["fraud_rate"] = (
        grouped["high_risk_transactions"]
        / grouped["transactions"].replace(
            0,
            pd.NA,
        )
    )

    return grouped


def build_high_risk_transactions(
    tx: pd.DataFrame,
) -> pd.DataFrame:

    if tx.empty:
        return pd.DataFrame()

    return (
        tx[
            tx["risk_band"].eq("HIGH")
        ]
        .sort_values(
            "risk_score",
            ascending=False,
        )
    )


def build_customer_risk_profile(
    tx: pd.DataFrame,
) -> pd.DataFrame:

    if tx.empty:
        return pd.DataFrame(
            columns=[
                "customer_id",
                "transactions",
                "total_amount",
                "max_risk_score",
                "high_risk_transactions",
            ]
        )

    tx = tx.copy()

    tx["amount"] = pd.to_numeric(
        tx["amount"],
        errors="coerce",
    ).fillna(0)

    return (
        tx.groupby(
            "customer_id",
            dropna=False,
        )
        .agg(
            transactions=(
                "transaction_id",
                "nunique",
            ),
            total_amount=(
                "amount",
                "sum",
            ),
            avg_risk_score=(
                "risk_score",
                "mean",
            ),
            max_risk_score=(
                "risk_score",
                "max",
            ),
            high_risk_transactions=(
                "risk_band",
                lambda s: s.eq("HIGH").sum(),
            ),
            countries=(
                "ip_country",
                "nunique",
            ),
            devices=(
                "device_id",
                "nunique",
            ),
        )
        .reset_index()
    )


def build_merchant_risk_profile(
    tx: pd.DataFrame,
) -> pd.DataFrame:

    if tx.empty:
        return pd.DataFrame(
            columns=[
                "merchant_id",
                "transactions",
                "total_amount",
                "avg_risk_score",
            ]
        )

    tx = tx.copy()

    tx["amount"] = pd.to_numeric(
        tx["amount"],
        errors="coerce",
    ).fillna(0)

    return (
        tx.groupby(
            [
                "merchant_id",
                "merchant_category",
            ],
            dropna=False,
        )
        .agg(
            transactions=(
                "transaction_id",
                "nunique",
            ),
            total_amount=(
                "amount",
                "sum",
            ),
            avg_risk_score=(
                "risk_score",
                "mean",
            ),
            high_risk_transactions=(
                "risk_band",
                lambda s: s.eq("HIGH").sum(),
            ),
        )
        .reset_index()
    )


def build_fraud_rule_performance(
    tx: pd.DataFrame,
) -> pd.DataFrame:

    if tx.empty:
        return pd.DataFrame(
            columns=[
                "rule_name",
                "triggered_count",
                "known_fraud_count",
            ]
        )

    rule_cols = [
        col
        for col in tx.columns
        if col.startswith("rule_")
    ]

    rows = []

    for col in rule_cols:

        triggered = tx[
            tx[col].astype(bool)
        ]

        rows.append(
            {
                "rule_name": col.replace(
                    "rule_",
                    "",
                ),
                "triggered_count": len(
                    triggered
                ),
                "known_fraud_count": int(
                    triggered.get(
                        "is_known_fraud",
                        pd.Series(
                            dtype=bool
                        ),
                    )
                    .fillna(False)
                    .sum()
                ),
                "avg_risk_score": (
                    float(
                        triggered[
                            "risk_score"
                        ].mean()
                    )
                    if len(triggered)
                    else 0
                ),
            }
        )

    return pd.DataFrame(rows)


def build_realtime_alerts(
    tx: pd.DataFrame,
) -> pd.DataFrame:

    high = build_high_risk_transactions(
        tx
    )

    if high.empty:
        return pd.DataFrame()

    cols = [
        "event_timestamp",
        "transaction_id",
        "customer_id",
        "merchant_id",
        "amount",
        "risk_score",
        "risk_band",
        "fraud_reason_codes",
    ]

    return high[
        [
            col
            for col in cols
            if col in high.columns
        ]
    ].head(100)


def build_data_quality_scorecard(
    s3_client,
    bucket: str,
    silver_prefix: str,
) -> pd.DataFrame:

    prefix = (
        f"{silver_prefix.rstrip('/')}/"
        f"_dq_results/"
    )

    return read_s3_dataset(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
    )


def build_all() -> dict[str, int]:

    s3_client = get_s3_client()
    bucket = get_s3_bucket()

    silver_prefix = os.getenv(
        "S3_SILVER_PREFIX",
        "silver/",
    )

    gold_prefix = os.getenv(
        "S3_GOLD_PREFIX",
        "gold/",
    ).rstrip("/")

    tx = _load(
        s3_client,
        bucket,
        silver_prefix,
        "scored_transactions",
    )

    builders = {
        "gold_daily_fraud_summary": (
            lambda: build_daily_fraud_summary(tx)
        ),
        "gold_high_risk_transactions": (
            lambda: build_high_risk_transactions(tx)
        ),
        "gold_customer_risk_profile": (
            lambda: build_customer_risk_profile(tx)
        ),
        "gold_merchant_risk_profile": (
            lambda: build_merchant_risk_profile(tx)
        ),
        "gold_fraud_rule_performance": (
            lambda: build_fraud_rule_performance(tx)
        ),
        "gold_realtime_alerts": (
            lambda: build_realtime_alerts(tx)
        ),
        "gold_data_quality_scorecard": (
            lambda: build_data_quality_scorecard(
                s3_client,
                bucket,
                silver_prefix,
            )
        ),
    }

    counts = {}

    for name, builder in builders.items():

        df = builder()

        gold_key = (
            f"{gold_prefix}/"
            f"{name}/"
            f"processing_date="
            f"{pd.Timestamp.now(tz='UTC').date()}/"
            f"part-{name}.parquet"
        )

        write_parquet_to_s3(
            s3_client=s3_client,
            bucket=bucket,
            key=gold_key,
            df=df,
        )

        counts[name] = len(df)

        print(
            f"Gold written: "
            f"{name} → "
            f"{len(df)} records → "
            f"s3://{bucket}/{gold_key}"
        )

    print(
        f"Gold aggregation completed. "
        f"Counts: {counts}"
    )

    return counts


if __name__ == "__main__":

    from dotenv import load_dotenv

    load_dotenv()

    build_all()