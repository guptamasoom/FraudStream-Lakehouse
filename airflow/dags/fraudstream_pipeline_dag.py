from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# Common environment for all pipeline tasks
# ============================================================
COMMON_ENV = {
    "PYTHONPATH": "/opt/airflow",
    "AWS_PROFILE": "dev1",
    "AWS_REGION": "us-east-1",
}


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="fraudstream_s3_lakehouse_pipeline",
    description=(
        "Orchestrates the FraudStream Kafka to S3 "
        "lakehouse pipeline from Landing through Gold."
    ),
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=[
        "fraudstream",
        "lakehouse",
        "kafka",
        "s3",
    ],
) as dag:

    # ========================================================
    # 1. Kafka → S3 Landing
    # ========================================================
    landing = BashOperator(
        task_id="landing_kafka_to_s3",

        bash_command=(
            "python -m "
            "fraudstream.consumers.landing_consumer"
        ),

        cwd="/opt/airflow",
        env=COMMON_ENV,
    )

    # ========================================================
    # 2. S3 Landing → Bronze
    # ========================================================
    bronze = BashOperator(
        task_id="bronze_ingestion",

        bash_command=(
            "python -m "
            "fraudstream.pipeline.bronze.ingest"
        ),

        cwd="/opt/airflow",
        env=COMMON_ENV,
    )

    # ========================================================
    # 3. Bronze → Silver
    # ========================================================
    silver = BashOperator(
        task_id="silver_transformation",

        bash_command=(
            "python -m "
            "fraudstream.pipeline.silver.transform"
        ),

        cwd="/opt/airflow",
        env=COMMON_ENV,
    )

    # ========================================================
    # 4. Silver → DQ / Quarantine
    # ========================================================
    quality = BashOperator(
        task_id="quality_validation",

        bash_command=(
            "python -m "
            "fraudstream.pipeline.quality.run"
        ),

        cwd="/opt/airflow",
        env=COMMON_ENV,
    )

    # ========================================================
    # 5. Silver → Fraud Scoring
    # ========================================================
    fraud = BashOperator(
        task_id="fraud_scoring",

        bash_command=(
            "python -m "
            "fraudstream.pipeline.fraud.score"
        ),

        cwd="/opt/airflow",
        env=COMMON_ENV,
    )

    # ========================================================
    # 6. Silver Scored → Gold
    # ========================================================
    gold = BashOperator(
        task_id="gold_aggregation",

        bash_command=(
            "python -m "
            "fraudstream.pipeline.gold.aggregate"
        ),

        cwd="/opt/airflow",
        env=COMMON_ENV,
    )

    # ========================================================
    # Pipeline Dependency
    # ========================================================
    (
        landing
        >> bronze
        >> silver
        >> quality
        >> fraud
        >> gold
    )