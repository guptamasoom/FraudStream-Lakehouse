from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="fraudstream_local_medallion_pipeline",
    description="Runs the local FraudStream Bronze, Silver, and Gold pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["fraudstream", "lakehouse", "portfolio"],
) as dag:
    generate_demo_data = BashOperator(
        task_id="generate_demo_data",
        bash_command="python -m fraudstream.producers.transaction_simulator --duration 10 --event-rate 20 --fraud-rate 12 --local-outbox data/landing",
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="python -m fraudstream.pipeline.local_pipeline --landing data/landing --bronze data/bronze --silver data/silver --gold data/gold --quarantine data/quarantine",
    )

    generate_demo_data >> run_pipeline

