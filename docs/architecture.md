# Architecture

FraudStream Lakehouse is a local-first, cloud-ready fraud analytics platform.

Local mode uses Docker Kafka and local files under `data/` so students can run the project without paid cloud services. Cloud mode maps the same architecture to managed Kafka, AWS S3, Databricks, and production orchestration.

## Design Principles

- Preserve raw events before transformation.
- Separate raw, trusted, and serving layers.
- Quarantine bad records instead of silently dropping them.
- Make fraud rules explainable with reason codes.
- Keep local defaults runnable and cloud secrets outside git.
- Build Gold outputs around business questions.

## Platform Layers

1. **Event simulation:** Generates transaction, login, device, and chargeback events.
2. **Streaming bus:** Kafka topics separate operational domains.
3. **Landing zone:** JSONL files are replayable and partition-friendly.
4. **Bronze:** Raw JSON plus ingestion metadata.
5. **Silver:** Parsed, typed, deduplicated, validated records.
6. **Fraud scoring:** Rule-based features and risk bands.
7. **Gold:** Fraud summaries, alert tables, customer/merchant profiles, rule performance, DQ scorecards.
8. **Serving:** Plotly Dash dashboard for operational monitoring.

## Cloud Upgrade Path

- Replace Docker Kafka with MSK or Confluent Cloud.
- Replace local `data/` with S3 paths.
- Replace Pandas fallback with Spark Structured Streaming and Delta Lake.
- Add Airflow/MWAA or Databricks Workflows.
- Add alert sinks such as Slack, PagerDuty, or case-management queues.
