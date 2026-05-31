# Interview Story

Use this project to explain how you think like a senior data engineer.

## Problem

Fraud teams need fast signals, explainable alerts, clean data, and an audit trail. A dashboard alone is not enough if the pipeline loses raw evidence or silently drops bad records.

## Solution

FraudStream builds a full pipeline:

1. Generate realistic fintech events.
2. Stream domain events through Kafka.
3. Preserve raw data in Bronze.
4. Enforce data quality in Silver.
5. Score fraud using explainable rules.
6. Publish Gold marts for analysts and operations.
7. Show the results in a dashboard.

## Senior Engineering Decisions

- Raw data is immutable and replayable.
- Quality failures go to quarantine, not the trash.
- Fraud decisions have reason codes.
- Local mode works without cloud spend.
- Cloud templates show how the design scales.
- Tests validate the end-to-end pipeline, not only helper functions.

## How to Pitch It

> I built FraudStream Lakehouse as a real-time fraud detection platform. It simulates payment, login, device, and chargeback events, processes them through a medallion lakehouse, applies data quality checks, scores fraud with explainable rules, and serves analyst-ready Gold tables plus a dashboard. The project is local-first for learning, but includes Terraform, Databricks, and Airflow templates to show the production migration path.

