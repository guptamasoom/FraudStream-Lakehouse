# Setup Guide

This guide takes you from a fresh clone to a working fraud analytics dashboard.

## 1. Clone

```bash
git clone https://github.com/analyticsdurgesh/FraudStream-Lakehouse.git
cd FraudStream-Lakehouse
```

## 2. Create Python Environment

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Create Environment Variables

```bash
cp .env.example .env
```

For the offline demo, the sample local values are enough.

## 4. Run Offline Demo

```bash
bash scripts/run_offline_demo.sh
```

Expected output includes generated events and populated Bronze, Silver, and Gold tables.

## 5. Start Dashboard

```bash
python -m fraudstream.dashboard.app
```

Open:

```text
http://localhost:8060
```

## 6. Run Tests

```bash
bash scripts/validate_project.sh
```

## Kafka Mode

```bash
docker compose up -d kafka minio minio-init postgres
bash scripts/init_kafka_topics.sh
python -m fraudstream.producers.transaction_simulator --event-rate 20 --duration 60 --fraud-rate 12
python -m fraudstream.consumers.landing_consumer --landing-root data/landing
bash scripts/run_local_pipeline.sh
```

## Reset Local Demo Data

```bash
make clean
```

