# End-to-End Runbook

## 1. Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## 2. Offline Demo

This mode does not require Kafka.

```bash
bash scripts/generate_offline_demo_data.sh
python -m fraudstream.dashboard.app
```

Open `http://localhost:8060`.

## 3. Kafka Demo

```bash
make up
make init
make produce
make consume
make run-pipeline
make dashboard
```

## 4. Validate

```bash
make validate
```

## 5. Common Troubleshooting

If the dashboard is empty, build Gold tables first:

```bash
bash scripts/generate_offline_demo_data.sh
```

If Kafka is not ready, wait 20-30 seconds after `make up`, then run:

```bash
make init
```
