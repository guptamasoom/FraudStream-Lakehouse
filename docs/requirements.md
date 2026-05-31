# Requirements

FraudStream Lakehouse has two installation profiles: local learning mode and optional cloud mode.

## Local Learning Mode

Use this mode first. It runs the full project on your laptop and does not require cloud services.

| Requirement | Version | Why it is needed |
|---|---:|---|
| Python | 3.10+ | Runs simulator, pipeline, tests, and dashboard. |
| pip | latest recommended | Installs project dependencies. |
| Git | latest recommended | Clones and versions the project. |
| Docker Desktop | optional | Runs Kafka, MinIO, Postgres, and dashboard containers. |

Install local dependencies:

```bash
pip install -r requirements.txt
```

## Optional Cloud Mode

Use this only when moving the project to AWS or Databricks.

| Requirement | Why it is needed |
|---|---|
| AWS account | S3 lakehouse storage and infrastructure templates. |
| Terraform | Creates the encrypted S3 bucket foundation. |
| Databricks workspace | Runs Spark/Delta notebooks and workflow template. |
| Kafka provider | MSK, Confluent Cloud, Redpanda, or another broker. |

Install optional cloud dependencies:

```bash
pip install -r requirements-cloud.txt
```

## Environment File

Create your local `.env` from the template:

```bash
cp .env.example .env
```

Keep `.env` private. Only `.env.example` belongs in GitHub.

