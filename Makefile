SHELL := /usr/bin/env bash

.PHONY: up init demo produce consume run-pipeline dashboard test validate down clean

up:
	docker compose up -d kafka minio minio-init postgres dashboard

init:
	bash scripts/init_kafka_topics.sh

demo:
	bash scripts/run_offline_demo.sh

produce:
	python -m fraudstream.producers.transaction_simulator --event-rate 10 --duration 60 --fraud-rate 8

consume:
	python -m fraudstream.consumers.landing_consumer --landing-root data/landing

run-pipeline:
	bash scripts/run_local_pipeline.sh

dashboard:
	python -m fraudstream.dashboard.app

test:
	pytest -q

validate:
	bash scripts/validate_project.sh

down:
	docker compose down --remove-orphans

clean:
	rm -rf data logs .pytest_cache htmlcov .coverage
