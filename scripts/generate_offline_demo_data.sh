#!/usr/bin/env bash
set -euo pipefail

python -m fraudstream.producers.transaction_simulator \
  --duration 10 \
  --event-rate 25 \
  --fraud-rate 12 \
  --bad-data-percentage 6 \
  --local-outbox data/landing
