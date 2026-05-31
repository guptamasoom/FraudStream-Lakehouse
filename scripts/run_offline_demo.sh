#!/usr/bin/env bash
set -euo pipefail

bash scripts/generate_offline_demo_data.sh
bash scripts/run_local_pipeline.sh

