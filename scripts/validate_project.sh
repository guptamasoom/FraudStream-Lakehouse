#!/usr/bin/env bash
set -euo pipefail

python -m compileall fraudstream scripts
pytest -q
