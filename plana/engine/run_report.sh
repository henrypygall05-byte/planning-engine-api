#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure imports work (plana_engine is under src/)
export PYTHONPATH="$ROOT/src"

# Default DB path if not already set
export PLANA_DB_PATH="${PLANA_DB_PATH:-$ROOT/../data/plana.sqlite}"

python -m plana_engine.report.report_entrypoint "$@"
