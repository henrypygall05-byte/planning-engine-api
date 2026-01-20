#!/usr/bin/env bash
set -euo pipefail

# Always run from the engine folder (the folder containing this script)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PYTHONPATH="$ROOT/src"
export PLANA_DB_PATH="${PLANA_DB_PATH:-$ROOT/../data/plana.sqlite}"

python -m plana_engine.report.report_entrypoint "$@"
