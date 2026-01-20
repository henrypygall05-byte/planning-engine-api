#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$ROOT/src"
python - <<'PY'
import sys
print("PYTHONPATH OK")
print("sys.path[0:3] =", sys.path[0:3])
import plana_engine
print("✅ import plana_engine OK")
import plana_engine.report.report_entrypoint
print("✅ import report_entrypoint OK")
import plana_engine.policies.retrieve_policies
print("✅ import retrieve_policies OK")
PY
