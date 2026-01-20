#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
PY="$BASE/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

mkdir -p "$BASE/logs"

echo "== Nightly: refresh + index + smoke test + auto-tune =="

# 1) refresh data/index (your script may be mostly placeholders right now, but keep it in the flow)
if [[ -x "$BASE/scripts/refresh_newcastle_data_and_index.zsh" ]]; then
  "$BASE/scripts/refresh_newcastle_data_and_index.zsh" >> "$BASE/logs/nightly_refresh.log" 2>&1 || true
fi

# 2) rebuild similarity index
"$PY" "$BASE/scripts/03_build_similarity_index.py" >> "$BASE/logs/nightly_index.log" 2>&1

# 3) run smoke test + generate final officer report + log feedback
"$BASE/run_newcastle_c3_pilot.zsh" "Change of use to a single dwellinghouse (Use Class C3), including internal alterations, at Newcastle upon Tyne." >> "$BASE/logs/nightly_smoketest.log" 2>&1 || true

# 4) auto-tune weights
"$PY" "$BASE/scripts/update_weights_from_feedback.py" --feedback "$BASE/logs/feedback/feedback.jsonl" --weights "$BASE/config/relevance_weights.json" --min_records 3 >> "$BASE/logs/nightly_weights.log" 2>&1 || true

echo "DONE nightly."
