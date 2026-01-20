#!/usr/bin/env zsh
set -euo pipefail

BASE="$(cd "$(dirname "$0")" && pwd)"
export PLANA_DB_PATH="${PLANA_DB_PATH:-$HOME/Desktop/Plana/Newcastle/plana/data/plana.sqlite}"

PROPOSAL="${1:-}"
if [[ -z "$PROPOSAL" ]]; then
  echo "Usage: ./run_newcastle_c3_pilot.zsh \"<proposal text>\""
  exit 1
fi

echo ""
echo "== Plana Newcastle C3 Pilot =="
echo "Using DB: $PLANA_DB_PATH"
echo ""

# Use venv python if present, else python3
PY="$BASE/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

mkdir -p "$BASE/logs" "$BASE/logs/runs" "$BASE/logs/feedback"

echo "Checking similarity index..."
test -f "$BASE/index/app_index.faiss" || { echo "ERROR: app_index.faiss missing"; exit 1; }
test -f "$BASE/index/app_index_meta.json" || { echo "ERROR: app_index_meta.json missing"; exit 1; }

echo "Running engine with proposal text..."
"$PY" "$BASE/plana/engine/run_engine.py" "$PROPOSAL" | tee "$BASE/logs/payload_latest.json" >/dev/null

echo "Validating JSON..."
"$PY" -m json.tool "$BASE/logs/payload_latest.json" >/dev/null
echo "OK: payload JSON valid"

echo "Rendering council-style report..."
"$PY" "$BASE/scripts/render_council_report.py" "$BASE/logs/payload_latest.json" -o "$BASE/logs/report_latest.md" >/dev/null
echo "OK: wrote $BASE/logs/report_latest.md"

echo "Archiving run..."
TS="$(date +%Y%m%d_%H%M%S)"
cp "$BASE/logs/payload_latest.json" "$BASE/logs/runs/payload_${TS}.json"
cp "$BASE/logs/report_latest.md" "$BASE/logs/runs/report_${TS}.md"

echo "Scoring report quality..."
# This assumes your existing quality scorer is already being called in your version.
# If your scorer is a script, call it here. If it was embedded earlier, keep your old scorer call.
# We'll keep the score output file name consistent:
SCORE_OUT="$BASE/logs/runs/score_${TS}.txt"
if [[ -x "$BASE/scripts/score_report_quality.py" ]]; then
  "$PY" "$BASE/scripts/score_report_quality.py" "$BASE/logs/payload_latest.json" > "$SCORE_OUT"
else
  # fallback: keep a minimal score file so feedback logging doesn't break
  echo "== Report Quality Check ==" > "$SCORE_OUT"
  echo "No scripts/score_report_quality.py found; using minimal placeholder." >> "$SCORE_OUT"
fi
cat "$SCORE_OUT"

echo "Rendering CASE OFFICER report (deterministic)..."
"$PY" "$BASE/scripts/render_case_officer_report.py" "$BASE/logs/payload_latest.json" -o "$BASE/logs/report_latest_case_officer.md" >/dev/null
echo "OK: wrote $BASE/logs/report_latest_case_officer.md"

echo "Logging feedback..."
"$PY" "$BASE/scripts/log_feedback.py" --payload "$BASE/logs/payload_latest.json" --score "$SCORE_OUT" --out "$BASE/logs/feedback/feedback.jsonl" >/dev/null

echo "Auto-updating weights (from recent feedback)..."
"$PY" "$BASE/scripts/update_weights_from_feedback.py" --feedback "$BASE/logs/feedback/feedback.jsonl" --weights "$BASE/config/relevance_weights.json" --min_records 3 || true

echo "Opening FINAL report..."
open "$BASE/logs/report_latest_case_officer.md"

echo ""
echo "DONE. Archived as:"
echo " - $BASE/logs/runs/payload_${TS}.json"
echo " - $BASE/logs/runs/report_${TS}.md"
echo " - $SCORE_OUT"
echo " - $BASE/logs/report_latest_case_officer.md"
