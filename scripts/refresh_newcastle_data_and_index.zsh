#!/usr/bin/env zsh
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
export PLANA_DB_PATH="${PLANA_DB_PATH:-$BASE/plana/data/plana.sqlite}"

cd "$BASE"
mkdir -p logs

echo "== Refresh Newcastle data + rebuild similarity index =="

# TODO: Replace these with your real scripts
# Examples (replace with what exists in your repo):
# python3 scripts/01_fetch_archives.py
# python3 scripts/02_parse_archives.py
# python3 scripts/03_build_similarity_index.py

echo "1) Fetch/update archives (EDIT ME)"
# python3 scripts/01_fetch_archives.py

echo "2) Parse/update planning.db (EDIT ME)"
# python3 scripts/02_parse_archives.py

echo "3) Rebuild similarity index (EDIT ME)"
# python3 scripts/03_build_similarity_index.py

echo "DONE"
