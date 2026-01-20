import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")

SQL = """
CREATE TABLE IF NOT EXISTS weekly_archives (
  week_start TEXT PRIMARY KEY,
  fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
  url TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  file_path TEXT NOT NULL,
  applications_found INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_weekly_archives_fetched_at
  ON weekly_archives(fetched_at);
"""

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SQL)
        conn.commit()
    finally:
        conn.close()

    print("✅ weekly_archives table ready")

if __name__ == "__main__":
    main()
