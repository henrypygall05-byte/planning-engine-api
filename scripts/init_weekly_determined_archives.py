import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_determined_archives (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            council TEXT NOT NULL,
            week_start TEXT NOT NULL,                 -- YYYY-MM-DD (Sunday)
            url TEXT NOT NULL,
            http_status INTEGER NOT NULL,
            applications_found INTEGER NOT NULL,
            html_path TEXT NOT NULL,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(council, week_start)
        );
        """)
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_weekly_determined_week
        ON weekly_determined_archives(week_start);
        """)
        conn.commit()
        print("✅ weekly_determined_archives ready")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
