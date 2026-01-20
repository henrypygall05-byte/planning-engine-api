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
            week_start TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            rows_found INTEGER NOT NULL,
            html_path TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        print("✅ weekly_determined_archives table ready")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
