import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")

def has_column(conn, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        if not has_column(conn, "applications", "week_decided"):
            conn.execute("ALTER TABLE applications ADD COLUMN week_decided TEXT;")
            conn.commit()
            print("✅ Added applications.week_decided")
        else:
            print("✅ applications.week_decided already exists")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
