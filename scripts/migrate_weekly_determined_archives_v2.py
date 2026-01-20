import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")

REQUIRED_COLUMNS = {
    "status_code": "INTEGER NOT NULL DEFAULT 0",
    "rows_found": "INTEGER NOT NULL DEFAULT 0",
}

def main():
    if not DB_PATH.exists():
        raise SystemExit("❌ Database not found")

    conn = sqlite3.connect(DB_PATH)
    try:
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(weekly_determined_archives)")
        }

        for col, ddl in REQUIRED_COLUMNS.items():
            if col not in existing:
                conn.execute(
                    f"ALTER TABLE weekly_determined_archives ADD COLUMN {col} {ddl};"
                )
                print(f"✅ Added column: {col}")
            else:
                print(f"ℹ️ Column already exists: {col}")

        conn.commit()
        print("✅ Migration complete")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
