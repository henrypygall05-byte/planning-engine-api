import sqlite3
from pathlib import Path
import glob
import os

DB_PATH = Path("data/processed/planning.db")
RAW_DIR = Path("data/raw/weekly_archives")

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DELETE FROM weekly_archives;")
        conn.commit()
    finally:
        conn.close()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    removed = 0
    for p in RAW_DIR.glob("weekly_received_*.html"):
        p.unlink()
        removed += 1

    print("✅ Reset complete")
    print("weekly_archives rows deleted")
    print("weekly HTML files deleted:", removed)

if __name__ == "__main__":
    main()
