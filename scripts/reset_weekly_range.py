import argparse
import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--to-date", required=True, help="YYYY-MM-DD inclusive")
    args = ap.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute(
            "DELETE FROM weekly_archives WHERE week_start >= ? AND week_start <= ?",
            (args.from_date, args.to_date),
        )
        conn.commit()
        print("✅ Deleted weekly_archives rows:", cur.rowcount)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
