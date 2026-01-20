import json
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path
import requests

DB_PATH = Path("data/processed/planning.db")
COUNCIL = "Newcastle City Council"

# Newcastle weekly archive endpoint (Idox)
BASE_URL = "https://publicaccess.newcastle.gov.uk/online-applications/weeklyList.do"
PARAMS = {
    "action": "getReceivedWeeklyList"
}

HEADERS = {
    "User-Agent": "planning-research-bot/1.0"
}

def fetch_week(start_date: date):
    params = PARAMS.copy()
    params["week"] = start_date.strftime("%d/%m/%Y")
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def parse_html(html: str):
    # We store raw HTML only; structured extraction happens later
    return html

def insert_week(conn, raw_html: str):
    # We do NOT parse individual applications yet
    # This guarantees zero row loss and deterministic reprocessing later
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO applications (
            council,
            application_ref,
            raw_json
        ) VALUES (?, ?, ?)
    """, (COUNCIL, f"WEEKLY_ARCHIVE_{hash(raw_html)}", raw_html))

    return cur.rowcount

def main():
    if not DB_PATH.exists():
        print("❌ Database not found:", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    today = date.today()
    start = today - timedelta(weeks=260)  # ~5 years

    inserted_total = 0
    current = start

    try:
        while current <= today:
            print("Fetching week starting:", current)
            html = fetch_week(current)
            inserted = insert_week(conn, html)
            conn.commit()
            print("  inserted rows:", inserted)
            inserted_total += inserted
            current += timedelta(weeks=1)
    finally:
        conn.close()

    print("\n✅ DONE")
    print("Total inserted rows:", inserted_total)

if __name__ == "__main__":
    main()
