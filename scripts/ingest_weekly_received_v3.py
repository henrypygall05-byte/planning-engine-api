import argparse
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

DB_PATH = Path("data/processed/planning.db")
RAW_DIR = Path("data/raw/weekly_archives")

COUNCIL = "Newcastle City Council"

BASE = "https://portal.newcastle.gov.uk/planning/"
URL = urljoin(BASE, "index.html")
FA_VALUE = "getReceivedWeeklyList"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": URL,
}

TIMEOUT = 45

@dataclass
class WeeklyRow:
    application_ref: str
    address: str
    proposal: str
    ward: str
    community: str
    details_url: Optional[str]

def week_start_sunday(d: date) -> date:
    # Sunday = 6 for weekday() (Mon=0..Sun=6)
    return d - timedelta(days=(d.weekday() + 1) % 7)

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def fetch(sess: requests.Session, params: dict) -> Tuple[str, str, int]:
    r = sess.get(URL, params=params, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code == 405:
        r = sess.post(URL, data=params, timeout=TIMEOUT, allow_redirects=True)
    return r.url, r.text, r.status_code

def find_results_table(soup: BeautifulSoup):
    # Identify the table by expected headers
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if headers and "application" in headers[0] and any("proposal" in h for h in headers):
            return table, headers
    return None, []

def parse_week_rows(html: str) -> List[WeeklyRow]:
    soup = BeautifulSoup(html, "lxml")
    table, headers = find_results_table(soup)
    if table is None:
        return []

    rows: List[WeeklyRow] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        # Based on observed headers:
        # 0 Application
        # 1 Location Details
        # 2 Proposal
        # 3 Ward
        # 4 Community
        # 5 Details Available
        # 6 Jump to Application (often contains link)
        app_ref = tds[0].get_text(" ", strip=True)
        address = tds[1].get_text(" ", strip=True)
        proposal = tds[2].get_text(" ", strip=True)
        ward = tds[3].get_text(" ", strip=True) if len(tds) > 3 else ""
        community = tds[4].get_text(" ", strip=True) if len(tds) > 4 else ""

        details_url = None
        a = tr.find("a")
        if a and a.get("href"):
            details_url = urljoin(BASE, a["href"])

        if app_ref and "/" in app_ref:
            rows.append(WeeklyRow(
                application_ref=app_ref,
                address=address,
                proposal=proposal,
                ward=ward,
                community=community,
                details_url=details_url
            ))

    # stable de-dupe
    dedup = {}
    for r in rows:
        dedup[r.application_ref] = r
    return list(dedup.values())

def already_fetched(conn: sqlite3.Connection, week_start_iso: str) -> bool:
    cur = conn.execute("SELECT 1 FROM weekly_archives WHERE week_start = ?", (week_start_iso,))
    return cur.fetchone() is not None

def save_week(conn: sqlite3.Connection, week_start_iso: str, url: str, html: str, apps_found: int) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    digest = sha256_text(html)
    file_path = RAW_DIR / f"weekly_received_{week_start_iso}_{digest[:12]}.html"
    file_path.write_text(html, encoding="utf-8")

    conn.execute(
        """INSERT OR REPLACE INTO weekly_archives
           (week_start, url, sha256, file_path, applications_found)
           VALUES (?, ?, ?, ?, ?)""",
        (week_start_iso, url, digest, str(file_path), apps_found)
    )

def upsert_applications(conn: sqlite3.Connection, week_start_iso: str, weekly_url: str, rows: List[WeeklyRow]) -> int:
    changed = 0
    for r in rows:
        raw = {
            "source": "weekly_received",
            "week_start": week_start_iso,
            "weekly_url": weekly_url,
            "details_url": r.details_url,
            "ward": r.ward,
            "community": r.community,
        }
        cur = conn.execute(
            """INSERT OR IGNORE INTO applications
               (council, application_ref, address, proposal, raw_json)
               VALUES (?, ?, ?, ?, ?)""",
            (COUNCIL, r.application_ref, r.address, r.proposal, json.dumps(raw, ensure_ascii=False))
        )
        if cur.rowcount == 1:
            changed += 1
            continue

        cur2 = conn.execute(
            """UPDATE applications
               SET
                 address = CASE WHEN address IS NULL OR address = '' THEN ? ELSE address END,
                 proposal = CASE WHEN proposal IS NULL OR proposal = '' THEN ? ELSE proposal END
               WHERE council = ? AND application_ref = ?""",
            (r.address, r.proposal, COUNCIL, r.application_ref)
        )
        if cur2.rowcount == 1:
            changed += 1
    return changed

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--max-weeks", type=int, default=0, help="If >0, ingest only N weeks (testing)")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--start-week", type=str, default="", help="Optional override start week DD/MM/YYYY")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("❌ DB not found:", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    sess = get_session()

    try:
        today = date.today()

        if args.start_week:
            # parse DD/MM/YYYY manually
            parts = args.start_week.split("/")
            if len(parts) != 3:
                print("❌ --start-week must be DD/MM/YYYY")
                sys.exit(1)
            d = date(int(parts[2]), int(parts[1]), int(parts[0]))
            start = week_start_sunday(d)
        else:
            start = week_start_sunday(today - timedelta(days=365 * args.years))

        end = week_start_sunday(today)

        weeks = []
        cur = start
        while cur <= end:
            weeks.append(cur)
            cur += timedelta(weeks=1)

        if args.max_weeks and args.max_weeks > 0:
            weeks = weeks[:args.max_weeks]

        print("Weeks to attempt:", len(weeks))
        total_changed = 0
        weeks_done = 0
        weeks_skipped_405 = 0

        for w in weeks:
            week_iso = w.isoformat()
            if already_fetched(conn, week_iso):
                continue

            week_val = w.strftime("%d/%m/%Y")
            params = {"fa": FA_VALUE, "week": week_val}

            url, html, status = fetch(sess, params)

            if status == 405:
                weeks_skipped_405 += 1
                print(f"Week {week_iso} | HTTP 405 - skipped")
                continue

            rows = parse_week_rows(html)

            save_week(conn, week_iso, url, html, len(rows))
            changed = upsert_applications(conn, week_iso, url, rows)
            conn.commit()

            weeks_done += 1
            total_changed += changed
            print(f"Week {week_iso} | apps: {len(rows)} | inserted/updated: {changed}")

            time.sleep(args.sleep)

        print("\n✅ DONE")
        print("Weeks ingested:", weeks_done)
        print("Weeks skipped (405):", weeks_skipped_405)
        print("Applications inserted/updated:", total_changed)

        if weeks_done == 0:
            print("❌ No weeks were ingested. Check connectivity / parameters.")
            sys.exit(1)

    finally:
        conn.close()
        sess.close()

if __name__ == "__main__":
    main()
