import argparse
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

DB_PATH = Path("data/processed/planning.db")
RAW_DIR = Path("data/raw/weekly_archives")

COUNCIL = "Newcastle City Council"

BASE = "https://portal.newcastle.gov.uk/planning/"
WEEKLY_URL = urljoin(BASE, "index.html")
FA_VALUE = "getReceivedWeeklyList"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": WEEKLY_URL,
}

TIMEOUT = 45

@dataclass
class WeeklyRow:
    application_ref: str
    address: str
    proposal: str
    ward: str
    details_url: Optional[str]

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def parse_ddmmyyyy(s: str) -> Optional[date]:
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

def get_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    return sess

def fetch(sess: requests.Session, params: dict) -> Tuple[str, str]:
    # Try GET; if 405, retry POST (some deployments do this intermittently)
    r = sess.get(WEEKLY_URL, params=params, timeout=TIMEOUT)
    if r.status_code == 405:
        r = sess.post(WEEKLY_URL, data=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.url, r.text

def discover_week_select(sess: requests.Session) -> Tuple[str, List[str]]:
    # Load base weekly received page (no week selected)
    url, html = fetch(sess, {"fa": FA_VALUE})
    soup = BeautifulSoup(html, "lxml")

    # Find select whose name contains "week"
    sel = None
    for s in soup.find_all("select"):
        name = (s.get("name") or "").lower()
        if "week" in name:
            sel = s
            break

    if sel is None:
        raise SystemExit("❌ Could not find Week dropdown on weekly list page.")

    week_param = sel.get("name")
    options = []
    for opt in sel.find_all("option"):
        val = (opt.get("value") or "").strip()
        # Some sites use the displayed date as the value; keep both
        if not val:
            val = opt.get_text(strip=True)
        if parse_ddmmyyyy(val):
            options.append(val)

    options = list(dict.fromkeys(options))  # stable de-dupe
    if not options:
        raise SystemExit("❌ Week dropdown found but no date options detected.")

    return week_param, options

def find_results_table(soup: BeautifulSoup):
    # Identify the table by headers containing "Application" and "Proposal"
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if any("application" in h for h in headers) and any("proposal" in h for h in headers):
            return table
    return None

def parse_week_rows(html: str) -> List[WeeklyRow]:
    soup = BeautifulSoup(html, "lxml")
    table = find_results_table(soup)
    if table is None:
        return []

    rows: List[WeeklyRow] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        app_ref = tds[0].get_text(" ", strip=True)
        address = tds[1].get_text(" ", strip=True)
        proposal = tds[3].get_text(" ", strip=True)

        ward = ""
        if len(tds) >= 5:
            ward = tds[4].get_text(" ", strip=True)

        details_url = None
        # "View" link is usually in the last column
        a = tr.find("a")
        if a and a.get("href"):
            details_url = urljoin(BASE, a["href"])

        if app_ref and "/" in app_ref:
            rows.append(WeeklyRow(
                application_ref=app_ref,
                address=address,
                proposal=proposal,
                ward=ward,
                details_url=details_url
            ))

    # stable de-dupe by application_ref
    dedup = {}
    for r in rows:
        dedup[r.application_ref] = r
    return list(dedup.values())

def already_fetched(conn: sqlite3.Connection, week_start_iso: str) -> bool:
    cur = conn.execute("SELECT 1 FROM weekly_archives WHERE week_start = ?", (week_start_iso,))
    return cur.fetchone() is not None

def save_week(conn: sqlite3.Connection, week_start_iso: str, url: str, html: str, apps_found: int) -> str:
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
    return str(file_path)

def upsert_applications(conn: sqlite3.Connection, week_start_iso: str, weekly_url: str, rows: List[WeeklyRow]) -> int:
    changed = 0
    for r in rows:
        raw = {
            "source": "weekly_received",
            "week_start": week_start_iso,
            "weekly_url": weekly_url,
            "details_url": r.details_url,
            "ward": r.ward,
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
    ap.add_argument("--years", type=int, default=5, help="How many years back to ingest")
    ap.add_argument("--max-weeks", type=int, default=0, help="If >0, ingest only this many weeks (for testing)")
    ap.add_argument("--sleep", type=float, default=0.25, help="Sleep seconds between requests")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("❌ DB not found:", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    sess = get_session()

    try:
        week_param, options = discover_week_select(sess)
        print("Using week parameter name:", week_param)
        print("Weeks advertised on site:", len(options))

        cutoff = date.today() - timedelta(days=365 * args.years)

        parsed = []
        for w in options:
            d = parse_ddmmyyyy(w)
            if d and d >= cutoff:
                parsed.append((d, w))

        parsed.sort(key=lambda x: x[0])  # oldest -> newest
        if not parsed:
            print("❌ No weeks found within cutoff window.")
            sys.exit(1)

        if args.max_weeks and args.max_weeks > 0:
            parsed = parsed[:args.max_weeks]

        total_changed = 0
        weeks_done = 0

        for d, week_val in parsed:
            week_iso = d.isoformat()
            if already_fetched(conn, week_iso):
                continue

            params = {"fa": FA_VALUE, week_param: week_val}
            url, html = fetch(sess, params)
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
        print("Applications inserted/updated:", total_changed)

    finally:
        conn.close()
        sess.close()

if __name__ == "__main__":
    main()
