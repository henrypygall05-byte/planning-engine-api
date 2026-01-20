import argparse
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta, datetime
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
BACKOFFS = [1.0, 2.0, 4.0]  # deterministic

@dataclass
class WeeklyRow:
    application_ref: str
    address: str
    proposal: str
    ward: str
    community: str
    details_url: Optional[str]

def parse_ddmmyyyy(s: str) -> date:
    return datetime.strptime(s, "%d/%m/%Y").date()

def week_start_sunday(d: date) -> date:
    return d - timedelta(days=(d.weekday() + 1) % 7)

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def fetch_get(sess: requests.Session, week_val: str) -> requests.Response:
    return sess.get(URL, params={"fa": FA_VALUE, "week": week_val}, timeout=TIMEOUT, allow_redirects=True)

def fetch_form(sess: requests.Session) -> str:
    r = sess.get(URL, params={"fa": FA_VALUE}, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text

def extract_form_payload(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    week_input = soup.find("input", attrs={"name": "week"})
    payload = {"fa": FA_VALUE}
    if not week_input:
        return payload
    form = week_input.find_parent("form")
    if not form:
        return payload
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        itype = (inp.get("type") or "").lower()
        val = inp.get("value") or ""
        if itype == "hidden":
            payload[name] = val
    payload["fa"] = FA_VALUE
    return payload

def fetch_post(sess: requests.Session, week_val: str) -> requests.Response:
    html0 = fetch_form(sess)  # sets cookies + gives us any hidden fields
    payload = extract_form_payload(html0)
    payload["week"] = week_val
    return sess.post(URL, data=payload, timeout=TIMEOUT, allow_redirects=True)

def find_results_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if headers and "application" in headers[0] and any("proposal" in h for h in headers):
            return table
    return None

def parse_week_rows(html: str) -> List[WeeklyRow]:
    soup = BeautifulSoup(html, "lxml")
    table = find_results_table(soup)
    if table is None:
        return []

    out: List[WeeklyRow] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

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
            out.append(WeeklyRow(app_ref, address, proposal, ward, community, details_url))

    # stable de-dupe
    dedup = {}
    for r in out:
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

def fetch_week_resilient(sess: requests.Session, week_val: str) -> Tuple[str, str, int, str]:
    """
    Returns: (final_url, html, status, method_used)
    """
    # Attempt 1: GET
    r = fetch_get(sess, week_val)
    if r.status_code == 200:
        return r.url, r.text, r.status_code, "GET"

    # Attempt 2: POST
    r = fetch_post(sess, week_val)
    if r.status_code == 200:
        return r.url, r.text, r.status_code, "POST"

    # Retries with deterministic backoff, recreating session each time
    for b in BACKOFFS:
        time.sleep(b)
        sess2 = get_session()
        try:
            r = fetch_get(sess2, week_val)
            if r.status_code == 200:
                return r.url, r.text, r.status_code, f"GET-retry({b})"
            r = fetch_post(sess2, week_val)
            if r.status_code == 200:
                return r.url, r.text, r.status_code, f"POST-retry({b})"
        finally:
            sess2.close()

    return r.url, r.text, r.status_code, "FAILED"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=3)
    ap.add_argument("--max-weeks", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.6, help="Polite delay between successful weeks")
    ap.add_argument("--start-week", type=str, default="", help="DD/MM/YYYY override start")
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
            start = week_start_sunday(parse_ddmmyyyy(args.start_week))
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
        weeks_failed = 0

        for w in weeks:
            week_iso = w.isoformat()
            if already_fetched(conn, week_iso):
                continue

            week_val = w.strftime("%d/%m/%Y")
            url, html, status, method = fetch_week_resilient(sess, week_val)

            if status != 200:
                weeks_failed += 1
                print(f"Week {week_iso} | HTTP {status} | {method}")
                continue

            rows = parse_week_rows(html)
            save_week(conn, week_iso, url, html, len(rows))
            changed = upsert_applications(conn, week_iso, url, rows)
            conn.commit()

            weeks_done += 1
            total_changed += changed
            print(f"Week {week_iso} | {method} | apps: {len(rows)} | inserted/updated: {changed}")

            time.sleep(args.sleep)

        print("\n✅ DONE")
        print("Weeks ingested:", weeks_done)
        print("Weeks failed:", weeks_failed)
        print("Applications inserted/updated:", total_changed)

        if weeks_done == 0:
            print("❌ No weeks were ingested. Stopping.")
            sys.exit(1)

    finally:
        conn.close()
        sess.close()

if __name__ == "__main__":
    main()
