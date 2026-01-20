import hashlib
import json
import sqlite3
import sys
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
WEEKLY_URL = urljoin(BASE, "index.html")
FA_VALUE = "getReceivedWeeklyList"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TIMEOUT = 45

@dataclass
class WeeklyRow:
    application_ref: str
    address: str
    proposal: str
    details_url: Optional[str]

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def week_start_sunday(d: date) -> date:
    # Weekly lists commonly anchor on Sunday in many Idox-style systems.
    # Deterministic: always compute the previous Sunday (or same day if Sunday).
    return d - timedelta(days=(d.weekday() + 1) % 7)

def fetch_weekly_page(week_start: date, week_param_name: str) -> Tuple[str, str]:
    week_str = week_start.strftime("%d/%m/%Y")
    params = {"fa": FA_VALUE, week_param_name: week_str}
    r = requests.get(WEEKLY_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.url, r.text

def discover_week_param_name() -> str:
    # Load the default page and find the "Week" input/select name.
    r = requests.get(WEEKLY_URL, params={"fa": FA_VALUE}, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Try common patterns: select/input near label "Week"
    # 1) any select element
    for sel in soup.find_all("select"):
        name = sel.get("name")
        if name and "week" in name.lower():
            return name

    # 2) any input element
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if name and "week" in name.lower():
            return name

    # 3) deterministic fallback used by some installs
    return "week"

def parse_rows(html: str) -> List[WeeklyRow]:
    soup = BeautifulSoup(html, "lxml")

    rows: List[WeeklyRow] = []

    # Heuristic: application refs look like "2025/1730/01/HOU"
    # We'll scan all links and nearby text blocks.
    text = soup.get_text(" ", strip=True)

    # The visible listing in this system is often not a classic <table>,
    # so we parse by walking through the page and collecting "View" links.
    for a in soup.find_all("a"):
        if a.get_text(" ", strip=True).lower() != "view":
            continue
        href = a.get("href")
        details_url = urljoin(BASE, href) if href else None

        # The row text is typically just before the "View" link in the DOM.
        container = a.parent
        chunk = container.get_text(" ", strip=True) if container else a.get_text(" ", strip=True)

        # Try broader: include previous siblings text
        prev_text = []
        node = a
        # collect a few previous strings deterministically
        for _ in range(6):
            node = node.previous_sibling
            if node is None:
                break
            if isinstance(node, str):
                t = node.strip()
                if t:
                    prev_text.append(t)
            else:
                t = node.get_text(" ", strip=True)
                if t:
                    prev_text.append(t)
        blob = " ".join(reversed(prev_text)) + " " + chunk
        blob = " ".join(blob.split())

        # Very simple extraction: first token with slashes is app ref
        tokens = blob.split()
        app_ref = next((t for t in tokens if t.count("/") >= 3), None)
        if not app_ref:
            continue

        # Address is usually between app_ref and proposal; proposal tends to be longer.
        # We split on the app_ref, then take next ~15 tokens as address until we hit two spaces?
        after = blob.split(app_ref, 1)[1].strip()
        # We attempt to split address vs proposal by finding double-space patterns in original HTML,
        # but we only have normalized text. Use a conservative heuristic:
        # take first comma-separated segment(s) as address until we see a ward-like token.
        # Better enrichment comes later; this is "good enough" for ingestion.
        parts = after.split("  ")
        # fallback: single space; take first 12 tokens as address then rest as proposal
        words = after.split()
        address = " ".join(words[:12]).strip()
        proposal = " ".join(words[12:]).strip()

        rows.append(WeeklyRow(
            application_ref=app_ref.strip(),
            address=address,
            proposal=proposal,
            details_url=details_url
        ))

    # Deduplicate by application_ref
    dedup = {}
    for r in rows:
        dedup[r.application_ref] = r
    return list(dedup.values())

def already_fetched(conn: sqlite3.Connection, week_start: date) -> bool:
    cur = conn.execute("SELECT 1 FROM weekly_archives WHERE week_start = ?", (week_start.isoformat(),))
    return cur.fetchone() is not None

def save_week(conn: sqlite3.Connection, week_start: date, url: str, html: str, apps: List[WeeklyRow]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    week_key = week_start.isoformat()
    digest = sha256_text(html)
    file_path = RAW_DIR / f"weekly_received_{week_key}_{digest[:12]}.html"
    file_path.write_text(html, encoding="utf-8")

    conn.execute(
        """INSERT OR REPLACE INTO weekly_archives
           (week_start, url, sha256, file_path, applications_found)
           VALUES (?, ?, ?, ?, ?)""",
        (week_key, url, digest, str(file_path), len(apps))
    )

def upsert_applications(conn: sqlite3.Connection, week_start: date, url: str, apps: List[WeeklyRow]) -> int:
    inserted_or_updated = 0
    for r in apps:
        raw = {
            "source": "weekly_received",
            "week_start": week_start.isoformat(),
            "weekly_url": url,
            "details_url": r.details_url,
        }
        # Insert if new; if existing, fill blanks deterministically.
        cur = conn.execute(
            """INSERT OR IGNORE INTO applications
               (council, application_ref, address, proposal, raw_json)
               VALUES (?, ?, ?, ?, ?)""",
            (COUNCIL, r.application_ref, r.address, r.proposal, json.dumps(raw, ensure_ascii=False))
        )
        if cur.rowcount == 1:
            inserted_or_updated += 1
            continue

        # Update only if current fields are NULL/empty
        cur2 = conn.execute(
            """UPDATE applications
               SET
                 address = CASE WHEN address IS NULL OR address = '' THEN ? ELSE address END,
                 proposal = CASE WHEN proposal IS NULL OR proposal = '' THEN ? ELSE proposal END
               WHERE council = ? AND application_ref = ?""",
            (r.address, r.proposal, COUNCIL, r.application_ref)
        )
        if cur2.rowcount == 1:
            inserted_or_updated += 1

    return inserted_or_updated

def main():
    if not DB_PATH.exists():
        print("❌ DB not found:", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    try:
        week_param = discover_week_param_name()
        print("Using week parameter name:", week_param)

        today = date.today()
        start = today - timedelta(weeks=260)  # ~5 years
        current = week_start_sunday(start)
        end = week_start_sunday(today)

        total_new_apps = 0
        weeks_fetched = 0

        while current <= end:
            if already_fetched(conn, current):
                current += timedelta(weeks=1)
                continue

            print("Fetching week:", current.isoformat())
            url, html = fetch_weekly_page(current, week_param)
            apps = parse_rows(html)

            save_week(conn, current, url, html, apps)
            changed = upsert_applications(conn, current, url, apps)

            conn.commit()

            weeks_fetched += 1
            total_new_apps += changed
            print(f"  applications found: {len(apps)} | inserted/updated: {changed}")

            current += timedelta(weeks=1)

        print("\n✅ DONE")
        print("Weeks fetched:", weeks_fetched)
        print("Applications inserted/updated:", total_new_apps)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
