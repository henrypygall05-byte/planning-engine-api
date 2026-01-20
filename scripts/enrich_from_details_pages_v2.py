import argparse
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

DB_PATH = Path("data/processed/planning.db")
COUNCIL = "Newcastle City Council"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://portal.newcastle.gov.uk/planning/index.html",
}

TIMEOUT = 45
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")

# deterministic backoff for 202 "not ready yet"
BACKOFFS = [1.0, 2.0, 4.0, 8.0]

def norm(s: str) -> str:
    return " ".join((s or "").strip().split())

def parse_date_any(s: str) -> Optional[str]:
    s = norm(s)
    m = DATE_RE.search(s)
    if not m:
        return None
    try:
        d = datetime.strptime(m.group(1), "%d/%m/%Y").date()
        return d.isoformat()
    except ValueError:
        return None

def decision_type_from_decision(decision: Optional[str]) -> Optional[str]:
    if not decision:
        return None
    d = decision.lower()
    if "approve" in d or "granted" in d or "permit" in d:
        return "approved"
    if "refus" in d or "rejected" in d:
        return "refused"
    if "withdraw" in d:
        return "withdrawn"
    if "declin" in d:
        return "declined"
    if "prior approval not required" in d:
        return "prior_approval_not_required"
    if "prior approval required" in d:
        return "prior_approval_required"
    return None

def fetch_details_with_retry(sess: requests.Session, url: str) -> Tuple[int, str]:
    """
    Returns (status, html). Retries if server returns 202.
    """
    r = sess.get(url, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code != 202:
        return r.status_code, r.text

    # Retry on 202 with deterministic backoff
    for b in BACKOFFS:
        time.sleep(b)
        r2 = sess.get(url, timeout=TIMEOUT, allow_redirects=True)
        if r2.status_code != 202:
            return r2.status_code, r2.text

    return r.status_code, r.text

def extract_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Collect label/value pairs from common structures: tables and dt/dd lists.
    """
    pairs: Dict[str, str] = {}

    # Table rows
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 2:
            k = norm(cells[0].get_text(" ", strip=True)).lower()
            v = norm(cells[1].get_text(" ", strip=True))
            if k and v:
                pairs[k] = v

    # Definition lists
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        for dt_tag in dts:
            dd_tag = dt_tag.find_next_sibling("dd")
            if not dd_tag:
                continue
            k = norm(dt_tag.get_text(" ", strip=True)).lower()
            v = norm(dd_tag.get_text(" ", strip=True))
            if k and v:
                pairs[k] = v

    return pairs

def extract_fields_from_details(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    pairs = extract_pairs(soup)

    proposal = None
    decision = None
    date_received = None
    date_decided = None

    # Label matching (flexible)
    for k, v in pairs.items():
        if proposal is None and ("proposal" in k or "description" in k):
            proposal = v

        if decision is None and ("decision" in k and "type" not in k):
            decision = v

        if date_received is None and ("received" in k or "valid" in k):
            d = parse_date_any(v)
            if d:
                date_received = d

        if date_decided is None and ("decision issued" in k or "decision date" in k or "decided" in k):
            d = parse_date_any(v)
            if d:
                date_decided = d

    return proposal, decision, date_received, date_decided

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--recent-years", type=int, default=3, help="Only enrich applications from last N years (by week_start in raw_json)")
    args = ap.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    sess = requests.Session()
    sess.headers.update(HEADERS)

    try:
        # Only last N years (based on recorded week_start in raw_json)
        cutoff = (date.today().replace(year=date.today().year - args.recent_years)).isoformat()

        rows = conn.execute(
            """
            SELECT id, application_ref, raw_json
            FROM applications
            WHERE council = ?
              AND raw_json LIKE '%"details_url"%'
              AND raw_json LIKE '%"week_start"%'
              AND (decision IS NULL OR decision = '' OR date_decided IS NULL OR date_decided = '')
              AND (
                   substr(
                     json_extract(raw_json, '$.week_start'), 1, 10
                   ) >= ?
              )
            ORDER BY id ASC
            LIMIT ?
            """,
            (COUNCIL, cutoff, args.limit),
        ).fetchall()

        if not rows:
            print("✅ Nothing to enrich (recent filter produced no rows).")
            return

        attempted = 0
        updated = 0
        failed = 0
        seen_202 = 0

        for (app_id, app_ref, raw_json) in rows:
            attempted += 1
            try:
                meta = json.loads(raw_json) if raw_json else {}
                url = meta.get("details_url")
                if not url:
                    continue

                status, html = fetch_details_with_retry(sess, url)
                if status == 202:
                    seen_202 += 1
                    failed += 1
                    print(f"{app_ref} | HTTP 202 persisted | {url}")
                    continue

                if status != 200:
                    failed += 1
                    print(f"{app_ref} | HTTP {status} | {url}")
                    continue

                proposal, decision, date_received, date_decided = extract_fields_from_details(html)
                decision_type = decision_type_from_decision(decision)

                # Only count as updated if we found *something* to write
                if not any([proposal, decision, decision_type, date_received, date_decided]):
                    failed += 1
                    print(f"{app_ref} | 200 but no fields extracted | {url}")
                    continue

                conn.execute(
                    """
                    UPDATE applications
                    SET
                      proposal = CASE WHEN proposal IS NULL OR proposal = '' THEN COALESCE(?, proposal) ELSE proposal END,
                      decision = COALESCE(?, decision),
                      decision_type = COALESCE(?, decision_type),
                      date_received = COALESCE(?, date_received),
                      date_decided = COALESCE(?, date_decided)
                    WHERE id = ?
                    """,
                    (proposal, decision, decision_type, date_received, date_decided, app_id),
                )
                conn.commit()
                updated += 1

                if updated % 25 == 0:
                    print(f"Updated {updated}/{attempted} ...")

                time.sleep(args.sleep)

            except Exception as e:
                failed += 1
                print(f"{app_ref} | ERROR | {e}")

        print("\n✅ DONE")
        print("Rows attempted:", attempted)
        print("Rows updated:", updated)
        print("Rows failed:", failed)
        print("Rows stuck at 202:", seen_202)

    finally:
        conn.close()
        sess.close()

if __name__ == "__main__":
    main()
