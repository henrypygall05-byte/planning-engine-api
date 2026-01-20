import argparse
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

DB_PATH = Path("data/processed/planning.db")

COUNCIL = "Newcastle City Council"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TIMEOUT = 45

DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")

def norm(s: str) -> str:
    return " ".join((s or "").strip().split())

def parse_date(s: str) -> Optional[str]:
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

def extract_fields_from_details(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (proposal, decision, date_received, date_decided)
    """
    soup = BeautifulSoup(html, "lxml")

    # Many Idox pages have key/value tables. We'll search for labels.
    text_pairs = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all(["th", "td"])
        if len(tds) >= 2:
            k = norm(tds[0].get_text(" ", strip=True))
            v = norm(tds[1].get_text(" ", strip=True))
            if k and v:
                text_pairs.append((k.lower(), v))

    proposal = None
    decision = None
    date_received = None
    date_decided = None

    for k, v in text_pairs:
        if "proposal" in k or "description" in k:
            if not proposal:
                proposal = v
        if "decision" in k and "type" not in k:
            if not decision:
                decision = v
        if "received" in k:
            if not date_received:
                date_received = parse_date(v)
        if "decision issued" in k or "decision date" in k or "decided" in k:
            if not date_decided:
                date_decided = parse_date(v)

    return proposal, decision, date_received, date_decided

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=300, help="How many applications to enrich this run")
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    sess = requests.Session()
    sess.headers.update(HEADERS)

    try:
        # Only enrich rows that have a details_url and are missing decision/date fields
        rows = conn.execute(
            """
            SELECT id, application_ref, raw_json
            FROM applications
            WHERE council = ?
              AND raw_json LIKE '%"details_url"%'
              AND (decision IS NULL OR decision = '' OR date_decided IS NULL OR date_decided = '')
            ORDER BY id ASC
            LIMIT ?
            """,
            (COUNCIL, args.limit),
        ).fetchall()

        if not rows:
            print("✅ Nothing to enrich (all done for this filter).")
            return

        updated = 0
        failed = 0

        for (app_id, app_ref, raw_json) in rows:
            try:
                meta = json.loads(raw_json) if raw_json else {}
                url = meta.get("details_url")
                if not url:
                    continue

                r = sess.get(url, timeout=TIMEOUT, allow_redirects=True)
                if r.status_code != 200:
                    failed += 1
                    print(f"{app_ref} | HTTP {r.status_code} | {url}")
                    continue

                proposal, decision, date_received, date_decided = extract_fields_from_details(r.text)

                # If proposal missing in DB but found on page, fill it.
                # Decision fields: fill if found.
                decision_type = decision_type_from_decision(decision)

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
                    print(f"Updated {updated}/{len(rows)} ...")
                time.sleep(args.sleep)

            except Exception as e:
                failed += 1
                print(f"{app_ref} | ERROR | {e}")

        print("\n✅ DONE")
        print("Rows attempted:", len(rows))
        print("Rows updated:", updated)
        print("Rows failed:", failed)

    finally:
        conn.close()
        sess.close()

if __name__ == "__main__":
    main()
