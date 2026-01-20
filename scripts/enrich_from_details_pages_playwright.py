import argparse
import json
import re
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

DB_PATH = Path("data/processed/planning.db")
COUNCIL = "Newcastle City Council"

DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")

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

def extract_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    pairs: Dict[str, str] = {}

    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 2:
            k = norm(cells[0].get_text(" ", strip=True)).lower()
            v = norm(cells[1].get_text(" ", strip=True))
            if k and v:
                pairs[k] = v

    for dl in soup.find_all("dl"):
        for dt_tag in dl.find_all("dt"):
            dd_tag = dt_tag.find_next_sibling("dd")
            if not dd_tag:
                continue
            k = norm(dt_tag.get_text(" ", strip=True)).lower()
            v = norm(dd_tag.get_text(" ", strip=True))
            if k and v:
                pairs[k] = v

    return pairs

def extract_fields(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    pairs = extract_pairs(soup)

    proposal = None
    decision = None
    date_received = None
    date_decided = None

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
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--recent-years", type=int, default=3)
    args = ap.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    cutoff = (date.today().replace(year=date.today().year - args.recent_years)).isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")

    rows = conn.execute(
        """
        SELECT id, application_ref, raw_json
        FROM applications
        WHERE council = ?
          AND raw_json LIKE '%"details_url"%'
          AND (decision IS NULL OR decision = '' OR date_decided IS NULL OR date_decided = '')
          AND (
               substr(json_extract(raw_json, '$.week_start'), 1, 10) >= ?
          )
        ORDER BY id ASC
        LIMIT ?
        """,
        (COUNCIL, cutoff, args.limit),
    ).fetchall()

    if not rows:
        print("✅ Nothing to enrich for this filter.")
        conn.close()
        return

    attempted = 0
    updated = 0
    failed = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Warm-up: visit weekly list once so WAF token/cookies get set
        page.goto("https://portal.newcastle.gov.uk/planning/index.html?fa=getReceivedWeeklyList", wait_until="domcontentloaded", timeout=60000)
        time.sleep(2.0)

        for (app_id, app_ref, raw_json) in rows:
            attempted += 1
            try:
                meta = json.loads(raw_json) if raw_json else {}
                url = meta.get("details_url")
                if not url:
                    failed += 1
                    continue

                page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # Give JS challenge time if it triggers; also allow auto reload.
                # We wait for either known content to appear, or just settle.
                try:
                    page.wait_for_timeout(1500)
                except PWTimeoutError:
                    pass

                html = page.content()

                # If we still got the AWS WAF challenge HTML, mark failed
                if "challenge.js" in html or "AwsWafIntegration" in html:
                    failed += 1
                    print(f"{app_ref} | WAF challenge still present")
                    continue

                proposal, decision, date_received, date_decided = extract_fields(html)
                decision_type = decision_type_from_decision(decision)

                if not any([proposal, decision, date_received, date_decided, decision_type]):
                    failed += 1
                    print(f"{app_ref} | 200-ish but no fields extracted")
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

                if updated % 10 == 0:
                    print(f"Updated {updated}/{attempted} ...")

                time.sleep(args.sleep)

            except Exception as e:
                failed += 1
                print(f"{app_ref} | ERROR | {e}")

        context.close()
        browser.close()

    conn.close()

    print("\n✅ DONE")
    print("Rows attempted:", attempted)
    print("Rows updated:", updated)
    print("Rows failed:", failed)

if __name__ == "__main__":
    main()
