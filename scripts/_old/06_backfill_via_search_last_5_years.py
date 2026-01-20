import os
import re
import sqlite3
import datetime as dt
from dataclasses import dataclass
from typing import List, Tuple, Optional

from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

DB_PATH = os.getenv("DB_PATH", "./db/newcastle_planning.sqlite")

SEARCH_URL = "https://portal.newcastle.gov.uk/planning/index.html?fa=search"
APP_URL_FMT = "https://portal.newcastle.gov.uk/planning/index.html?fa=getApplication&id={idv}"

REF_RE = re.compile(r"\b(19|20)\d{2}/\d{1,6}\b")

@dataclass
class Row:
    ref: str
    idv: str
    url: str

def db_connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS applications (
      application_ref TEXT PRIMARY KEY,
      url TEXT,
      source TEXT,
      first_seen_utc TEXT
    )
    """)
    con.commit()
    return con

def upsert(con: sqlite3.Connection, row: Row):
    cur = con.cursor()
    cur.execute("""
      INSERT INTO applications(application_ref, url, source, first_seen_utc)
      VALUES(?, ?, ?, ?)
      ON CONFLICT(application_ref) DO UPDATE SET
        url=COALESCE(excluded.url, applications.url),
        source=excluded.source
    """, (row.ref, row.url, "search", dt.datetime.utcnow().isoformat()))
    con.commit()

def parse_results(page) -> List[Row]:
    """
    Newcastle search results include links like:
      ...index.html?fa=getApplication&id=324289
    We extract id + ref from each row.
    """
    rows: List[Row] = []

    # Grab ALL links on page that look like getApplication&id=...
    links = page.locator("a[href*='fa=getApplication'][href*='id=']")
    n = links.count()
    for i in range(n):
        href = links.nth(i).get_attribute("href") or ""
        m = re.search(r"[?&]id=(\d+)", href)
        if not m:
            continue
        idv = m.group(1)

        # Try to find ref text in the same row area
        # (results are usually in a table, so walk up to nearest <tr>)
        el = links.nth(i)
        tr = el.locator("xpath=ancestor::tr[1]")
        blob = (tr.inner_text() if tr.count() else el.inner_text()) or ""
        refm = REF_RE.search(blob)
        if not refm:
            # Sometimes ref appears near link text itself
            blob2 = (el.inner_text() or "") + " " + (el.locator("xpath=..").inner_text() or "")
            refm = REF_RE.search(blob2)
        if not refm:
            continue

        ref = refm.group(0)
        url = APP_URL_FMT.format(idv=idv)
        rows.append(Row(ref=ref, idv=idv, url=url))

    # De-dup by ref
    dedup = {}
    for r in rows:
        dedup[r.ref] = r
    return list(dedup.values())

def set_dates_and_submit(page, start_date: dt.date, end_date: dt.date):
    """
    Newcastle search form uses dd/mm/yyyy in many deployments.
    We try dd/mm/yyyy first; if results look empty, we still proceed to parse links.
    """
    s = start_date.strftime("%d/%m/%Y")
    e = end_date.strftime("%d/%m/%Y")

    # Fill common received date fields
    # (names vary, but your earlier diagnostics showed these)
    def try_fill(name: str, value: str):
        loc = page.locator(f"input[name='{name}'], input[id='{name}']")
        if loc.count():
            loc.first.fill(value)

    try_fill("received_date_from", s)
    try_fill("received_date_to", e)
    try_fill("valid_date_from", s)
    try_fill("valid_date_to", e)

    # Also try application type blank (all) so no restrictions
    # Click submit
    btn = page.locator("button[type='submit'], input[type='submit']")
    if btn.count():
        btn.first.click()
    else:
        # Some portals use a specific Search button
        page.get_by_role("button", name=re.compile("search", re.I)).click()

    page.wait_for_timeout(2500)

def paginate_and_collect(page) -> List[Row]:
    all_rows: List[Row] = []
    seen_pages = 0

    while True:
        rows = parse_results(page)
        all_rows.extend(rows)

        # Look for a Next link/button
        next_btn = page.locator("a:has-text('Next'), button:has-text('Next')")
        if next_btn.count() == 0:
            break

        # Stop if disabled
        if "disabled" in (next_btn.first.get_attribute("class") or "").lower():
            break

        next_btn.first.click()
        page.wait_for_timeout(2000)

        seen_pages += 1
        if seen_pages > 50:
            break

    # De-dup by ref across pages
    dedup = {}
    for r in all_rows:
        dedup[r.ref] = r
    return list(dedup.values())

def main():
    con = db_connect()

    today = dt.date.today()
    cutoff = today - dt.timedelta(days=365*5)

    # Chunk by 30 days to keep result sets reasonable
    chunk = dt.timedelta(days=30)
    start = cutoff
    total_rows = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        while start <= today:
            end = min(start + chunk, today)
            print(f"[CHUNK {start} -> {end}]")

            page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=120000)
            set_dates_and_submit(page, start, end)

            rows = paginate_and_collect(page)
            print(f"  found={len(rows)}")

            for r in rows:
                upsert(con, r)
            total_rows += len(rows)

            start = end + dt.timedelta(days=1)

        browser.close()

    # Final stats
    cur = con.cursor()
    cur.execute("select count(*) from applications")
    total = cur.fetchone()[0]
    cur.execute("select count(*) from applications where url is not null and url != ''")
    with_url = cur.fetchone()[0]
    con.close()

    print("\nDONE")
    print("total_rows_ingested (chunk sum):", total_rows)
    print("unique apps in DB:", total)
    print("apps with URL:", with_url)
    print("\nNext:")
    print("  python scripts/03_build_similarity_index.py")

if __name__ == "__main__":
    main()
