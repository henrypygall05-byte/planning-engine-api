import os, re, sqlite3, time, hashlib, json
from datetime import datetime, timezone

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./db/newcastle_planning.sqlite")
UA = os.getenv("USER_AGENT", "PlanaAI-Pilot/0.1")
WEEKLY_URL = os.getenv("NEWCASTLE_WEEKLY_RECEIVED_URL")

if not WEEKLY_URL:
    raise SystemExit("Missing NEWCASTLE_WEEKLY_RECEIVED_URL in .env")

RAW_DIR = "./data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

def upsert(ref, url):
    now = datetime.now(timezone.utc).isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    INSERT INTO applications(application_ref,url,source,last_seen_utc)
    VALUES(?,?,?,?)
    ON CONFLICT(application_ref) DO UPDATE SET
        url=excluded.url,
        source=excluded.source,
        last_seen_utc=excluded.last_seen_utc
    """, (ref, url, "weekly_received_idox", now))
    con.commit()
    con.close()

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA)
        page = context.new_page()
        page.goto(WEEKLY_URL, wait_until="networkidle", timeout=90000)

        # Wait for Idox data grid
        page.wait_for_selector("table", timeout=30000)

        # Newcastle uses an AJAX-loaded Idox table; grab its JSON state
        html = page.content()

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        snap = f"./data/raw/weekly_received_idox_{ts}.html"
        with open(snap, "w", encoding="utf-8") as f:
            f.write(html)

        # Pull the data rows directly from the table
        rows = page.query_selector_all("table tr")

        items = []
        for r in rows:
            cells = r.query_selector_all("td")
            if len(cells) < 3:
                continue

            text_cells = [c.inner_text().strip() for c in cells]
            joined = " | ".join(text_cells)

            # Look for an application reference pattern
            m = re.search(r"\b\d{4}[/\-]\d+\b|\b[A-Z]{1,6}\d{2,}\b", joined)
            if not m:
                continue

            ref = m.group(0)

            # Look for clickable link in the row
            a = r.query_selector("a")
            if a:
                href = a.get_attribute("href")
                if href and href.startswith("/"):
                    href = "https://portal.newcastle.gov.uk" + href
            else:
                href = None

            items.append((ref, href))

        browser.close()

    print(f"Snapshot saved: {snap}")
    print(f"Found {len(items)} applications in Idox table")

    if not items:
        print("❌ Something changed in Newcastle's portal. We'll handle that next.")
        return

    for ref, url in items:
        upsert(ref, url)

    print("✅ Weekly list successfully ingested")

if __name__ == "__main__":
    main()
