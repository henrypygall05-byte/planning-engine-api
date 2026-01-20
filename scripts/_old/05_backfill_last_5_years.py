import os, re, sqlite3, time
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./db/newcastle_planning.sqlite")
UA = os.getenv("USER_AGENT", "PlanaAI-Pilot/0.1")
BASE_WEEKLY_URL = os.getenv("NEWCASTLE_WEEKLY_RECEIVED_URL", "").strip()

if not BASE_WEEKLY_URL:
    raise SystemExit("Missing NEWCASTLE_WEEKLY_RECEIVED_URL in .env")

PORTAL_BASE = "https://portal.newcastle.gov.uk"

def upsert(ref, url, source):
    now = datetime.now(timezone.utc).isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    INSERT INTO applications(application_ref,url,source,last_seen_utc)
    VALUES(?,?,?,?)
    ON CONFLICT(application_ref) DO UPDATE SET
        url=COALESCE(excluded.url, applications.url),
        source=excluded.source,
        last_seen_utc=excluded.last_seen_utc
    """, (ref, url, source, now))
    con.commit()
    con.close()

def extract_from_table(page):
    rows = page.query_selector_all("table tr")
    items = []
    for r in rows:
        cells = r.query_selector_all("td")
        if len(cells) < 2:
            continue
        text_cells = [c.inner_text().strip() for c in cells]
        joined = " | ".join(text_cells)

        m = re.search(r"\b\d{4}[/\-]\d+\b|\b[A-Z]{1,6}\d{2,}\b", joined)
        if not m:
            continue
        ref = m.group(0)

        a = r.query_selector("a")
        href = None
        if a:
            href = a.get_attribute("href")
            if href and href.startswith("/"):
                href = PORTAL_BASE + href
            elif href and href.startswith("index.html"):
                href = PORTAL_BASE + "/planning/" + href

        items.append((ref, href))

    # dedupe
    out, seen = [], set()
    for ref, url in items:
        if ref in seen:
            continue
        seen.add(ref)
        out.append((ref, url))
    return out

def safe_goto(page, url, attempts=3):
    # Long timeouts + retries
    for i in range(1, attempts + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=180000)  # 3 minutes
            # give JS time
            page.wait_for_timeout(2500)
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            return True
        except PWTimeoutError:
            print(f"  ⚠️ goto timeout (attempt {i}/{attempts})")
            # back off a bit then retry
            page.wait_for_timeout(2000 * i)
        except Exception as e:
            print(f"  ⚠️ goto error (attempt {i}/{attempts}): {e}")
            page.wait_for_timeout(2000 * i)
    return False

def set_week_range_if_possible(page, start_date, end_date):
    """
    Best-effort: if date inputs exist, fill them and submit.
    If not, the page may be fixed-week; backfill won't work from this screen alone.
    """
    cand_from = [
        'input[name="dateReceivedFrom"]',
        'input[name="receivedDateFrom"]',
        'input[id*="dateReceivedFrom"]',
        'input[id*="receivedDateFrom"]',
    ]
    cand_to = [
        'input[name="dateReceivedTo"]',
        'input[name="receivedDateTo"]',
        'input[id*="dateReceivedTo"]',
        'input[id*="receivedDateTo"]',
    ]

    def first(sel_list):
        for sel in sel_list:
            el = page.query_selector(sel)
            if el:
                return el
        return None

    f = first(cand_from)
    t = first(cand_to)

    if not (f and t):
        return False

    # fill
    f.click(); f.fill(""); f.type(start_date, delay=15)
    t.click(); t.fill(""); t.type(end_date, delay=15)

    # submit
    for btn_sel in [
        'button:has-text("Search")',
        'input[value*="Search"]',
        'button[type="submit"]',
        'input[type="submit"]'
    ]:
        b = page.query_selector(btn_sel)
        if b:
            b.click()
            break

    # wait for update
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass
    page.wait_for_timeout(1500)
    return True

def main():
    end = datetime.now().date()
    start = (datetime.now() - timedelta(days=365*5)).date()

    cursor = end
    weeks_done = 0
    total_rows_seen = 0
    total_weeks_skipped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA)
        page = context.new_page()

        # Set generous defaults
        page.set_default_timeout(60000)
        page.set_default_navigation_timeout(180000)

        while cursor >= start:
            week_end = cursor
            week_start = max(start, cursor - timedelta(days=6))

            label = f"{week_start} -> {week_end}"
            print(f"\n[{label}]")

            ok = safe_goto(page, BASE_WEEKLY_URL, attempts=3)
            if not ok:
                print(f"  ❌ Skipping week (site not responding): {label}")
                total_weeks_skipped += 1
                cursor = week_start - timedelta(days=1)
                time.sleep(2.0)
                continue

            # Try to change dates (may not exist)
            set_week_range_if_possible(
                page,
                week_start.strftime("%d/%m/%Y"),
                week_end.strftime("%d/%m/%Y"),
            )

            # Wait for any table
            try:
                page.wait_for_selector("table", timeout=30000)
            except Exception:
                print("  ⚠️ No table found, skipping.")
                total_weeks_skipped += 1
                cursor = week_start - timedelta(days=1)
                time.sleep(1.5)
                continue

            items = extract_from_table(page)
            print(f"  rows_seen={len(items)}")
            total_rows_seen += len(items)

            for ref, url in items:
                upsert(ref, url, source=f"weekly_backfill_{week_start}_{week_end}")

            weeks_done += 1

            # polite pacing to reduce throttling
            time.sleep(1.2)

            cursor = week_start - timedelta(days=1)

        browser.close()

    print("\nDONE")
    print(f"weeks_done={weeks_done}  weeks_skipped={total_weeks_skipped}  rows_seen_total={total_rows_seen}")
    print("Now rebuild index:")
    print("  python scripts/03_build_similarity_index.py")

if __name__ == "__main__":
    main()
