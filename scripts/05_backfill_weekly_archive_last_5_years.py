import os, re, sqlite3, time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(dotenv_path=".env")

DB_PATH = os.getenv("DB_PATH")
if not DB_PATH:
    raise SystemExit("Missing DB_PATH in .env")

BASE = "https://portal.newcastle.gov.uk/planning/index.html"
UA = os.getenv("USER_AGENT", "Mozilla/5.0")

REF_RE = re.compile(r"\b\d{4}/\d+\b")
ID_RE  = re.compile(r"(?:\bid=|getApplication\D+)(\d{5,})")

os.makedirs("data/raw", exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

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

def find_application_id_in_row(row):
    for a in row.query_selector_all("a"):
        href = (a.get_attribute("href") or "")
        onclick = (a.get_attribute("onclick") or "")
        blob = " ".join([href, onclick])
        m = ID_RE.search(blob)
        if m:
            return m.group(1)

    onclick = (row.get_attribute("onclick") or "")
    m = ID_RE.search(onclick)
    if m:
        return m.group(1)

    html = row.inner_html() or ""
    m = ID_RE.search(html)
    if m:
        return m.group(1)

    return None

def extract_rows(page):
    items = []
    rows = page.query_selector_all("table tr")
    for r in rows:
        tds = r.query_selector_all("td")
        if len(tds) < 2:
            continue
        row_text = " | ".join([(td.inner_text() or "").strip() for td in tds])
        mref = REF_RE.search(row_text)
        if not mref:
            continue
        ref = mref.group(0)

        app_id = find_application_id_in_row(r)
        url = f"{BASE}?fa=getApplication&id={app_id}" if app_id else None
        items.append((ref, url))

    seen=set(); out=[]
    for ref,url in items:
        if ref in seen:
            continue
        seen.add(ref)
        out.append((ref,url))
    return out

def main():
    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=365*5)

    print(">>> USING DB_PATH:", DB_PATH)
    print("Backfill cutoff date:", cutoff)

    weeks_done=0
    rows_ingested=0
    urls_found=0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA)
        page = ctx.new_page()
        page.set_default_timeout(60000)
        page.set_default_navigation_timeout(180000)

        cur = today
        while cur >= cutoff:
            weekly_url = f"{BASE}?fa=getReceivedWeeklyList&weekEnding={cur.strftime('%Y-%m-%d')}"
            try:
                page.goto(weekly_url, wait_until="domcontentloaded", timeout=180000)
                page.wait_for_timeout(1200)
            except Exception as e:
                print(f"[WEEK {cur}] fetch failed: {e}")
                cur -= timedelta(days=7)
                continue

            items = extract_rows(page)
            found = len(items)
            found_urls = sum(1 for _,u in items if u)
            print(f"[WEEK {cur}] refs_found={found}  urls_found={found_urls}")

            if weeks_done < 2:
                open(f"data/raw/weekly_debug_{cur}.html","w",encoding="utf-8").write(page.content())

            for ref, url in items:
                upsert(ref, url, source=f"weekly_{cur}")

            weeks_done += 1
            rows_ingested += found
            urls_found += found_urls

            cur -= timedelta(days=7)
            time.sleep(0.2)

        browser.close()

    print("\nDONE")
    print("weeks_done =", weeks_done)
    print("rows_ingested_total =", rows_ingested)
    print("urls_found_total =", urls_found)
    print("Next: python scripts/07_enrich_application_details.py")
    print("Then: python scripts/03_build_similarity_index.py")

if __name__ == "__main__":
    main()
