import sqlite3, time, re
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

DB="./db/newcastle_planning.sqlite"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

def ensure_cols():
    con=sqlite3.connect(DB)
    cur=con.cursor()
    # add columns if they don't exist
    cols=[r[1] for r in cur.execute("PRAGMA table_info(applications)").fetchall()]
    def add(name, typ):
        if name not in cols:
            cur.execute(f"ALTER TABLE applications ADD COLUMN {name} {typ}")
    add("site_address","TEXT")
    add("proposal","TEXT")
    add("status","TEXT")
    add("decision","TEXT")
    add("received_date","TEXT")
    add("validated_date","TEXT")
    add("case_officer","TEXT")
    add("ward","TEXT")
    add("parish","TEXT")
    add("enriched_utc","TEXT")
    con.commit(); con.close()

def pick_label_value(page, label_patterns):
    # Idox pages usually show label/value pairs in tables or definition lists
    text = page.inner_text("body") or ""
    for pat in label_patterns:
        m=re.search(pat+r"\s*[:\-]?\s*(.+)", text, re.IGNORECASE)
        if m:
            val=m.group(1).strip()
            val=re.split(r"\s{2,}|\n", val)[0].strip()
            if 0 < len(val) < 400:
                return val
    return None

def main(limit=None, sleep=0.4):
    ensure_cols()
    con=sqlite3.connect(DB)
    cur=con.cursor()
    # only enrich rows that have a URL and aren't enriched yet
    q="""
    SELECT application_ref, url
    FROM applications
    WHERE url IS NOT NULL AND url != ''
      AND (enriched_utc IS NULL OR enriched_utc = '')
    """
    rows=cur.execute(q).fetchall()
    con.close()

    if limit:
        rows=rows[:limit]

    print("To enrich:", len(rows))

    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        ctx=browser.new_context(user_agent=UA)
        page=ctx.new_page()
        page.set_default_timeout(60000)
        page.set_default_navigation_timeout(180000)

        con=sqlite3.connect(DB)
        cur=con.cursor()

        for i,(ref,url) in enumerate(rows, start=1):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=180000)
                page.wait_for_timeout(1200)
            except Exception as e:
                print(f"[{i}/{len(rows)}] {ref} FAIL goto: {e}")
                continue

            # best-effort extraction from page text
            site_address = pick_label_value(page, [r"Site\s*Address", r"Address"])
            proposal     = pick_label_value(page, [r"Proposal", r"Description"])
            status       = pick_label_value(page, [r"Status"])
            decision     = pick_label_value(page, [r"Decision"])
            received     = pick_label_value(page, [r"Received\s*Date", r"Date\s*Received"])
            validated    = pick_label_value(page, [r"Validated\s*Date", r"Date\s*Validated"])
            officer      = pick_label_value(page, [r"Case\s*Officer", r"Officer"])
            ward         = pick_label_value(page, [r"Ward"])
            parish       = pick_label_value(page, [r"Parish"])

            enriched = datetime.now(timezone.utc).isoformat()

            cur.execute("""
            UPDATE applications
            SET site_address=?,
                proposal=?,
                status=?,
                decision=?,
                received_date=?,
                validated_date=?,
                case_officer=?,
                ward=?,
                parish=?,
                enriched_utc=?
            WHERE application_ref=?
            """, (site_address, proposal, status, decision, received, validated, officer, ward, parish, enriched, ref))

            if i % 25 == 0:
                con.commit()
                print(f"[{i}/{len(rows)}] enriched...")

            time.sleep(sleep)

        con.commit()
        con.close()
        browser.close()

    print("DONE enrichment.")

if __name__ == "__main__":
    main()
