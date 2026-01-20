import argparse, re, sqlite3, time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

DB_DEFAULT = "./db/newcastle_planning.sqlite"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LABEL_MAP = {
    "decision": ["decision"],
    "decision_type": ["decision type", "decisiontype"],
    "decision_date": ["decision date", "date of decision"],
    "status": ["status", "current status"],
}

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def extract_kv_pairs(soup: BeautifulSoup) -> dict:
    """
    Try multiple common portal layouts:
    - table rows with th/td
    - definition lists dt/dd
    - generic label/value blocks
    Returns dict of {label_lower: value_text}.
    """
    kv = {}

    # tables: th/td or td/td
    for tr in soup.select("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
        if len(cells) >= 2:
            k = norm(cells[0]).rstrip(":")
            v = cells[1].strip()
            if k and v and k not in kv:
                kv[k] = v

    # dl: dt/dd
    for dt in soup.select("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            k = norm(dt.get_text(" ", strip=True)).rstrip(":")
            v = dd.get_text(" ", strip=True)
            if k and v and k not in kv:
                kv[k] = v

    # fallback: scan text for "Label: Value" patterns
    text = soup.get_text("\n", strip=True)
    for line in text.splitlines():
        if ":" in line:
            left, right = line.split(":", 1)
            k = norm(left).rstrip(":")
            v = right.strip()
            if k and v and k not in kv:
                kv[k] = v

    return kv

def pick_field(kv: dict, field: str) -> str | None:
    targets = LABEL_MAP.get(field, [])
    for k, v in kv.items():
        kk = norm(k).rstrip(":")
        for t in targets:
            if t in kk:
                return v.strip() if v else None
    return None

def fetch_html(url: str, timeout=30) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap.add_argument("--only-missing", action="store_true", help="only rows missing decision/status/etc")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    where = "url IS NOT NULL AND url != ''"
    if args.only_missing:
        where += " AND (decision IS NULL OR decision='' OR status IS NULL OR status='' OR decision_type IS NULL OR decision_type='' OR decision_date IS NULL OR decision_date='')"

    q = f"SELECT application_ref, url FROM applications WHERE {where}"
    cur.execute(q)
    rows = cur.fetchall()
    if args.limit and args.limit > 0:
        rows = rows[:args.limit]

    print(f"Found {len(rows)} rows to enrich")

    updated = 0
    failed = 0

    for i, (ref, url) in enumerate(rows, 1):
        try:
            html = fetch_html(url)
            soup = BeautifulSoup(html, "html.parser")
            kv = extract_kv_pairs(soup)

            decision = pick_field(kv, "decision")
            decision_type = pick_field(kv, "decision_type")
            decision_date = pick_field(kv, "decision_date")
            status = pick_field(kv, "status")

            cur.execute("""
                UPDATE applications
                SET decision = COALESCE(?, decision),
                    decision_type = COALESCE(?, decision_type),
                    decision_date = COALESCE(?, decision_date),
                    status = COALESCE(?, status)
                WHERE application_ref = ?
            """, (decision, decision_type, decision_date, status, ref))
            con.commit()
            updated += 1

            if i % 10 == 0:
                print(f"{i}/{len(rows)} enriched...")

            time.sleep(args.sleep)

        except Exception as e:
            failed += 1
            print(f"[FAIL] {ref} {url} -> {e}")

    con.close()
    print("\nDONE")
    print("updated:", updated)
    print("failed:", failed)
    print("Next: rebuild index -> python scripts/03_build_similarity_index.py")

if __name__ == "__main__":
    main()
