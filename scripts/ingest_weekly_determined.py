import argparse
import datetime as dt
import os
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

DB_PATH = Path("data/processed/planning.db")
OUT_DIR = Path("data/raw/weekly_determined")
BASE = "https://portal.newcastle.gov.uk/planning/index.html"
COUNCIL = "Newcastle City Council"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"

WS_RE = re.compile(r"\s+")

def iso_sunday(d: dt.date) -> dt.date:
    # Newcastle weekly lists in your runs align to Sundays already; enforce Sunday.
    # Python weekday: Mon=0..Sun=6
    return d - dt.timedelta(days=(d.weekday() + 1) % 7)

def fmt_week(d: dt.date) -> str:
    return d.strftime("%d/%m/%Y")

def parse_date_any(s: str) -> str | None:
    s = (s or "").strip()
    if not s:
        return None
    # expected dd/mm/yyyy
    try:
        d = dt.datetime.strptime(s, "%d/%m/%Y").date()
        return d.isoformat()
    except Exception:
        return None

def norm_text(s: str) -> str:
    s = (s or "").strip()
    s = WS_RE.sub(" ", s)
    return s

def normalize_decision_type(decision: str) -> str | None:
    d = (decision or "").strip().lower()
    if not d:
        return None
    # coarse buckets (extend later)
    if "approve" in d or "granted" in d or "permit" in d:
        return "approved"
    if "refus" in d or "reject" in d:
        return "refused"
    if "withdraw" in d:
        return "withdrawn"
    if "prior approval" in d and ("not required" in d or "not needed" in d):
        return "prior_approval_not_required"
    if "prior approval" in d:
        return "prior_approval"
    if "no objection" in d:
        return "no_objection"
    if "split" in d or "part" in d:
        return "part_approved"
    if "declin" in d:
        return "declined"
    return "other"

def fetch_week(sess: requests.Session, week_val: str) -> tuple[str, int, str]:
    params = {"fa": "getDeterminedWeeklyList", "week": week_val}
    url = BASE + "?" + urlencode(params)
    r = sess.get(BASE, params=params, timeout=45, allow_redirects=True)
    return url, r.status_code, r.text

def extract_table_rows(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    # headers
    headers = []
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
    if not headers:
        # fallback: first row th
        first_tr = table.find("tr")
        if first_tr:
            headers = [th.get_text(" ", strip=True) for th in first_tr.find_all("th")]

    rows = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        vals = [td.get_text(" ", strip=True) for td in tds]
        # try map by header count, else positional
        row = {}
        for i, v in enumerate(vals):
            key = headers[i] if i < len(headers) else f"col_{i}"
            row[key] = norm_text(v)
        rows.append(row)
    return rows

def pick_fields(row: dict) -> tuple[str | None, str | None, str | None]:
    """
    Return (application_ref, decision_text, date_decided_iso)
    """
    # Common header names on Idox "determined" list pages vary.
    # We'll search keys case-insensitively.
    lower_map = {k.lower(): k for k in row.keys()}

    def get_any(cands):
        for c in cands:
            k = lower_map.get(c.lower())
            if k and row.get(k):
                return row.get(k)
        return None

    app_ref = get_any(["Application", "Application Reference", "Reference", "App Ref", "Application Ref"])
    decision = get_any(["Decision", "Decision Type", "Decision Details", "Decision Outcome", "Result"])
    decided = get_any(["Decision Date", "Date Decided", "Decided", "Determination Date", "Date of Decision"])

    # Sometimes the application ref is the first column but header text differs.
    if not app_ref:
        # try first value that looks like yyyy/nnnn
        for v in row.values():
            if isinstance(v, str) and re.match(r"^\d{4}/\d{3,4}/", v.strip()):
                app_ref = v.strip()
                break

    date_iso = parse_date_any(decided) if decided else None
    return app_ref, decision, date_iso

def ensure_tables(conn: sqlite3.Connection):
    # already created by init script, but fail fast if missing
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weekly_determined_archives'")
    if not cur.fetchone():
        raise SystemExit("❌ weekly_determined_archives table missing. Run scripts/init_weekly_determined_archives.py first.")

def archive_html(week_iso: str, html: str) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p = OUT_DIR / f"{week_iso}.html"
    p.write_text(html, encoding="utf-8")
    return p

def upsert_archive(conn, week_iso: str, url: str, status: int, apps_found: int, html_path: Path):
    conn.execute(
        """
        INSERT INTO weekly_determined_archives
        (council, week_start, url, http_status, applications_found, html_path)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(council, week_start) DO UPDATE SET
            url=excluded.url,
            http_status=excluded.http_status,
            applications_found=excluded.applications_found,
            html_path=excluded.html_path,
            fetched_at=CURRENT_TIMESTAMP
        """,
        (COUNCIL, week_iso, url, status, apps_found, str(html_path)),
    )

def update_application_decision(conn, application_ref: str, decision: str | None, decision_type: str | None, date_decided: str | None) -> int:
    # only update if we have something meaningful
    if not application_ref:
        return 0
    if not (decision or decision_type or date_decided):
        return 0
    cur = conn.execute(
        """
        UPDATE applications
        SET
            decision = COALESCE(?, decision),
            decision_type = COALESCE(?, decision_type),
            date_decided = COALESCE(?, date_decided)
        WHERE council = ?
          AND application_ref = ?
        """,
        (decision, decision_type, date_decided, COUNCIL, application_ref),
    )
    return cur.rowcount

def iter_weeks(start_sunday: dt.date, weeks: int):
    for i in range(weeks):
        yield start_sunday + dt.timedelta(days=7*i)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=3)
    ap.add_argument("--start-week", default=None, help="DD/MM/YYYY (Sunday). If omitted, starts years back from today.")
    ap.add_argument("--max-weeks", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.8)
    args = ap.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    # build week range
    if args.start_week:
        try:
            start = dt.datetime.strptime(args.start_week, "%d/%m/%Y").date()
        except Exception:
            raise SystemExit("❌ --start-week must be DD/MM/YYYY")
        start = iso_sunday(start)
    else:
        today = dt.date.today()
        start = iso_sunday(today - dt.timedelta(days=365 * args.years))

    total_weeks = args.years * 52 + 2
    if args.max_weeks is not None:
        total_weeks = min(total_weeks, args.max_weeks)

    print("Weeks to attempt:", total_weeks)
    print("Start week:", start.isoformat())

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_tables(conn)

        sess = requests.Session()
        sess.headers.update({"User-Agent": UA, "Accept": "text/html,*/*", "Referer": BASE})

        weeks_done = 0
        total_updates = 0

        for w in iter_weeks(start, total_weeks):
            week_iso = w.isoformat()
            week_val = fmt_week(w)

            # skip if already archived with http_status 200 and apps_found > 0
            row = conn.execute(
                """
                SELECT http_status, applications_found
                FROM weekly_determined_archives
                WHERE council=? AND week_start=?
                """,
                (COUNCIL, week_iso),
            ).fetchone()
            if row and row[0] == 200 and row[1] > 0:
                continue

            url, status, html = fetch_week(sess, week_val)
            rows = extract_table_rows(html)

            # pull decisions
            updates = 0
            for r in rows:
                app_ref, decision, date_decided = pick_fields(r)
                decision = norm_text(decision) if decision else None
                d_type = normalize_decision_type(decision) if decision else None
                updates += update_application_decision(conn, app_ref, decision, d_type, date_decided)

            html_path = archive_html(week_iso, html)
            upsert_archive(conn, week_iso, url, status, len(rows), html_path)
            conn.commit()

            weeks_done += 1
            total_updates += updates
            print(f"Week {week_iso} | status {status} | rows {len(rows)} | application_updates {updates}")

            time.sleep(args.sleep)

        print("\n✅ DONE")
        print("Weeks processed:", weeks_done)
        print("Application rows updated:", total_updates)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
