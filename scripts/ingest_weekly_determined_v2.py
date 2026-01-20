import argparse
import datetime as dt
import time
import sqlite3
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

DB_PATH = Path("data/processed/planning.db")
RAW_DIR = Path("data/raw/weekly_determined")
RAW_DIR.mkdir(parents=True, exist_ok=True)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"
BASE = "https://portal.newcastle.gov.uk/planning/index.html"

# Column mapping confirmed by your debug:
# 0 Application | 2 Proposal | 5 Decision
APPLICATION_COL = 0
PROPOSAL_COL = 2
DECISION_COL = 5

def parse_ddmmyyyy(s: str) -> dt.date:
    d, m, y = s.split("/")
    return dt.date(int(y), int(m), int(d))

def to_ddmmyyyy(d: dt.date) -> str:
    return d.strftime("%d/%m/%Y")

def to_iso(d: dt.date) -> str:
    return d.isoformat()

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html,*/*", "Referer": BASE})

    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[403, 405, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def decision_type_from(decision: str) -> str:
    d = (decision or "").strip().lower()
    if not d:
        return ""
    if "withdraw" in d:
        return "withdrawn"
    if "refus" in d:
        return "refused"
    if "grant" in d or "permit" in d or "approve" in d or "consent" in d:
        return "grant"
    return "other"

def fetch_week_html(sess: requests.Session, week_ddmmyyyy: str) -> tuple[int, str, str]:
    # Warmup base page to establish cookies/session state
    warm = sess.get(BASE, params={"fa": "getDeterminedWeeklyList"}, timeout=45, allow_redirects=True)
    if warm.status_code >= 500:
        # let retries handle, but keep moving
        pass

    r = sess.get(
        BASE,
        params={"fa": "getDeterminedWeeklyList", "week": week_ddmmyyyy},
        timeout=45,
        allow_redirects=True,
    )
    return r.status_code, r.url, (r.text or "")

def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    rows = []
    tr_list = table.find_all("tr")
    for tr in tr_list[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        cells = [td.get_text(" ", strip=True) for td in tds]
        if len(cells) <= DECISION_COL:
            continue

        app_ref = cells[APPLICATION_COL].strip()
        proposal = cells[PROPOSAL_COL].strip()
        decision = cells[DECISION_COL].strip()

        if not app_ref:
            continue

        rows.append({
            "application_ref": app_ref,
            "proposal": proposal,
            "decision": decision,
            "decision_type": decision_type_from(decision),
        })
    return rows

def save_archive(
    conn: sqlite3.Connection,
    council: str,
    week_iso: str,
    url: str,
    status: int,
    applications_found: int,
    html_path: str,
) -> None:
    conn.execute(
        """
        INSERT INTO weekly_determined_archives (
            council,
            week_start,
            url,
            http_status,
            applications_found,
            html_path
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(council, week_start) DO UPDATE SET
            url = excluded.url,
            http_status = excluded.http_status,
            applications_found = excluded.applications_found,
            html_path = excluded.html_path
        """,
        (council, week_iso, url, status, applications_found, html_path),
    )

def upsert_decisions(conn: sqlite3.Connection, council: str, week_iso: str, rows: list[dict]) -> int:
    changed = 0
    for r in rows:
        cur = conn.execute(
            """
            UPDATE applications
            SET
              proposal = CASE
                WHEN (proposal IS NULL OR proposal = '') AND (? IS NOT NULL AND ? <> '') THEN ?
                ELSE proposal
              END,
              decision = CASE
                WHEN decision IS NULL OR decision = '' THEN ?
                ELSE decision
              END,
              decision_type = CASE
                WHEN decision_type IS NULL OR decision_type = '' THEN ?
                ELSE decision_type
              END,
              week_decided = CASE
                WHEN week_decided IS NULL OR week_decided = '' THEN ?
                ELSE week_decided
              END
            WHERE council = ? AND application_ref = ?
            """,
            (
                r["proposal"], r["proposal"], r["proposal"],
                r["decision"],
                r["decision_type"],
                week_iso,
                council, r["application_ref"],
            ),
        )
        if cur.rowcount:
            changed += cur.rowcount
    return changed

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=3)
    ap.add_argument("--sleep", type=float, default=0.9)
    ap.add_argument("--max-weeks", type=int, default=None)
    ap.add_argument("--start-week", type=str, default=None, help="DD/MM/YYYY (Sunday) start; if omitted uses today-3y aligned to Sunday")
    args = ap.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    council = "Newcastle City Council"

    today = dt.date.today()
    if args.start_week:
        start = parse_ddmmyyyy(args.start_week)
    else:
        start = today - dt.timedelta(days=365 * args.years)

    # align to Sunday (weekly list appears to be Sunday week-start)
    start = start - dt.timedelta(days=(start.weekday() + 1) % 7)
    end = today - dt.timedelta(days=(today.weekday() + 1) % 7)

    weeks = []
    d = start
    while d <= end:
        weeks.append(d)
        d += dt.timedelta(days=7)

    if args.max_weeks is not None:
        weeks = weeks[: args.max_weeks]

    print("Weeks to attempt:", len(weeks))
    print("Start week:", to_iso(weeks[0]) if weeks else "NONE")
    print("End week:", to_iso(weeks[-1]) if weeks else "NONE")

    sess = make_session()
    conn = sqlite3.connect(DB_PATH)
    try:
        processed = 0
        updated_total = 0

        for w in weeks:
            week_dd = to_ddmmyyyy(w)
            week_iso = to_iso(w)

            try:
                status, url, html = fetch_week_html(sess, week_dd)
            except requests.exceptions.RequestException as e:
                print(f"Week {week_iso} | REQUEST ERROR | {e}")
                time.sleep(args.sleep * 2)
                continue

            html_path = str(RAW_DIR / f"determined_{week_iso}.html")
            Path(html_path).write_text(html, encoding="utf-8", errors="replace")

            rows = parse_table(html)

            save_archive(conn, council, week_iso, url, status, len(rows), html_path)

            changed = 0
            if status == 200 and rows:
                changed = upsert_decisions(conn, council, week_iso, rows)

            conn.commit()
            processed += 1
            updated_total += changed

            print(f"Week {week_iso} | status {status} | rows {len(rows)} | application_updates {changed}")

            time.sleep(args.sleep)

        print("\n✅ DONE")
        print("Weeks processed:", processed)
        print("Application rows updated:", updated_total)

    finally:
        conn.close()
        sess.close()

if __name__ == "__main__":
    main()
