import argparse
import datetime as dt
import sys
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

BASE = "https://portal.newcastle.gov.uk/planning/index.html"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"

CANDIDATES = [
    # Common Idox patterns
    ("GET", {"fa": "getDecidedWeeklyList", "week": None}),
    ("GET", {"fa": "getWeeklyList", "week": None, "type": "decided"}),
    ("GET", {"fa": "getWeeklyList", "week": None, "listType": "decided"}),
    ("GET", {"fa": "getWeeklyList", "week": None, "searchType": "Decided"}),
    ("GET", {"fa": "getWeeklyList", "week": None, "weeklyListType": "Decided"}),
    ("GET", {"fa": "weeklyList", "week": None, "type": "decided"}),
    ("GET", {"fa": "weeklyList", "week": None, "listType": "decided"}),
    # Sometimes it's a different parameter name
    ("GET", {"fa": "getDecidedWeeklyList", "weekBeginning": None}),
    ("GET", {"fa": "getDecidedWeeklyList", "date": None}),
]

def fmt_week(d: dt.date) -> str:
    return d.strftime("%d/%m/%Y")

def pick_sample_weeks():
    # choose a few Sundays around periods you already successfully ingested
    # (Newcastle weekly lists are Sunday-start based in your runs)
    return [
        dt.date(2023, 11, 5),
        dt.date(2024, 2, 4),
        dt.date(2025, 2, 9),
    ]

def fetch(sess: requests.Session, method: str, params: dict) -> requests.Response:
    if method == "GET":
        return sess.get(BASE, params=params, timeout=45, allow_redirects=True)
    raise ValueError("unsupported method")

def table_headers(html: str):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    ths = table.find_all("th")
    return [th.get_text(" ", strip=True) for th in ths]

def count_app_links(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return 0
    # weekly lists contain "Jump to Application" links or app refs; count links that look like application navigation
    return len(table.find_all("a"))

def looks_like_weekly_list(headers):
    if not headers:
        return False
    joined = " | ".join(h.lower() for h in headers)
    # received list includes "proposal" "ward" etc; decided list often includes "decision"
    return ("application" in joined and "proposal" in joined)

def has_decision_column(headers):
    j = " | ".join(h.lower() for h in headers)
    return ("decision" in j) or ("decided" in j) or ("decision date" in j)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--show-html", action="store_true")
    args = ap.parse_args()

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept": "text/html,*/*"})

    weeks = pick_sample_weeks()
    print("Testing weeks:", ", ".join(w.isoformat() for w in weeks))
    print("Base:", BASE)
    print()

    best = None

    for method, template in CANDIDATES:
        ok_hits = 0
        decision_hits = 0

        for w in weeks:
            params = dict(template)
            for k in list(params.keys()):
                if params[k] is None:
                    params[k] = fmt_week(w)

            try:
                r = fetch(sess, method, params)
            except Exception as e:
                print(f"[FAIL] {method} {template} -> {e}")
                continue

            hdrs = table_headers(r.text)
            links = count_app_links(r.text)

            is_weekly = looks_like_weekly_list(hdrs)
            has_dec = has_decision_column(hdrs)

            if r.status_code == 200 and is_weekly and links > 5:
                ok_hits += 1
                if has_dec:
                    decision_hits += 1

            # print one line per week attempt
            print(f"{method} {r.status_code}  params={{{', '.join(f'{k}={params[k]}' for k in params)}}}  headers={len(hdrs)} links={links} decision_col={has_dec}")

        print(f"== Candidate summary: ok_hits={ok_hits}/{len(weeks)} decision_hits={decision_hits}/{len(weeks)} template={template}\n")

        # prefer candidates that consistently return a weekly table AND include decision col
        score = (decision_hits * 10) + ok_hits
        if best is None or score > best["score"]:
            best = {"score": score, "method": method, "template": template}

    if not best or best["score"] == 0:
        print("❌ No decided-weekly endpoint found from candidates.")
        sys.exit(2)

    print("✅ BEST CANDIDATE:")
    print("method:", best["method"])
    print("template:", best["template"])
    print("score:", best["score"])

    # show example url
    sample = dict(best["template"])
    for k in list(sample.keys()):
        if sample[k] is None:
            sample[k] = fmt_week(weeks[0])
    print("\nExample querystring:")
    print(urlencode(sample))

if __name__ == "__main__":
    main()
