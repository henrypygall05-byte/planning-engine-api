import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://portal.newcastle.gov.uk/planning/"
URL = urljoin(BASE, "index.html")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": URL,
}

def dump(html: str):
    soup = BeautifulSoup(html, "lxml")

    title = soup.title.get_text(" ", strip=True) if soup.title else "(no title)"
    print("TITLE:", title)

    print("\n--- FORM FIELDS (select) ---")
    for s in soup.find_all("select"):
        name = s.get("name")
        if not name:
            continue
        opts = [o.get("value") or o.get_text(strip=True) for o in s.find_all("option")][:5]
        print(f"select name={name!r} sample_options={opts}")

    print("\n--- FORM FIELDS (input) ---")
    for i in soup.find_all("input"):
        name = i.get("name")
        if not name:
            continue
        itype = i.get("type") or ""
        val = i.get("value") or ""
        print(f"input name={name!r} type={itype!r} value={val!r}")

    # Try to detect if a results table exists
    tables = soup.find_all("table")
    print("\nTABLE COUNT:", len(tables))
    if tables:
        # Print first table headers
        ths = [th.get_text(" ", strip=True) for th in tables[0].find_all("th")]
        print("FIRST TABLE HEADERS:", ths)

def fetch(params: dict):
    r = requests.get(URL, params=params, headers=HEADERS, timeout=45, allow_redirects=True)
    print("\n=============================")
    print("REQUEST PARAMS:", params)
    print("STATUS:", r.status_code)
    print("FINAL URL:", r.url)
    print("=============================")
    dump(r.text[:200000])  # enough to include forms and first results

def main():
    # 1) base weekly received page
    fetch({"fa": "getReceivedWeeklyList"})

    # 2) if user provided a week on CLI, try it as week=...
    if len(sys.argv) > 1:
        week = sys.argv[1]
        fetch({"fa": "getReceivedWeeklyList", "week": week})

if __name__ == "__main__":
    main()
