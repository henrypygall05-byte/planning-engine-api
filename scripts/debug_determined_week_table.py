from pathlib import Path
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"
BASE = "https://portal.newcastle.gov.uk/planning/index.html"
OUT = Path("data/raw/debug_determined_week.html")
OUT.parent.mkdir(parents=True, exist_ok=True)

WEEK = "12/11/2023"  # known week you already saw rows for

def main():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html,*/*", "Referer": BASE})

    # warmup
    s.get(BASE, params={"fa": "getDeterminedWeeklyList"}, timeout=45, allow_redirects=True)

    r = s.get(BASE, params={"fa": "getDeterminedWeeklyList", "week": WEEK}, timeout=45, allow_redirects=True)
    print("STATUS:", r.status_code)
    print("FINAL:", r.url)

    OUT.write_text(r.text, encoding="utf-8", errors="replace")
    print("SAVED:", OUT)

    soup = BeautifulSoup(r.text, "lxml")
    tables = soup.find_all("table")
    print("TABLES:", len(tables))
    if not tables:
        raise SystemExit("❌ No tables found")

    t = tables[0]
    headers = [th.get_text(" ", strip=True) for th in t.select("th")]
    print("\nHEADERS:")
    for h in headers:
        print(" -", h)

    first_row = t.select_one("tr + tr")
    if not first_row:
        print("\n(No data rows)")
        return

    cells = [td.get_text(" ", strip=True) for td in first_row.select("td")]
    print("\nFIRST ROW CELLS:")
    for i, c in enumerate(cells):
        print(f"{i:02d}:", c[:140])

if __name__ == "__main__":
    main()
