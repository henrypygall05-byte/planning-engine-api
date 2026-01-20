import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"
BASE = "https://portal.newcastle.gov.uk/planning/index.html"

TEST_URLS = [
    BASE + "?fa=getDecidedWeeklyList&week=05%2F11%2F2023",
    BASE + "?fa=getDecidedWeeklyList&week=04%2F02%2F2024",
    BASE + "?fa=getDecidedWeeklyList&week=09%2F02%2F2025",
]

def summarize(url: str):
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html,*/*"})
    r = s.get(url, timeout=45, allow_redirects=True)

    soup = BeautifulSoup(r.text, "lxml")
    title = (soup.title.get_text(" ", strip=True) if soup.title else "NO TITLE")

    # first non-empty text snippet
    body_text = " ".join(soup.get_text(" ", strip=True).split())
    snippet = body_text[:220]

    tables = len(soup.find_all("table"))
    links = len(soup.find_all("a"))

    print("\n==============================")
    print("URL:", url)
    print("STATUS:", r.status_code)
    print("FINAL:", r.url)
    print("TITLE:", title)
    print("TABLES:", tables, "LINKS:", links)
    print("SNIPPET:", snippet)
    print("==============================")

def main():
    for u in TEST_URLS:
        summarize(u)

if __name__ == "__main__":
    main()
