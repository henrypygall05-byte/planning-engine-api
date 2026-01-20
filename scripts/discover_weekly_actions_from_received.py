import re
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"
URL = "https://portal.newcastle.gov.uk/planning/index.html?fa=getReceivedWeeklyList"

def main():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html,*/*"})
    r = s.get(URL, timeout=45, allow_redirects=True)

    soup = BeautifulSoup(r.text, "lxml")
    title = soup.title.get_text(" ", strip=True) if soup.title else "NO TITLE"
    print("STATUS:", r.status_code)
    print("FINAL:", r.url)
    print("TITLE:", title)

    hrefs = []
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True)
        hrefs.append((txt, a["href"]))

    print("\nLinks containing 'decid' or 'decision':")
    any_links = False
    for txt, href in hrefs:
        if ("decid" in txt.lower() or "decision" in txt.lower()
            or "decid" in href.lower() or "decision" in href.lower()):
            print(" -", txt, "=>", href)
            any_links = True
    if not any_links:
        print(" (none found)")

    html = r.text
    fas = sorted(set(re.findall(r"fa=([A-Za-z0-9_]+)", html)))
    print("\nAll fa= actions found in page HTML:")
    for x in fas:
        print(" -", x)

    decided_strings = sorted(set(re.findall(r"[A-Za-z0-9_]*Decid[A-Za-z0-9_]*", html)))
    print("\nStrings containing 'Decid' in HTML:")
    if decided_strings:
        for x in decided_strings[:200]:
            print(" -", x)
    else:
        print(" (none found)")

if __name__ == "__main__":
    main()
