import requests

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"
BASE = "https://portal.newcastle.gov.uk/planning/index.html"

tests = [
    ("BASE determined (no week)", {"fa": "getDeterminedWeeklyList"}),
    ("BEFORE cutoff", {"fa": "getDeterminedWeeklyList", "week": "12/05/2024"}),
    ("AFTER cutoff", {"fa": "getDeterminedWeeklyList", "week": "02/06/2024"}),
    ("RECENT", {"fa": "getDeterminedWeeklyList", "week": "06/07/2025"}),
]

def go(label, params):
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html,*/*", "Referer": BASE})
    r = s.get(BASE, params=params, timeout=45, allow_redirects=True)
    print("\n==", label, "==")
    print("status:", r.status_code)
    print("final:", r.url)
    print("content-type:", r.headers.get("Content-Type"))
    print("server:", r.headers.get("Server"))
    print("first80:", " ".join((r.text or "")[:200].split()))

def main():
    for label, params in tests:
        go(label, params)

if __name__ == "__main__":
    main()
