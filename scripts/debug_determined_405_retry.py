import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) planning-bot/1.0"
BASE = "https://portal.newcastle.gov.uk/planning/index.html"

tests = [
    ("BASE determined (no week)", {"fa": "getDeterminedWeeklyList"}),
    ("BEFORE cutoff", {"fa": "getDeterminedWeeklyList", "week": "12/05/2024"}),
    ("AFTER cutoff", {"fa": "getDeterminedWeeklyList", "week": "02/06/2024"}),
    ("RECENT", {"fa": "getDeterminedWeeklyList", "week": "06/07/2025"}),
]

def make_session():
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

def go(label, params):
    s = make_session()
    print("\n==", label, "==")
    try:
        r = s.get(BASE, params=params, timeout=45, allow_redirects=True)
        print("status:", r.status_code)
        print("final:", r.url)
        print("content-type:", r.headers.get("Content-Type"))
        print("server:", r.headers.get("Server"))
        print("allow:", r.headers.get("Allow"))
        txt = (r.text or "")
        print("first120:", " ".join(txt[:240].split()))
    except requests.exceptions.SSLError as e:
        print("SSL ERROR:", repr(e))
    except requests.exceptions.RequestException as e:
        print("REQUEST ERROR:", repr(e))
    finally:
        s.close()

def main():
    for label, params in tests:
        go(label, params)
        time.sleep(1.2)

if __name__ == "__main__":
    main()
