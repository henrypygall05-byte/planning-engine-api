import time
from pathlib import Path
from playwright.sync_api import sync_playwright

URL = "https://portal.newcastle.gov.uk/planning/index.html?fa=getApplication&id=129834"

OUT_HTML = Path("data/raw/debug_details_playwright.html")
OUT_PNG = Path("data/raw/debug_details_playwright.png")
OUT_HTML.parent.mkdir(parents=True, exist_ok=True)

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # Warm-up to get cookies/tokens
        page.goto("https://portal.newcastle.gov.uk/planning/index.html?fa=getReceivedWeeklyList", wait_until="networkidle", timeout=60000)
        time.sleep(1.5)

        page.goto(URL, wait_until="networkidle", timeout=60000)
        time.sleep(2.0)

        html = page.content()
        OUT_HTML.write_text(html, encoding="utf-8")

        page.screenshot(path=str(OUT_PNG), full_page=True)

        print("SAVED HTML:", OUT_HTML)
        print("SAVED PNG:", OUT_PNG)

        # quick structural hints
        print("IFRAMES:", len(page.frames))
        for i, fr in enumerate(page.frames):
            print(f"FRAME[{i}] url:", fr.url[:120])

        browser.close()

if __name__ == "__main__":
    main()
