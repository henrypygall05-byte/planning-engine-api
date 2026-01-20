import time
from pathlib import Path
from playwright.sync_api import sync_playwright

PROFILE = Path("data/raw/playwright_profile")
URL = "https://portal.newcastle.gov.uk/planning/index.html?fa=getApplication&id=129834"
OUT_HTML = Path("data/raw/debug_details_playwright_warm.html")
OUT_PNG = Path("data/raw/debug_details_playwright_warm.png")

def main():
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE),
            headless=False,
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        page.goto(URL, timeout=60000)

        # give JS time to populate fields
        page.wait_for_timeout(8000)

        html = page.content()
        OUT_HTML.write_text(html, encoding="utf-8")

        page.screenshot(path=str(OUT_PNG), full_page=True)

        print("✅ Saved HTML:", OUT_HTML)
        print("✅ Saved PNG:", OUT_PNG)

        context.close()

if __name__ == "__main__":
    main()
