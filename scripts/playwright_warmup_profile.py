import time
from pathlib import Path
from playwright.sync_api import sync_playwright

PROFILE = Path("data/raw/playwright_profile")
URL = "https://portal.newcastle.gov.uk/planning/index.html?fa=getReceivedWeeklyList"

def main():
    PROFILE.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE),
            headless=False,
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        print("✅ Browser opened.")
        print("If you see a verification/challenge screen, wait until the weekly list loads.")
        print("Keeping window open for 25 seconds...")

        time.sleep(25)

        print("✅ Closing browser (profile saved).")
        context.close()

if __name__ == "__main__":
    main()
