# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "requests",
#   "playwright",
# ]
# ///

import os
import json
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright

BASE_URL = "https://discourse.onlinedegree.iitm.ac.in"
CATEGORY_JSON_URL = f"{BASE_URL}/c/courses/tds-kb/34.json"
COOKIE_PATH = "preprocessing/cookies.txt"
OUTPUT_DIR = "data/discourse_posts"
DATE_FROM = datetime(2025, 1, 1)
DATE_TO = datetime(2025, 4, 14)

os.makedirs(OUTPUT_DIR, exist_ok=True)


def update_cookie():
    print("Opening browser for login...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(f"{BASE_URL}/login")
        print("Log in with Google. Then click Resume in Playwright.")
        page.pause()
        cookies = context.cookies()
        for c in cookies:
            if c["name"] == "_t":
                with open(COOKIE_PATH, "w") as f:
                    f.write(f"_t={c['value']}")
                break
        browser.close()
        print("Cookie saved.")


def get_cookie():
    if not os.path.exists(COOKIE_PATH):
        update_cookie()
    with open(COOKIE_PATH) as f:
        return {"cookie": f.read().strip()}


def safe_get(url):
    headers = get_cookie()
    r = requests.get(url, headers=headers)
    if r.status_code in [401, 403]:
        print("Cookie expired. Logging in again...")
        update_cookie()
        headers = get_cookie()
        r = requests.get(url, headers=headers)
    return r


def scrape_topic_urls():
    page = 0
    urls = []
    while True:
        url = f"{CATEGORY_JSON_URL}?page={page}"
        res = safe_get(url)
        if res.status_code != 200:
            break
        data = res.json()
        topics = data.get("topic_list", {}).get("topics", [])
        if not topics:
            break
        for topic in topics:
            created = datetime.strptime(topic["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if DATE_FROM <= created <= DATE_TO:
                tid = topic["id"]
                slug = topic["slug"]
                urls.append(f"{BASE_URL}/t/{slug}/{tid}.json")
        page += 1
    return urls


def save_post(url, index):
    res = safe_get(url)
    if res.status_code == 200:
        data = {
            "source_url": url,             # ✅ Track source URL
            "post_data": res.json()        # ✅ Store original data
        }
        with open(f"{OUTPUT_DIR}/discourse{index+1}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"Saved discourse{index+1}.json")
    else:
        print(f"Failed: {url}")


def main():
    urls = scrape_topic_urls()
    print(f"Found {len(urls)} valid post URLs.")
    for i, url in enumerate(urls):
        save_post(url, i)


if __name__ == "__main__":
    main()
