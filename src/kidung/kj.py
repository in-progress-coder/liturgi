#!/usr/bin/env python3
import re
import json
import time
from typing import List, Set
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

TARGET_PAGE = "https://www.gkiharapanindah.org/download/rekap-kidung-jemaat/"
PATTERN_PREFIX = "https://www.gkiharapanindah.org/nyanyian-jemaat/kidung-jemaat/kj"
# Additional site endpoints to fall back on (WordPress typical endpoints)
FALLBACK_ENDPOINTS = [
    # WP sitemap index & post sitemap(s)
    "https://www.gkiharapanindah.org/sitemap_index.xml",
    "https://www.gkiharapanindah.org/post-sitemap.xml",
    # Kategori & feed (if available)
    "https://www.gkiharapanindah.org/category/nyanyian-jemaat/kidung-jemaat/",
    "https://www.gkiharapanindah.org/category/nyanyian-jemaat/kidung-jemaat/feed/",
]

HEADERS_BASE = {
    # Pretend to be a recent desktop Chrome
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,image/apng,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.google.com/",
}

def extract_matching_links_from_html(html: str, base_url: str = "") -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: Set[str] = set()

    # 1) Clean <a href> collection
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/"):
            href = urljoin(base_url or TARGET_PAGE, href)
        if href.startswith(PATTERN_PREFIX):
            links.add(href)

    # 2) Regex fallback to catch any inline strings
    #    Broad but safe: anything starting with the expected prefix up to a delimiter/quote/space.
    rx = re.compile(r"https://www\.gkiharapanindah\.org/nyanyian-jemaat/kidung-jemaat/kj[^\s\"'<)]+", re.I)
    for m in rx.findall(html):
        links.add(m)

    return sorted(links)

def get_with_retries(url: str, session: requests.Session, headers: dict, tries: int = 3, sleep_s: float = 1.0):
    last_exc = None
    for i in range(tries):
        try:
            resp = session.get(url, headers=headers, timeout=25)
            # Some servers return 406 unless exact headers are present. Consider 2xx as success.
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            time.sleep(sleep_s)
    raise last_exc

def collect_links() -> List[str]:
    session = requests.Session()
    all_links: Set[str] = set()

    # 0) Primary attempt: target page with hardened headers
    try_headers = [
        HEADERS_BASE,
        {**HEADERS_BASE, "Accept": "text/html"},  # minimal Accept
        {**HEADERS_BASE, "Upgrade-Insecure-Requests": "1"},  # extra header some WP stacks like
    ]
    for h in try_headers:
        try:
            r = get_with_retries(TARGET_PAGE, session, headers=h, tries=3)
            links = extract_matching_links_from_html(r.text, base_url=TARGET_PAGE)
            all_links.update(links)
            break  # success, proceed to dedupe/finish + fallbacks to enrich
        except Exception:
            # try next header profile
            continue

    # 1) Fall back to known site endpoints (sitemaps, category, feed)
    for endpoint in FALLBACK_ENDPOINTS:
        try:
            r = get_with_retries(endpoint, session, headers=HEADERS_BASE, tries=2)
            content_type = r.headers.get("Content-Type", "").lower()
            text = r.text
            # If XML sitemap/feed, still parse with BeautifulSoup (it can handle XML)
            links = extract_matching_links_from_html(text, base_url=endpoint)
            all_links.update(links)
        except Exception:
            # Ignore individual endpoint failures
            continue

    # 2) Final list
    final_links = sorted(all_links)
    return final_links

def main():
    links = collect_links()
    print(f"Found {len(links)} links.")
    for x in links[:20]:  # preview
        print(x)

    # Save outputs for reuse
    with open("kj_links.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(links))
    with open("kj_links.json", "w", encoding="utf-8") as f:
        json.dump(links, f, indent=2, ensure_ascii=False)

    # Example: iterate later to scrape contents
    # for url in links:
    #     scrape_kj_page(url)

if __name__ == "__main__":
    main()
