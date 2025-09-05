import requests
from bs4 import BeautifulSoup

# URLs to scrape
urls = [
    "https://www.yamuger.or.id/Lirik-Lagu/?bpage=1&bcpp=100#wbb1",
    "https://www.yamuger.or.id/Lirik-Lagu/?bpage=2&bcpp=100#wbb1"
]

base_url = "https://www.yamuger.or.id"
all_links = []

for url in urls:
    print(f"Fetching {url} ...")
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find all <a class="wb-blog-item" ...>
    for a in soup.find_all("a", class_="wb-blog-item"):
        href = a.get("href")
        if href:
            # Prepend base URL
            full_link = base_url + href
            all_links.append(full_link)

# Sort ascending
all_links = sorted(set(all_links))

# Save to file
with open("kk_links.txt", "w", encoding="utf-8") as f:
    for link in all_links:
        f.write(link + "\n")

print(f"Saved {len(all_links)} links to kk_links.txt")
