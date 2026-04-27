import argparse
import json
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


DHAKA = ZoneInfo("Asia/Dhaka")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


def dhaka_now() -> datetime:
    return datetime.now(DHAKA)


def iso_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=DHAKA).isoformat()


def fetch_json(url: str) -> Dict:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def scrape_prothomalo(cutoff: datetime, page_size: int = 25) -> List[Dict[str, str]]:
    articles: List[Dict[str, str]] = []
    seen = set()
    offset = 0

    while True:
        fields = "headline,slug,published-at,sections"
        url = (
            "https://www.prothomalo.com/api/v1/collections/latest-all"
            f"?limit={page_size}&offset={offset}&fields={fields}"
        )
        payload = fetch_json(url)
        items = payload.get("items", [])
        if not items:
            break

        oldest_on_page: Optional[datetime] = None

        for row in items:
            story = row.get("story") or {}
            published_ms = story.get("published-at")
            slug = story.get("slug")
            headline = story.get("headline")
            if not published_ms or not slug or not headline:
                continue

            published = datetime.fromtimestamp(published_ms / 1000, tz=DHAKA)
            oldest_on_page = published if oldest_on_page is None else min(oldest_on_page, published)

            link = urljoin("https://www.prothomalo.com/", slug)
            if published < cutoff or link in seen:
                continue

            sections = story.get("sections") or []
            category = ""
            if sections:
                category = sections[0].get("display-name") or sections[0].get("name") or ""

            seen.add(link)
            articles.append(
                {
                    "Headline": headline,
                    "Link": link,
                    "PublishedTime": published.isoformat(),
                    "Publisher": "Prothom Alo",
                    "Category": category,
                }
            )

        if oldest_on_page is not None and oldest_on_page < cutoff:
            break

        offset += page_size

    return articles


def parse_tbs_age_minutes(text: str) -> Optional[int]:
    normalized = " ".join(text.split()).lower()
    if "now" in normalized:
        return 0

    match = re.search(r"\b(\d+)\s*(m|min|mins|minute|minutes)\b", normalized)
    if match:
        return int(match.group(1))

    match = re.search(r"\b(\d+)\s*(h|hr|hrs|hour|hours)\b", normalized)
    if match:
        return int(match.group(1)) * 60

    return None


def tbs_latest_candidates(page_url: str, max_age_minutes: int = 60) -> Iterable[Tuple[str, Optional[int]]]:
    soup = BeautifulSoup(fetch_html(page_url), "html.parser")
    for anchor in soup.select(".view-todays-news h3 a[href], h3.card-title a[href]"):
        href = anchor.get("href")
        if not href:
            continue
        link = urljoin("https://www.tbsnews.net/", href)
        if not re.search(r"[-/]\d{5,}$", link):
            continue

        card = anchor
        for _ in range(4):
            if card.parent is None:
                break
            card = card.parent

        age_minutes = parse_tbs_age_minutes(card.get_text(" ", strip=True))
        if age_minutes is None or age_minutes <= max_age_minutes:
            yield link, age_minutes


def tbs_latest_links(page_url: str) -> Iterable[str]:
    for link, _age_minutes in tbs_latest_candidates(page_url):
        yield link


def tbs_next_page(page_url: str) -> Optional[str]:
    soup = BeautifulSoup(fetch_html(page_url), "html.parser")
    rel_next = soup.select_one('a[rel="next"][href], li.pager-next a[href]')
    if rel_next and rel_next.get("href"):
        return urljoin(page_url, rel_next["href"])
    return None


def extract_tbs_article(link: str) -> Optional[Dict[str, str]]:
    soup = BeautifulSoup(fetch_html(link), "html.parser")

    published = None
    headline = None
    publisher = "The Business Standard"

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text(strip=True))
        except json.JSONDecodeError:
            continue

        graph = data.get("@graph") if isinstance(data, dict) else None
        nodes = graph if isinstance(graph, list) else [data]
        for node in nodes:
            if not isinstance(node, dict) or node.get("@type") != "NewsArticle":
                continue
            headline = node.get("headline") or headline
            if node.get("datePublished"):
                published = datetime.fromisoformat(node["datePublished"]).astimezone(DHAKA)
            pub = node.get("publisher")
            if isinstance(pub, dict) and pub.get("name"):
                publisher = pub["name"]

    if published is None:
        meta = soup.select_one('meta[property="article:published_time"][content]')
        if meta:
            published = datetime.fromisoformat(meta["content"]).astimezone(DHAKA)

    if headline is None:
        title = soup.select_one('meta[property="og:title"][content]')
        headline = title["content"] if title else ""

    breadcrumb_names = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text(strip=True))
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
            for item in data.get("itemListElement", []):
                name = item.get("name")
                if name and name.lower() != "home":
                    breadcrumb_names.append(name)

    if not published or not headline:
        return None

    return {
        "Headline": headline,
        "Link": link,
        "PublishedTime": published.isoformat(),
        "Publisher": publisher,
        "Category": breadcrumb_names[-1] if breadcrumb_names else "",
    }


def scrape_tbs(cutoff: datetime, max_pages: int = 2) -> List[Dict[str, str]]:
    articles: List[Dict[str, str]] = []
    seen = set()
    page_url: Optional[str] = "https://www.tbsnews.net/latest"
    max_age_minutes = max(1, int((dhaka_now() - cutoff).total_seconds() // 60) + 5)

    for _ in range(max_pages):
        if not page_url:
            break

        candidates = list(dict.fromkeys(tbs_latest_candidates(page_url, max_age_minutes=max_age_minutes)))
        if not candidates:
            break

        oldest_on_page: Optional[datetime] = None
        for link, age_minutes in candidates:
            if link in seen:
                continue
            seen.add(link)

            if age_minutes is not None and age_minutes > max_age_minutes:
                continue

            article = extract_tbs_article(link)
            if not article:
                continue

            published = datetime.fromisoformat(article["PublishedTime"])
            oldest_on_page = published if oldest_on_page is None else min(oldest_on_page, published)

            if published >= cutoff:
                articles.append(article)

        if oldest_on_page is not None and oldest_on_page < cutoff:
            break

        page_url = tbs_next_page(page_url)

    return articles


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=float, default=5)
    parser.add_argument("--site", choices=["all", "prothomalo", "tbs"], default="all")
    args = parser.parse_args()

    cutoff = dhaka_now() - timedelta(hours=args.hours)
    results: List[Dict[str, str]] = []

    if args.site in ("all", "prothomalo"):
        results.extend(scrape_prothomalo(cutoff))
    if args.site in ("all", "tbs"):
        results.extend(scrape_tbs(cutoff))

    results.sort(key=lambda item: item["PublishedTime"], reverse=True)
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
