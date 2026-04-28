import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
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
SCRAPE_DELAY_MIN = float(os.getenv("SCRAPE_DELAY_MIN", "0.5"))
SCRAPE_DELAY_MAX = float(os.getenv("SCRAPE_DELAY_MAX", "2.0"))
SCRAPE_MAX_RETRIES = int(os.getenv("SCRAPE_MAX_RETRIES", "3"))
DAILY_STAR_WORKERS = int(os.getenv("DAILY_STAR_WORKERS", "4"))
HOST_LOCKS: Dict[str, threading.Lock] = {}
HOST_LAST_REQUEST: Dict[str, float] = {}
HOST_LOCKS_GUARD = threading.Lock()


class ScrapeBlockedError(RuntimeError):
    pass


def dhaka_now() -> datetime:
    return datetime.now(DHAKA)


def host_lock(host: str) -> threading.Lock:
    with HOST_LOCKS_GUARD:
        if host not in HOST_LOCKS:
            HOST_LOCKS[host] = threading.Lock()
        return HOST_LOCKS[host]


def wait_for_host(url: str) -> None:
    delay_min = max(0.0, SCRAPE_DELAY_MIN)
    delay_max = max(delay_min, SCRAPE_DELAY_MAX)
    if delay_max <= 0:
        return

    host = urlparse(url).netloc
    lock = host_lock(host)
    with lock:
        now = time.monotonic()
        wait_for = random.uniform(delay_min, delay_max)
        elapsed = now - HOST_LAST_REQUEST.get(host, 0)
        if elapsed < wait_for:
            time.sleep(wait_for - elapsed)
        HOST_LAST_REQUEST[host] = time.monotonic()


def polite_get(url: str) -> requests.Response:
    last_error = None
    for attempt in range(SCRAPE_MAX_RETRIES):
        wait_for_host(url)
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code in (403, 429):
                raise ScrapeBlockedError(f"{response.status_code} from {url}")
            if response.status_code >= 500 and attempt < SCRAPE_MAX_RETRIES - 1:
                time.sleep(min(30, 2 ** attempt + random.uniform(0, 1)))
                continue
            response.raise_for_status()
            return response
        except (requests.RequestException, ScrapeBlockedError) as exc:
            last_error = exc
            if isinstance(exc, ScrapeBlockedError) or attempt == SCRAPE_MAX_RETRIES - 1:
                raise
            time.sleep(min(30, 2 ** attempt + random.uniform(0, 1)))
    raise last_error or RuntimeError(f"Failed to fetch {url}")


def fetch_json(url: str) -> Dict:
    return polite_get(url).json()


def fetch_html(url: str) -> str:
    return polite_get(url).text


def normalize_category(value: str) -> str:
    cleaned = " ".join((value or "").replace("-", " ").replace("_", " ").split())
    return cleaned.title()


def clean_text(value: str, limit: int = 1200) -> str:
    return " ".join((value or "").split())[:limit]


def meta_description(soup: BeautifulSoup) -> str:
    meta = soup.select_one('meta[name="description"][content], meta[property="og:description"][content]')
    return clean_text(meta.get("content", "") if meta else "")


def body_snippet(soup: BeautifulSoup, selectors: str, limit: int = 1200) -> str:
    paragraphs = []
    for paragraph in soup.select(selectors):
        text = clean_text(paragraph.get_text(" ", strip=True), limit=limit)
        if text:
            paragraphs.append(text)
        if len(" ".join(paragraphs)) >= limit:
            break
    return clean_text(" ".join(paragraphs), limit=limit)


def parse_relative_minutes(text: str) -> Optional[int]:
    normalized = " ".join(text.split()).lower()
    if not normalized:
        return None
    match = re.search(r"\b(\d+)\s*(sec(?:\(s\))?|secs|second(?:s)?)\s*ago\b", normalized)
    if match:
        return 0

    match = re.search(r"\b(\d+)\s*(m|min(?:\(s\))?|mins|minute(?:s)?)\s*ago\b", normalized)
    if match:
        return int(match.group(1))

    match = re.search(r"\b(\d+)\s*(h|hr|hrs|hour(?:\(s\))?|hours)\s*ago\b", normalized)
    if match:
        return int(match.group(1)) * 60

    return None


def parse_compact_age_minutes(text: str) -> Optional[int]:
    normalized = " ".join(text.split()).lower()
    match = re.search(r"\b(\d+)\s*m\b", normalized)
    if match:
        return int(match.group(1))
    match = re.search(r"\b(\d+)\s*h\b", normalized)
    if match:
        return int(match.group(1)) * 60
    return parse_relative_minutes(text)


def cached_article(link: str, article_cache: Optional[Dict[str, Dict[str, str]]]) -> Optional[Dict[str, str]]:
    if not article_cache:
        return None
    article = article_cache.get(link)
    if not article:
        return None
    return dict(article)


def has_match_context(article: Dict[str, str]) -> bool:
    return bool(clean_text(article.get("Summary", "")) or clean_text(article.get("BodySnippet", "")))


def cached_recent_article(link: str, cutoff: datetime, article_cache: Optional[Dict[str, Dict[str, str]]]) -> Optional[Dict[str, str]]:
    article = cached_article(link, article_cache)
    if not article:
        return None
    try:
        published = datetime.fromisoformat(article["PublishedTime"]).astimezone(DHAKA)
    except (KeyError, ValueError):
        return None
    if published < cutoff:
        return None
    return dict(article)


def scrape_prothomalo(cutoff: datetime, page_size: int = 25, article_cache: Optional[Dict[str, Dict[str, str]]] = None) -> List[Dict[str, str]]:
    articles: List[Dict[str, str]] = []
    seen = set()
    offset = 0

    while True:
        fields = "headline,slug,published-at,sections,summary,subheadline"
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
                    "Summary": clean_text(story.get("summary") or story.get("subheadline") or ""),
                    "BodySnippet": clean_text(story.get("summary") or story.get("subheadline") or ""),
                }
            )

        if oldest_on_page is not None and oldest_on_page < cutoff:
            break

        offset += page_size

    return articles


def daily_star_category_from_link(link: str) -> str:
    segments = [segment for segment in urlparse(link).path.split("/") if segment]
    ignored = {"news", "sports", "business", "opinion", "entertainment", "lifestyle", "tech-startup"}

    for segment in segments[:-1]:
        if segment in ignored:
            continue
        if re.search(r"\d{5,}$", segment):
            continue
        return normalize_category(segment)

    if len(segments) >= 2:
        return normalize_category(segments[-2])
    return ""


DAILY_STAR_LISTING_URLS = [
    "https://www.thedailystar.net/todays-news",
    "https://www.thedailystar.net/",
    "https://www.thedailystar.net/news",
    "https://www.thedailystar.net/news/bangladesh",
    "https://www.thedailystar.net/news/world",
    "https://www.thedailystar.net/business",
    "https://www.thedailystar.net/sports",
    "https://www.thedailystar.net/tech-startup",
    "https://www.thedailystar.net/entertainment",
    "https://www.thedailystar.net/lifestyle",
    "https://www.thedailystar.net/opinion",
]


def daily_star_card_age(anchor) -> Optional[int]:
    node = anchor
    for _ in range(8):
        if node is None:
            break
        age_minutes = parse_relative_minutes(node.get_text(" ", strip=True))
        if age_minutes is not None:
            return age_minutes
        node = node.parent
    return None


def daily_star_listing_cards(soup: BeautifulSoup):
    cards = soup.select(".views-row")
    if cards:
        return cards
    return soup.select("article, .card, .card-content, .story-card")


def extract_daily_star_article(
    link: str,
    headline: str,
    fallback_published: datetime,
    cutoff: datetime,
) -> Optional[Dict[str, str]]:
    try:
        soup = BeautifulSoup(fetch_html(link), "html.parser")
    except requests.RequestException:
        return None

    title = soup.select_one("h1")
    if title:
        headline = " ".join(title.get_text(" ", strip=True).split()) or headline
    summary = meta_description(soup)
    snippet = body_snippet(soup, ".node-content p, article p, .field--name-body p")

    meta_block = soup.select_one(".block-article-meta-block")
    meta_text = meta_block.get_text(" ", strip=True) if meta_block else ""
    age_minutes = parse_relative_minutes(meta_text)
    published = dhaka_now() - timedelta(minutes=age_minutes) if age_minutes is not None else fallback_published
    if published < cutoff:
        return None

    category = daily_star_category_from_link(link)
    if meta_block:
        category_link = meta_block.select_one('a[href*="/news/"], a[href*="/business/"], a[href*="/sports/"], a[href*="/opinion/"], a[href*="/life-living/"], a[href*="/culture/"], a[href*="/tech-startup/"]')
        if category_link:
            category_text = category_link.get_text(" ", strip=True)
            if category_text:
                category = normalize_category(category_text)

    return {
        "Headline": headline,
        "Link": link,
        "PublishedTime": published.isoformat(),
        "Publisher": "The Daily Star",
        "Category": category,
        "Summary": summary,
        "BodySnippet": snippet or summary,
    }


def scrape_daily_star(cutoff: datetime, article_cache: Optional[Dict[str, Dict[str, str]]] = None) -> List[Dict[str, str]]:
    candidates: Dict[str, Tuple[str, datetime]] = {}
    seen = set()
    now = dhaka_now()

    for listing_url in DAILY_STAR_LISTING_URLS:
        soup = BeautifulSoup(fetch_html(listing_url), "html.parser")
        for card in daily_star_listing_cards(soup):
            anchors = card.select("h1 a[href], h2 a[href], h3 a[href], h4 a[href], h5 a[href], h6 a[href]")
            if not anchors:
                continue

            for anchor in anchors:
                href = anchor.get("href")
                headline = " ".join(anchor.get_text(" ", strip=True).split())
                if not href or not headline:
                    continue

                link = urljoin("https://www.thedailystar.net", href)
                if link in seen or not re.search(r"[-/]\d{5,}$", urlparse(link).path):
                    continue

                age_minutes = daily_star_card_age(anchor)
                if age_minutes is None:
                    continue

                published = now - timedelta(minutes=age_minutes)
                if published < cutoff:
                    continue

                seen.add(link)
                candidates[link] = (headline, published)

    articles: List[Dict[str, str]] = []
    uncached_candidates = {}
    for link, (headline, published) in candidates.items():
        cached = cached_recent_article(link, cutoff, article_cache)
        if cached and cached.get("Publisher") == "The Daily Star" and has_match_context(cached):
            articles.append(cached)
        else:
            uncached_candidates[link] = (headline, published)

    with ThreadPoolExecutor(max_workers=max(1, DAILY_STAR_WORKERS)) as executor:
        futures = {
            executor.submit(extract_daily_star_article, link, headline, published, cutoff): link
            for link, (headline, published) in uncached_candidates.items()
        }
        for future in as_completed(futures):
            article = future.result()
            if article:
                articles.append(article)

    articles.sort(key=lambda item: item["PublishedTime"], reverse=True)
    return articles


def tbs_latest_candidates(page_url: str, max_age_minutes: int = 60) -> Iterable[Tuple[str, Optional[int]]]:
    soup = BeautifulSoup(fetch_html(page_url), "html.parser")
    for anchor in soup.select(".view-todays-news h3 a[href], h3.card-title a[href]"):
        href = anchor.get("href")
        if not href:
            continue
        link = urljoin("https://www.tbsnews.net/", href)
        if not re.search(r"[-/]\d{5,}$", link):
            continue

        card = anchor.find_parent(["article", "li"])
        if card is None:
            card = anchor.find_parent("div", class_=re.compile(r"(views-row|card|news|item|content)"))
        if card is None:
            card = anchor.parent

        age_minutes = parse_compact_age_minutes(card.get_text(" ", strip=True))
        if age_minutes is None or age_minutes <= max_age_minutes:
            yield link, age_minutes


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
    summary = meta_description(soup)
    snippet = body_snippet(soup, "article p, .news-details p, .field--name-body p, .article-content p")

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
        "Publisher": "The Business Standard",
        "Category": breadcrumb_names[-1] if breadcrumb_names else "",
        "Summary": summary,
        "BodySnippet": snippet or summary,
    }


def scrape_tbs(cutoff: datetime, max_pages: int = 10, article_cache: Optional[Dict[str, Dict[str, str]]] = None) -> List[Dict[str, str]]:
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

            cached = cached_article(link, article_cache)
            if cached and cached.get("Publisher") == "The Business Standard":
                cached_published = datetime.fromisoformat(cached["PublishedTime"]).astimezone(DHAKA)
                oldest_on_page = cached_published if oldest_on_page is None else min(oldest_on_page, cached_published)
                if cached_published < cutoff:
                    continue
                if cached_published >= cutoff and has_match_context(cached):
                    articles.append(cached)
                    continue

            article = extract_tbs_article(link)
            if not article:
                continue
            if article_cache is not None:
                article_cache[link] = article

            published = datetime.fromisoformat(article["PublishedTime"])
            oldest_on_page = published if oldest_on_page is None else min(oldest_on_page, published)

            if published >= cutoff:
                articles.append(article)

        if oldest_on_page is not None and oldest_on_page < cutoff:
            break

        page_url = tbs_next_page(page_url)

    return articles


SOURCE_REGISTRY: List[Dict[str, object]] = [
    {"id": "daily_star", "publisher": "The Daily Star", "scraper": scrape_daily_star},
    {"id": "prothomalo", "publisher": "Prothom Alo", "scraper": scrape_prothomalo},
    {"id": "tbs", "publisher": "The Business Standard", "scraper": scrape_tbs},
]

SOURCE_LOOKUP = {source["id"]: source for source in SOURCE_REGISTRY}


def publisher_order() -> List[str]:
    return [str(source["publisher"]) for source in SOURCE_REGISTRY]


def scrape_sources(
    cutoff: datetime,
    source_ids: Optional[List[str]] = None,
    article_cache: Optional[Dict[str, Dict[str, str]]] = None,
) -> List[Dict[str, str]]:
    requested_ids = source_ids or list(SOURCE_LOOKUP.keys())
    articles: List[Dict[str, str]] = []
    seen = set()

    for source_id in requested_ids:
        source = SOURCE_LOOKUP.get(source_id)
        if not source:
            continue
        scraper = source["scraper"]
        if not callable(scraper):
            continue

        for article in scraper(cutoff, article_cache=article_cache):
            link = article.get("Link")
            if not link or link in seen:
                continue
            seen.add(link)
            articles.append(article)

    articles.sort(key=lambda item: item["PublishedTime"], reverse=True)
    return articles


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=float, default=5)
    parser.add_argument("--site", choices=["all", *SOURCE_LOOKUP.keys()], default="all")
    args = parser.parse_args()

    cutoff = dhaka_now() - timedelta(hours=args.hours)
    requested_sources = None if args.site == "all" else [args.site]
    results = scrape_sources(cutoff, requested_sources)
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
