import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape
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
DAILY_STAR_DETAIL_LIMIT = int(os.getenv("DAILY_STAR_DETAIL_LIMIT", "25"))
SAMAKAL_WORKERS = int(os.getenv("SAMAKAL_WORKERS", "8"))
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


def polite_get(url: str, *, timeout: int = 30, max_retries: Optional[int] = None) -> requests.Response:
    last_error = None
    retries = SCRAPE_MAX_RETRIES if max_retries is None else max(1, max_retries)
    for attempt in range(retries):
        wait_for_host(url)
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            if response.status_code in (403, 429):
                raise ScrapeBlockedError(f"{response.status_code} from {url}")
            if response.status_code >= 500 and attempt < retries - 1:
                time.sleep(min(30, 2 ** attempt + random.uniform(0, 1)))
                continue
            response.raise_for_status()
            return response
        except (requests.RequestException, ScrapeBlockedError) as exc:
            last_error = exc
            if isinstance(exc, ScrapeBlockedError) or attempt == retries - 1:
                raise
            time.sleep(min(30, 2 ** attempt + random.uniform(0, 1)))
    raise last_error or RuntimeError(f"Failed to fetch {url}")


def polite_post(url: str, *, headers: Optional[Dict[str, str]] = None, data: str = "") -> requests.Response:
    last_error = None
    request_headers = {**HEADERS, **(headers or {})}
    for attempt in range(SCRAPE_MAX_RETRIES):
        wait_for_host(url)
        try:
            response = requests.post(url, headers=request_headers, data=data, timeout=30)
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
    raise last_error or RuntimeError(f"Failed to post {url}")


def fetch_json(url: str, *, timeout: int = 30, max_retries: Optional[int] = None) -> Dict:
    return polite_get(url, timeout=timeout, max_retries=max_retries).json()


def fetch_html(url: str, *, timeout: int = 30, max_retries: Optional[int] = None) -> str:
    return polite_get(url, timeout=timeout, max_retries=max_retries).text


def normalize_category(value: str) -> str:
    cleaned = " ".join((value or "").replace("-", " ").replace("_", " ").split())
    return cleaned.title()


def clean_text(value: str, limit: int = 1200) -> str:
    return " ".join(unescape(value or "").split())[:limit]


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
    match = re.search(r"\b(\d+)\s*(sec(?:\(s\))?|secs|second(?:s)?)(?:\s*ago\b|(?=\s*[•|]|$))", normalized)
    if match:
        return 0

    match = re.search(r"\b(\d+)\s*(m|min(?:\(s\))?|mins|minute(?:s)?)(?:\s*ago\b|(?=\s*[•|]|$))", normalized)
    if match:
        return int(match.group(1))

    match = re.search(r"\b(\d+)\s*(h|hr|hrs|hour(?:\(s\))?|hours)(?:\s*ago\b|(?=\s*[•|]|$))", normalized)
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


def parse_site_datetime(value: str, default_tz=DHAKA) -> Optional[datetime]:
    cleaned = clean_text(value)
    if not cleaned:
        return None

    normalized = cleaned.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z"):
            try:
                parsed = datetime.strptime(normalized, pattern)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=default_tz)
    return parsed.astimezone(DHAKA)


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
        for time_node in node.select(".card-info > span, time"):
            age_minutes = parse_relative_minutes(time_node.get_text(" ", strip=True))
            if age_minutes is not None:
                return age_minutes
        age_minutes = parse_relative_minutes(node.get_text(" ", strip=True))
        if age_minutes is not None:
            return age_minutes
        node = node.parent
    return None


def daily_star_listing_cards(soup: BeautifulSoup):
    cards = soup.select(".views-row")
    if cards:
        return cards
    return soup.select(".card, .story-card") or soup.select("article, .card-content")


def extract_daily_star_article(
    link: str,
    headline: str,
    fallback_published: datetime,
    cutoff: datetime,
) -> Optional[Dict[str, str]]:
    try:
        soup = BeautifulSoup(fetch_html(link), "html.parser")
    except (requests.RequestException, ScrapeBlockedError):
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
    listing_errors = []
    successful_listings = 0

    for listing_url in DAILY_STAR_LISTING_URLS:
        try:
            soup = BeautifulSoup(fetch_html(listing_url, timeout=15, max_retries=1), "html.parser")
        except (requests.RequestException, ScrapeBlockedError) as exc:
            listing_errors.append(exc)
            continue
        successful_listings += 1
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

    if not successful_listings and listing_errors:
        raise listing_errors[-1]

    articles: List[Dict[str, str]] = []
    uncached_candidates = {}
    for link, (headline, published) in candidates.items():
        cached = cached_recent_article(link, cutoff, article_cache)
        if cached and cached.get("Publisher") == "The Daily Star" and has_match_context(cached):
            articles.append(cached)
        else:
            uncached_candidates[link] = (headline, published)

    # Fast path: build listing-level articles so the dashboard refresh can complete quickly.
    # We then fetch full article pages for only the most recent candidates.
    listing_articles: Dict[str, Dict[str, str]] = {}
    for link, (headline, published) in uncached_candidates.items():
        listing_articles[link] = {
            "Headline": headline,
            "Link": link,
            "PublishedTime": published.isoformat(),
            "Publisher": "The Daily Star",
            "Category": daily_star_category_from_link(link),
            "Summary": "",
            "BodySnippet": "",
        }
    articles.extend(list(listing_articles.values()))

    detail_limit = max(0, DAILY_STAR_DETAIL_LIMIT)
    if detail_limit and uncached_candidates:
        to_fetch = sorted(uncached_candidates.items(), key=lambda item: item[1][1], reverse=True)[:detail_limit]
        with ThreadPoolExecutor(max_workers=max(1, DAILY_STAR_WORKERS)) as executor:
            futures = {
                executor.submit(extract_daily_star_article, link, headline, published, cutoff): link
                for link, (headline, published) in to_fetch
            }
            for future in as_completed(futures):
                article = future.result()
                if article:
                    listing_articles[article["Link"]] = article

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


def samakal_category_from_link(link: str) -> str:
    segments = [segment for segment in urlparse(link).path.split("/") if segment]
    if segments:
        return normalize_category(segments[0])
    return ""


def path_category_from_link(link: str) -> str:
    segments = [segment for segment in urlparse(link).path.split("/") if segment]
    if not segments:
        return ""
    return normalize_category(segments[0])


def extract_samakal_article(link: str, fallback_headline: str, cutoff: datetime) -> Optional[Dict[str, str]]:
    try:
        soup = BeautifulSoup(fetch_html(link), "html.parser")
    except requests.RequestException:
        return None

    headline = fallback_headline
    published = None
    category = samakal_category_from_link(link)
    summary = meta_description(soup)
    snippet = ""

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text(strip=True))
        except json.JSONDecodeError:
            continue

        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if node.get("@type") == "NewsArticle":
                headline = clean_text(node.get("headline") or headline)
                category = clean_text(node.get("articleSection") or category)
                summary = clean_text(node.get("description") or summary)
                snippet = clean_text(node.get("articleBody") or snippet)
                published = parse_site_datetime(node.get("datePublished", ""))
            elif node.get("@type") == "BreadcrumbList":
                breadcrumb_names = []
                for item in node.get("itemListElement", []):
                    name = clean_text(item.get("name") if isinstance(item, dict) else "")
                    if name and name.lower() != "home":
                        breadcrumb_names.append(name)
                if len(breadcrumb_names) > 1:
                    category = breadcrumb_names[-2]

    if not headline:
        title = soup.select_one("h1")
        headline = clean_text(title.get_text(" ", strip=True) if title else "")
    if not snippet:
        snippet = body_snippet(soup, "article p, .news-details p, .details p, .DNewsDetails p, .content p")

    if not published or not headline or published < cutoff:
        return None

    return {
        "Headline": headline,
        "Link": link,
        "PublishedTime": published.isoformat(),
        "Publisher": "Samakal",
        "Category": category,
        "Summary": summary,
        "BodySnippet": snippet or summary,
    }


def samakal_latest_candidates(page_url: str) -> Iterable[Tuple[str, str]]:
    soup = BeautifulSoup(fetch_html(page_url), "html.parser")
    seen = set()
    for anchor in soup.select('a[href*="/article/"]'):
        href = anchor.get("href")
        if not href:
            continue
        link = urljoin("https://samakal.com/", href)
        if link in seen or not re.search(r"/article/\d+/", urlparse(link).path):
            continue
        headline = clean_text(anchor.get_text(" ", strip=True), limit=250)
        seen.add(link)
        yield link, headline


def scrape_samakal(cutoff: datetime, max_pages: int = 12, article_cache: Optional[Dict[str, Dict[str, str]]] = None) -> List[Dict[str, str]]:
    articles: List[Dict[str, str]] = []
    seen = set()

    for page in range(1, max_pages + 1):
        page_url = "https://samakal.com/latest/news" if page == 1 else f"https://samakal.com/latest/news?page={page}"
        candidates = list(samakal_latest_candidates(page_url))
        if not candidates:
            break

        oldest_on_page: Optional[datetime] = None
        uncached_candidates: Dict[str, str] = {}
        for link, headline in candidates:
            if link in seen:
                continue
            seen.add(link)

            cached = cached_article(link, article_cache)
            if cached and cached.get("Publisher") == "Samakal":
                cached_published = datetime.fromisoformat(cached["PublishedTime"]).astimezone(DHAKA)
                oldest_on_page = cached_published if oldest_on_page is None else min(oldest_on_page, cached_published)
                if cached_published >= cutoff and has_match_context(cached):
                    articles.append(cached)
                continue

            uncached_candidates[link] = headline

        with ThreadPoolExecutor(max_workers=max(1, SAMAKAL_WORKERS)) as executor:
            futures = {
                executor.submit(extract_samakal_article, link, headline, cutoff): link
                for link, headline in uncached_candidates.items()
            }
            for future in as_completed(futures):
                article = future.result()
                if not article:
                    continue
                if article_cache is not None:
                    article_cache[futures[future]] = article

                published = datetime.fromisoformat(article["PublishedTime"]).astimezone(DHAKA)
                oldest_on_page = published if oldest_on_page is None else min(oldest_on_page, published)
                articles.append(article)

        if oldest_on_page is not None and oldest_on_page < cutoff:
            break

    return articles

BONIK_BARTA_FILTER_IDS = (52, 41)
BONIK_BARTA_SEARCH_PAGE_SIZE = 20


def bonik_barta_link(url_path: str) -> str:
    parts = [part for part in (url_path or "").split("/") if part]
    if not parts:
        return "https://www.bonikbarta.com/"
    if parts[0] == "en":
        return "https://en.bonikbarta.com/" + "/".join(parts[1:])
    if parts[0] == "home":
        return "https://www.bonikbarta.com/" + "/".join(parts[1:])
    return "https://www.bonikbarta.com/" + "/".join(parts)


def bonik_barta_article_from_post(post: Dict, cutoff: datetime) -> Optional[Dict[str, str]]:
    published = parse_site_datetime(post.get("first_published_at", ""))
    title = clean_text(post.get("title", ""), limit=300)
    url_path = post.get("url_path", "")
    if not published or not title or not url_path or published < cutoff:
        return None

    link = bonik_barta_link(url_path)
    summary = clean_text(BeautifulSoup(post.get("summary") or "", "html.parser").get_text(" ", strip=True))
    category = clean_text(
        post.get("primary_category_title")
        or post.get("primary_category_slug")
        or path_category_from_link(link)
    )
    return {
        "Headline": title,
        "Link": link,
        "PublishedTime": published.isoformat(),
        "Publisher": "Bonik Barta",
        "Category": category,
        "Summary": summary,
        "BodySnippet": summary,
    }


def scrape_bonik_barta(cutoff: datetime, max_pages: int = 25, article_cache: Optional[Dict[str, Dict[str, str]]] = None) -> List[Dict[str, str]]:
    articles: List[Dict[str, str]] = []
    seen = set()

    for page in range(1, max_pages + 1):
        url = "https://www.bonikbarta.com/api/search?query=" if page == 1 else f"https://www.bonikbarta.com/api/search?query=&page={page}"
        payload = fetch_json(url)
        posts = payload.get("posts") or []
        if not posts:
            break

        oldest_on_page: Optional[datetime] = None
        for post in posts:
            published = parse_site_datetime(post.get("first_published_at", ""))
            if published:
                oldest_on_page = published if oldest_on_page is None else min(oldest_on_page, published)

            article = bonik_barta_article_from_post(post, cutoff)
            if not article:
                continue

            link = article["Link"]
            if link in seen:
                continue
            seen.add(link)

            cached = cached_article(link, article_cache)
            if cached and cached.get("Publisher") == "Bonik Barta" and has_match_context(cached):
                articles.append(cached)
                continue

            if article_cache is not None:
                article_cache[link] = article
            articles.append(article)

        if len(posts) < BONIK_BARTA_SEARCH_PAGE_SIZE or (oldest_on_page is not None and oldest_on_page < cutoff):
            break

    return articles


def decode_embedded_js_value(value: str) -> str:
    cleaned = (
        (value or "")
        .replace(r"\/", "/")
        .replace(r"\u0026", "&")
        .replace(r"\"", '"')
        .replace(r"\n", " ")
        .replace(r"\r", " ")
        .replace(r"\t", " ")
    )
    return clean_text(cleaned)


def escaped_js_field(chunk: str, field: str, limit: int = 1200) -> str:
    match = re.search(rf'\\"{re.escape(field)}\\":\\"(.*?)\\"', chunk, re.DOTALL)
    if not match:
        return ""
    return clean_text(decode_embedded_js_value(match.group(1)), limit=limit)


BENGALI_DIGIT_TRANSLATION = str.maketrans("০১২৩৪৫৬৭৮৯", "0123456789")
BANGLA_MONTHS = {
    "জানুয়ারি": 1,
    "জানুয়ারি": 1,
    "ফেব্রুয়ারি": 2,
    "ফেব্রুয়ারি": 2,
    "মার্চ": 3,
    "এপ্রিল": 4,
    "মে": 5,
    "জুন": 6,
    "জুলাই": 7,
    "আগস্ট": 8,
    "সেপ্টেম্বর": 9,
    "অক্টোবর": 10,
    "নভেম্বর": 11,
    "ডিসেম্বর": 12,
}


def dhaka_post_time_from_created_at(value: str) -> Optional[datetime]:
    text = clean_text(value or "").translate(BENGALI_DIGIT_TRANSLATION)
    match = re.search(r"(\d{1,2})\s+([^,\s]+)\s+(\d{4}),?\s+(\d{1,2}):(\d{2})", text)
    if not match:
        return None

    day, month_name, year, hour, minute = match.groups()
    month = BANGLA_MONTHS.get(month_name)
    if not month:
        return None

    try:
        return datetime(int(year), month, int(day), int(hour), int(minute), tzinfo=DHAKA)
    except ValueError:
        return None


def dhaka_post_time_from_image(image_url: str) -> Optional[datetime]:
    match = re.search(r"(?<!\d)(20\d{12})(?!\d)", image_url or "")
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=DHAKA)
    except ValueError:
        return None


def dhaka_post_row_published(row: Dict) -> Optional[datetime]:
    return (
        dhaka_post_time_from_created_at(str(row.get("CreatedAtBangla") or ""))
        or dhaka_post_time_from_created_at(str(row.get("created_at_bangla") or ""))
        or dhaka_post_time_from_image(
            clean_text(
                row.get("ImagePath")
                or row.get("ImagePathSm")
                or row.get("ImagePathMd")
                or row.get("ImagePathXs")
                or "",
                limit=500,
            )
        )
    )


def dhaka_post_article_from_row(row: Dict, cutoff: datetime) -> Optional[Dict[str, str]]:
    headline = clean_text(row.get("Heading") or row.get("headline") or "", limit=300)
    link = clean_text(row.get("URL") or row.get("url") or "", limit=500)
    if not headline or not link or "dhakapost.com" not in urlparse(link).netloc:
        return None

    category = path_category_from_link(link)
    if category.lower() in {"jobs career", "jobs"}:
        return None

    published = dhaka_post_row_published(row)
    if not published or published < cutoff:
        return None

    summary = clean_text(row.get("Brief") or row.get("brief") or "")
    return {
        "Headline": headline,
        "Link": link,
        "PublishedTime": published.isoformat(),
        "Publisher": "Dhaka Post",
        "Category": category,
        "Summary": summary,
        "BodySnippet": summary,
    }


def dhaka_post_rows_from_embedded_html(html: str) -> List[Dict]:
    decoder = json.JSONDecoder()
    parsed_rows = []
    for script in BeautifulSoup(html, "html.parser").find_all("script"):
        text = script.string or script.get_text()
        for match in re.finditer(r"self\.__next_f\.push\(", text):
            try:
                payload, _ = decoder.raw_decode(text[match.end():].lstrip())
            except (ValueError, TypeError):
                continue
            if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], str):
                continue
            for start in re.finditer(r'\{\s*"(?:Id|Heading)"\s*:', payload[1]):
                try:
                    row, _ = decoder.raw_decode(payload[1][start.start():])
                except ValueError:
                    continue
                if isinstance(row, dict) and row.get("Heading") and row.get("URL"):
                    parsed_rows.append(row)
    if parsed_rows:
        return parsed_rows
    rows = []
    starts = [match.start() for match in re.finditer(r'\{\\"Heading\\":', html)]
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else start + 4000
        chunk = html[start:end]
        rows.append(
            {
                "Heading": escaped_js_field(chunk, "Heading", limit=300),
                "URL": escaped_js_field(chunk, "URL", limit=500),
                "CreatedAtBangla": escaped_js_field(chunk, "CreatedAtBangla", limit=120),
                "ImagePath": (
                    escaped_js_field(chunk, "ImagePath", limit=500)
                    or escaped_js_field(chunk, "ImagePathSm", limit=500)
                    or escaped_js_field(chunk, "ImagePathMd", limit=500)
                    or escaped_js_field(chunk, "ImagePathXs", limit=500)
                ),
                "Brief": escaped_js_field(chunk, "Brief"),
            }
        )
    return rows


def dhaka_post_latest_action_id(html: str) -> Optional[str]:
    urls = sorted(
        set(
            re.findall(
                r'https://cdn\.dhakapost\.com/_next/static/[^"\\]+/latest-news/page-[^"\\]+?\.js\?dpl=[^"\\]+',
                html,
            )
        )
    )
    if not urls:
        urls = [
            tag["src"] for tag in BeautifulSoup(html, "html.parser").select("script[src]")
            if tag["src"].startswith("https://cdn.dhakapost.com/_next/static/")
        ][:24]
    for url in urls:
        try:
            script = fetch_html(url, timeout=10, max_retries=1)
        except (requests.RequestException, ScrapeBlockedError):
            continue
        match = re.search(r'createServerReference\)\("([0-9a-f]+)".*?"getMoreLatest"', script)
        if match:
            return match.group(1)
    return None


def dhaka_post_more_latest(action_id: str, limit: int, offset: int) -> List[Dict]:
    response = polite_post(
        "https://www.dhakapost.com/latest-news",
        headers={
            "Accept": "text/x-component",
            "Content-Type": "text/plain;charset=UTF-8",
            "Next-Action": action_id,
            "Origin": "https://www.dhakapost.com",
            "Referer": "https://www.dhakapost.com/latest-news",
        },
        data=json.dumps([limit, offset]),
    )
    text = response.content.decode("utf-8", "replace")
    for line in text.splitlines():
        _, separator, value = line.partition(":")
        if not separator:
            continue
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            continue
        data = payload.get("data") if isinstance(payload, dict) else None
        contents = data.get("contents") if isinstance(data, dict) else None
        if isinstance(contents, list):
            return contents
    return []


def scrape_dhaka_post(cutoff: datetime, article_cache: Optional[Dict[str, Dict[str, str]]] = None) -> List[Dict[str, str]]:
    html = fetch_html("https://www.dhakapost.com/latest-news")
    articles: List[Dict[str, str]] = []
    seen = set()
    page_size = 12
    try:
        action_id = dhaka_post_latest_action_id(html)
    except (requests.RequestException, ScrapeBlockedError):
        action_id = None
    offset = 0

    while True:
        rows = dhaka_post_rows_from_embedded_html(html) if offset == 0 else []
        if action_id and (offset or not rows):
            try:
                rows = dhaka_post_more_latest(action_id, page_size, offset)
            except (requests.RequestException, ScrapeBlockedError) as exc:
                print(f"Scrape warning: Dhaka Post pagination: {exc}", file=sys.stderr)
                break
        if not rows:
            break

        oldest_on_page: Optional[datetime] = None
        previous_seen = len(seen)
        for row in rows:
            row_published = dhaka_post_row_published(row)
            if row_published:
                oldest_on_page = row_published if oldest_on_page is None else min(oldest_on_page, row_published)

            article = dhaka_post_article_from_row(row, cutoff)
            if not article:
                continue

            link = article["Link"]
            if link in seen:
                continue
            seen.add(link)

            cached = cached_article(link, article_cache)
            if cached and cached.get("Publisher") == "Dhaka Post" and has_match_context(cached):
                articles.append(cached)
                continue

            if article_cache is not None:
                article_cache[link] = article
            articles.append(article)

        if not action_id or len(rows) < page_size or len(seen) == previous_seen or offset >= 1200 or (oldest_on_page is not None and oldest_on_page < cutoff):
            break
        offset += len(rows)

    return articles


def json_ld_nodes(data) -> List[Dict]:
    if isinstance(data, dict) and isinstance(data.get("@graph"), list):
        data = data["@graph"]
    if isinstance(data, list):
        return [node for node in data if isinstance(node, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def json_ld_has_type(node: Dict, type_name: str) -> bool:
    node_type = node.get("@type")
    if isinstance(node_type, list):
        return type_name in node_type
    return node_type == type_name




SOURCE_REGISTRY: List[Dict[str, object]] = [
    {"id": "prothomalo", "publisher": "Prothom Alo", "scraper": scrape_prothomalo},
    {"id": "tbs", "publisher": "The Business Standard", "scraper": scrape_tbs},
    {"id": "daily_star", "publisher": "The Daily Star", "scraper": scrape_daily_star},
    {"id": "samakal", "publisher": "Samakal", "scraper": scrape_samakal},
    {"id": "bonik_barta", "publisher": "Bonik Barta", "scraper": scrape_bonik_barta},
    {"id": "dhaka_post", "publisher": "Dhaka Post", "scraper": scrape_dhaka_post},
]

SOURCE_LOOKUP = {source["id"]: source for source in SOURCE_REGISTRY}


def publisher_order() -> List[str]:
    return [str(source["publisher"]) for source in SOURCE_REGISTRY]


def scrape_sources(
    cutoff: datetime,
    source_ids: Optional[List[str]] = None,
    article_cache: Optional[Dict[str, Dict[str, str]]] = None,
    source_status: Optional[Dict] = None,
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

        try:
            source_articles = scraper(cutoff, article_cache=article_cache)
        except Exception as exc:
            publisher = source.get("publisher", source_id)
            if source_status is not None:
                source_status[str(publisher)] = {"status": "error", "error": str(exc), "count": 0}
            print(f"Scrape warning: {publisher}: {exc}", file=sys.stderr)
            continue

        if source_status is not None:
            source_status[str(source["publisher"])] = {"status": "ready" if source_articles else "empty", "count": len(source_articles)}

        for article in source_articles:
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
