import json
import math
import os
import hashlib
import re
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, render_template_string, request

from news_scraper import DHAKA, dhaka_now, publisher_order, scrape_sources


REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", str(10 * 60)))
WINDOW_HOURS = float(os.getenv("WINDOW_HOURS", "5"))
LIVE_HOURS = 1.0
ARTICLE_CACHE_HOURS = float(os.getenv("ARTICLE_CACHE_HOURS", "24"))
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_FILE = DATA_DIR / "latest_news.json"
ENV_FILE = Path(__file__).resolve().parent / ".env"


def load_dotenv() -> None:
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small").strip() or "text-embedding-3-small"
OPENAI_MATCH_MODEL = os.getenv("OPENAI_MATCH_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
BASELINE_PUBLISHER = os.getenv("BASELINE_PUBLISHER", "The Daily Star").strip() or "The Daily Star"
MATCH_CANDIDATE_LIMIT = int(os.getenv("MATCH_CANDIDATE_LIMIT", "3"))
MATCH_MIN_SIMILARITY = float(os.getenv("MATCH_MIN_SIMILARITY", "0.58"))
MATCH_MIN_CONFIDENCE = float(os.getenv("MATCH_MIN_CONFIDENCE", "0.75"))
MATCH_BATCH_SIZE = int(os.getenv("MATCH_BATCH_SIZE", "5"))
OPENAI_TIMEOUT_SECONDS = int(os.getenv("OPENAI_TIMEOUT_SECONDS", "120"))
OPENAI_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "3"))
SOURCE_PUBLISHERS = publisher_order()
PUBLISHER_ORDER = [BASELINE_PUBLISHER] + [name for name in SOURCE_PUBLISHERS if name != BASELINE_PUBLISHER]
COMPETITOR_PUBLISHERS = [name for name in PUBLISHER_ORDER if name != BASELINE_PUBLISHER]
ACTION_STATUSES = ("New", "Watching", "Assigned", "Reported", "Ignored")
CATEGORY_ALIASES = {
    "Politics / রাজনীতি": {
        "politics",
        "political",
        "রাজনীতি",
    },
    "Bangladesh / বাংলাদেশ": {
        "bangladesh",
        "national",
        "nation",
        "country",
        "whole country",
        "capital",
        "বাংলাদেশ",
        "জাতীয়",
        "জাতীয়",
        "সারাদেশ",
        "রাজধানী",
        "দেশের বার্তা",
    },
    "World / বিশ্ব": {
        "world",
        "international",
        "global",
        "বিশ্ব",
        "আন্তর্জাতিক",
        "ইউরোপ",
        "আফ্রিকা",
        "দক্ষিণ এশিয়া",
        "দক্ষিণ এশিয়া",
        "মধ্যপ্রাচ্য",
    },
    "Business / বাণিজ্য": {
        "business",
        "economy",
        "economics",
        "economic",
        "market",
        "markets",
        "trade",
        "বানিজ্য",
        "বাণিজ্য",
        "অর্থনীতি",
    },
    "Sports / খেলা": {
        "sport",
        "sports",
        "game",
        "games",
        "খেলা",
        "ফুটবল",
        "ক্রিকেট",
        "ক্রীড়া",
        "ক্রীড়া",
    },
    "Entertainment / বিনোদন": {
        "entertainment",
        "showbiz",
        "talkies",
        "ott",
        "culture",
        "arts",
        "বিনোদন",
        "সংস্কৃতি",
    },
    "Technology / প্রযুক্তি": {
        "technology",
        "tech",
        "scitech",
        "startup",
        "startups",
        "প্রযুক্তি",
    },
}

app = Flask(__name__)
state_lock = threading.Lock()
background_lock = threading.Lock()
background_started = False
state: Dict = {
    "articles": [],
    "last_updated": None,
    "next_run": None,
    "refreshing": False,
    "error": None,
    "article_cache": {},
    "embedding_cache": {},
    "match_cache": {},
    "comparison": {},
    "actions": {},
}


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(DHAKA)


def hours_limit(value: float) -> float:
    if value <= 0:
        return LIVE_HOURS
    return min(value, WINDOW_HOURS)


def filter_recent_articles(articles: List[Dict], hours: float) -> List[Dict]:
    cutoff = dhaka_now() - timedelta(hours=hours_limit(hours))
    filtered = [article for article in articles if parse_time(article["PublishedTime"]) >= cutoff]
    filtered.sort(key=lambda item: item["PublishedTime"], reverse=True)
    return filtered


def prune_articles(articles: List[Dict]) -> List[Dict]:
    return filter_recent_articles(articles, WINDOW_HOURS)


def dedupe_articles(articles: List[Dict]) -> List[Dict]:
    deduped = {}
    for article in articles:
        link = article.get("Link")
        if link:
            deduped[link] = article
    return list(deduped.values())


def sorted_publisher_counts(articles: List[Dict]) -> Dict[str, int]:
    counts = {name: 0 for name in PUBLISHER_ORDER}
    for article in articles:
        publisher = article.get("Publisher", "")
        counts[publisher] = counts.get(publisher, 0) + 1
    return counts


def normalize_category_key(value: str) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"[&/,_-]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def canonical_category_name(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "Uncategorized"

    category_key = normalize_category_key(raw)
    for canonical_name, aliases in CATEGORY_ALIASES.items():
        if category_key in aliases:
            return canonical_name
    return raw


def top_categories(articles: List[Dict], limit: int = 8) -> List[Dict]:
    counts = Counter(canonical_category_name(article.get("Category", "")) for article in articles)
    return [{"name": name, "count": count} for name, count in counts.most_common(limit)]


def latest_article_time(articles: List[Dict]) -> str:
    if not articles:
        return ""
    return max(article["PublishedTime"] for article in articles)


def hourly_breakdown(articles: List[Dict], hours: float) -> List[Dict]:
    bucket_map: Dict[str, Dict[str, int]] = {}
    cutoff = dhaka_now() - timedelta(hours=hours_limit(hours))
    for article in articles:
        published = parse_time(article["PublishedTime"])
        if published < cutoff:
            continue
        bucket = published.replace(minute=0, second=0, microsecond=0)
        key = bucket.isoformat()
        if key not in bucket_map:
            bucket_map[key] = {"time": key, "label": bucket.strftime("%d %b, %H:%M"), "total": 0}
            for publisher in PUBLISHER_ORDER:
                bucket_map[key][publisher] = 0
        bucket_map[key]["total"] += 1
        bucket_map[key][article["Publisher"]] = bucket_map[key].get(article["Publisher"], 0) + 1
    return [bucket_map[key] for key in sorted(bucket_map.keys(), reverse=True)]


def shared_category_breakdown(articles: List[Dict], limit: int = 8) -> List[Dict]:
    category_map: Dict[str, Dict[str, int]] = defaultdict(lambda: {name: 0 for name in PUBLISHER_ORDER})
    for article in articles:
        category = canonical_category_name(article.get("Category", ""))
        category_map[category][article["Publisher"]] = category_map[category].get(article["Publisher"], 0) + 1

    rows = []
    for category, counts in category_map.items():
        total = sum(counts.values())
        rows.append({"category": category, "total": total, "publishers": counts})

    rows.sort(key=lambda item: (item["total"], item["category"]), reverse=True)
    return rows[:limit]


def action_payload(value: Optional[Dict]) -> Dict[str, str]:
    value = value or {}
    status = value.get("status") or "New"
    if status not in ACTION_STATUSES:
        status = "New"
    return {"status": status, "note": str(value.get("note") or "")[:500]}


def action_for_link(actions: Dict[str, Dict], link: str) -> Dict[str, str]:
    return action_payload((actions or {}).get(link))


def enrich_articles(articles: List[Dict], actions: Dict[str, Dict], comparison: Optional[Dict] = None) -> List[Dict]:
    enriched = []
    for article in articles:
        row = dict(article)
        canonical = canonical_category_name(row.get("Category", ""))
        action = action_for_link(actions, row.get("Link", ""))
        row["CanonicalCategory"] = canonical
        row["ActionStatus"] = action["status"]
        row["ActionNote"] = action["note"]
        enriched.append(row)
    return enriched


def command_metrics(articles: List[Dict]) -> Dict[str, int]:
    cutoff = dhaka_now() - timedelta(minutes=15)
    new_items = 0
    competitor_only = 0
    daily_star_last_hour = 0
    active_actions = 0
    categories_with_baseline = {
        article.get("CanonicalCategory")
        for article in articles
        if article.get("Publisher") == BASELINE_PUBLISHER
    }

    for article in articles:
        try:
            published = parse_time(article["PublishedTime"])
        except (KeyError, ValueError):
            continue
        if published >= cutoff:
            new_items += 1
        if article.get("Publisher") == BASELINE_PUBLISHER and published >= dhaka_now() - timedelta(hours=LIVE_HOURS):
            daily_star_last_hour += 1
        if article.get("Publisher") != BASELINE_PUBLISHER and article.get("CanonicalCategory") not in categories_with_baseline:
            competitor_only += 1
        if article.get("ActionStatus") in {"Watching", "Assigned"}:
            active_actions += 1

    return {
        "new_15m": new_items,
        "competitor_only": competitor_only,
        "daily_star_last_hour": daily_star_last_hour,
        "active_actions": active_actions,
    }


def live_signal(articles: List[Dict]) -> List[Dict]:
    return [
        {
            "headline": article["Headline"],
            "publisher": article["Publisher"],
            "category": article.get("Category") or "Uncategorized",
            "canonical_category": article.get("CanonicalCategory") or canonical_category_name(article.get("Category", "")),
            "action_status": article.get("ActionStatus", "New"),
            "published_time": article["PublishedTime"],
            "link": article["Link"],
        }
        for article in articles[:12]
    ]


def empty_comparison(status: str = "disabled", error: Optional[str] = None) -> Dict:
    return {
        "baseline_publisher": BASELINE_PUBLISHER,
        "comparison_status": status,
        "comparison_error": error,
        "window_hours": WINDOW_HOURS,
        "coverage_gaps": [],
        "comparison_summary": {"covered": 0, "needs_review": 0, "potential_gap": 0, "competitor_total": 0},
        "missed_by_source": [],
        "category_pressure": [],
        "architecture_note": (
            "Sources are registry-based. Add a scraper entry and publisher name, and the same "
            "comparison model can evaluate future outlets without UI rewrites."
        ),
    }


def embedding_text(article: Dict) -> str:
    category = article.get("Category") or "Uncategorized"
    summary = article.get("Summary") or ""
    snippet = article.get("BodySnippet") or ""
    return f'{article["Headline"]}\nCategory: {category}\nSummary: {summary}\nArticle text: {snippet}'


def request_embeddings(texts: List[str]) -> List[List[float]]:
    response = openai_post(
        "https://api.openai.com/v1/embeddings",
        {"model": OPENAI_EMBEDDING_MODEL, "input": texts},
        timeout=OPENAI_TIMEOUT_SECONDS,
    )
    payload = response.json()
    items = sorted(payload.get("data", []), key=lambda item: item.get("index", 0))
    return [item["embedding"] for item in items]


def openai_post(url: str, payload: Dict, timeout: int) -> requests.Response:
    last_error = None
    for attempt in range(OPENAI_MAX_RETRIES):
        try:
            response = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=timeout,
            )
            if response.status_code in (408, 409, 429) or response.status_code >= 500:
                response.raise_for_status()
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            if attempt == OPENAI_MAX_RETRIES - 1:
                raise
            time.sleep(min(20, 2 ** attempt))
    raise last_error or RuntimeError("OpenAI request failed")


def ensure_embeddings(articles: List[Dict], embedding_cache: Dict[str, Dict]) -> Tuple[Dict[str, List[float]], Dict[str, Dict]]:
    updated_cache = dict(embedding_cache or {})
    embeddings: Dict[str, List[float]] = {}
    pending_texts: List[str] = []
    pending_links: List[str] = []

    for article in articles:
        link = article["Link"]
        text = embedding_text(article)
        cached = updated_cache.get(link)
        if cached and cached.get("model") == OPENAI_EMBEDDING_MODEL and cached.get("text") == text:
            embeddings[link] = cached.get("embedding", [])
            continue
        pending_links.append(link)
        pending_texts.append(text)

    if pending_texts:
        vectors = request_embeddings(pending_texts)
        for link, text, vector in zip(pending_links, pending_texts, vectors):
            updated_cache[link] = {
                "model": OPENAI_EMBEDDING_MODEL,
                "text": text,
                "embedding": vector,
                "updated_at": dhaka_now().isoformat(),
            }
            embeddings[link] = vector

    for article in articles:
        link = article["Link"]
        if link not in embeddings:
            embeddings[link] = updated_cache.get(link, {}).get("embedding", [])

    return embeddings, updated_cache


def cosine_similarity(left: List[float], right: List[float]) -> float:
    if not left or not right:
        return 0.0
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if not left_norm or not right_norm:
        return 0.0
    return dot / (left_norm * right_norm)


def article_match_payload(article: Dict) -> Dict:
    return {
        "headline": article.get("Headline", ""),
        "publisher": article.get("Publisher", ""),
        "category": article.get("Category") or "Uncategorized",
        "summary": article.get("Summary", ""),
        "article_text_snippet": article.get("BodySnippet", ""),
        "published_time": article.get("PublishedTime", ""),
        "link": article.get("Link", ""),
    }


def match_cache_key(competitor: Dict, candidates: List[Tuple[Dict, float]]) -> str:
    fingerprint = {
        "model": OPENAI_MATCH_MODEL,
        "competitor": article_match_payload(competitor),
        "candidates": [
            {"candidate": article_match_payload(candidate), "score": round(score, 4)}
            for candidate, score in candidates
        ],
    }
    raw = json.dumps(fingerprint, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def exact_match_candidate_payload(candidates: List[Tuple[Dict, float]]) -> List[Dict]:
    return [
        {
            "candidate_id": index,
            "similarity": round(score, 4),
            **article_match_payload(candidate),
        }
        for index, (candidate, score) in enumerate(candidates)
    ]


def normalize_match_result(result: Dict) -> Dict:
    candidate_id = result.get("candidate_id")
    if isinstance(candidate_id, str) and candidate_id.isdigit():
        candidate_id = int(candidate_id)
    return {
        "exact_match": bool(result.get("exact_match")),
        "candidate_id": candidate_id,
        "confidence": float(result.get("confidence") or 0),
        "reason": str(result.get("reason") or ""),
    }


def request_exact_match_batch(items: List[Dict]) -> Dict[str, Dict]:
    payload = {
        "items": [
            {
                "item_id": item["item_id"],
                "competitor_story": article_match_payload(item["competitor"]),
                "daily_star_candidates": exact_match_candidate_payload(item["candidates"]),
            }
            for item in items
        ]
    }
    response = openai_post(
        "https://api.openai.com/v1/chat/completions",
        {
            "model": OPENAI_MATCH_MODEL,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a strict newsroom coverage analyst. Decide whether a competitor "
                        "story and a Daily Star candidate are the same exact news event/story. "
                        "Use headline, summary, article text snippet, category, source, and time. "
                        "Same topic, same beat, same person, same country, same issue, or similar "
                        "wording is not enough. Mark exact_match true only when article details "
                        "show they report the same event, development, announcement, incident, "
                        "match, case, decision, or market move. If either article lacks enough "
                        "detail, return exact_match false with low confidence. Return only JSON "
                        "with a results array. Each result must "
                        "include item_id, exact_match, candidate_id, confidence, reason."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False),
                },
            ],
            "temperature": 0,
        },
        timeout=OPENAI_TIMEOUT_SECONDS,
    )
    content = response.json()["choices"][0]["message"]["content"]
    result = json.loads(content)
    rows = result.get("results", [])
    return {
        str(row.get("item_id")): normalize_match_result(row)
        for row in rows
        if isinstance(row, dict) and row.get("item_id") is not None
    }


def verify_exact_matches(items: List[Dict], match_cache: Dict[str, Dict]) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    updated_cache = dict(match_cache)
    results: Dict[str, Dict] = {}
    pending = []

    for item in items:
        competitor = item["competitor"]
        candidates = item["candidates"]
        if not candidates:
            results[competitor["Link"]] = {
                "exact_match": False,
                "candidate_id": None,
                "confidence": 0,
                "reason": "No close Daily Star candidate.",
            }
            continue

        cache_key = match_cache_key(competitor, candidates)
        cached = updated_cache.get(cache_key)
        if cached:
            results[competitor["Link"]] = cached
            continue

        pending.append({**item, "cache_key": cache_key})

    for index in range(0, len(pending), MATCH_BATCH_SIZE):
        batch = pending[index : index + MATCH_BATCH_SIZE]
        batch_payload = [
            {"item_id": item["competitor"]["Link"], "competitor": item["competitor"], "candidates": item["candidates"]}
            for item in batch
        ]
        try:
            batch_results = request_exact_match_batch(batch_payload)
        except requests.RequestException as exc:
            batch_results = {
                item["competitor"]["Link"]: {
                    "exact_match": False,
                    "candidate_id": None,
                    "confidence": 0,
                    "reason": f"OpenAI exact-match check timed out or failed: {exc}",
                }
                for item in batch
            }
        for item in batch:
            competitor_link = item["competitor"]["Link"]
            result = batch_results.get(competitor_link) or {
                "exact_match": False,
                "candidate_id": None,
                "confidence": 0,
                "reason": "No exact same-event match returned.",
            }
            cached_result = {**result, "checked_at": dhaka_now().isoformat()}
            updated_cache[item["cache_key"]] = cached_result
            results[competitor_link] = cached_result

    return results, updated_cache


def prune_match_cache(cache: Dict[str, Dict], limit: int = 3000) -> Dict[str, Dict]:
    if len(cache) <= limit:
        return cache
    rows = sorted(cache.items(), key=lambda item: item[1].get("checked_at", ""), reverse=True)
    return dict(rows[:limit])


def prune_embedding_cache(cache: Dict[str, Dict], articles: List[Dict]) -> Dict[str, Dict]:
    allowed_links = {article["Link"] for article in articles if article.get("Link")}
    return {link: payload for link, payload in cache.items() if link in allowed_links}


def prune_article_cache(cache: Dict[str, Dict], articles: Optional[List[Dict]] = None) -> Dict[str, Dict]:
    cutoff = dhaka_now() - timedelta(hours=max(WINDOW_HOURS, ARTICLE_CACHE_HOURS))
    pruned = {}
    for link, payload in (cache or {}).items():
        try:
            published = parse_time(payload["PublishedTime"])
        except (KeyError, ValueError):
            continue
        if published >= cutoff:
            pruned[link] = payload
    return pruned


def update_article_cache(cache: Dict[str, Dict], articles: List[Dict]) -> Dict[str, Dict]:
    updated_cache = dict(cache or {})
    for article in articles:
        link = article.get("Link")
        if link:
            updated_cache[link] = article
    return prune_article_cache(updated_cache, articles)


def build_category_pressure(articles: List[Dict]) -> List[Dict]:
    baseline_counts = Counter()
    competitor_counts = Counter()

    for article in articles:
        category = canonical_category_name(article.get("Category", ""))
        if article["Publisher"] == BASELINE_PUBLISHER:
            baseline_counts[category] += 1
        else:
            competitor_counts[category] += 1

    rows = []
    for category, competitor_count in competitor_counts.items():
        baseline_count = baseline_counts.get(category, 0)
        delta = competitor_count - baseline_count
        if delta <= 0:
            continue
        rows.append(
            {
                "category": category,
                "competitor_count": competitor_count,
                "baseline_count": baseline_count,
                "delta": delta,
            }
        )

    rows.sort(key=lambda item: (item["delta"], item["competitor_count"], item["category"]), reverse=True)
    return rows[:10]


def build_comparison(articles: List[Dict], embedding_cache: Dict[str, Dict], match_cache: Dict[str, Dict]) -> Tuple[Dict, Dict[str, Dict], Dict[str, Dict]]:
    if not OPENAI_API_KEY:
        return empty_comparison(status="disabled"), prune_embedding_cache(embedding_cache, articles), match_cache

    window_articles = filter_recent_articles(articles, WINDOW_HOURS)
    baseline_articles = [article for article in window_articles if article["Publisher"] == BASELINE_PUBLISHER]
    competitor_articles = [article for article in window_articles if article["Publisher"] != BASELINE_PUBLISHER]

    comparison = empty_comparison(status="ready")
    comparison["comparison_summary"]["competitor_total"] = len(competitor_articles)
    comparison["category_pressure"] = build_category_pressure(window_articles)

    if not baseline_articles or not competitor_articles:
        return comparison, prune_embedding_cache(embedding_cache, window_articles), prune_match_cache(match_cache)

    relevant_articles = baseline_articles + competitor_articles
    embeddings, updated_cache = ensure_embeddings(relevant_articles, embedding_cache)
    updated_match_cache = dict(match_cache or {})
    missed_counter = Counter()
    status_counter = Counter()
    coverage_rows = []
    candidate_map = {}

    for competitor in competitor_articles:
        competitor_vector = embeddings.get(competitor["Link"], [])
        scored_candidates = []

        for baseline in baseline_articles:
            score = cosine_similarity(competitor_vector, embeddings.get(baseline["Link"], []))
            if score >= MATCH_MIN_SIMILARITY:
                scored_candidates.append((baseline, score))

        scored_candidates.sort(key=lambda item: item[1], reverse=True)
        candidate_map[competitor["Link"]] = scored_candidates[:MATCH_CANDIDATE_LIMIT]

    verifier_results, updated_match_cache = verify_exact_matches(
        [
            {"competitor": competitor, "candidates": candidate_map.get(competitor["Link"], [])}
            for competitor in competitor_articles
        ],
        updated_match_cache,
    )

    for competitor in competitor_articles:
        candidates = candidate_map.get(competitor["Link"], [])
        best_match = candidates[0][0] if candidates else None
        best_score = round(candidates[0][1], 4) if candidates else 0.0
        verifier_result = verifier_results.get(
            competitor["Link"],
            {"exact_match": False, "candidate_id": None, "confidence": 0, "reason": "No verifier result."},
        )

        matched_index = verifier_result.get("candidate_id")
        verifier_confidence = float(verifier_result.get("confidence") or 0)
        if (
            verifier_result.get("exact_match")
            and verifier_confidence >= MATCH_MIN_CONFIDENCE
            and isinstance(matched_index, int)
            and 0 <= matched_index < len(candidates)
        ):
            best_match = candidates[matched_index][0]
            best_score = round(candidates[matched_index][1], 4)
            status = "Covered"
        elif candidates:
            status = "Needs Review"
        else:
            status = "Potential Gap"

        status_counter[status] += 1
        if status != "Covered":
            missed_counter[competitor["Publisher"]] += 1

        coverage_rows.append(
            {
                "Headline": competitor["Headline"],
                "Link": competitor["Link"],
                "PublishedTime": competitor["PublishedTime"],
                "Publisher": competitor["Publisher"],
                "Category": competitor.get("Category") or "Uncategorized",
                "status": status,
                "similarity": best_score,
                "match_confidence": round(float(verifier_result.get("confidence") or 0), 2),
                "match_reason": verifier_result.get("reason", ""),
                "best_match_headline": best_match["Headline"] if best_match else "",
                "best_match_link": best_match["Link"] if best_match else "",
                "best_match_published_time": best_match["PublishedTime"] if best_match else "",
                "best_match_category": (best_match.get("Category") or "Uncategorized") if best_match else "",
            }
        )

    coverage_rows.sort(
        key=lambda item: (
            0 if item["status"] == "Potential Gap" else 1 if item["status"] == "Needs Review" else 2,
            item["similarity"],
            item["PublishedTime"],
        )
    )

    comparison["coverage_gaps"] = coverage_rows
    comparison["comparison_summary"] = {
        "covered": status_counter.get("Covered", 0),
        "needs_review": status_counter.get("Needs Review", 0),
        "potential_gap": status_counter.get("Potential Gap", 0),
        "competitor_total": len(competitor_articles),
    }
    comparison["missed_by_source"] = [
        {"publisher": publisher, "count": missed_counter.get(publisher, 0)}
        for publisher in COMPETITOR_PUBLISHERS
        if publisher in {article["Publisher"] for article in competitor_articles}
    ]
    return comparison, prune_embedding_cache(updated_cache, window_articles), prune_match_cache(updated_match_cache)


def feed_snapshot(hours: float) -> Dict:
    with state_lock:
        state["articles"] = prune_articles(state["articles"])
        base_articles = list(state["articles"])
        comparison = dict(state.get("comparison") or empty_comparison(status="disabled"))
        actions = dict(state.get("actions") or {})
        last_updated = state["last_updated"]
        next_run = state["next_run"]
        refreshing = state["refreshing"]
        error = state["error"]

    articles = enrich_articles(filter_recent_articles(base_articles, hours), actions, comparison)
    live_articles = enrich_articles(filter_recent_articles(base_articles, LIVE_HOURS), actions, comparison)
    competitor_total = sum(
        count for publisher, count in sorted_publisher_counts(articles).items() if publisher != BASELINE_PUBLISHER
    )

    return {
        "articles": articles,
        "count": len(articles),
        "last_updated": last_updated,
        "next_run": next_run,
        "refreshing": refreshing,
        "error": error,
        "window_hours": hours_limit(hours),
        "publisher_counts": sorted_publisher_counts(articles),
        "last_hour_counts": sorted_publisher_counts(live_articles),
        "top_categories": top_categories(articles),
        "live_signal": live_signal(live_articles),
        "command_metrics": command_metrics(live_articles),
        "baseline_publisher": BASELINE_PUBLISHER,
        "publishers": PUBLISHER_ORDER,
        "competitor_total": competitor_total,
    }


def analysis_snapshot(hours: float) -> Dict:
    feed = feed_snapshot(hours)
    articles = feed["articles"]

    with state_lock:
        comparison = dict(state.get("comparison") or empty_comparison(status="disabled"))
        actions = dict(state.get("actions") or {})

    coverage_gaps = enrich_articles(comparison.get("coverage_gaps", []), actions, comparison)

    publisher_summaries = []
    for publisher in PUBLISHER_ORDER:
        publisher_articles = [article for article in articles if article["Publisher"] == publisher]
        live_articles = [
            article
            for article in publisher_articles
            if parse_time(article["PublishedTime"]) >= dhaka_now() - timedelta(hours=LIVE_HOURS)
        ]
        publisher_summaries.append(
            {
                "name": publisher,
                "count": len(publisher_articles),
                "last_hour_count": len(live_articles),
                "latest_published_time": latest_article_time(publisher_articles),
                "top_categories": top_categories(publisher_articles, limit=5),
            }
        )

    payload = {
        "window_hours": feed["window_hours"],
        "count": feed["count"],
        "last_updated": feed["last_updated"],
        "next_run": feed["next_run"],
        "refreshing": feed["refreshing"],
        "error": feed["error"],
        "publisher_counts": feed["publisher_counts"],
        "last_hour_counts": feed["last_hour_counts"],
        "top_categories": feed["top_categories"],
        "hourly_breakdown": hourly_breakdown(articles, hours),
        "shared_categories": shared_category_breakdown(articles),
        "publisher_summaries": publisher_summaries,
        "live_signal": feed["live_signal"],
        "baseline_publisher": BASELINE_PUBLISHER,
        "comparison_status": comparison.get("comparison_status", "disabled"),
        "comparison_error": comparison.get("comparison_error"),
        "coverage_gaps": coverage_gaps,
        "comparison_summary": comparison.get("comparison_summary", {}),
        "missed_by_source": comparison.get("missed_by_source", []),
        "category_pressure": comparison.get("category_pressure", []),
        "architecture_note": comparison.get("architecture_note", empty_comparison()["architecture_note"]),
    }
    return payload


def save_state() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as file:
        json.dump(
            {
                "articles": state["articles"],
                "last_updated": state["last_updated"],
                "next_run": state["next_run"],
                "error": state["error"],
                "article_cache": state.get("article_cache", {}),
                "embedding_cache": state.get("embedding_cache", {}),
                "match_cache": state.get("match_cache", {}),
                "comparison": state.get("comparison", {}),
                "actions": state.get("actions", {}),
            },
            file,
            ensure_ascii=False,
            indent=2,
        )


def load_state() -> None:
    if not DATA_FILE.exists():
        return
    try:
        payload = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return

    with state_lock:
        articles = prune_articles(payload.get("articles", []))
        state["articles"] = articles
        state["last_updated"] = payload.get("last_updated")
        state["next_run"] = payload.get("next_run")
        state["error"] = payload.get("error")
        state["article_cache"] = prune_article_cache(payload.get("article_cache") or {article["Link"]: article for article in articles if article.get("Link")}, articles)
        state["embedding_cache"] = prune_embedding_cache(payload.get("embedding_cache", {}), articles)
        state["match_cache"] = prune_match_cache(payload.get("match_cache", {}))
        state["comparison"] = payload.get("comparison") or empty_comparison(status="disabled")
        state["actions"] = {
            str(link): action_payload(value)
            for link, value in (payload.get("actions") or {}).items()
            if link
        }


def refresh_news() -> None:
    with state_lock:
        if state["refreshing"]:
            return
        state["refreshing"] = True
        state["error"] = None
        current_article_cache = dict(state.get("article_cache", {}))
        current_cache = dict(state.get("embedding_cache", {}))
        current_match_cache = dict(state.get("match_cache", {}))
        current_comparison = dict(state.get("comparison") or empty_comparison(status="disabled"))

    try:
        cutoff = dhaka_now() - timedelta(hours=WINDOW_HOURS)
        articles = dedupe_articles(scrape_sources(cutoff, article_cache=current_article_cache))
        fresh_articles = prune_articles(articles)
        article_cache = update_article_cache(current_article_cache, fresh_articles)

        try:
            comparison, embedding_cache, match_cache = build_comparison(fresh_articles, current_cache, current_match_cache)
        except Exception as comparison_exc:
            embedding_cache = prune_embedding_cache(current_cache, fresh_articles)
            match_cache = prune_match_cache(current_match_cache)
            comparison = dict(current_comparison) if current_comparison else empty_comparison(status="error")
            comparison["baseline_publisher"] = BASELINE_PUBLISHER
            comparison["comparison_status"] = "error"
            comparison["comparison_error"] = str(comparison_exc)

        now = dhaka_now()
        with state_lock:
            state["articles"] = fresh_articles
            state["article_cache"] = article_cache
            state["embedding_cache"] = embedding_cache
            state["match_cache"] = match_cache
            state["comparison"] = comparison
            state["last_updated"] = now.isoformat()
            state["next_run"] = (now + timedelta(seconds=REFRESH_SECONDS)).isoformat()
            state["refreshing"] = False
            save_state()
    except Exception as exc:
        now = dhaka_now()
        with state_lock:
            state["articles"] = prune_articles(state["articles"])
            state["article_cache"] = prune_article_cache(state.get("article_cache", {}), state["articles"])
            state["embedding_cache"] = prune_embedding_cache(state.get("embedding_cache", {}), state["articles"])
            state["match_cache"] = prune_match_cache(state.get("match_cache", {}))
            state["next_run"] = (now + timedelta(seconds=REFRESH_SECONDS)).isoformat()
            state["refreshing"] = False
            state["error"] = str(exc)
            save_state()


def scheduler_loop() -> None:
    while True:
        time.sleep(REFRESH_SECONDS)
        refresh_news()


PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }}</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f1e7;
      --panel: #fffdf9;
      --panel-alt: #f6efe3;
      --text: #1e2430;
      --muted: #667085;
      --line: #ded5c7;
      --accent: #0b6e4f;
      --accent-dark: #124e78;
      --accent-soft: #e8f4ef;
      --warning: #b54708;
      --danger: #b42318;
      --shadow: 0 10px 28px rgba(41, 34, 24, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Noto Sans Bengali", serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(11,110,79,0.08), transparent 24%),
        radial-gradient(circle at top right, rgba(18,78,120,0.08), transparent 28%),
        linear-gradient(180deg, #f7f2ea 0%, #efe6d9 100%);
    }
    a { color: var(--accent-dark); text-decoration: none; }
    a:hover { text-decoration: underline; }
    header {
      position: sticky;
      top: 0;
      z-index: 5;
      background: rgba(255,253,249,0.92);
      backdrop-filter: blur(16px);
      border-bottom: 1px solid rgba(102,112,133,0.16);
    }
    .shell, main {
      width: min(1400px, calc(100% - 32px));
      margin: 0 auto;
    }
    .header-row {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      padding: 18px 0 16px;
    }
    .title-block h1 {
      margin: 0;
      font-size: 30px;
      line-height: 1.05;
    }
    .title-block p {
      margin: 8px 0 0;
      max-width: 820px;
      font-size: 14px;
      color: var(--muted);
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    .button, button {
      appearance: none;
      border: 1px solid var(--accent);
      background: var(--accent);
      color: white;
      border-radius: 999px;
      font-size: 14px;
      font-weight: 700;
      padding: 0 16px;
      height: 40px;
      cursor: pointer;
    }
    button:disabled { opacity: 0.72; cursor: wait; }
    nav {
      display: flex;
      gap: 10px;
      padding-bottom: 16px;
      flex-wrap: wrap;
    }
    .nav-link {
      display: inline-flex;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 10px 14px;
      background: rgba(255,253,249,0.9);
      color: var(--muted);
      font-size: 14px;
      font-weight: 700;
    }
    .nav-link.active {
      background: var(--text);
      border-color: var(--text);
      color: white;
    }
    main { margin: 24px auto 48px; }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 14px;
    }
    .summary-card, .panel {
      background: var(--panel);
      border: 1px solid rgba(102,112,133,0.14);
      border-radius: 8px;
      box-shadow: var(--shadow);
      min-width: 0;
    }
    .summary-card { padding: 16px; }
    .summary-card strong {
      display: block;
      font-size: 30px;
      line-height: 1;
      margin-bottom: 8px;
    }
    .summary-card span {
      display: block;
      color: var(--muted);
      font-size: 13px;
    }
    .summary-card em {
      display: block;
      margin-top: 8px;
      color: var(--accent-dark);
      font-size: 13px;
      font-style: normal;
      font-weight: 700;
    }
    .status {
      margin: 16px 0;
      border-radius: 8px;
      padding: 12px 14px;
      font-size: 14px;
      border: 1px solid #b8ddd2;
      background: var(--accent-soft);
      color: #115e59;
    }
    .status.error {
      border-color: #efc4bc;
      background: #fff2ef;
      color: var(--danger);
    }
    .page-grid {
      display: grid;
      grid-template-columns: minmax(760px, 1fr) minmax(300px, 360px);
      gap: 16px;
      align-items: start;
    }
    .live-layout {
      grid-template-columns: minmax(780px, 1fr) minmax(300px, 360px);
    }
    .stack { display: grid; gap: 16px; }
    .page-grid > *,
    .command-grid > *,
    .analysis-grid > *,
    .stack > * {
      min-width: 0;
    }
    .panel-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 14px;
      padding: 18px 18px 0;
    }
    .panel-header h2, .panel-header h3 {
      margin: 0;
      font-size: 19px;
      line-height: 1.15;
    }
    .panel-header p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 13px;
    }
    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      padding: 16px 18px;
      border-top: 1px solid rgba(102,112,133,0.12);
    }
    input, select {
      height: 38px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: white;
      padding: 0 14px;
      color: var(--text);
      font-size: 14px;
    }
    input { flex: 1; min-width: 240px; }
    select { min-width: 180px; }
    .publisher-stack, .panel-body, .analysis-block {
      padding: 0 18px 18px;
    }
    .publisher-stack {
      overflow-x: auto;
    }
    .publisher-section + .publisher-section { margin-top: 18px; }
    .publisher-heading {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 8px;
    }
    .publisher-heading h3 {
      margin: 0;
      font-size: 18px;
    }
    .meta {
      color: var(--muted);
      font-size: 13px;
    }
    table {
      width: 100%;
      min-width: 760px;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid rgba(102,112,133,0.12);
      border-radius: 8px;
      overflow: hidden;
    }
    th, td {
      padding: 11px 12px;
      border-bottom: 1px solid rgba(102,112,133,0.12);
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }
    th {
      background: var(--panel-alt);
      font-size: 12px;
      text-transform: uppercase;
      color: var(--muted);
    }
    tr:last-child td { border-bottom: 0; }
    #compareResults table {
      min-width: 0;
      table-layout: fixed;
    }
    #compareResults th:nth-child(1), #compareResults td:nth-child(1) { width: 15%; }
    #compareResults th:nth-child(2), #compareResults td:nth-child(2) { width: 12%; }
    #compareResults th:nth-child(3), #compareResults td:nth-child(3) { width: 8%; }
    #compareResults th:nth-child(4), #compareResults td:nth-child(4) { width: 15%; }
    #compareResults th:nth-child(5), #compareResults td:nth-child(5) { width: 35%; }
    #compareResults th:nth-child(6), #compareResults td:nth-child(6) { width: 15%; }
    #compareResults th,
    #compareResults td {
      overflow-wrap: anywhere;
    }
    #compareResults .publication-pill,
    #compareResults .category-chip {
      min-width: 0;
      max-width: 100%;
      white-space: normal;
      line-height: 1.2;
    }
    #compareResults .action-box {
      min-width: 0;
    }
    .publisher-count-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(132px, 1fr));
      gap: 8px;
      width: 100%;
      min-width: 0;
    }
    .publisher-count-chip {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr) auto;
      align-items: center;
      gap: 8px;
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 7px 9px;
      background: #fff;
    }
    .publisher-logo {
      width: 18px;
      height: 18px;
      flex: 0 0 18px;
      object-fit: contain;
      border-radius: 4px;
      background: rgba(255,255,255,0.8);
    }
    .publisher-count-chip .publisher-logo {
      width: 16px;
      height: 16px;
      flex-basis: 16px;
    }
    .publisher-count-chip .publisher-count-name {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 12px;
      font-weight: 800;
    }
    .publisher-count-chip .publisher-count-value {
      min-width: 24px;
      height: 24px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      background: rgba(255,255,255,0.74);
      font-size: 13px;
      font-weight: 900;
    }
    .publisher-count-chip.publisher-daily-star {
      background: #eaf1fb;
      border-color: #b8cdee;
    }
    .publisher-count-chip.publisher-prothom-alo {
      background: #fff2ef;
      border-color: #efc4bc;
    }
    .publisher-count-chip.publisher-tbs {
      background: #eaf7ef;
      border-color: #b6dec3;
    }
    .publisher-count-chip.publisher-samakal {
      background: #f4ecfb;
      border-color: #d8bfef;
    }
    .publisher-count-chip.publisher-bonik-barta {
      background: #fff4e8;
      border-color: #e9c59b;
    }
    .publisher-count-chip.publisher-unknown {
      background: #f8f7f3;
    }
    .hourly-table {
      min-width: 0;
      table-layout: fixed;
    }
    .hourly-table th:nth-child(1),
    .hourly-table td:nth-child(1) {
      width: 22%;
    }
    .hourly-table th:nth-child(2),
    .hourly-table td:nth-child(2) {
      width: 12%;
    }
    .hourly-table th:nth-child(3),
    .hourly-table td:nth-child(3) {
      width: 66%;
    }
    .age, .tag, .status-pill {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 72px;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 13px;
      font-weight: 700;
      border: 1px solid var(--line);
      white-space: nowrap;
    }
    .age { background: #f7efe2; color: #2f3a44; }
    .age.new { background: #d8f0e7; border-color: #9fd2bf; color: #115e59; }
    .tag {
      background: #f8f7f3;
      color: #344054;
    }
    .publication-pill {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 7px;
      min-width: 118px;
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 13px;
      font-weight: 800;
      border: 1px solid var(--line);
      white-space: nowrap;
    }
    .publication-pill .publisher-name {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .publisher-daily-star {
      background: #eaf1fb;
      border-color: #b8cdee;
      color: #123b73;
    }
    .publisher-prothom-alo {
      background: #fff2ef;
      border-color: #efc4bc;
      color: #b42318;
    }
    .publisher-tbs {
      background: #eaf7ef;
      border-color: #b6dec3;
      color: #11643f;
    }
    .publisher-samakal {
      background: #f4ecfb;
      border-color: #d8bfef;
      color: #64328f;
    }
    .publisher-bonik-barta {
      background: #fff4e8;
      border-color: #e9c59b;
      color: #8a4a10;
    }
    .publisher-unknown {
      background: #f8f7f3;
      color: #344054;
    }
    .category-chip {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 10px;
      background: #eef6fb;
      color: #195b84;
      font-size: 13px;
      font-weight: 700;
    }
    .status-pill.covered { background: #e8f4ef; border-color: #b8ddd2; color: #115e59; }
    .status-pill.review { background: #fff2df; border-color: #f1c88b; color: var(--warning); }
    .status-pill.gap { background: #fff2ef; border-color: #efc4bc; color: var(--danger); }
    .filter-segments {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .filter-dropdown {
      position: relative;
      width: min(260px, 100%);
      flex: 0 0 260px;
    }
    .filter-dropdown-button {
      width: 100%;
      height: 38px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: white;
      color: var(--text);
      padding: 0 14px;
      font-size: 14px;
      font-weight: 700;
      box-shadow: none;
    }
    .filter-dropdown-button span {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .filter-dropdown-button::after {
      content: "";
      width: 8px;
      height: 8px;
      border-right: 2px solid var(--muted);
      border-bottom: 2px solid var(--muted);
      transform: rotate(45deg) translateY(-2px);
      flex: 0 0 auto;
    }
    .filter-dropdown.open .filter-dropdown-button::after {
      transform: rotate(225deg) translateY(-1px);
    }
    .filter-menu {
      position: absolute;
      z-index: 20;
      top: calc(100% + 6px);
      left: 0;
      width: 100%;
      max-height: 260px;
      overflow-y: auto;
      padding: 6px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: white;
      box-shadow: 0 18px 36px rgba(41, 34, 24, 0.18);
    }
    .filter-dropdown:not(.open) .filter-menu {
      display: none;
    }
    .filter-option {
      width: 100%;
      min-height: 34px;
      height: auto;
      display: block;
      border: 0;
      border-radius: 6px;
      background: transparent;
      color: var(--text);
      padding: 8px 10px;
      text-align: left;
      font-size: 13px;
      font-weight: 700;
      line-height: 1.25;
      overflow-wrap: anywhere;
    }
    .filter-option:hover,
    .filter-option.active {
      background: var(--panel-alt);
      color: var(--accent-dark);
    }
    .filter-chip {
      height: 38px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: white;
      color: var(--muted);
      padding: 0 13px;
      font-size: 13px;
      font-weight: 800;
    }
    .filter-chip.active {
      background: var(--text);
      border-color: var(--text);
      color: white;
    }
    .action-box {
      display: grid;
      gap: 6px;
      min-width: 170px;
    }
    .action-box input {
      width: 100%;
      min-width: 0;
      height: 32px;
      border-radius: 7px;
      font-size: 12px;
      padding: 0 9px;
    }
    .action-box input { flex: none; }
    .action-status-picker {
      position: relative;
      display: grid;
      gap: 4px;
    }
    .action-status-button {
      width: 100%;
      height: 32px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: white;
      color: var(--text);
      padding: 0 9px;
      font-size: 12px;
      font-weight: 700;
      box-shadow: none;
    }
    .action-status-button span {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .action-status-button::after {
      content: "";
      width: 7px;
      height: 7px;
      border-right: 2px solid var(--muted);
      border-bottom: 2px solid var(--muted);
      transform: rotate(45deg) translateY(-2px);
      flex: 0 0 auto;
    }
    .action-status-picker.open .action-status-button::after {
      transform: rotate(225deg) translateY(-1px);
    }
    .action-status-menu {
      display: none;
      position: fixed;
      z-index: 60;
      width: min(184px, calc(100vw - 24px));
      gap: 3px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      padding: 4px;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: white;
      box-shadow: 0 16px 34px rgba(41, 34, 24, 0.2);
    }
    .action-status-picker.open .action-status-menu {
      display: grid;
    }
    .action-status-option {
      width: 100%;
      height: 28px;
      border: 0;
      border-radius: 5px;
      background: transparent;
      color: var(--text);
      padding: 0 7px;
      text-align: left;
      font-size: 12px;
      font-weight: 700;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .action-status-option:hover,
    .action-status-option.active {
      background: var(--panel-alt);
      color: var(--accent-dark);
    }
    .live-feed {
      border: 1px solid rgba(102,112,133,0.12);
      border-radius: 8px;
      overflow: hidden;
      background: var(--panel);
    }
    .live-feed-head,
    .live-feed-row {
      display: grid;
      grid-template-columns: 86px 128px 128px minmax(0, 1fr) 146px;
      gap: 12px;
      align-items: start;
      padding: 12px;
    }
    .live-feed-head {
      background: var(--panel-alt);
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .02em;
    }
    .live-feed-row {
      border-top: 1px solid rgba(102,112,133,0.12);
    }
    .live-feed-row > div { min-width: 0; }
    .live-feed-row .publication-pill {
      min-width: 0;
      max-width: 100%;
      white-space: normal;
      text-align: center;
      line-height: 1.15;
    }
    .live-feed-row .category-chip {
      max-width: 100%;
      white-space: normal;
      justify-content: flex-start;
      border-radius: 16px;
      line-height: 1.2;
    }
    .live-feed-row .headline-cell a {
      overflow-wrap: anywhere;
    }
    .live-feed-row .action-box {
      min-width: 0;
      max-width: 146px;
    }
    .live-feed-row .action-box input {
      height: 30px;
    }
    .command-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 16px;
    }
    .command-card {
      background: var(--panel);
      border: 1px solid rgba(102,112,133,0.14);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 15px 16px;
    }
    .command-card strong {
      display: block;
      font-size: 28px;
      line-height: 1;
      margin-bottom: 7px;
    }
    .command-card span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
    }
    .command-card em {
      display: block;
      margin-top: 7px;
      color: var(--accent-dark);
      font-size: 13px;
      font-style: normal;
      font-weight: 700;
    }
    .headline-cell { min-width: 340px; }
    .headline-cell a { font-weight: 700; }
    .headline-link-daily-star { color: #123b73; }
    .headline-link-prothom-alo { color: #b42318; }
    .headline-link-tbs { color: #11643f; }
    .headline-link-samakal { color: #64328f; }
    .headline-link-bonik-barta { color: #8a4a10; }
    .headline-link-unknown { color: var(--accent-dark); }
    .headline-cell a:hover { text-decoration-thickness: 2px; }
    .analysis-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }
    .analysis-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .analysis-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      padding-bottom: 10px;
      border-bottom: 1px solid rgba(102,112,133,0.1);
    }
    .analysis-row:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }
    .analysis-row strong { font-size: 15px; }
    .analysis-row span {
      color: var(--muted);
      font-size: 13px;
    }
    .analysis-row .publisher-count-grid {
      flex: 1 1 360px;
      max-width: 560px;
    }
    .signal-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .signal-item {
      padding-bottom: 10px;
      border-bottom: 1px solid rgba(102,112,133,0.1);
    }
    .signal-item:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }
    .signal-item a {
      display: block;
      margin: 5px 0;
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .signal-item .meta,
    .analysis-row strong,
    .analysis-row span {
      overflow-wrap: anywhere;
    }
    .note {
      padding: 14px 16px;
      border-radius: 8px;
      background: #f7f3ea;
      border: 1px solid rgba(102,112,133,0.12);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .empty {
      padding: 34px 18px;
      text-align: center;
      color: var(--muted);
      border-top: 1px solid rgba(102,112,133,0.1);
    }
    @media (max-width: 1180px) {
      .summary-grid, .analysis-grid, .page-grid, .command-grid {
        grid-template-columns: 1fr;
      }
      .live-layout {
        grid-template-columns: 1fr;
      }
    }
    @media (max-width: 900px) {
      .header-row { flex-direction: column; }
      .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .toolbar {
        display: grid;
        grid-template-columns: 1fr;
      }
      .filter-dropdown {
        width: 100%;
        flex-basis: auto;
      }
      .filter-menu {
        width: 100%;
        max-height: 280px;
      }
      input, select {
        width: 100%;
        min-width: 0;
      }
      .live-feed-head { display: none; }
      .live-feed {
        border: 0;
        background: transparent;
        display: grid;
        gap: 10px;
      }
      .live-feed-row {
        display: grid;
        grid-template-columns: 1fr;
        gap: 8px;
        padding: 12px;
        border: 1px solid rgba(102,112,133,0.12);
        border-radius: 8px;
        background: var(--panel);
      }
      .live-feed-row > div::before {
        content: attr(data-label);
        display: block;
        color: var(--muted);
        font-size: 11px;
        font-weight: 800;
        text-transform: uppercase;
        margin-bottom: 3px;
      }
      .live-feed-row .action-box {
        max-width: none;
      }
      table, thead, tbody, tr, th, td { display: block; }
      table { min-width: 0; }
      thead { display: none; }
      tbody {
        display: grid;
        gap: 10px;
      }
      tr {
        border: 1px solid rgba(102,112,133,0.12);
        border-radius: 8px;
        background: var(--panel);
        overflow: hidden;
      }
      td {
        border-bottom: 0;
        padding: 6px 12px;
        width: 100% !important;
      }
      td::before {
        content: attr(data-label);
        display: block;
        color: var(--muted);
        font-size: 12px;
        margin-bottom: 3px;
      }
      .publisher-count-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .publisher-count-chip {
        padding: 6px 8px;
      }
      .publisher-count-chip .publisher-count-name {
        font-size: 11px;
      }
      #compareResults table {
        border: 0;
        background: transparent;
      }
      #compareResults th,
      #compareResults td {
        width: 100% !important;
      }
      #compareResults .publisher-section {
        margin-bottom: 0;
      }
      #compareResults .headline-cell {
        min-width: 0;
      }
      #compareResults .action-box {
        width: 100%;
        min-width: 0;
      }
      #compareResults .action-status-picker,
      #compareResults .action-status-button,
      #compareResults .action-note {
        width: 100%;
      }
      .publisher-heading, .analysis-row { flex-direction: column; align-items: flex-start; }
      .analysis-row .publisher-count-grid {
        flex-basis: auto;
        max-width: none;
      }
      .headline-cell { min-width: 0; }
    }
  </style>
</head>
<body>
  <header>
    <div class="shell">
      <div class="header-row">
        <div class="title-block">
          <h1>{{ title }}</h1>
          <p>{{ subtitle }}</p>
        </div>
        <div class="actions">
          <button id="refreshBtn" type="button">Refresh now</button>
        </div>
      </div>
      <nav>
        <a class="nav-link {% if view_mode == 'compare' %}active{% endif %}" href="/">Compare</a>
        <a class="nav-link {% if view_mode == 'live' %}active{% endif %}" href="/live">Live 1h</a>
        <a class="nav-link {% if view_mode == 'analysis' %}active{% endif %}" href="/analysis">Analysis</a>
      </nav>
    </div>
  </header>
  <main>
    <section class="summary-grid" id="summaryGrid"></section>
    <div id="status" class="status">Loading latest scrape...</div>
    <section id="content"></section>
  </main>
  <script>
    const pageConfig = {{ page_config | tojson }};
    let feedData = null;
    let analysisData = null;
    const statusBox = document.getElementById("status");
    const summaryGrid = document.getElementById("summaryGrid");
    const content = document.getElementById("content");
    const refreshBtn = document.getElementById("refreshBtn");

    function formatTime(value) {
      return new Intl.DateTimeFormat("en-GB", {
        dateStyle: "medium",
        timeStyle: "short",
        timeZone: "Asia/Dhaka"
      }).format(new Date(value));
    }

    function ageText(value) {
      const minutes = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 60000));
      if (minutes < 1) return "Now";
      if (minutes < 60) return `${minutes}m`;
      const hours = Math.floor(minutes / 60);
      const rest = minutes % 60;
      return rest ? `${hours}h ${rest}m` : `${hours}h`;
    }

    function ageClass(value) {
      const minutes = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 60000));
      return minutes <= 10 ? "age new" : "age";
    }

    function statusClass(value) {
      if (value === "Covered") return "status-pill covered";
      if (value === "Needs Review") return "status-pill review";
      return "status-pill gap";
    }

    function publisherClass(value) {
      if (value === "The Daily Star") return "publisher-daily-star";
      if (value === "Prothom Alo") return "publisher-prothom-alo";
      if (value === "The Business Standard" || value === "TBS") return "publisher-tbs";
      if (value === "Samakal") return "publisher-samakal";
      if (value === "Bonik Barta") return "publisher-bonik-barta";
      return "publisher-unknown";
    }

    function publisherHeadlineClass(value) {
      if (value === "The Daily Star") return "headline-link-daily-star";
      if (value === "Prothom Alo") return "headline-link-prothom-alo";
      if (value === "The Business Standard" || value === "TBS") return "headline-link-tbs";
      if (value === "Samakal") return "headline-link-samakal";
      if (value === "Bonik Barta") return "headline-link-bonik-barta";
      return "headline-link-unknown";
    }

    function publisherLogo(value) {
      if (value === "The Daily Star") return "https://www.thedailystar.net/themes/custom/swallow/favicon.ico";
      if (value === "Prothom Alo") return "https://www.prothomalo.com/favicon.ico";
      if (value === "The Business Standard" || value === "TBS") return "https://www.tbsnews.net/favicon.ico";
      if (value === "Samakal") return "https://samakal.com/frontend/media/common/favicon/favicon-32x32.png";
      if (value === "Bonik Barta") return "https://www.bonikbarta.com/favicon.webp";
      return "";
    }

    function publisherLogoMarkup(name) {
      const logo = publisherLogo(name);
      if (!logo) return "";
      return `<img class="publisher-logo" src="${logo}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()">`;
    }

    function publisherPill(name) {
      return `
        <span class="publication-pill ${publisherClass(name)}" title="${escapeHtml(name)}">
          ${publisherLogoMarkup(name)}
          <span class="publisher-name">${escapeHtml(name)}</span>
        </span>
      `;
    }

    function escapeHtml(value) {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    function actionControl(article) {
      const link = escapeHtml(article.Link || "");
      const status = article.ActionStatus || "New";
      const note = escapeHtml(article.ActionNote || "");
      const statuses = ["New", "Watching", "Assigned", "Reported", "Ignored"];
      return `
        <div class="action-box" data-link="${link}" data-status="${escapeHtml(status)}">
          <div class="action-status-picker">
            <button type="button" class="action-status-button" aria-haspopup="listbox" aria-expanded="false">
              <span>${escapeHtml(status)}</span>
            </button>
            <div class="action-status-menu" role="listbox">
              ${statuses.map((item) => `<button type="button" class="action-status-option ${item === status ? "active" : ""}" data-status="${item}">${item}</button>`).join("")}
            </div>
          </div>
          <input class="action-note" type="text" value="${note}" placeholder="Note">
        </div>
      `;
    }

    function sortByTime(rows) {
      return [...rows].sort((left, right) => {
        return new Date(right.PublishedTime) - new Date(left.PublishedTime);
      });
    }

    function beatOptions(articles) {
      return [...new Set((articles || []).map((article) => article.CanonicalCategory || article.Category || "Uncategorized"))]
        .filter(Boolean)
        .sort((left, right) => left.localeCompare(right));
    }

    function publisherLabel(name) {
      return name === "The Business Standard" ? "TBS" : name;
    }

    function publisherCountGrid(source) {
      return `
        <div class="publisher-count-grid">
          ${pageConfig.publishers.map((name) => `
            <div class="publisher-count-chip ${publisherClass(name)}" title="${escapeHtml(name)}">
              ${publisherLogoMarkup(name)}
              <span class="publisher-count-name">${escapeHtml(publisherLabel(name))}</span>
              <span class="publisher-count-value">${source[name] || 0}</span>
            </div>
          `).join("")}
        </div>
      `;
    }

    function placeActionMenu(picker) {
      const button = picker.querySelector(".action-status-button");
      const menu = picker.querySelector(".action-status-menu");
      const rect = button.getBoundingClientRect();
      const menuWidth = Math.min(184, window.innerWidth - 24);
      const left = Math.min(Math.max(12, rect.left), window.innerWidth - menuWidth - 12);
      menu.style.width = `${menuWidth}px`;
      menu.style.left = `${left}px`;
      menu.style.top = `${rect.bottom + 6}px`;
      const menuHeight = menu.getBoundingClientRect().height || 70;
      if (rect.bottom + 6 + menuHeight > window.innerHeight - 12) {
        menu.style.top = `${Math.max(12, rect.top - menuHeight - 6)}px`;
      }
    }

    function closeOpenMenus() {
      document.querySelectorAll(".action-status-picker.open").forEach((picker) => {
        picker.classList.remove("open");
        picker.querySelector(".action-status-button")?.setAttribute("aria-expanded", "false");
      });
      document.querySelectorAll(".filter-dropdown.open").forEach((dropdown) => {
        dropdown.classList.remove("open");
        dropdown.querySelector(".filter-dropdown-button")?.setAttribute("aria-expanded", "false");
      });
    }

    function updateLocalAction(link, action) {
      const collections = [
        feedData && feedData.articles,
        feedData && feedData.live_signal,
        analysisData && analysisData.coverage_gaps
      ];
      collections.forEach((items) => {
        (items || []).forEach((item) => {
          const itemLink = item.Link || item.link;
          if (itemLink === link) {
            item.ActionStatus = action.status;
            item.ActionNote = action.note;
            item.action_status = action.status;
          }
        });
      });
    }

    async function saveAction(link, status, note) {
      const response = await fetch("/api/actions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ link, status, note })
      });
      const payload = await response.json();
      if (payload.ok) {
        updateLocalAction(link, payload.action);
      }
      return payload;
    }

    function bindActionControls(root = document) {
      root.querySelectorAll(".action-box").forEach((box) => {
        const picker = box.querySelector(".action-status-picker");
        const statusButton = box.querySelector(".action-status-button");
        const statusLabel = statusButton.querySelector("span");
        const note = box.querySelector(".action-note");
        const persist = () => saveAction(box.dataset.link, box.dataset.status || "New", note.value).catch(() => {});
        statusButton.addEventListener("click", (event) => {
          event.stopPropagation();
          document.querySelectorAll(".filter-dropdown.open").forEach((dropdown) => {
            dropdown.classList.remove("open");
            dropdown.querySelector(".filter-dropdown-button")?.setAttribute("aria-expanded", "false");
          });
          document.querySelectorAll(".action-status-picker.open").forEach((item) => {
            if (item !== picker) {
              item.classList.remove("open");
              item.querySelector(".action-status-button")?.setAttribute("aria-expanded", "false");
            }
          });
          const open = !picker.classList.contains("open");
          picker.classList.toggle("open", open);
          statusButton.setAttribute("aria-expanded", open ? "true" : "false");
          if (open) {
            placeActionMenu(picker);
          }
        });
        box.querySelectorAll(".action-status-option").forEach((option) => {
          option.addEventListener("click", (event) => {
            event.stopPropagation();
            box.dataset.status = option.dataset.status || "New";
            statusLabel.textContent = box.dataset.status;
            box.querySelectorAll(".action-status-option").forEach((item) => item.classList.toggle("active", item === option));
            picker.classList.remove("open");
            statusButton.setAttribute("aria-expanded", "false");
            persist();
          });
        });
        note.addEventListener("change", persist);
      });
    }

    document.addEventListener("click", closeOpenMenus);
    window.addEventListener("scroll", (event) => {
      const target = event.target;
      if (target && target.closest && target.closest(".filter-menu")) {
        return;
      }
      closeOpenMenus();
    }, true);
    window.addEventListener("resize", closeOpenMenus);

    function renderSummaryCards(data) {
      const baseline = pageConfig.baselinePublisher;
      const counts = data.publisher_counts || {};
      const liveCounts = data.last_hour_counts || {};
      const liveTotal = Object.values(liveCounts).reduce((sum, count) => sum + count, 0);
      const competitorTotal = Object.entries(counts)
        .filter(([name]) => name !== baseline)
        .reduce((sum, [, count]) => sum + count, 0);
      const topCategory = (data.top_categories || [])[0];
      const cards = [
        { value: data.count || 0, label: `Last ${data.window_hours}h total`, note: `${liveTotal} in the last 1h` },
        { value: counts[baseline] || 0, label: baseline, note: `${liveCounts[baseline] || 0} in the last 1h` },
        { value: competitorTotal, label: "Competitors", note: `${pageConfig.competitors.join(", ")}` },
        ...pageConfig.competitors.map((name) => ({
          value: counts[name] || 0,
          label: publisherLabel(name),
          note: `${liveCounts[name] || 0} in the last 1h`
        })),
        { value: topCategory ? topCategory.count : 0, label: "Hot beat", note: topCategory ? topCategory.name : "No category yet" }
      ];

      summaryGrid.innerHTML = cards.map((card) => `
        <article class="summary-card">
          <strong>${card.value}</strong>
          <span>${card.label}</span>
          <em>${card.note}</em>
        </article>
      `).join("");
    }

    function renderStatus(feed, analysis) {
      const lastUpdated = feed.last_updated ? formatTime(feed.last_updated) : "not yet";
      const nextRun = feed.next_run ? formatTime(feed.next_run) : "pending";
      const comparisonBits = [];

      if (analysis && analysis.comparison_status) {
        if (analysis.comparison_status === "disabled") {
          comparisonBits.push("exact matching disabled");
        } else if (analysis.comparison_status === "error") {
          comparisonBits.push(`exact matching error: ${analysis.comparison_error}`);
        } else {
          comparisonBits.push("exact matching ready");
        }
      }

      const extra = comparisonBits.length ? ` Comparison: ${comparisonBits.join("; ")}.` : "";
      statusBox.className = feed.error ? "status error" : "status";
      statusBox.textContent = feed.error
        ? `Scrape error: ${feed.error}`
        : `Last updated: ${lastUpdated}. Next scheduled scrape: ${nextRun}.${extra}`;
      refreshBtn.disabled = Boolean(feed.refreshing);
    }

    function renderSignalCard(items) {
      return `
        <section class="panel">
          <div class="panel-header">
            <div>
              <h3>Fresh signal</h3>
              <p>The first items editors and reporters should scan.</p>
            </div>
          </div>
          <div class="analysis-block">
            ${items.length ? `
              <div class="signal-list">
                ${items.map((item) => `
                  <div class="signal-item">
                    <div class="meta">${item.publisher} | ${item.category} | ${ageText(item.published_time)}</div>
                    <a class="${publisherHeadlineClass(item.publisher)}" href="${item.link}" target="_blank" rel="noreferrer">${item.headline}</a>
                  </div>
                `).join("")}
              </div>
            ` : '<div class="empty">No fresh articles yet.</div>'}
          </div>
        </section>
      `;
    }

    function compareTableRows(rows) {
      return rows.map((article) => `
        <tr>
          <td data-label="Publisher">${publisherPill(article.Publisher)}</td>
          <td data-label="Published">${formatTime(article.PublishedTime)}</td>
          <td data-label="Age"><span class="${ageClass(article.PublishedTime)}">${ageText(article.PublishedTime)}</span></td>
          <td data-label="Category"><span class="category-chip">${article.CanonicalCategory || article.Category || "Uncategorized"}</span></td>
          <td data-label="Headline" class="headline-cell"><a class="${publisherHeadlineClass(article.Publisher)}" href="${article.Link}" target="_blank" rel="noreferrer">${article.Headline}</a></td>
          <td data-label="Action">${actionControl(article)}</td>
        </tr>
      `).join("");
    }

    function renderCompareView(data) {
      const beats = beatOptions(data.articles || []);
      let selectedBeat = "";
      let selectedPublisher = "";
      const beatOptionsHtml = ["", ...beats].map((name) => `
        <button type="button" class="filter-option ${name ? "" : "active"}" data-beat="${escapeHtml(name)}">${name || "All beats"}</button>
      `).join("");
      const publisherOptionsHtml = ["", ...pageConfig.publishers].map((name) => `
        <button type="button" class="filter-option ${name ? "" : "active"}" data-publisher="${escapeHtml(name)}">${name || "All publishers"}</button>
      `).join("");
      content.innerHTML = `
        <section class="page-grid">
          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Publisher compare</h2>
                <p>All-publication timeline sorted newest to oldest so reporters can spot the latest move first.</p>
              </div>
            </div>
            <div class="toolbar">
              <input id="search" type="search" placeholder="Search headline, category, publisher">
              <div class="filter-dropdown" id="compareBeat">
                <button type="button" class="filter-dropdown-button" aria-haspopup="listbox" aria-expanded="false">
                  <span>All beats</span>
                </button>
                <div class="filter-menu" role="listbox">${beatOptionsHtml}</div>
              </div>
              <div class="filter-dropdown" id="comparePublisher">
                <button type="button" class="filter-dropdown-button" aria-haspopup="listbox" aria-expanded="false">
                  <span>All publishers</span>
                </button>
                <div class="filter-menu" role="listbox">${publisherOptionsHtml}</div>
              </div>
            </div>
            <div class="publisher-stack" id="compareResults"></div>
          </section>
          <section class="stack">
            ${renderSignalCard(data.live_signal || [])}
            <section class="panel">
              <div class="panel-header">
                <div>
                  <h3>Top categories</h3>
                  <p>Where this window is concentrating coverage.</p>
                </div>
              </div>
              <div class="analysis-block">
                <div class="analysis-list">
                  ${(data.top_categories || []).map((item) => `
                    <div class="analysis-row">
                      <strong>${item.name}</strong>
                      <span>${item.count} articles</span>
                    </div>
                  `).join("")}
                </div>
              </div>
            </section>
          </section>
        </section>
      `;

      const search = document.getElementById("search");
      const beat = document.getElementById("compareBeat");
      const beatButton = beat.querySelector(".filter-dropdown-button");
      const beatLabel = beatButton.querySelector("span");
      const publisher = document.getElementById("comparePublisher");
      const publisherButton = publisher.querySelector(".filter-dropdown-button");
      const publisherLabel = publisherButton.querySelector("span");
      const compareResults = document.getElementById("compareResults");

      function paintCompare() {
        const term = search.value.trim().toLowerCase();
        const filtered = (data.articles || []).filter((article) => {
          const text = `${article.Headline} ${article.Category} ${article.CanonicalCategory} ${article.Publisher}`.toLowerCase();
          const beatName = article.CanonicalCategory || article.Category || "Uncategorized";
          return (!selectedBeat || beatName === selectedBeat) &&
            (!selectedPublisher || article.Publisher === selectedPublisher) &&
            (!term || text.includes(term));
        }).sort((left, right) => new Date(right.PublishedTime) - new Date(left.PublishedTime));

        if (!filtered.length) {
          compareResults.innerHTML = '<div class="empty">No matching articles in this view.</div>';
          return;
        }

        compareResults.innerHTML = `
          <section class="publisher-section">
            <div class="publisher-heading">
              <h3>All publications</h3>
              <div class="meta">${filtered.length} articles, newest first</div>
            </div>
            <table>
              <thead>
                <tr>
                  <th>Publisher</th>
                  <th>Published</th>
                  <th>Age</th>
                  <th>Category</th>
                  <th>Headline</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>${compareTableRows(filtered)}</tbody>
            </table>
          </section>
        `;
        bindActionControls(compareResults);
      }

      search.addEventListener("input", paintCompare);
      beatButton.addEventListener("click", (event) => {
        event.stopPropagation();
        document.querySelectorAll(".action-status-picker.open").forEach((picker) => {
          picker.classList.remove("open");
          picker.querySelector(".action-status-button")?.setAttribute("aria-expanded", "false");
        });
        if (publisher.classList.contains("open")) {
          publisher.classList.remove("open");
          publisherButton.setAttribute("aria-expanded", "false");
        }
        const open = !beat.classList.contains("open");
        beat.classList.toggle("open", open);
        beatButton.setAttribute("aria-expanded", open ? "true" : "false");
      });
      beat.querySelectorAll(".filter-option").forEach((option) => {
        option.addEventListener("click", (event) => {
          event.stopPropagation();
          selectedBeat = option.dataset.beat || "";
          beatLabel.textContent = selectedBeat || "All beats";
          beat.querySelectorAll(".filter-option").forEach((item) => item.classList.toggle("active", item === option));
          beat.classList.remove("open");
          beatButton.setAttribute("aria-expanded", "false");
          paintCompare();
        });
      });
      publisherButton.addEventListener("click", (event) => {
        event.stopPropagation();
        document.querySelectorAll(".action-status-picker.open").forEach((picker) => {
          picker.classList.remove("open");
          picker.querySelector(".action-status-button")?.setAttribute("aria-expanded", "false");
        });
        if (beat.classList.contains("open")) {
          beat.classList.remove("open");
          beatButton.setAttribute("aria-expanded", "false");
        }
        const open = !publisher.classList.contains("open");
        publisher.classList.toggle("open", open);
        publisherButton.setAttribute("aria-expanded", open ? "true" : "false");
      });
      publisher.querySelectorAll(".filter-option").forEach((option) => {
        option.addEventListener("click", (event) => {
          event.stopPropagation();
          selectedPublisher = option.dataset.publisher || "";
          publisherLabel.textContent = selectedPublisher || "All publishers";
          publisher.querySelectorAll(".filter-option").forEach((item) => item.classList.toggle("active", item === option));
          publisher.classList.remove("open");
          publisherButton.setAttribute("aria-expanded", "false");
          paintCompare();
        });
      });
      paintCompare();
    }

    function renderLiveView(data) {
      const metrics = data.command_metrics || {};
      const beats = beatOptions(data.articles || []);
      let selectedBeat = "";
      let selectedPublisher = "";
      const beatOptionsHtml = ["", ...beats].map((name) => `
        <button type="button" class="filter-option ${name ? "" : "active"}" data-beat="${escapeHtml(name)}">${name || "All beats"}</button>
      `).join("");
      const publisherChips = ["", ...pageConfig.publishers].map((name) => `
        <button type="button" class="filter-chip ${name ? "" : "active"}" data-publisher="${escapeHtml(name)}">${name || "All publishers"}</button>
      `).join("");

      content.innerHTML = `
        <section class="command-grid">
          <article class="command-card">
            <strong>${metrics.new_15m || 0}</strong>
            <span>New in 15m</span>
            <em>Fresh movement</em>
          </article>
          <article class="command-card">
            <strong>${metrics.competitor_only || 0}</strong>
            <span>Competitor-only</span>
            <em>No Daily Star beat match</em>
          </article>
          <article class="command-card">
            <strong>${metrics.daily_star_last_hour || 0}</strong>
            <span>Daily Star 1h</span>
            <em>Baseline pace</em>
          </article>
          <article class="command-card">
            <strong>${metrics.active_actions || 0}</strong>
            <span>Watching/assigned</span>
            <em>Desk action queue</em>
          </article>
        </section>
        <section class="page-grid live-layout">
          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Reporter command</h2>
                <p>Newest-first factual live feed for deciding what to watch, assign, or report now.</p>
              </div>
            </div>
            <div class="toolbar">
              <input id="liveSearch" type="search" placeholder="Search live feed">
              <div class="filter-dropdown" id="liveBeat">
                <button type="button" class="filter-dropdown-button" aria-haspopup="listbox" aria-expanded="false">
                  <span>All beats</span>
                </button>
                <div class="filter-menu" role="listbox">${beatOptionsHtml}</div>
              </div>
              <div class="filter-segments" id="livePublisher">${publisherChips}</div>
            </div>
            <div class="publisher-stack" id="liveResults"></div>
          </section>
          <section class="stack">
            <section class="panel">
              <div class="panel-header">
                <div>
                  <h3>Hot beats</h3>
                  <p>Where the last hour is concentrating.</p>
                </div>
              </div>
              <div class="analysis-block">
                <div class="analysis-list">
                  ${(data.top_categories || []).slice(0, 6).map((item) => `
                    <div class="analysis-row">
                      <strong>${item.name}</strong>
                      <span>${item.count} articles</span>
                    </div>
                  `).join("")}
                </div>
              </div>
            </section>
            <section class="panel">
              <div class="panel-header">
                <div>
                  <h3>Publisher pace</h3>
                  <p>Who is moving fastest in the last hour.</p>
                </div>
              </div>
              <div class="analysis-block">
                <div class="analysis-list">
                  ${pageConfig.publishers.map((name) => `
                    <div class="analysis-row">
                      <strong>${name}</strong>
                      <span>${(data.publisher_counts || {})[name] || 0} articles</span>
                    </div>
                  `).join("")}
                </div>
              </div>
            </section>
            ${renderSignalCard(data.live_signal || [])}
          </section>
        </section>
      `;

      const search = document.getElementById("liveSearch");
      const beat = document.getElementById("liveBeat");
      const beatButton = beat.querySelector(".filter-dropdown-button");
      const beatLabel = beatButton.querySelector("span");
      const publisher = document.getElementById("livePublisher");
      const liveResults = document.getElementById("liveResults");

      function liveRows(rows) {
        return rows.map((article) => `
          <article class="live-feed-row">
            <div data-label="Published">${formatTime(article.PublishedTime)}<br><span class="${ageClass(article.PublishedTime)}">${ageText(article.PublishedTime)}</span></div>
            <div data-label="Publisher">${publisherPill(article.Publisher)}</div>
            <div data-label="Beat"><span class="category-chip">${article.CanonicalCategory || article.Category || "Uncategorized"}</span></div>
            <div data-label="Headline" class="headline-cell">
              <a class="${publisherHeadlineClass(article.Publisher)}" href="${article.Link}" target="_blank" rel="noreferrer">${article.Headline}</a>
            </div>
            <div data-label="Action">${actionControl(article)}</div>
          </article>
        `).join("");
      }

      function paintLive() {
        const term = search.value.trim().toLowerCase();
        const rows = sortByTime((data.articles || []).filter((article) => {
          const text = `${article.Headline} ${article.Category} ${article.CanonicalCategory} ${article.Publisher}`.toLowerCase();
          const beatName = article.CanonicalCategory || article.Category || "Uncategorized";
          return (!selectedBeat || beatName === selectedBeat) &&
            (!selectedPublisher || article.Publisher === selectedPublisher) &&
            (!term || text.includes(term));
        }));

        liveResults.innerHTML = rows.length ? `
          <div class="live-feed">
            <div class="live-feed-head">
              <span>Published</span>
              <span>Publisher</span>
              <span>Beat</span>
              <span>Headline</span>
              <span>Action</span>
            </div>
            ${liveRows(rows)}
          </div>
        ` : '<div class="empty">No live items match these filters.</div>';
        bindActionControls(liveResults);
      }

      search.addEventListener("input", paintLive);
      beatButton.addEventListener("click", (event) => {
        event.stopPropagation();
        const open = !beat.classList.contains("open");
        beat.classList.toggle("open", open);
        beatButton.setAttribute("aria-expanded", open ? "true" : "false");
      });
      beat.querySelectorAll(".filter-option").forEach((option) => {
        option.addEventListener("click", (event) => {
          event.stopPropagation();
          selectedBeat = option.dataset.beat || "";
          beatLabel.textContent = selectedBeat || "All beats";
          beat.querySelectorAll(".filter-option").forEach((item) => item.classList.toggle("active", item === option));
          beat.classList.remove("open");
          beatButton.setAttribute("aria-expanded", "false");
          paintLive();
        });
      });
      publisher.querySelectorAll(".filter-chip").forEach((chip) => {
        chip.addEventListener("click", () => {
          selectedPublisher = chip.dataset.publisher || "";
          publisher.querySelectorAll(".filter-chip").forEach((item) => item.classList.toggle("active", item === chip));
          paintLive();
        });
      });
      paintLive();
    }

    function renderCoverageRows(rows) {
      if (!rows.length) {
        return '<div class="empty">No competitor stories in the active window.</div>';
      }
      return `
        <div class="publisher-stack">
          <table>
            <thead>
              <tr>
                <th>Publisher</th>
                <th>Published</th>
                <th>Status</th>
                <th>Similarity</th>
                <th>Confidence</th>
                <th>Category</th>
                <th>Competitor story</th>
                <th>Best Daily Star match</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map((item) => `
                <tr>
                  <td data-label="Publisher">${publisherPill(item.Publisher)}</td>
                  <td data-label="Published">${formatTime(item.PublishedTime)}<br><span class="meta">${ageText(item.PublishedTime)}</span></td>
                  <td data-label="Status"><span class="${statusClass(item.status)}">${item.status}</span></td>
                  <td data-label="Similarity">${(item.similarity || 0).toFixed(2)}</td>
                  <td data-label="Confidence">${(item.match_confidence || 0).toFixed(2)}<div class="meta">${item.match_reason || ""}</div></td>
                  <td data-label="Category"><span class="category-chip">${item.CanonicalCategory || item.Category || "Uncategorized"}</span></td>
                  <td data-label="Competitor story" class="headline-cell"><a class="${publisherHeadlineClass(item.Publisher)}" href="${item.Link}" target="_blank" rel="noreferrer">${item.Headline}</a></td>
                  <td data-label="Best Daily Star match" class="headline-cell">
                    ${item.best_match_link
                      ? `<a class="${publisherHeadlineClass(pageConfig.baselinePublisher)}" href="${item.best_match_link}" target="_blank" rel="noreferrer">${item.best_match_headline}</a><div class="meta">${item.best_match_category || ""}</div>`
                      : '<span class="meta">No baseline match found</span>'}
                  </td>
                  <td data-label="Action">${actionControl(item)}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      `;
    }

    function renderAnalysisView(data) {
      const comparisonStatus = data.comparison_status || "disabled";
      const gapRows = sortByTime((data.coverage_gaps || []).filter((row) => row.status !== "Covered"));
      const comparisonMessage = comparisonStatus === "disabled"
        ? "Exact matching is disabled"
        : comparisonStatus === "error"
          ? `OpenAI exact matching failed. Showing last saved results. ${data.comparison_error || ""}`
          : "Exact coverage matching is active. Embeddings shortlist candidates, then OpenAI confirms only same-event matches as covered.";

      content.innerHTML = `
        <section class="stack">
          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Coverage gap analysis</h2>
                <p>${pageConfig.baselinePublisher} is the baseline. Competitor stories are matched against its recent output.</p>
              </div>
            </div>
            <div class="analysis-block">
              <div class="note">${comparisonMessage}</div>
              <div class="analysis-list">
                <div class="analysis-row">
                  <strong>Potential gaps</strong>
                  <span>${(data.comparison_summary || {}).potential_gap || 0}</span>
                </div>
                <div class="analysis-row">
                  <strong>Needs review</strong>
                  <span>${(data.comparison_summary || {}).needs_review || 0}</span>
                </div>
                <div class="analysis-row">
                  <strong>Covered competitor stories</strong>
                  <span>${(data.comparison_summary || {}).covered || 0}</span>
                </div>
                <div class="analysis-row">
                  <strong>Total competitor stories scored</strong>
                  <span>${(data.comparison_summary || {}).competitor_total || 0}</span>
                </div>
              </div>
            </div>
            ${renderCoverageRows(gapRows)}
          </section>

          <section class="analysis-grid">
            <section class="panel analysis-block">
              <div class="panel-header">
                <div>
                  <h3>Missed by source</h3>
                  <p>How often each competitor produced a non-covered story.</p>
                </div>
              </div>
              <div class="analysis-list">
                ${(data.missed_by_source || []).map((item) => `
                  <div class="analysis-row">
                    <strong>${item.publisher}</strong>
                    <span>${item.count} flagged stories</span>
                  </div>
                `).join("") || '<div class="empty">No missed-by-source data yet.</div>'}
              </div>
            </section>

            <section class="panel analysis-block">
              <div class="panel-header">
                <div>
                  <h3>Category pressure</h3>
                  <p>Topics where competitors are publishing more than the baseline.</p>
                </div>
              </div>
              <div class="analysis-list">
                ${(data.category_pressure || []).map((item) => `
                  <div class="analysis-row">
                    <div>
                      <strong>${item.category}</strong>
                      <span>Competitors ${item.competitor_count}, ${pageConfig.baselinePublisher} ${item.baseline_count}</span>
                    </div>
                    <span>+${item.delta}</span>
                  </div>
                `).join("") || '<div class="empty">No category pressure detected.</div>'}
              </div>
            </section>

            <section class="panel analysis-block">
              <div class="panel-header">
                <div>
                  <h3>Publisher pace</h3>
                  <p>Volume in the full window and in the last hour.</p>
                </div>
              </div>
              <div class="analysis-list">
                ${(data.publisher_summaries || []).map((item) => `
                  <div class="analysis-row">
                    <div>
                      <strong>${item.name}</strong>
                      <span>${item.count} in ${data.window_hours}h, ${item.last_hour_count} in 1h</span>
                    </div>
                    <span>${item.latest_published_time ? `Latest ${formatTime(item.latest_published_time)}` : "No recent items"}</span>
                  </div>
                `).join("")}
              </div>
            </section>

            <section class="panel analysis-block">
              <div class="panel-header">
                <div>
                  <h3>Source architecture</h3>
                  <p>Future source additions should not need a new dashboard model.</p>
                </div>
              </div>
              <div class="note">${data.architecture_note}</div>
            </section>
          </section>

          <section class="analysis-grid">
            <section class="panel analysis-block">
              <div class="panel-header">
                <div>
                  <h3>Hourly breakdown</h3>
                  <p>Publishing rhythm over the active window.</p>
                </div>
              </div>
              ${(data.hourly_breakdown || []).length ? `
                <table class="hourly-table">
                  <thead>
                    <tr>
                      <th>Hour</th>
                      <th>Total</th>
                      <th>Publishers</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${(data.hourly_breakdown || []).map((item) => `
                      <tr>
                        <td data-label="Hour">${item.label}</td>
                        <td data-label="Total">${item.total}</td>
                        <td data-label="Publishers">${publisherCountGrid(item)}</td>
                      </tr>
                    `).join("")}
                  </tbody>
                </table>
              ` : '<div class="empty">No hourly data yet.</div>'}
            </section>

            <section class="stack">
              ${renderSignalCard(data.live_signal || [])}
              <section class="panel analysis-block">
                <div class="panel-header">
                  <div>
                    <h3>Shared categories</h3>
                    <p>Where the whole market is clustering coverage.</p>
                  </div>
                </div>
                <div class="analysis-list">
                  ${(data.shared_categories || []).map((item) => `
                    <div class="analysis-row">
                      <div>
                        <strong>${item.category}</strong>
                        <span>${item.total} total</span>
                      </div>
                      ${publisherCountGrid(item.publishers || {})}
                    </div>
                  `).join("")}
                </div>
              </section>
            </section>
          </section>
        </section>
      `;
      bindActionControls(content);
    }

    async function loadData() {
      const feedUrl = `/api/news?hours=${pageConfig.hours}`;
      const analysisUrl = `/api/analysis?hours=${pageConfig.hours}`;
      const requests = pageConfig.viewMode === "analysis"
        ? [fetch(feedUrl), fetch(analysisUrl)]
        : [fetch(feedUrl)];

      const responses = await Promise.all(requests);
      feedData = await responses[0].json();
      if (responses[1]) {
        analysisData = await responses[1].json();
      }

      renderSummaryCards(feedData);
      renderStatus(feedData, analysisData);

      if (pageConfig.viewMode === "live") {
        renderLiveView(feedData);
      } else if (pageConfig.viewMode === "analysis") {
        renderAnalysisView(analysisData);
      } else {
        renderCompareView(feedData);
      }
    }

    async function refreshNow() {
      refreshBtn.disabled = true;
      await fetch("/api/refresh", { method: "POST" });
      await loadData();
    }

    refreshBtn.addEventListener("click", refreshNow);
    loadData();
    setInterval(loadData, 60000);
  </script>
</body>
</html>
"""


def render_page(view_mode: str, title: str, subtitle: str, hours: float):
    return render_template_string(
        PAGE,
        title=title,
        subtitle=subtitle,
        view_mode=view_mode,
        page_config={
            "viewMode": view_mode,
            "hours": hours,
            "publishers": PUBLISHER_ORDER,
            "baselinePublisher": BASELINE_PUBLISHER,
            "competitors": COMPETITOR_PUBLISHERS,
        },
    )


@app.get("/")
def index():
    return render_page(
        "compare",
        "Reporter News Dashboard",
        "Five-hour all-publication timeline across every tracked publisher, with the newest story first.",
        WINDOW_HOURS,
    )


@app.get("/live")
def live():
    return render_page(
        "live",
        "Reporter Live Wire",
        "One-hour mixed feed for immediate detection of what the market is pushing right now.",
        LIVE_HOURS,
    )


@app.get("/analysis")
def analysis():
    return render_page(
        "analysis",
        "Coverage Gap Analysis",
        "Daily Star centric analysis for spotting competitor stories that may need follow-up from your newsroom.",
        WINDOW_HOURS,
    )


@app.get("/api/news")
def api_news():
    requested_hours = request.args.get("hours", default=WINDOW_HOURS, type=float)
    return jsonify(feed_snapshot(requested_hours))


@app.get("/api/analysis")
def api_analysis():
    requested_hours = request.args.get("hours", default=WINDOW_HOURS, type=float)
    return jsonify(analysis_snapshot(requested_hours))


@app.get("/api/actions")
def api_actions():
    with state_lock:
        return jsonify({"actions": state.get("actions", {})})


@app.post("/api/actions")
def api_update_action():
    payload = request.get_json(silent=True) or {}
    link = str(payload.get("link") or "").strip()
    if not link:
        return jsonify({"ok": False, "error": "Missing article link."}), 400

    action = action_payload({"status": payload.get("status"), "note": payload.get("note")})
    with state_lock:
        actions = dict(state.get("actions") or {})
        if action["status"] == "New" and not action["note"]:
            actions.pop(link, None)
        else:
            actions[link] = action
        state["actions"] = actions
        save_state()
    return jsonify({"ok": True, "link": link, "action": action})


@app.post("/api/refresh")
def api_refresh():
    threading.Thread(target=refresh_news, daemon=True).start()
    return jsonify({"ok": True})


def start_background() -> None:
    global background_started
    with background_lock:
        if background_started:
            return
        background_started = True

    load_state()
    threading.Thread(target=refresh_news, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()


if __name__ == "__main__":
    start_background()
    app.run(host="0.0.0.0", port=5000, debug=False)


if os.getenv("DASHBOARD_BACKGROUND", "").strip() == "1":
    start_background()
