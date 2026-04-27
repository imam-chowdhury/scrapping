import json
import os
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from flask import Flask, jsonify, render_template_string, request

from news_scraper import DHAKA, dhaka_now, scrape_prothomalo, scrape_tbs


REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", str(10 * 60)))
WINDOW_HOURS = float(os.getenv("WINDOW_HOURS", "5"))
LIVE_HOURS = 1.0
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_FILE = DATA_DIR / "latest_news.json"
PUBLISHER_ORDER = ["Prothom Alo", "The Business Standard"]

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


def sorted_publisher_counts(articles: List[Dict]) -> Dict[str, int]:
    counts = {name: 0 for name in PUBLISHER_ORDER}
    for article in articles:
        counts[article["Publisher"]] = counts.get(article["Publisher"], 0) + 1
    return counts


def top_categories(articles: List[Dict], limit: int = 8) -> List[Dict]:
    counts = Counter(article["Category"] or "Uncategorized" for article in articles)
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
        category = article["Category"] or "Uncategorized"
        category_map[category][article["Publisher"]] = category_map[category].get(article["Publisher"], 0) + 1

    rows = []
    for category, counts in category_map.items():
        total = sum(counts.values())
        rows.append(
            {
                "category": category,
                "total": total,
                "publishers": counts,
            }
        )

    rows.sort(key=lambda item: (item["total"], item["category"]), reverse=True)
    return rows[:limit]


def live_signal(articles: List[Dict]) -> List[Dict]:
    rows = []
    for article in articles[:12]:
        rows.append(
            {
                "headline": article["Headline"],
                "publisher": article["Publisher"],
                "category": article["Category"] or "Uncategorized",
                "published_time": article["PublishedTime"],
                "link": article["Link"],
            }
        )
    return rows


def feed_snapshot(hours: float) -> Dict:
    with state_lock:
        state["articles"] = prune_articles(state["articles"])
        base_articles = list(state["articles"])
        last_updated = state["last_updated"]
        next_run = state["next_run"]
        refreshing = state["refreshing"]
        error = state["error"]

    articles = filter_recent_articles(base_articles, hours)
    live_articles = filter_recent_articles(base_articles, LIVE_HOURS)

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
    }


def analysis_snapshot(hours: float) -> Dict:
    feed = feed_snapshot(hours)
    articles = feed["articles"]
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

    return {
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
    }


def save_state() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as file:
        json.dump(
            {
                "articles": state["articles"],
                "last_updated": state["last_updated"],
                "next_run": state["next_run"],
                "error": state["error"],
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
        state["articles"] = prune_articles(payload.get("articles", []))
        state["last_updated"] = payload.get("last_updated")
        state["next_run"] = payload.get("next_run")
        state["error"] = payload.get("error")


def refresh_news() -> None:
    with state_lock:
        if state["refreshing"]:
            return
        state["refreshing"] = True
        state["error"] = None

    try:
        cutoff = dhaka_now() - timedelta(hours=WINDOW_HOURS)
        articles = scrape_prothomalo(cutoff)
        articles.extend(scrape_tbs(cutoff))

        deduped = {}
        for article in articles:
            deduped[article["Link"]] = article

        fresh_articles = prune_articles(list(deduped.values()))
        now = dhaka_now()

        with state_lock:
            state["articles"] = fresh_articles
            state["last_updated"] = now.isoformat()
            state["next_run"] = (now + timedelta(seconds=REFRESH_SECONDS)).isoformat()
            state["refreshing"] = False
            save_state()
    except Exception as exc:
        now = dhaka_now()
        with state_lock:
            state["articles"] = prune_articles(state["articles"])
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
      --bg: #f3efe7;
      --panel: #fffdf8;
      --panel-alt: #f8f2e7;
      --text: #1f2430;
      --muted: #6c7484;
      --line: #ddd2c2;
      --accent: #0b6e4f;
      --accent-strong: #124e78;
      --accent-soft: #e7f4ef;
      --ink-soft: #f4eadb;
      --warning: #b45309;
      --danger: #b42318;
      --shadow: 0 10px 30px rgba(52, 44, 31, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Noto Sans Bengali", serif;
      background:
        radial-gradient(circle at top left, rgba(11,110,79,0.08), transparent 22%),
        radial-gradient(circle at top right, rgba(18,78,120,0.08), transparent 26%),
        linear-gradient(180deg, #f7f2ea 0%, #efe7da 100%);
      color: var(--text);
    }
    a {
      color: var(--accent-strong);
      text-decoration: none;
    }
    a:hover { text-decoration: underline; }
    header {
      border-bottom: 1px solid rgba(108, 116, 132, 0.16);
      background: rgba(255, 253, 248, 0.9);
      backdrop-filter: blur(16px);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    .shell {
      width: min(1380px, calc(100% - 32px));
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
      font-size: 29px;
      line-height: 1.05;
      letter-spacing: 0;
    }
    .title-block p {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
      max-width: 760px;
    }
    .actions {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
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
    button:disabled {
      cursor: wait;
      opacity: 0.72;
    }
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
      background: rgba(255, 253, 248, 0.9);
      color: var(--muted);
      font-size: 14px;
      font-weight: 700;
    }
    .nav-link.active {
      background: var(--text);
      border-color: var(--text);
      color: white;
    }
    main {
      width: min(1380px, calc(100% - 32px));
      margin: 24px auto 48px;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
    }
    .panel, .summary-card, .signal-card {
      background: var(--panel);
      border: 1px solid rgba(108, 116, 132, 0.14);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .summary-card {
      padding: 16px;
    }
    .summary-card strong {
      display: block;
      font-size: 30px;
      line-height: 1;
      margin-bottom: 8px;
    }
    .summary-card span {
      color: var(--muted);
      font-size: 13px;
    }
    .summary-card em {
      display: block;
      margin-top: 8px;
      font-style: normal;
      color: var(--accent-strong);
      font-size: 13px;
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
      grid-template-columns: minmax(0, 1.7fr) minmax(320px, 0.9fr);
      gap: 16px;
    }
    .panel-header {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-start;
      padding: 18px 18px 0;
    }
    .panel-header h2,
    .panel-header h3 {
      margin: 0;
      font-size: 19px;
      line-height: 1.15;
      letter-spacing: 0;
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
      border-top: 1px solid rgba(108, 116, 132, 0.12);
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
    .publisher-stack {
      padding: 0 18px 18px;
    }
    .publisher-section + .publisher-section {
      margin-top: 18px;
    }
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
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid rgba(108, 116, 132, 0.12);
      border-radius: 8px;
      overflow: hidden;
    }
    th, td {
      padding: 11px 12px;
      border-bottom: 1px solid rgba(108, 116, 132, 0.12);
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
    .age {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 72px;
      border-radius: 999px;
      background: var(--ink-soft);
      border: 1px solid var(--line);
      padding: 4px 9px;
      font-size: 13px;
      font-weight: 700;
      color: #2f3a44;
      white-space: nowrap;
    }
    .age.new {
      background: #d8f0e7;
      border-color: #9fd2bf;
      color: #115e59;
    }
    .publisher-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 4px 10px;
      border: 1px solid var(--line);
      background: #f8f7f3;
      font-size: 13px;
      white-space: nowrap;
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
    .headline-cell {
      min-width: 340px;
    }
    .headline-cell a {
      font-weight: 700;
    }
    .analysis-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }
    .analysis-block {
      padding: 18px;
    }
    .analysis-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .analysis-row {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: center;
      padding-bottom: 10px;
      border-bottom: 1px solid rgba(108, 116, 132, 0.1);
    }
    .analysis-row:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }
    .analysis-row strong {
      font-size: 15px;
    }
    .analysis-row span {
      color: var(--muted);
      font-size: 13px;
    }
    .signal-card {
      padding: 18px;
    }
    .signal-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .signal-item {
      padding-bottom: 10px;
      border-bottom: 1px solid rgba(108, 116, 132, 0.1);
    }
    .signal-item:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }
    .signal-item a {
      display: block;
      margin: 5px 0;
      font-weight: 700;
    }
    .stack {
      display: grid;
      gap: 16px;
    }
    .empty {
      padding: 34px 18px;
      text-align: center;
      color: var(--muted);
      border-top: 1px solid rgba(108, 116, 132, 0.1);
    }
    @media (max-width: 1080px) {
      .summary-grid,
      .analysis-grid,
      .page-grid {
        grid-template-columns: 1fr;
      }
    }
    @media (max-width: 760px) {
      .header-row {
        flex-direction: column;
      }
      .summary-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      table, thead, tbody, tr, th, td {
        display: block;
      }
      thead {
        display: none;
      }
      td {
        border-bottom: 0;
        padding: 6px 12px;
      }
      td::before {
        content: attr(data-label);
        display: block;
        color: var(--muted);
        font-size: 12px;
        margin-bottom: 3px;
      }
      .publisher-heading,
      .analysis-row {
        flex-direction: column;
        align-items: flex-start;
      }
      .headline-cell {
        min-width: 0;
      }
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

    function renderSummaryCards(data) {
      const liveTotal = Object.values(data.last_hour_counts || {}).reduce((sum, count) => sum + count, 0);
      const cards = [
        { value: data.count || 0, label: `Last ${data.window_hours}h total`, note: `${liveTotal} in the last 1h` },
        { value: (data.publisher_counts || {})["Prothom Alo"] || 0, label: "Prothom Alo", note: `${(data.last_hour_counts || {})["Prothom Alo"] || 0} in the last 1h` },
        { value: (data.publisher_counts || {})["The Business Standard"] || 0, label: "TBS", note: `${(data.last_hour_counts || {})["The Business Standard"] || 0} in the last 1h` },
        { value: (data.top_categories || [])[0] ? data.top_categories[0].count : 0, label: "Top category volume", note: (data.top_categories || [])[0] ? data.top_categories[0].name : "No category yet" }
      ];

      summaryGrid.innerHTML = cards.map((card) => `
        <article class="summary-card">
          <strong>${card.value}</strong>
          <span>${card.label}</span>
          <em>${card.note}</em>
        </article>
      `).join("");
    }

    function renderStatus(data) {
      statusBox.className = data.error ? "status error" : "status";
      const lastUpdated = data.last_updated ? formatTime(data.last_updated) : "not yet";
      const nextRun = data.next_run ? formatTime(data.next_run) : "pending";
      statusBox.textContent = data.error
        ? `Scrape error: ${data.error}`
        : `Last updated: ${lastUpdated}. Next scheduled scrape: ${nextRun}.`;
      refreshBtn.disabled = Boolean(data.refreshing);
    }

    function renderSignalCard(items) {
      return `
        <aside class="signal-card">
          <div class="panel-header">
            <div>
              <h3>Fresh signal</h3>
              <p>The first items reporters should scan right now.</p>
            </div>
          </div>
          ${items.length ? `
            <div class="signal-list">
              ${items.map((item) => `
                <div class="signal-item">
                  <div class="meta">${item.publisher} · ${item.category} · ${ageText(item.published_time)}</div>
                  <a href="${item.link}" target="_blank" rel="noreferrer">${item.headline}</a>
                </div>
              `).join("")}
            </div>
          ` : '<div class="empty">No fresh articles yet.</div>'}
        </aside>
      `;
    }

    function compareTableRows(rows) {
      return rows.map((article) => `
        <tr>
          <td data-label="Published">${formatTime(article.PublishedTime)}</td>
          <td data-label="Age"><span class="${ageClass(article.PublishedTime)}">${ageText(article.PublishedTime)}</span></td>
          <td data-label="Category"><span class="category-chip">${article.Category || "Uncategorized"}</span></td>
          <td data-label="Headline" class="headline-cell"><a href="${article.Link}" target="_blank" rel="noreferrer">${article.Headline}</a></td>
        </tr>
      `).join("");
    }

    function renderCompareView(data) {
      const searchBar = `
        <div class="toolbar">
          <input id="search" type="search" placeholder="Search headline, category, publisher">
          <select id="publisher">
            <option value="">All publishers</option>
            <option value="Prothom Alo">Prothom Alo</option>
            <option value="The Business Standard">The Business Standard</option>
          </select>
        </div>
      `;

      content.innerHTML = `
        <section class="page-grid">
          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Publisher compare</h2>
                <p>Five-hour grouped feed for side-by-side scanning.</p>
              </div>
            </div>
            ${searchBar}
            <div class="publisher-stack" id="compareResults"></div>
          </section>
          <section class="stack">
            ${renderSignalCard(data.live_signal || [])}
            <section class="panel">
              <div class="panel-header">
                <div>
                  <h3>Top categories</h3>
                  <p>Where today’s publishing volume is landing.</p>
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
      const publisher = document.getElementById("publisher");
      const compareResults = document.getElementById("compareResults");

      function paintCompare() {
        const term = search.value.trim().toLowerCase();
        const selectedPublisher = publisher.value;
        const filtered = (data.articles || []).filter((article) => {
          const text = `${article.Headline} ${article.Category} ${article.Publisher}`.toLowerCase();
          return (!selectedPublisher || article.Publisher === selectedPublisher) &&
            (!term || text.includes(term));
        });

        if (!filtered.length) {
          compareResults.innerHTML = '<div class="empty">No matching articles in this view.</div>';
          return;
        }

        compareResults.innerHTML = pageConfig.publishers
          .filter((name) => !selectedPublisher || name === selectedPublisher)
          .map((name) => {
            const rows = filtered.filter((article) => article.Publisher === name);
            if (!rows.length) {
              return "";
            }
            return `
              <section class="publisher-section">
                <div class="publisher-heading">
                  <h3>${name}</h3>
                  <div class="meta">${rows.length} articles, newest first</div>
                </div>
                <table>
                  <thead>
                    <tr>
                      <th>Published</th>
                      <th>Age</th>
                      <th>Category</th>
                      <th>Headline</th>
                    </tr>
                  </thead>
                  <tbody>${compareTableRows(rows)}</tbody>
                </table>
              </section>
            `;
          }).join("");
      }

      search.addEventListener("input", paintCompare);
      publisher.addEventListener("change", paintCompare);
      paintCompare();
    }

    function renderLiveView(data) {
      const rows = (data.articles || []).map((article) => `
        <tr>
          <td data-label="Published">${formatTime(article.PublishedTime)}</td>
          <td data-label="Age"><span class="${ageClass(article.PublishedTime)}">${ageText(article.PublishedTime)}</span></td>
          <td data-label="Publisher"><span class="publisher-badge">${article.Publisher}</span></td>
          <td data-label="Category"><span class="category-chip">${article.Category || "Uncategorized"}</span></td>
          <td data-label="Headline" class="headline-cell"><a href="${article.Link}" target="_blank" rel="noreferrer">${article.Headline}</a></td>
        </tr>
      `).join("");

      content.innerHTML = `
        <section class="page-grid">
          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Live wire</h2>
                <p>One-hour mixed stream for immediate newsroom monitoring.</p>
              </div>
            </div>
            ${rows ? `
              <div class="publisher-stack">
                <table>
                  <thead>
                    <tr>
                      <th>Published</th>
                      <th>Age</th>
                      <th>Publisher</th>
                      <th>Category</th>
                      <th>Headline</th>
                    </tr>
                  </thead>
                  <tbody>${rows}</tbody>
                </table>
              </div>
            ` : '<div class="empty">No articles in the last 1 hour.</div>'}
          </section>
          <section class="stack">
            ${renderSignalCard(data.live_signal || [])}
            <section class="panel">
              <div class="panel-header">
                <div>
                  <h3>Publisher pace</h3>
                  <p>Who is publishing fastest inside the live window.</p>
                </div>
              </div>
              <div class="analysis-block">
                <div class="analysis-list">
                  ${pageConfig.publishers.map((name) => `
                    <div class="analysis-row">
                      <strong>${name}</strong>
                      <span>${(data.publisher_counts || {})[name] || 0} articles in 1h</span>
                    </div>
                  `).join("")}
                </div>
              </div>
            </section>
          </section>
        </section>
      `;
    }

    function renderAnalysisView(data) {
      content.innerHTML = `
        <section class="analysis-grid">
          <section class="panel analysis-block">
            <div class="panel-header">
              <div>
                <h2>Publisher summaries</h2>
                <p>Quick read on who is pushing volume and where.</p>
              </div>
            </div>
            <div class="analysis-list">
              ${(data.publisher_summaries || []).map((item) => `
                <div class="analysis-row">
                  <div>
                    <strong>${item.name}</strong>
                    <span>${item.count} in last ${data.window_hours}h, ${item.last_hour_count} in last 1h</span>
                  </div>
                  <span>${item.latest_published_time ? `Latest ${formatTime(item.latest_published_time)}` : "No recent items"}</span>
                </div>
              `).join("")}
            </div>
          </section>
          <section class="panel analysis-block">
            <div class="panel-header">
              <div>
                <h2>Shared categories</h2>
                <p>Where both publishers are clustering coverage.</p>
              </div>
            </div>
            <div class="analysis-list">
              ${(data.shared_categories || []).map((item) => `
                <div class="analysis-row">
                  <div>
                    <strong>${item.category}</strong>
                    <span>${item.total} total</span>
                  </div>
                  <span>PA ${(item.publishers || {})["Prothom Alo"] || 0} · TBS ${(item.publishers || {})["The Business Standard"] || 0}</span>
                </div>
              `).join("")}
            </div>
          </section>
          <section class="panel analysis-block">
            <div class="panel-header">
              <div>
                <h2>Hourly breakdown</h2>
                <p>Publishing rhythm over the current five-hour window.</p>
              </div>
            </div>
            ${(data.hourly_breakdown || []).length ? `
              <table>
                <thead>
                  <tr>
                    <th>Hour</th>
                    <th>Total</th>
                    <th>Prothom Alo</th>
                    <th>TBS</th>
                  </tr>
                </thead>
                <tbody>
                  ${(data.hourly_breakdown || []).map((item) => `
                    <tr>
                      <td data-label="Hour">${item.label}</td>
                      <td data-label="Total">${item.total}</td>
                      <td data-label="Prothom Alo">${item["Prothom Alo"] || 0}</td>
                      <td data-label="TBS">${item["The Business Standard"] || 0}</td>
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
                  <h3>Top categories</h3>
                  <p>Highest-volume coverage buckets right now.</p>
                </div>
              </div>
              <div class="analysis-list">
                ${(data.top_categories || []).map((item) => `
                  <div class="analysis-row">
                    <strong>${item.name}</strong>
                    <span>${item.count} articles</span>
                  </div>
                `).join("")}
              </div>
            </section>
          </section>
        </section>
      `;
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
      renderStatus(feedData);

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
        },
    )


@app.get("/")
def index():
    return render_page(
        "compare",
        "Reporter News Dashboard",
        "Five-hour compare view for Prothom Alo and TBS, grouped so reporters can scan coverage side by side.",
        WINDOW_HOURS,
    )


@app.get("/live")
def live():
    return render_page(
        "live",
        "Reporter Live Wire",
        "One-hour mixed feed for fast monitoring, new-story detection, and immediate follow-up decisions.",
        LIVE_HOURS,
    )


@app.get("/analysis")
def analysis():
    return render_page(
        "analysis",
        "Reporter Analysis View",
        "A compact readout of pace, category concentration, and overlap so the desk can spot patterns quickly.",
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


@app.post("/api/refresh")
def api_refresh():
    if request.method == "POST":
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
