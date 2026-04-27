import json
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from flask import Flask, jsonify, render_template_string, request

from news_scraper import DHAKA, dhaka_now, scrape_prothomalo, scrape_tbs


REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", str(10 * 60)))
WINDOW_HOURS = float(os.getenv("WINDOW_HOURS", "5"))
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_FILE = DATA_DIR / "latest_news.json"

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


def prune_articles(articles: List[Dict]) -> List[Dict]:
    cutoff = dhaka_now() - timedelta(hours=WINDOW_HOURS)
    return [article for article in articles if parse_time(article["PublishedTime"]) >= cutoff]


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
        fresh_articles.sort(key=lambda item: item["PublishedTime"], reverse=True)
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
            state["last_updated"] = state["last_updated"]
            state["next_run"] = (now + timedelta(seconds=REFRESH_SECONDS)).isoformat()
            state["refreshing"] = False
            state["error"] = str(exc)
            save_state()


def scheduler_loop() -> None:
    while True:
        time.sleep(REFRESH_SECONDS)
        refresh_news()


def snapshot() -> Dict:
    with state_lock:
        state["articles"] = prune_articles(state["articles"])
        articles = list(state["articles"])
        payload = {
            "articles": articles,
            "count": len(articles),
            "last_updated": state["last_updated"],
            "next_run": state["next_run"],
            "refreshing": state["refreshing"],
            "error": state["error"],
            "window_hours": WINDOW_HOURS,
        }
        payload["publisher_counts"] = {
            "Prothom Alo": sum(1 for item in articles if item["Publisher"] == "Prothom Alo"),
            "The Business Standard": sum(
                1 for item in articles if item["Publisher"] == "The Business Standard"
            ),
        }
        return payload


PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Reporter News Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #1f2933;
      --muted: #64748b;
      --line: #d9dee7;
      --accent: #0f766e;
      --accent-weak: #e7f5f2;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, "Noto Sans Bengali", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      padding: 18px 24px;
      position: sticky;
      top: 0;
      z-index: 2;
    }
    .header-row, .toolbar, .stats {
      display: flex;
      gap: 12px;
      align-items: center;
      flex-wrap: wrap;
    }
    .header-row { justify-content: space-between; }
    h1 {
      font-size: 22px;
      line-height: 1.2;
      margin: 0;
      letter-spacing: 0;
    }
    main {
      width: min(1220px, calc(100% - 32px));
      margin: 20px auto 40px;
    }
    .stat {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px 14px;
      min-width: 150px;
    }
    .stat strong {
      display: block;
      font-size: 24px;
      line-height: 1;
      margin-bottom: 5px;
    }
    .stat span, .meta {
      color: var(--muted);
      font-size: 13px;
    }
    .toolbar {
      justify-content: space-between;
      margin: 18px 0 12px;
    }
    input, select, button {
      height: 38px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
      padding: 0 10px;
      font-size: 14px;
    }
    input { min-width: min(420px, 100%); flex: 1; }
    button {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
      cursor: pointer;
      font-weight: 600;
    }
    button:disabled {
      opacity: .65;
      cursor: wait;
    }
    .status {
      background: var(--accent-weak);
      color: #115e59;
      border: 1px solid #b7ded8;
      border-radius: 8px;
      padding: 10px 12px;
      margin: 14px 0;
      font-size: 14px;
    }
    .status.error {
      background: #fff1f0;
      color: var(--danger);
      border-color: #f3b5ae;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 11px 12px;
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }
    th {
      background: #eef1f5;
      font-size: 12px;
      text-transform: uppercase;
      color: #475569;
      letter-spacing: 0;
    }
    tr:last-child td { border-bottom: 0; }
    a {
      color: #075985;
      text-decoration: none;
      font-weight: 600;
    }
    a:hover { text-decoration: underline; }
    .publisher {
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 8px;
      white-space: nowrap;
      background: #fafafa;
    }
    .publisher-section {
      margin-top: 16px;
    }
    .publisher-heading {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin: 0 0 8px;
    }
    .publisher-heading h2 {
      font-size: 18px;
      margin: 0;
      letter-spacing: 0;
    }
    .age {
      display: inline-block;
      min-width: 68px;
      color: #334155;
      background: #f1f5f9;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 8px;
      text-align: center;
      white-space: nowrap;
      font-weight: 600;
    }
    .headline-cell {
      min-width: 360px;
    }
    .empty {
      background: var(--panel);
      border: 1px dashed var(--line);
      border-radius: 8px;
      padding: 28px;
      text-align: center;
      color: var(--muted);
    }
    @media (max-width: 760px) {
      header { position: static; }
      table, thead, tbody, th, td, tr { display: block; }
      thead { display: none; }
      tr {
        border-bottom: 1px solid var(--line);
        padding: 8px 0;
      }
      .headline-cell { min-width: 0; }
      .publisher-heading {
        align-items: flex-start;
        flex-direction: column;
      }
      td {
        border: 0;
        padding: 6px 12px;
      }
      td::before {
        content: attr(data-label);
        display: block;
        color: var(--muted);
        font-size: 12px;
        margin-bottom: 3px;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="header-row">
      <div>
        <h1>Reporter News Dashboard</h1>
        <div class="meta">Prothom Alo and TBS, last 5 hours, refreshed every 10 minutes</div>
      </div>
      <button id="refreshBtn" type="button">Refresh now</button>
    </div>
  </header>
  <main>
    <section class="stats">
      <div class="stat"><strong id="totalCount">0</strong><span>Total articles</span></div>
      <div class="stat"><strong id="paCount">0</strong><span>Prothom Alo</span></div>
      <div class="stat"><strong id="tbsCount">0</strong><span>TBS</span></div>
    </section>
    <div id="status" class="status">Loading latest scrape...</div>
    <section class="toolbar">
      <input id="search" type="search" placeholder="Search headline, category, publisher">
      <select id="publisher">
        <option value="">All publishers</option>
        <option value="Prothom Alo">Prothom Alo</option>
        <option value="The Business Standard">The Business Standard</option>
      </select>
    </section>
    <section id="content"></section>
  </main>
  <script>
    let articles = [];
    const totalCount = document.getElementById("totalCount");
    const paCount = document.getElementById("paCount");
    const tbsCount = document.getElementById("tbsCount");
    const statusBox = document.getElementById("status");
    const content = document.getElementById("content");
    const search = document.getElementById("search");
    const publisher = document.getElementById("publisher");
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

    function renderPublisherSection(name, rows) {
      if (!rows.length) return "";

      const tableRows = rows.map((article) => `
        <tr>
          <td data-label="Published">${formatTime(article.PublishedTime)}</td>
          <td data-label="Age"><span class="age">${ageText(article.PublishedTime)}</span></td>
          <td data-label="Category">${article.Category || ""}</td>
          <td data-label="Headline" class="headline-cell"><a href="${article.Link}" target="_blank" rel="noreferrer">${article.Headline}</a></td>
        </tr>
      `).join("");

      return `
        <section class="publisher-section">
          <div class="publisher-heading">
            <h2>${name}</h2>
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
            <tbody>${tableRows}</tbody>
          </table>
        </section>
      `;
    }

    function render() {
      const term = search.value.trim().toLowerCase();
      const selectedPublisher = publisher.value;
      const filtered = articles.filter((article) => {
        const text = `${article.Headline} ${article.Category} ${article.Publisher}`.toLowerCase();
        return (!selectedPublisher || article.Publisher === selectedPublisher) &&
          (!term || text.includes(term));
      });

      if (!filtered.length) {
        content.innerHTML = '<div class="empty">No matching articles in the last 5 hours.</div>';
        return;
      }

      const orderedPublishers = ["Prothom Alo", "The Business Standard"];
      content.innerHTML = orderedPublishers
        .filter((name) => !selectedPublisher || name === selectedPublisher)
        .map((name) => {
          const rows = filtered
            .filter((article) => article.Publisher === name)
            .sort((a, b) => new Date(b.PublishedTime) - new Date(a.PublishedTime));
          return renderPublisherSection(name, rows);
        })
        .join("");
    }

    async function loadNews() {
      const response = await fetch("/api/news");
      const data = await response.json();
      articles = data.articles || [];
      totalCount.textContent = data.count || 0;
      paCount.textContent = data.publisher_counts["Prothom Alo"] || 0;
      tbsCount.textContent = data.publisher_counts["The Business Standard"] || 0;

      statusBox.className = data.error ? "status error" : "status";
      const lastUpdated = data.last_updated ? formatTime(data.last_updated) : "not yet";
      const nextRun = data.next_run ? formatTime(data.next_run) : "pending";
      statusBox.textContent = data.error
        ? `Scrape error: ${data.error}`
        : `Last updated: ${lastUpdated}. Next scheduled scrape: ${nextRun}. Showing last ${data.window_hours} hours.${data.refreshing ? " Refreshing now..." : ""}`;
      refreshBtn.disabled = Boolean(data.refreshing);
      render();
    }

    async function refreshNow() {
      refreshBtn.disabled = true;
      await fetch("/api/refresh", { method: "POST" });
      await loadNews();
    }

    search.addEventListener("input", render);
    publisher.addEventListener("change", render);
    refreshBtn.addEventListener("click", refreshNow);
    loadNews();
    setInterval(loadNews, 60000);
  </script>
</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(PAGE)


@app.get("/api/news")
def api_news():
    return jsonify(snapshot())


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
