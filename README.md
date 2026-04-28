# Scrapping Dashboard

Local newsroom dashboard for comparing recent output from:

- The Daily Star
- Prothom Alo
- The Business Standard

It keeps a rolling Asia/Dhaka time window, refreshes on a schedule, and can optionally use OpenAI to flag competitor stories that do not have an exact Daily Star match.

## Run

```powershell
python -m pip install -r requirements.txt
python dashboard.py
```

Then open:

- http://localhost:5000/
- http://localhost:5000/live
- http://localhost:5000/analysis

## Views

- `/` shows the 5-hour grouped compare view with The Daily Star first.
- `/live` shows the last 1 hour mixed feed.
- `/analysis` shows Daily Star centric coverage-gap analysis, category pressure, pace, and overlap.

## Optional OpenAI setup

Exact coverage comparison is optional. Without an API key, the feed still works and the analysis API reports `comparison_status = disabled`.

Create a local `.env` file beside `dashboard.py`:

```powershell
copy .env.example .env
notepad .env
```

Then set `OPENAI_API_KEY` in `.env` and run:

```powershell
python dashboard.py
```

## Production-safe scraping

The scraper spaces requests per host and caches verified article details to reduce repeat hits.

```powershell
$env:SCRAPE_DELAY_MIN="0.5"
$env:SCRAPE_DELAY_MAX="2.0"
$env:SCRAPE_MAX_RETRIES="3"
$env:DAILY_STAR_WORKERS="4"
python dashboard.py
```

## Source architecture

Scrapers are registry-based in [news_scraper.py](C:/Users/ihc/Documents/New project 2/news_scraper.py). To add a future outlet, add a scraper function and a registry entry. The dashboard and comparison model will pick it up without a UI rewrite.

## Cache and schedule

- Refresh cadence is controlled in [dashboard.py](C:/Users/ihc/Documents/New project 2/dashboard.py).
- Cached state is written to `data/latest_news.json`.
- Article details are cached by link so repeated refreshes do not re-open the same TBS/Daily Star story pages.
- Embeddings are cached by article link so unchanged stories are not re-embedded every refresh.
- Exact-match decisions are cached so repeated refreshes do not re-check the same competitor and Daily Star candidate set.

## Ubuntu Docker + Domain + SSL

See `DEPLOY_UBUNTU_DOCKER.md`.
