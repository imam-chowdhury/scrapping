# Scrapping Dashboard (Prothom Alo + TBS)

Local dashboard that shows the latest articles from Prothom Alo and The Business Standard (TBS), filtered by a rolling time window (Asia/Dhaka).

## Run

```powershell
python -m pip install -r requirements.txt
python dashboard.py
```

Then open:

- http://localhost:5000/
- http://localhost:5000/live
- http://localhost:5000/analysis

## Notes

- Auto refresh runs on a schedule (see constants in `dashboard.py`).
- The dashboard stores a local cache JSON in `data/latest_news.json` (ignored by git).
- `/` shows the 5-hour publisher compare view.
- `/live` shows the last 1 hour mixed feed.
- `/analysis` shows category and pace summaries for quick newsroom analysis.

## Ubuntu Docker + Domain + SSL

See `DEPLOY_UBUNTU_DOCKER.md`.
