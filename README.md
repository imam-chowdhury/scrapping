# Scrapping Dashboard (Prothom Alo + TBS)

Local dashboard that shows the latest articles from Prothom Alo and The Business Standard (TBS), filtered by a rolling time window (Asia/Dhaka).

## Run

```powershell
python -m pip install -r requirements.txt
python dashboard.py
```

Then open:

- http://localhost:5000/

## Notes

- Auto refresh runs on a schedule (see constants in `dashboard.py`).
- The dashboard stores a local cache JSON in `data/latest_news.json` (ignored by git).
