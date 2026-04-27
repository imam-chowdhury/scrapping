FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY dashboard.py /app/dashboard.py
COPY news_scraper.py /app/news_scraper.py
COPY README.md /app/README.md

RUN mkdir -p /app/data

EXPOSE 8000

ENV DASHBOARD_BACKGROUND=1
ENV WINDOW_HOURS=5
ENV REFRESH_SECONDS=600

CMD ["python", "-m", "gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "4", "dashboard:app"]
