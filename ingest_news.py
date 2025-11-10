import os
import requests
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
import time

DATABASE_URL = os.getenv("DATABASE_URL")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")

FINNHUB_NEWS_URL = "https://finnhub.io/api/v1/company-news"
HF_API_URL = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"

HEADERS = {
    "Authorization": f"Bearer {HF_TOKEN}",
    "x-use-cache": "0",
}

vader = SentimentIntensityAnalyzer()


# ======================================================
# ✅ DATABASE HELPER
# ======================================================
def db():
    return psycopg2.connect(DATABASE_URL)


# ======================================================
# ✅ SAFE FINBERT CALL — with retry + fallback
# ======================================================
def finbert_sentiment(text):
    if HF_TOKEN is None or HF_TOKEN.strip() == "":
        return None  # will fallback to VADER

    for attempt in range(3):
        try:
            resp = requests.post(HF_API_URL, json={"inputs": text}, headers=HEADERS, timeout=25)

            if resp.status_code != 200:
                print(f"[FinBERT HTTP ERROR] {resp.status_code}: {resp.text}")
                continue

            data = resp.json()

            if "error" in data:
                print(f"[FinBERT ERROR] {data['error']}")
                continue

            # Expected output:
            # [[{"label": "positive", "score": 0.98}, ...]]
            scores = data[0]
            score_map = {s["label"]: s["score"] for s in scores}

            compound = score_map.get("positive", 0) - score_map.get("negative", 0)
            return compound

        except Exception as e:
            print(f"[FinBERT Exception] {e}")
            time.sleep(1)

    return None  # fallback triggers


# ======================================================
# ✅ FALLBACK SENTIMENT (VADER)
# ======================================================
def vader_sentiment(text):
    s = vader.polarity_scores(text)
    return s["compound"]


# ======================================================
# ✅ MAIN NEWS INGEST FUNCTION
# ======================================================
def fetch_news_data():
    print("News ingestion starting...")
    print("===============================")

    # ----- 1️⃣ Fetch symbols from DB -----
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT symbol FROM tracked_symbols")
        symbols = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        print("[ERROR] Could not fetch tracked symbols:", e)
        return

    print(f"[INFO] Tracking {len(symbols)} symbols...")

    # ----- 2️⃣ Fetch FINNHUB news for each symbol -----
    all_articles = []

    for sym in symbols:
        print(f"[FETCH] Fetching news for {sym}...")
        try:
            url = f"{FINNHUB_NEWS_URL}?symbol={sym}&from=2020-01-01&to=2030-01-01&token={FINNHUB_API_KEY}"
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                print(f"[WARN] Finnhub returned {r.status_code} for {sym}")
                continue

            data = r.json()

            for item in data:
                if "headline" not in item:
                    continue

                article = {
                    "symbol": sym,
                    "headline": item["headline"],
                    "source_name": item.get("source", ""),
                    "time": datetime.utcfromtimestamp(item["datetime"]),
                    "url": item.get("url", ""),
                    "finnhub_id": item.get("id", None),
                }
                all_articles.append(article)

        except Exception as e:
            print(f"[FETCH ERROR] {sym}: {e}")

    print(f"[INFO] Unique articles found: {len(all_articles)}")

    # ----- 3️⃣ Remove duplicates by FINNHUB ID in-memory -----
    seen = set()
    filtered = []

    for a in all_articles:
        fid = a["finnhub_id"]
        key = fid or a["headline"]  # safe fallback
        if key not in seen:
            seen.add(key)
            filtered.append(a)

    print(f"[INFO] Filtered unique: {len(filtered)}")

    # ----- 4️⃣ Compute sentiment for each article -----
    print("[INFO] Starting sentiment analysis...")

    for a in filtered:
        headline = a["headline"]
        score = finbert_sentiment(headline)

        if score is None:
            score = vader_sentiment(headline)

        a["sentiment_score"] = score

    # ----- 5️⃣ Insert into DB -----
    inserted = 0

    try:
        conn = db()
        cur = conn.cursor()

        for a in filtered:
            cur.execute("""
                INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url, finnhub_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                a["time"],
                a["symbol"],
                a["headline"],
                a["source_name"],
                a["sentiment_score"],
                a["url"],
                a["finnhub_id"]
            ))
            inserted += cur.rowcount

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print("[DATABASE INSERT ERROR]", e)
        return

    avg_sentiment = sum(a["sentiment_score"] for a in filtered) / len(filtered)
    print(f"[SUCCESS] Inserted {inserted} new articles.")
    print(f"[SENTIMENT] Average sentiment: {avg_sentiment:.4f}")
    print("News ingestion complete.")
