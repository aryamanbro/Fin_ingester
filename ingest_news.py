import os
import psycopg2
import finnhub
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")

finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

FINBERT_API_URL = "https://router.huggingface.co/hf-inference/ProsusAI/finbert"
FINBERT_HEADERS = {
    "Authorization": f"Bearer {HF_TOKEN}",
    "Content-Type": "application/json"
}

def get_db():
    return psycopg2.connect(DATABASE_URL)

def clean_text(text: str) -> str:
    return text.replace("\n", " ").replace("\r", " ").strip()[:450]

def analyze_sentiment_finbert(text):
    """
    FinBERT-only sentiment evaluation.
    If request fails -> return 0.0 (neutral).
    """
    try:
        response = requests.post(
            FINBERT_API_URL,
            headers=FINBERT_HEADERS,
            json={"inputs": text},
            timeout=10
        )

        if response.status_code != 200:
            print(f"[FinBERT ERROR] status={response.status_code} → {response.text}")
            return 0.0

        data = response.json()

        # FinBERT returns list of dicts:
        # [{"label": "positive", "score": 0.98}, ...]
        if isinstance(data, list) and len(data) > 0:
            label = data[0]["label"].lower()
            score = float(data[0]["score"])

            if label == "positive":
                return score
            elif label == "negative":
                return -score
            else:
                return 0.0

        return 0.0

    except Exception as e:
        print(f"[FinBERT ERROR] {e}")
        return 0.0


def fetch_news_data():
    print("News ingestion starting...")
    print("===============================")

    conn = get_db()
    cur = conn.cursor()

    # ✅ Ensure columns exist without UNIQUE constraints
    # Do NOT add UNIQUE here – it breaks hypertable partition rules.
    cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS url TEXT;")
    cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS finnhub_id BIGINT;")
    conn.commit()
    print("[INFO] Columns 'url' and 'finnhub_id' confirmed.")

    # ✅ Fetch tracked symbols
    cur.execute("SELECT symbol FROM tracked_symbols;")
    symbols = [row[0] for row in cur.fetchall()]
    print(f"[INFO] Tracking {len(symbols)} symbols...")

    raw_articles = []

    for sym in symbols:
        print(f"[FETCH] Fetching news for {sym}...")
        try:
            news = finnhub_client.company_news(sym, _from="2024-01-01", to="2030-01-01")
            raw_articles.extend(news)
        except Exception as e:
            print(f"[ERROR] Cannot fetch news for {sym}: {e}")

    print(f"[INFO] Unique articles found: {len(raw_articles)}")

    insert_count = 0
    total_sentiment_sum = 0

    for item in raw_articles:
        try:
            headline = clean_text(item.get("headline", ""))
            if not headline:
                continue

            url = item.get("url", "")
            ts = datetime.utcfromtimestamp(item["datetime"])
            symbol = item.get("related", "").split(",")[0]
            source = item.get("source", "")
            fin_id = item.get("id", None)

            sentiment = analyze_sentiment_finbert(headline)
            total_sentiment_sum += sentiment

            cur.execute("""
                INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url, finnhub_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (ts, symbol, headline, source, sentiment, url, fin_id))

            if cur.rowcount > 0:
                insert_count += 1

        except Exception as e:
            print(f"[INSERT ERROR] {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()

    avg_sentiment = total_sentiment_sum / max(1, len(raw_articles))
    print(f"[SUCCESS] Inserted {insert_count} new articles.")
    print(f"[SENTIMENT] Average sentiment: {avg_sentiment}")
    print("News ingestion complete.")
