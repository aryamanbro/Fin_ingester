import os
import psycopg2
import finnhub
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")

FINBERT_API_URL = "https://api-inference.huggingface.co/models/ProsusAI/finbert"
HF_HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)


def analyze_with_finbert(headline: str) -> float:
    """
    Calls HuggingFace FinBERT model and returns sentiment score.
    Score = positive - negative
    """
    try:
        payload = {"inputs": headline}
        r = requests.post(FINBERT_API_URL, headers=HF_HEADERS, json=payload)

        # Model loading delay handling
        if r.status_code == 503:
            time.sleep(8)
            r = requests.post(FINBERT_API_URL, headers=HF_HEADERS, json=payload)

        if r.status_code != 200:
            print("[FINBERT ERROR]", r.text)
            return 0.0

        result = r.json()
        if not isinstance(result, list) or len(result) == 0:
            return 0.0

        scores = result[0]
        sentiment = {item["label"]: item["score"] for item in scores}

        pos = sentiment.get("positive", 0)
        neg = sentiment.get("negative", 0)

        return pos - neg

    except Exception as e:
        print("ERROR in FinBERT:", e)
        return 0.0


def fetch_news_data():
    print("News ingestion starting...")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Ensure schema fixes
    cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS url TEXT;")
    cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS finnhub_id BIGINT UNIQUE;")
    conn.commit()

    cur.execute("SELECT symbol, type FROM tracked_symbols")
    symbols = cur.fetchall()

    print(f"Tracking {len(symbols)} symbols")

    from_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    to_date = datetime.utcnow().strftime("%Y-%m-%d")

    seen_articles = {}

    # 1. Fetch news
    for symbol, sym_type in symbols:
        try:
            if sym_type == "stock":
                articles = finnhub_client.company_news(symbol, _from=from_date, to=to_date)
            else:
                general = finnhub_client.general_news("crypto", min_id=0)
                articles = [a for a in general if symbol.lower() in a["headline"].lower()]

            for a in articles:
                if a["id"] not in seen_articles:
                    seen_articles[a["id"]] = a

        except Exception as e:
            print(f"Error fetching {symbol}: {e}")

    print(f"Found {len(seen_articles)} unique news articles")

    # 2. Insert with FinBERT
    inserted = 0

    for fid, article in seen_articles.items():
        try:
            headline = article["headline"]
            url = article.get("url", "")
            ts = datetime.utcfromtimestamp(article["datetime"])  # fix: valid timestamp

            score = analyze_with_finbert(headline)

            cur.execute("""
                INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url, finnhub_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (finnhub_id) DO NOTHING;
            """, (
                ts,
                article["related"] or symbol,
                headline,
                article["source"],
                score,
                url,
                fid
            ))

            inserted += cur.rowcount

        except Exception as e:
            print("Insert error:", e)

    conn.commit()
    conn.close()

    print(f"Inserted {inserted} new articles using FINBERT.")
