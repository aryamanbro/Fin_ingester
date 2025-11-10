import os
import psycopg2
import requests
import finnhub
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")

# ✅ Correct HuggingFace Router endpoint
FINBERT_API_URL = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"

FINBERT_HEADERS = {
    "Authorization": f"Bearer {HF_TOKEN}",
    "Content-Type": "application/json"
}

# Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)


# ---------------------------------------------
# ✅ FinBERT sentiment scoring (ONLY)
# ---------------------------------------------
def analyze_sentiment_finbert(text):
    try:
        response = requests.post(
            FINBERT_API_URL,
            headers=FINBERT_HEADERS,
            json={"inputs": text},
            timeout=30
        )

        if response.status_code != 200:
            print(f"[FinBERT ERROR] {response.status_code}: {response.text}")
            return 0

        output = response.json()
        scores = output[0]

        # Convert to positive-negative scale
        sentiment = {item["label"]: item["score"] for item in scores}
        score = sentiment.get("positive", 0) - sentiment.get("negative", 0)
        return score

    except Exception as e:
        print(f"[FinBERT EXCEPTION] {e}")
        return 0


# ---------------------------------------------
# ✅ MAIN NEWS INGESTION FUNCTION
# ---------------------------------------------
def fetch_news_data():
    print("News ingestion starting...")
    print("===============================")

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # ✅ Ensure schema columns exist
        print("[INFO] Ensuring schema columns exist...")
        cur.execute("""
            ALTER TABLE news_articles 
            ADD COLUMN IF NOT EXISTS url TEXT;
        """)
        cur.execute("""
            ALTER TABLE news_articles 
            ADD COLUMN IF NOT EXISTS finnhub_id BIGINT;
        """)
        conn.commit()

        print("[INFO] Columns 'url' and 'finnhub_id' confirmed.")

        # ✅ Fetch symbols to track
        cur.execute("SELECT symbol, type FROM tracked_symbols")
        symbols = cur.fetchall()

        if not symbols:
            print("[WARN] No symbols found in tracked_symbols")
            return

        print(f"[INFO] Tracking {len(symbols)} symbols...")

        to_date = datetime.utcnow().strftime("%Y-%m-%d")
        from_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        all_articles = {}

        # ---------------------------------------------
        # ✅ Fetch news from Finnhub for each symbol
        # ---------------------------------------------
        for symbol, sym_type in symbols:
            print(f"[FETCH] Fetching news for {symbol}...")

            articles = []
            try:
                if sym_type == "stock":
                    articles = finnhub_client.company_news(symbol, _from=from_date, to=to_date)
                else:
                    cryptos = finnhub_client.general_news("crypto", min_id=0)
                    articles = [
                        a for a in cryptos
                        if symbol.lower() in a["headline"].lower() or symbol.lower() in a["summary"].lower()
                    ]
            except Exception as e:
                print(f"[ERROR] Failed fetching {symbol}: {e}")
                continue

            for art in articles:
                finnhub_id = art.get("id")
                if not finnhub_id:
                    continue
                if finnhub_id not in all_articles:
                    all_articles[finnhub_id] = (art, symbol)

        print(f"[INFO] Unique articles found: {len(all_articles)}")

        new_count = 0
        sentiment_sum = 0

        # ---------------------------------------------
        # ✅ Process and insert each article
        # ---------------------------------------------
        for finnhub_id, (article, symbol) in all_articles.items():
            try:
                headline = article.get("headline")
                url = article.get("url")
                source = article.get("source")
                ts = article.get("datetime")

                if not headline or not ts:
                    continue

                dt = datetime.fromtimestamp(ts)

                # ✅ FinBERT sentiment
                score = analyze_sentiment_finbert(headline)
                sentiment_sum += score

                # ✅ Insert with unique ID
                cur.execute("""
                    INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url, finnhub_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (finnhub_id, time) DO NOTHING;
                """, (dt, symbol, headline, source, score, url, finnhub_id))

                if cur.rowcount > 0:
                    new_count += 1

            except Exception as e:
                print(f"[ERROR] Inserting article {finnhub_id}: {e}")
                conn.rollback()

        conn.commit()

        print(f"[SUCCESS] Inserted {new_count} new articles.")

        if new_count > 0:
            avg = sentiment_sum / new_count
            print(f"[SENTIMENT] Average sentiment: {avg}")
        else:
            print("[SENTIMENT] No new sentiment computed.")

        print("News ingestion complete.")

    except Exception as e:
        print(f"[FATAL ERROR] {e}")
        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
