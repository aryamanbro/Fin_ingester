import os
import psycopg2
import requests
import finnhub
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
FINNHUB_KEY = os.getenv('FINNHUB_API_KEY')
HF_TOKEN = os.getenv('HF_TOKEN')
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
FINBERT_API_URL = "https://api-inference.huggingface.co/models/ProsusAI/finbert"

HF_HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

# ------------------------------------
# HuggingFace FinBERT sentiment helper
# ------------------------------------
def get_finbert_sentiment(headline: str) -> float:
    try:
        url = "https://router.huggingface.co/v1/models/ProsusAI/finbert"
        
        payload = {
            "inputs": headline
        }

        response = requests.post(url, json=payload, headers={
            "Authorization": f"Bearer {HF_TOKEN}",
            "Content-Type": "application/json"
        })

        if response.status_code == 529:  # model loading
            print("[FinBERT] Model loading. Retrying...")
            time.sleep(8)
            response = requests.post(url, json=payload, headers={
                "Authorization": f"Bearer {HF_TOKEN}",
                "Content-Type": "application/json"
            })

        if response.status_code != 200:
            print(f"[FinBERT ERROR] {response.status_code}: {response.text}")
            return 0

        # Response format:
        # [{"label": "positive", "score": X}, ...]
        data = response.json()

        if isinstance(data, dict) and "error" in data:
            print("[FinBERT ERROR]", data["error"])
            return 0

        scores = data[0]
        sentiment_map = {s["label"]: s["score"] for s in scores}
        return sentiment_map.get("positive", 0) - sentiment_map.get("negative", 0)

    except Exception as e:
        print(f"[FinBERT EXCEPTION] {e}")
        return 0


# ------------------------------------
# Discord Alert Helper
# ------------------------------------
def send_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print("[ALERT DISABLED] Missing webhook")
        return
    
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={'content': message})
    except Exception as e:
        print("[ALERT ERROR]", e)

# ------------------------------------
# Main ingestion function
# ------------------------------------
def fetch_news_data():
    print("\n===============================")
    print("News ingestion starting...")
    print("===============================\n")

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # ---------------------------------------
        # 1. Make sure columns exist (NO UNIQUE)
        # ---------------------------------------
        try:
            cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS url TEXT;")
            cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS finnhub_id BIGINT;")
            conn.commit()
            print("[INFO] Columns 'url' and 'finnhub_id' confirmed.")
        except Exception as e:
            print("[WARN] Column add failure:", e)
            conn.rollback()

        # ---------------------------------------
        # 2. Fetch tracking symbols
        # ---------------------------------------
        cur.execute("SELECT symbol, type FROM tracked_symbols;")
        symbols = cur.fetchall()

        if not symbols:
            print("[WARN] No symbols found in tracked_symbols.")
            return

        print(f"[INFO] Tracking {len(symbols)} symbols...")

        all_articles = {}

        to_date = datetime.utcnow().strftime('%Y-%m-%d')
        from_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

        finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

        # ---------------------------------------
        # 3. Fetch news for each symbol
        # ---------------------------------------
        for symbol, type_ in symbols:
            print(f"[FETCH] Fetching news for {symbol}...")

            try:
                if type_ == "stock":
                    data = finnhub_client.company_news(symbol, _from=from_date, to=to_date)
                else:
                    # crypto fallback
                    general = finnhub_client.general_news('crypto', min_id=0)
                    data = [a for a in general if symbol.lower() in a['headline'].lower()]
                
                for a in data:
                    all_articles[a['id']] = (a, symbol)

            except Exception as e:
                print(f"[ERROR] Failed fetching {symbol}: {e}")

        print(f"[INFO] Unique articles found: {len(all_articles)}")

        # ---------------------------------------
        # 4. Insert articles
        # ---------------------------------------
        insert_count = 0
        total_sentiment = 0

        for finnhub_id, (a, symbol) in all_articles.items():
            try:
                # Check duplicates manually (NO UNIQUE INDEX NEEDED)
                cur.execute("SELECT 1 FROM news_articles WHERE finnhub_id = %s LIMIT 1", (finnhub_id,))
                if cur.fetchone():
                    continue

                headline = a['headline']
                if not headline:
                    continue

                article_time = datetime.fromtimestamp(a['datetime'])
                sentiment = get_finbert_sentiment(headline)
                url = a.get('url', None)
                source = a.get('source', None)

                cur.execute("""
                    INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url, finnhub_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    article_time,
                    symbol,
                    headline,
                    source,
                    sentiment,
                    url,
                    finnhub_id
                ))

                insert_count += 1
                total_sentiment += sentiment

            except Exception as ex:
                print(f"[INSERT ERROR] {ex}")
                conn.rollback()

        conn.commit()
        print(f"[SUCCESS] Inserted {insert_count} new articles.")

        if insert_count > 0:
            avg = total_sentiment / insert_count
            print(f"[SENTIMENT] Average sentiment: {avg}")
            if avg < -0.3:
                send_alert(f"🚨 Negative sentiment detected: {avg}")

    except Exception as e:
        print("[FATAL ERROR]", e)
        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("News ingestion complete.\n")

# ------------------------------------
# Direct execution
# ------------------------------------
if __name__ == "__main__":
    fetch_news_data()
