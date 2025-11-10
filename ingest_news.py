import os
import psycopg2
import finnhub
import time
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)
sentiment = SentimentIntensityAnalyzer()


def fetch_news_data():
    print("News Ingest Task: Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ----------------------------------------------------------------------
    # 🛑 ENSURE columns exist (safe)
    # ----------------------------------------------------------------------
    try:
        cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS url TEXT;")
        cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS finnhub_id BIGINT;")
        conn.commit()
        print("News Ingest Task: Ensured 'url' and 'finnhub_id' columns exist.")
    except Exception as e:
        print("Warning: Could not alter table:", e)

    # ----------------------------------------------------------------------
    # ✅ Fetch tracked symbols 
    # ----------------------------------------------------------------------
    cur.execute("SELECT symbol FROM tracked_symbols;")
    symbols = [row[0] for row in cur.fetchall()]
    print(f"News Ingest Task: Tracking {len(symbols)} symbols.")

    all_articles = []
    for sym in symbols:
        print(f"News Ingest Task: Fetching news for {sym}...")
        try:
            articles = finnhub_client.company_news(sym, _from="2024-01-01", to=time.strftime("%Y-%m-%d"))
        except Exception as e:
            print(f"Error fetching news for {sym}: {e}")
            continue

        for a in articles:
            if "headline" in a and "id" in a:
                all_articles.append({
                    "symbol": sym,
                    "time": a.get("datetime"),
                    "headline": a.get("headline"),
                    "source": a.get("source"),
                    "url": a.get("url", None),
                    "finnhub_id": a.get("id"),
                })

    print(f"News Ingest Task: Found {len(all_articles)} raw articles.")

    # ----------------------------------------------------------------------
    # ✅ Remove duplicates before sentiment
    # ----------------------------------------------------------------------
    seen_ids = set()
    unique_articles = []
    for a in all_articles:
        if a["finnhub_id"] not in seen_ids:
            seen_ids.add(a["finnhub_id"])
            unique_articles.append(a)

    print(f"News Ingest Task: {len(unique_articles)} unique articles after deduplication.")

    # ----------------------------------------------------------------------
    # ✅ Insert using UPSERT with UNIQUE(time, finnhub_id)
    # ----------------------------------------------------------------------
    insert_sql = """
        INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url, finnhub_id)
        VALUES (TO_TIMESTAMP(%s), %s, %s, %s, %s, %s, %s)
        ON CONFLICT (time, finnhub_id) DO NOTHING;
    """

    insert_count = 0

    for art in unique_articles:
        sent = sentiment.polarity_scores(art["headline"])["compound"]

        try:
            cur.execute(
                insert_sql,
                (art["time"], art["symbol"], art["headline"],
                 art["source"], sent, art["url"], art["finnhub_id"])
            )
            insert_count += cur.rowcount
        except Exception as e:
            print("Insert error:", e)
            continue

    conn.commit()
    cur.close()
    conn.close()

    print(f"News Ingest Task: Successfully inserted {insert_count} articles.")
    print("News Ingest Task: Complete.")

