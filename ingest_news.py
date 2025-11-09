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

# Setup Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

def get_finbert_sentiment(headline):
    try:
        json_payload = {"inputs": headline}
        response = requests.post(FINBERT_API_URL, headers=HF_HEADERS, json=json_payload)
        if response.status_code != 200:
            if response.status_code == 503: # Model is loading
                time.sleep(10) # Wait 10 seconds
                response = requests.post(FINBERT_API_URL, headers=HF_HEADERS, json=json_payload)
                if response.status_code != 200:
                     return 0
            else:
                return 0
        scores = response.json()[0]
        sentiment = {s['label']: s['score'] for s in scores}
        compound_score = sentiment.get('positive', 0) - sentiment.get('negative', 0)
        return compound_score
    except Exception:
        return 0

def send_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print("ALERT (Discord webhook not set):", message)
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={'content': message})
    except Exception as e:
        print(f"Error sending Discord alert: {e}")

def fetch_news_data():
    print("News Ingest Task: Connecting to database...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # --- FIX 1: Add 'url' and 'finnhub_id' columns if they don't exist ---
        try:
            cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS url TEXT;")
            cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS finnhub_id BIGINT;")
            conn.commit()
            print("News Ingest Task: Ensured 'url' and 'finnhub_id' columns exist.")
        except Exception as alter_e:
            print(f"Warning: Could not alter table (may be a permissions issue): {alter_e}")
            conn.rollback()
        # --------------------------------------------------------------------
            
        print("News Ingest Task: Fetching symbols to track from database...")
        cur.execute("SELECT symbol, type FROM tracked_symbols")
        symbols_to_track = cur.fetchall()
        
        if not symbols_to_track:
            print("News Ingest Task: No symbols in tracked_symbols table. Exiting.")
            return

        print(f"News Ingest Task: Tracking {len(symbols_to_track)} symbols.")
        
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        total_sentiment = 0
        articles_analyzed = 0
        
        all_articles_to_process = {} # Use a dict to avoid duplicate processing

        for symbol, type in symbols_to_track:
            print(f"News Ingest Task: Fetching news for {symbol}...")
            articles = []
            try:
                if type == 'stock':
                    articles = finnhub_client.company_news(symbol, _from=from_date, to=to_date)
                elif type == 'crypto':
                    # Get general crypto news and filter by symbol
                    general_news = finnhub_client.general_news('crypto', min_id=0)
                    for article in general_news:
                        if symbol.lower() in article['headline'].lower() or symbol.lower() in article['summary'].lower():
                            articles.append(article)
                
                if not articles:
                    print(f"News Ingest Task: No new articles found for {symbol}.")
                    continue
                
                # Add articles to our processing dict, keyed by Finnhub ID
                # This automatically handles duplicates
                for article in articles:
                    if article['id'] not in all_articles_to_process:
                        all_articles_to_process[article['id']] = (article, symbol)

            except Exception as e:
                print(f"Error fetching news for {symbol}: {e}")

        
        print(f"News Ingest Task: Found {len(all_articles_to_process)} unique articles. Analyzing...")
        
        for finnhub_id, (article, symbol) in all_articles_to_process.items():
            try:
                headline = article['headline']
                article_url = article['url']
                article_time = datetime.fromtimestamp(article['datetime'])

                if not headline:
                    continue

                sentiment_score = get_finbert_sentiment(headline)
                
                # --- FIX 2: Insert with 'finnhub_id' and 'url' ---
                insert_query = """
                    INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url, finnhub_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """

                cur.execute(insert_query, (
                    article_time,
                    symbol,
                    headline,
                    article['source'],
                    sentiment_score,
                    article_url,
                    finnhub_id
                ))
                
                if cur.rowcount > 0: # Only count if it was a new article
                    articles_analyzed += 1
                    total_sentiment += sentiment_score
            
            except Exception as insert_e:
                print(f"Error inserting article {finnhub_id}: {insert_e}")
                conn.rollback()

        conn.commit()
        print(f"News Ingest Task: Successfully inserted {articles_analyzed} new articles.")

        if articles_analyzed > 0:
            avg_sentiment = total_sentiment / articles_analyzed
            print(f"News Ingest Task: Average sentiment: {avg_sentiment:.4f}")
            if avg_sentiment < -0.3:
                send_alert(f"🚨 SENTIMENT ALERT 🚨\nAverage sentiment {avg_sentiment:.4f} for tracked symbols")

    except Exception as e:
        print(f"Error in fetch_news_data: {e}")
        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("News Ingest Task: Complete.")

if __name__ == "__main__":
    fetch_news_data()
