import os
import psycopg2
import requests
import finnhub
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
FINNHUB_KEY = os.getenv('FINNHUB_API_KEY') # Use the same key
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
        
        # --- This is a failsafe. If the 'url' column doesn't exist, add it. ---
        # This makes the fix idempotent.
        try:
            cur.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS url TEXT;")
            conn.commit()
            print("News Ingest Task: Ensured 'url' column exists.")
        except Exception as alter_e:
            print(f"Warning: Could not add 'url' column (it might exist): {alter_e}")
            conn.rollback() # Rollback the ALTER TABLE, but continue the script
        # ----------------------------------------------------------------------
            
        print("News Ingest Task: Fetching symbols to track from database...")
        cur.execute("SELECT symbol, type FROM tracked_symbols")
        symbols_to_track = cur.fetchall()
        
        if not symbols_to_track:
            print("News Ingest Task: No symbols in tracked_symbols table. Exiting.")
            return

        print(f"News Ingest Task: Tracking {len(symbols_to_track)} symbols.")
        
        # Get news from 1 day ago
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        total_sentiment = 0
        articles_analyzed = 0
        
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

                print(f"News Ingest Task: Found {len(articles)} articles for {symbol}. Analyzing...")

                for article in articles:
                    headline = article['headline']
                    article_url = article['url'] # Get the URL
                    
                    if not headline:
                        continue
                    
                    # Check if headline already exists to avoid re-analyzing
                    cur.execute("SELECT 1 FROM news_articles WHERE headline = %s AND symbol = %s", (headline, symbol))
                    if cur.fetchone():
                        continue # Skip if already processed

                    sentiment_score = get_finbert_sentiment(headline)
                    article_time = datetime.fromtimestamp(article['datetime'])
                    
                    # --- THIS IS THE FIX ---
                    # Added 'url' to the query
                    insert_query = """
                    INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score, url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (time, headline) DO NOTHING;
                    """
                    # Added 'article_url' to the parameters
                    cur.execute(insert_query, (
                        article_time,
                        symbol,
                        headline,
                        article['source'],
                        sentiment_score,
                        article_url 
                    ))
                    
                    total_sentiment += sentiment_score
                    articles_analyzed += 1
                
                conn.commit()

            except Exception as e:
                print(f"Error processing news for {symbol}: {e}")
                conn.rollback()

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
