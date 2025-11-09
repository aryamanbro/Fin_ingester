import os
import psycopg2
import requests
from dotenv import load_dotenv
import time

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
HF_TOKEN = os.getenv('HF_TOKEN')
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
FINBERT_API_URL = "https://api-inference.huggingface.co/models/ProsusAI/finbert"
HF_HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

# (Your get_finbert_sentiment and send_alert functions go here... no changes needed)
def get_finbert_sentiment(headline):
    # (Same as before)
    try:
        json_payload = {"inputs": headline}
        response = requests.post(FINBERT_API_URL, headers=HF_HEADERS, json=json_payload)
        if response.status_code != 200:
            if response.status_code == 503:
                time.sleep(10)
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
    # (Same as before)
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
        
        # --- THIS IS THE NEW DYNAMIC LOGIC ---
        print("News Ingest Task: Fetching symbols to track from database...")
        cur.execute("SELECT symbol FROM tracked_symbols")
        # Convert list of tuples [('TSLA',), ('BTC',)] to a simple list ['TSLA', 'BTC']
        symbols_list = [row[0] for row in cur.fetchall()]
        
        if not symbols_list:
            print("News Ingest Task: No symbols in tracked_symbols table. Exiting.")
            return

        print(f"News Ingest Task: Tracking {len(symbols_list)} symbols.")
        # Build a search query like "(TSLA OR BTC OR AAPL)"
        search_query = f"({' OR '.join(symbols_list)})"
        # --- END NEW LOGIC ---
        
        url = (f"https://newsapi.org/v2/everything?"
               f"q={search_query}&"
               f"language=en&"
               f"sortBy=publishedAt&"
               f"apiKey={NEWS_API_KEY}")
        
        response = requests.get(url)
        response.raise_for_status()
        articles = response.json().get('articles', [])
        
        if not articles:
            print("News Ingest Task: No new articles found.")
            return

        print(f"News Ingest Task: Found {len(articles)} articles. Analyzing...")
        
        total_sentiment = 0
        articles_analyzed = 0
        
        for article in articles:
            headline = article['title']
            if not headline:
                continue

            sentiment_score = get_finbert_sentiment(headline)
            
            article_symbol = None
            # Check which of our symbols is in the headline
            for symbol in symbols_list:
                if symbol.lower() in headline.lower():
                    article_symbol = symbol
                    break
            
            if not article_symbol:
                continue
            
            insert_query = """
            INSERT INTO news_articles (time, symbol, headline, source_name, sentiment_score)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (time, headline) DO NOTHING;
            """
            cur.execute(insert_query, (
                article['publishedAt'],
                article_symbol,
                headline,
                article['source']['name'],
                sentiment_score
            ))
            
            total_sentiment += sentiment_score
            articles_analyzed += 1

        conn.commit()
        print(f"News Ingest Task: Successfully inserted/updated {articles_analyzed} articles.")

        if articles_analyzed > 0:
            avg_sentiment = total_sentiment / articles_analyzed
            print(f"News Ingest Task: Average sentiment: {avg_sentiment:.4f}")
            if avg_sentiment < -0.3:
                send_alert(f"🚨 SENTIMENT ALERT 🚨\nAverage sentiment {avg_sentiment:.4f} for {search_query}")

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
