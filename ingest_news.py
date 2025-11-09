import os
import psycopg2
import requests
from dotenv import load_dotenv
import time

# Load all environment variables from .env
load_dotenv()

# --- Config ---
DATABASE_URL = os.getenv('DATABASE_URL')
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
HF_TOKEN = os.getenv('HF_TOKEN') # Your new Hugging Face key
SYMBOLS_TO_TRACK = ['TSLA', 'BTC']
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
# ----------------

# --- AI Model Setup ---
# We now call the Hugging Face API instead of loading the model
FINBERT_API_URL = "https://api-inference.huggingface.co/models/ProsusAI/finbert"
HF_HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}
# --------------------

def get_finbert_sentiment(headline):
    """
    Analyzes a single headline using the Hugging Face Inference API.
    Returns a compound score from -1 to 1.
    """
    try:
        # Send the headline to the HF API
        json_payload = {"inputs": headline}
        response = requests.post(FINBERT_API_URL, headers=HF_HEADERS, json=json_payload)

        # Check for errors (like 503 if model is loading)
        if response.status_code != 200:
            print(f"HF API Error: {response.status_code} {response.text}")
            # If the model is loading, wait 10 seconds and try once more
            if response.status_code == 503:
                print("Model is loading, retrying in 10s...")
                time.sleep(10)
                response = requests.post(FINBERT_API_URL, headers=HF_HEADERS, json=json_payload)
                if response.status_code != 200:
                     print("HF API Retry failed.")
                     return 0 # Default to neutral on error
            else:
                return 0 # Default to neutral on other errors

        # Parse the scores from the successful response
        scores = response.json()[0]
        sentiment = {s['label']: s['score'] for s in scores}

        # Calculate a single compound score: (positive - negative)
        compound_score = sentiment.get('positive', 0) - sentiment.get('negative', 0)
        return compound_score

    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return 0 # Default to neutral on error

def send_alert(message):
    # (This function is the same, no changes)
    if not DISCORD_WEBHOOK_URL:
        print("ALERT (Discord webhook not set):", message)
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={'content': message})
    except Exception as e:
        print(f"Error sending Discord alert: {e}")

def fetch_news_data():
    # (This entire function is the same as before)
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    search_query = " OR ".join(SYMBOLS_TO_TRACK)
    url = (f"https://newsapi.org/v2/everything?"
           f"q=({search_query})&"
           f"language=en&"
           f"sortBy=publishedAt&"
           f"apiKey={NEWS_API_KEY}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        articles = response.json().get('articles', [])

        if not articles:
            print("No new articles found.")
            return

        print(f"Found {len(articles)} articles. Analyzing sentiment (via API) and inserting...")

        total_sentiment = 0
        articles_analyzed = 0

        for article in articles:
            headline = article['title']
            if not headline:
                continue

            sentiment_score = get_finbert_sentiment(headline)

            article_symbol = None
            for symbol in SYMBOLS_TO_TRACK:
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
        print(f"Successfully inserted/updated {articles_analyzed} articles.")

        if articles_analyzed > 0:
            avg_sentiment = total_sentiment / articles_analyzed
            print(f"Average sentiment of this batch: {avg_sentiment:.4f}")
            if avg_sentiment < -0.3:
                send_alert(f"🚨 SENTIMENT ALERT 🚨\nAverage sentiment has dropped to {avg_sentiment:.4f} in the last batch of {articles_analyzed} articles.")

    except Exception as e:
        print(f"Error fetching news: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()
        print("News ingest complete.")

if __name__ == "__main__":
    fetch_news_data()
