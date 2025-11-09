import os
import psycopg2
import requests
from dotenv import load_dotenv
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# Load all environment variables from .env
load_dotenv()

# --- Config ---
DATABASE_URL = os.getenv('DATABASE_URL')
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
SYMBOLS_TO_TRACK = ['TSLA', 'BTC']
# Your Discord webhook for alerts (optional, but good to have)
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
# ----------------

# --- AI Model Setup ---
# Load the pre-trained FinBERT model and tokenizer
# This will download the model (300-500MB) the first time it runs
print("Loading FinBERT model... (This may take a moment)")
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
print("FinBERT model loaded.")
# --------------------

def get_finbert_sentiment(headline):
    """
    Analyzes a single headline and returns a compound sentiment score.
    Score: +1 (Positive), 0 (Neutral), -1 (Negative)
    """
    try:
        # Tokenize the headline
        inputs = tokenizer(headline, return_tensors="pt", truncation=True, max_length=512)
        
        # Get the model's output (logits)
        with torch.no_grad():
            outputs = model(**inputs)
        
        # Convert logits to probabilities (softmax)
        scores = torch.nn.functional.softmax(outputs.logits, dim=-1)[0]
        
        # FinBERT outputs [positive, negative, neutral]
        # We want a single score from -1 to 1
        # Score = (positive_prob - negative_prob)
        compound_score = scores[0].item() - scores[1].item()
        
        return compound_score
        
    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return 0 # Default to neutral on error

def send_alert(message):
    """Sends a simple message to a Discord webhook."""
    if not DISCORD_WEBHOOK_URL:
        print("ALERT (Discord webhook not set):", message)
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={'content': message})
    except Exception as e:
        print(f"Error sending Discord alert: {e}")

def fetch_news_data():
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # --- 1. Fetch News ---
    # We will search for all symbols in one go
    search_query = " OR ".join(SYMBOLS_TO_TRACK)
    url = (f"https://newsapi.org/v2/everything?"
           f"q=({search_query})&"
           f"language=en&"
           f"sortBy=publishedAt&"
           f"apiKey={NEWS_API_KEY}")
    
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise error on 4xx/5xx
        articles = response.json().get('articles', [])
        
        if not articles:
            print("No new articles found.")
            return

        print(f"Found {len(articles)} articles. Analyzing sentiment and inserting...")
        
        total_sentiment = 0
        articles_analyzed = 0
        
        for article in articles:
            headline = article['title']
            if not headline:
                continue

            # --- 2. Analyze Sentiment ---
            sentiment_score = get_finbert_sentiment(headline)
            
            # Figure out which symbol this article is about
            # This is a simple check; more complex logic could be added
            article_symbol = None
            for symbol in SYMBOLS_TO_TRACK:
                if symbol.lower() in headline.lower():
                    article_symbol = symbol
                    break # Assign to first symbol found
            
            if not article_symbol:
                continue # Skip article if not clearly about our symbols
            
            # --- 3. Insert into Database ---
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

        # --- 4. Check for Alert ---
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

# This makes the script runnable
if __name__ == "__main__":
    fetch_news_data()
