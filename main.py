import os
import psycopg2
import finnhub
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# Load all environment variables from .env
load_dotenv()

# --- Config ---
DATABASE_URL = os.getenv('DATABASE_URL')
FINNHUB_KEY = os.getenv('FINNHUB_API_KEY')
# ----------------

#
# THIS IS THE CRITICAL LINE THAT WAS MISSING
# It must be defined BEFORE you use @app
#
app = FastAPI()

# Initialize Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

# --- CORS Middleware ---
# This allows your Vercel React app to call your Render backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, change this to your Vercel URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helper Function to get DB Connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# --- API Endpoints ---

@app.get("/")
def read_root():
    return {"status": "Sentiment API is running"}

@app.get("/api/v1/live-price")
def get_live_price(symbol: str = Query(..., min_length=1)):
    """
    Securely fetches the current quote for a symbol.
    This is for your "live ticker" component in React.
    """
    try:
        # This is a secure, backend-to-backend API call
        quote = finnhub_client.quote(symbol.upper())
        if quote['c'] == 0:
            raise HTTPException(status_code=404, detail="Symbol not found or no data")
        
        return {
            "symbol": symbol,
            "price": quote['c'],       # Current price
            "change": quote['d'],      # Change
            "percent_change": quote['dp'] # Percent change
        }
    except Exception as e:
        print(f"Error fetching live price for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching live data")

@app.get("/api/v1/chart-data")
def get_chart_data(symbol: str = Query(..., min_length=1)):
    """
    Fetches all historical data (prices, sentiment, trends)
    for the main dashboard chart. [CORRECTED VERSION]
    """
    
    # This is the "Ultimate" query, now fixed.
    sql_query = """
    WITH daily_sentiment AS (
      SELECT 
        time_bucket('1 day', time) AS day,
        symbol,
        avg(sentiment_score) AS avg_sentiment
      FROM news_articles
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '1 year')
      GROUP BY day, symbol
    ),
    google_trends_daily AS (
      SELECT 
        time_bucket('1 day', time) AS day,
        symbol,
        avg(score) AS google_score
      FROM google_trends
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '1 year')
      GROUP BY day, symbol
    ),
    prices_daily AS (
      SELECT
        time_bucket('1 day', time) AS day,
        symbol,
        first(price, time) as "open",
        max(price) as "high",
        min(price) as "low",
        last(price, time) as "close"
      FROM prices
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '1 year')
      GROUP BY day, symbol
    )
    
    -- The final JOIN now correctly joins on both day AND symbol
    SELECT
      p.day AS "time",
      p.close,
      g.google_score,
      d.avg_sentiment
    FROM prices_daily AS p
    LEFT JOIN daily_sentiment AS d ON p.day = d.day AND p.symbol = d.symbol
    LEFT JOIN google_trends_daily AS g ON p.day = g.day AND p.symbol = g.symbol
    WHERE 
      p.day > (NOW() - INTERVAL '1 year')
    ORDER BY p.day ASC;
    """
    
    chart_data = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Pass the symbol three times, once for each WHERE clause
        cur.execute(sql_query, (symbol, symbol, symbol))
        
        rows = cur.fetchall()
        
        # Get column names from the cursor
        colnames = [desc[0] for desc in cur.description]
        
        for row in rows:
            chart_data.append(dict(zip(colnames, row)))
            
        cur.close()
        conn.close()
        
        return {"data": chart_data}
        
    except Exception as e:
        print(f"Error fetching chart data for {symbol}: {e}")
        # Send the actual database error to the client
        raise HTTPException(status_code=500, detail=f"Error fetching chart data: {e}")

@app.get("/api/v1/positive-news")
def get_positive_news(symbol: str = Query(..., min_length=1)):
    """
    Fetches the 10 most recent POSITIVE news articles.
    """
    sql = """
    SELECT headline, source_name, time
    FROM news_articles
    WHERE symbol = %s AND sentiment_score > 0.3
    ORDER BY time DESC
    LIMIT 10;
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, (symbol,))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        news_list = [dict(zip(colnames, row)) for row in rows]
        cur.close()
        conn.close()
        return {"data": news_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching news: {e}")

@app.get("/api/v1/negative-news")
def get_negative_news(symbol: str = Query(..., min_length=1)):
    """
    Fetches the 10 most recent NEGATIVE news articles.
    """
    sql = """
    SELECT headline, source_name, time
    FROM news_articles
    WHERE symbol = %s AND sentiment_score < -0.3
    ORDER BY time DESC
    LIMIT 10;
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, (symbol,))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        news_list = [dict(zip(colnames, row)) for row in rows]
        cur.close()
        conn.close()
        return {"data": news_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching news: {e}")
