import os
import psycopg2
import finnhub
import time
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---
# IMPORT YOUR INGESTOR SCRIPTS AS FUNCTIONS
# ---
from ingest_prices import fetch_price_data
from ingest_news import fetch_news_data
from ingest_trends import fetch_trends_data

# Load all environment variables from .env
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.getenv('DATABASE_URL')
FINNHUB_KEY = os.getenv('FINNHUB_API_KEY')
TASK_SECRET_KEY = os.getenv('TASK_SECRET_KEY') # For cron jobs
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')   # For manual admin actions

# Check for essential keys
if not TASK_SECRET_KEY or not ADMIN_PASSWORD:
    print("Error: TASK_SECRET_KEY or ADMIN_PASSWORD not found in .env file.")
    # In a real app, you might exit here
    # exit(1)

# --- App Setup ---
app = FastAPI()
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

# --- Pydantic Model ---
class NewSymbol(BaseModel):
    symbol: str
    type: str  # e.g., 'stock' or 'crypto'


# --- CORS Middleware ---
# TODO: In production, change "*" to your Vercel app's URL
allow_origins = [
    "http://127.0.0.1:8080",  # Local dev
    "http://localhost:8080",   # Local dev
    "*"                      # Placeholder for your production URL
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins, 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Security Helpers ---
async def verify_secret(x_task_secret: str = Header(None)):
    """Verifies the secret key for automated cron job tasks."""
    if x_task_secret != TASK_SECRET_KEY:
        print(f"Failed task auth: Invalid key received.")
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid Task Secret Key")

async def verify_admin_password(x_admin_password: str = Header(None)):
    """Verifies the admin password for manual admin endpoints."""
    if x_admin_password != ADMIN_PASSWORD:
        print(f"Failed admin auth: Invalid password received.")
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid Admin Password")

# --- Database Helper ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# --- Finnhub Symbol Helper ---
def get_finnhub_symbol(symbol, type):
    """Helper to format symbol for Finnhub crypto"""
    if type == 'crypto':
        # Assumes Binance and USDT.
        if symbol == 'BTC':
            return "BINANCE:BTCUSDT"
        elif symbol == 'ETH':
            return "BINANCE:ETHUSDT"
        # Add other specific cryptos if needed
        return f"BINANCE:{symbol.upper()}USDT"
    return symbol.upper() # Return the plain symbol for stocks

#
# --- Public API Endpoints (for your React App) ---
#

@app.get("/")
def read_root():
    return {"status": f"Sentiment API at {os.getenv('RENDER_EXTERNAL_URL', 'local')} is running"}

@app.get("/api/v1/live-price")
def get_live_price(symbol: str = Query(..., min_length=1)):
    """
    Securely fetches the current quote for a symbol.
    This function now looks up the symbol type to correctly call Finnhub.
    """
    symbol_type = 'stock' # Default to stock
    finnhub_symbol = symbol.upper()

    try:
        # 1. Get symbol type from DB
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT type FROM tracked_symbols WHERE symbol = %s", (symbol.upper(),))
        result = cur.fetchone()
        if result:
            symbol_type = result[0]
        
        cur.close()
        conn.close()

        # 2. Format symbol for Finnhub
        finnhub_symbol = get_finnhub_symbol(symbol, symbol_type)
        
        # 3. Get quote
        print(f"Fetching live price for {symbol} as {finnhub_symbol}...")
        quote = finnhub_client.quote(finnhub_symbol)
        
        if quote['c'] == 0 and quote['dp'] == 0:
            if symbol_type == 'crypto':
                 # Fallback for crypto: use candle
                to_ts = int(time.time())
                from_ts = to_ts - (2 * 24 * 60 * 60) # 2 days ago
                candle = finnhub_client.crypto_candles(finnhub_symbol, 'D', from_ts, to_ts)
                if candle['s'] == 'ok' and len(candle['c']) > 0:
                    return { "symbol": symbol, "price": candle['c'][-1], "change": 0, "percent_change": 0 }
            
            raise HTTPException(status_code=404, detail="Symbol not found or no data")
        
        return {
            "symbol": symbol,
            "price": quote['c'],
            "change": quote['d'],
            "percent_change": quote['dp']
        }
    except Exception as e:
        print(f"Error getting live price for {symbol} (as {finnhub_symbol}): {e}")
        raise HTTPException(status_code=500, detail="Error fetching live data")

@app.get("/api/v1/chart-data")
def get_chart_data(
    symbol: str = Query(..., min_length=1),
    timeframe: str = Query("1Y", min_length=2)
):
    """
    Fetches all historical data (prices, sentiment, trends)
    for the main dashboard chart, with a dynamic timeframe.
    """

    # 1. Convert timeframe to a SQL INTERVAL
    time_interval = "1 year" # Default
    if timeframe == "1W":
        time_interval = "7 days"
    elif timeframe == "1M":
        time_interval = "1 month"
    elif timeframe == "1Y":
        time_interval = "1 year"
    elif timeframe == "ALL":
        time_interval = "10 years" # "All"

    # 2. Determine the bucket size for grouping
    bucket_size = "1 day"
    if timeframe in ["1W", "1M"]:
        bucket_size = "1 hour"

    # 3. This is the final query (no fill-forward)
    sql_query = f"""
    WITH time_series AS (
      -- Generate a complete series of timestamps (hourly or daily)
      SELECT time_bucket('{bucket_size}', g.time) AS bucket
      FROM generate_series(
        NOW() - INTERVAL '{time_interval}',
        NOW(),
        INTERVAL '{bucket_size}'
      ) AS g(time)
      GROUP BY bucket
    ),
    sentiment_data AS (
      SELECT 
        time_bucket('{bucket_size}', time) AS bucket,
        avg(sentiment_score) AS avg_sentiment
      FROM news_articles
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '{time_interval}')
      GROUP BY bucket
    ),
    trends_data AS (
      SELECT 
        time_bucket('1 day', time) AS bucket, -- Trends data is always daily
        avg(score) AS google_score
      FROM google_trends
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '{time_interval}')
      GROUP BY bucket
    ),
    price_data AS (
      SELECT
        time_bucket('{bucket_size}', time) AS bucket,
        last(price, time) as "close"
      FROM prices
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '{time_interval}')
      GROUP BY bucket
    )
    -- Final SELECT. This will have NULL values where no data exists.
    SELECT
      t.bucket AS "time",
      p.close,
      g.google_score,
      s.avg_sentiment
    FROM time_series AS t
    LEFT JOIN price_data AS p ON t.bucket = p.bucket
    LEFT JOIN trends_data AS g ON time_bucket('1 day', t.bucket) = g.bucket
    LEFT JOIN sentiment_data AS s ON t.bucket = s.bucket
    WHERE t.bucket > (NOW() - INTERVAL '{time_interval}')
    ORDER BY t.bucket ASC;
    """
    
    chart_data = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Pass the symbol 3 times (once for each data CTE)
        cur.execute(sql_query, (symbol, symbol, symbol))
        
        rows = cur.fetchall()
        
        colnames = [desc[0] for desc in cur.description]
        
        for row in rows:
            chart_data.append(dict(zip(colnames, row)))
            
        cur.close()
        conn.close()
        
        return {"data": chart_data}
        
    except Exception as e:
        print(f"Error fetching chart data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching chart data: {e}")

@app.get("/api/v1/positive-news")
def get_positive_news(symbol: str = Query(..., min_length=1)):
    # Fetches 10 most recent POSITIVE news articles
    sql = """
    SELECT headline, source_name, time, url
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
    # Fetches 10 most recent NEGATIVE news articles
    sql = """
    SELECT headline, source_name, time, url
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

@app.get("/api/v1/search")
def search_symbols(query: str = Query(..., min_length=1)):
    """
    Searches for symbols in the master tracked_symbols table.
    """
    sql = """
        SELECT symbol, type FROM tracked_symbols 
        WHERE symbol ILIKE %s 
        LIMIT 10;
    """
    search_query = f"%{query}%"
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, (search_query,))
        rows = cur.fetchall()
        
        results = []
        for row in rows:
            symbol = row[0]
            type = row[1]
            results.append({
                "symbol": symbol,
                "name": f"{symbol} ({type.capitalize()})",
                "exchange": "Tracked"
            })
            
        cur.close()
        conn.close()
        return {"data": results}
    except Exception as e:
        raise HTTPException(status_code=5.00, detail=f"Error searching: {e}")

#
# --- Private Task Endpoints (for Cron-Job.org & Admin) ---
#
@app.post("/api/v1/tasks/run-prices", dependencies=[Depends(verify_secret)])
async def trigger_price_ingest(background_tasks: BackgroundTasks):
    """
    Secure endpoint to trigger the price ingest task (for cron).
    """
    print("Task received: run-prices")
    background_tasks.add_task(fetch_price_data)
    return {"message": "Price ingest task started in the background."}

@app.post("/api/v1/tasks/run-news", dependencies=[Depends(verify_secret)])
async def trigger_news_ingest(background_tasks: BackgroundTasks):
    """
    Secure endpoint to trigger the news ingest task (for cron).
    """
    print("Task received: run-news")
    background_tasks.add_task(fetch_news_data)
    return {"message": "News ingest task started in the background."}

@app.post("/api/v1/tasks/run-trends", dependencies=[Depends(verify_secret)])
async def trigger_trends_ingest(background_tasks: BackgroundTasks): # <-- THE FIX IS HERE
    """
    Secure endpoint to trigger the Google Trends ingest task (for cron).
    """
    print("Task received: run-trends")
    background_tasks.add_task(fetch_trends_data)
    return {"message": "Trends ingest task started in the background."}

@app.post("/api/v1/add-symbol", dependencies=[Depends(verify_admin_password)])
async def add_new_symbol(new_symbol: NewSymbol, background_tasks: BackgroundTasks):
    """
    Adds a new symbol to our tracked_symbols table.
    Secured by ADMIN_PASSWORD.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "INSERT INTO tracked_symbols (symbol, type) VALUES (%s, %s) ON CONFLICT(symbol) DO NOTHING",
            (new_symbol.symbol, new_symbol.type)
        )
        rows_added = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        
        if rows_added == 0:
            return {"message": f"Symbol {new_symbol.symbol} is already being tracked."}

        print(f"New symbol {new_symbol.symbol} added. Triggering background backfill...")
        background_tasks.add_task(fetch_price_data)
        background_tasks.add_task(fetch_news_data)
        background_tasks.add_task(fetch_trends_data)

        return {"message": f"Symbol {new_symbol.symbol} added. Backfilling data now. It will appear on the dashboard shortly."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding symbol: {e}")
