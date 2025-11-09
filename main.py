import os
import psycopg2
import finnhub
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

# ---
# IMPORT YOUR INGESTOR SCRIPTS AS FUNCTIONS
# This assumes ingest_prices.py has a function fetch_price_data()
# and ingest_news.py has fetch_news_data(), etc.
# ---
from ingest_prices import fetch_price_data
from ingest_news import fetch_news_data
from ingest_trends import fetch_trends_data

# Load all environment variables from .env
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.getenv('DATABASE_URL')
FINNHUB_KEY = os.getenv('FINNHUB_API_KEY')
TASK_SECRET_KEY = os.getenv('TASK_SECRET_KEY') # Your new secret key for cron jobs

# Check for essential keys
if not TASK_SECRET_KEY:
    print("Error: TASK_SECRET_KEY not found in .env file.")
    # In a real app, you might exit here
    # exit(1)
class NewSymbol(BaseModel):
    symbol: str
    type: str
# --- App Setup ---
app = FastAPI()
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

# --- CORS Middleware ---
# This allows your Vercel React app to call this API
app.add_middleware(
    CORSMiddleware,
    # In production, change "*" to your Vercel app's URL
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Security Helper ---
async def verify_secret(x_task_secret: str = Header(None)):
    """Verifies the secret key for cron job tasks."""
    if x_task_secret != TASK_SECRET_KEY:
        print(f"Failed task auth: Invalid key received.")
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid Task Secret Key")

# --- Database Helper ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

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
    """
    try:
        quote = finnhub_client.quote(symbol.upper())
        if quote['c'] == 0 and quote['dp'] == 0:
            raise HTTPException(status_code=404, detail="Symbol not found or no data")
        
        return {
            "symbol": symbol,
            "price": quote['c'],
            "change": quote['d'],
            "percent_change": quote['dp']
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error fetching live data")

@app.get("/api/v1/chart-data")
def get_chart_data(
    symbol: str = Query(..., min_length=1),
    timeframe: str = Query("1Y", min_length=2) # Add timeframe, default to 1Y
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
    # For 1W/1M, we bucket by hour. For 1Y/ALL, we bucket by day.
    bucket_size = "1 day"
    if timeframe in ["1W", "1M"]:
        bucket_size = "1 hour"

    # This is the "Ultimate" query, now with dynamic timeframe and bucketing
    sql_query = f"""
    WITH daily_sentiment AS (
      SELECT 
        time_bucket('{bucket_size}', time) AS day,
        symbol,
        avg(sentiment_score) AS avg_sentiment
      FROM news_articles
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '{time_interval}')
      GROUP BY day, symbol
    ),
    google_trends_daily AS (
      SELECT 
        time_bucket('{bucket_size}', time) AS day,
        symbol,
        avg(score) AS google_score
      FROM google_trends
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '{time_interval}')
      GROUP BY day, symbol
    ),
    prices_daily AS (
      SELECT
        time_bucket('{bucket_size}', time) AS day,
        symbol,
        first(price, time) as "open",
        max(price) as "high",
        min(price) as "low",
        last(price, time) as "close"
      FROM prices
      WHERE 
        symbol = %s AND time > (NOW() - INTERVAL '{time_interval}')
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
      p.day > (NOW() - INTERVAL '{time_interval}')
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
    # Fetches 10 most recent POSITIVE news articles
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
    # Fetches 10 most recent NEGATIVE news articles
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

#
# --- Private Task Endpoints (for Cron-Job.org) ---
#
@app.post("/api/v1/tasks/run-prices", dependencies=[Depends(verify_secret)])
async def trigger_price_ingest(background_tasks: BackgroundTasks):
    """
    Secure endpoint to trigger the price ingest task.
    Runs in the background to avoid a timeout.
    """
    print("Task received: run-prices")
    background_tasks.add_task(fetch_price_data)
    return {"message": "Price ingest task started in the background."}


@app.get("/api/v1/search")
def search_symbols(query: str = Query(..., min_length=1)):
    """
    Searches for symbols in the database.
    """
    # We will search for symbols that appear in EITHER
    # the prices table OR the news table for broader results.
    sql = """
        (SELECT DISTINCT symbol FROM prices WHERE symbol ILIKE %s LIMIT 5)
        UNION
        (SELECT DISTINCT symbol FROM news_articles WHERE symbol ILIKE %s LIMIT 5)
        LIMIT 10;
    """
    # We add wildcards to the query
    search_query = f"%{query}%"
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, (search_query, search_query))
        rows = cur.fetchall()
        
        # We need to format this to match the mock data structure
        results = []
        for row in rows:
            symbol = row[0]
            # You can add more details here if you have a 'symbols' table
            results.append({
                "symbol": symbol,
                "name": f"{symbol} Data",
                "exchange": "Database"
            })
            
        cur.close()
        conn.close()
        return {"data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching: {e}")

@app.post("/api/v1/tasks/run-news", dependencies=[Depends(verify_secret)])
async def trigger_news_ingest(background_tasks: BackgroundTasks):
    """
    Secure endpoint to trigger the news and sentiment ingest task.
    """
    print("Task received: run-news")
    background_tasks.add_task(fetch_news_data)
    return {"message": "News ingest task started in the background."}

@app.post("/api/v1/tasks/run-trends", dependencies=[Depends(verify_secret)])
async def trigger_trends_ingest(background_tasks: BackgroundTasks):
    """
    Secure endpoint to trigger the Google Trends ingest task.
    """
    print("Task received: run-trends")
    background_tasks.add_task(fetch_trends_data)
    return {"message": "Trends ingest task started in the background."}

@app.post("/api/v1/add-symbol", dependencies=[Depends(verify_secret)])
async def add_new_symbol(new_symbol: NewSymbol, background_tasks: BackgroundTasks):
    """
    Adds a new symbol to our tracked_symbols table.
    The cron jobs will automatically pick it up on their next run.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Add the new symbol to the master list
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

        # --- This is the key ---
        # We don't need to run the full ingest.
        # We just "pre-fill" the data by calling the scripts in the background.
        # The cron jobs will take over from here.
        print(f"New symbol {new_symbol.symbol} added. Triggering background backfill...")
        background_tasks.add_task(fetch_price_data)
        background_tasks.add_task(fetch_news_data)
        background_tasks.add_task(fetch_trends_data)

        return {"message": f"Symbol {new_symbol.symbol} added. Backfilling data now. It will appear on the dashboard shortly."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding symbol: {e}")
