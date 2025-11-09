import os
import psycopg2
import finnhub
import time
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Depends, Header
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
TASK_SECRET_KEY = os.getenv('TASK_SECRET_KEY')  # For cron jobs
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')    # For manual admin actions

# --- App Setup ---
app = FastAPI()
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

# --- Pydantic Model ---
class NewSymbol(BaseModel):
    symbol: str
    type: str  # e.g., 'stock' or 'crypto'

# --- CORS Middleware ---
allow_origins = [
    "http://127.0.0.1:8080",
    "http://localhost:8080",
    "*"  # Replace with Vercel URL in production
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
    if x_task_secret != TASK_SECRET_KEY:
        print(f"Failed task auth: Invalid key received.")
        raise HTTPException(status_code=401, detail="Unauthorized")

async def verify_admin_password(x_admin_password: str = Header(None)):
    if x_admin_password != ADMIN_PASSWORD:
        print(f"Failed admin auth: Invalid admin password.")
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- Database Helper ---
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# --- Finnhub Helper ---
def get_finnhub_symbol(symbol, type):
    if type == 'crypto':
        return f"BINANCE:{symbol.upper()}USDT"
    return symbol.upper()

# ROOT
@app.get("/")
def read_root():
    return {"status": f"Sentiment API at {os.getenv('RENDER_EXTERNAL_URL', 'local')} is running"}

# LIVE PRICE
@app.get("/api/v1/live-price")
def get_live_price(symbol: str = Query(..., min_length=1)):
    symbol_type = 'stock'
    finnhub_symbol = symbol.upper()

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT type FROM tracked_symbols WHERE symbol = %s", (symbol.upper(),))
        result = cur.fetchone()
        if result:
            symbol_type = result[0]
        cur.close()
        conn.close()

        finnhub_symbol = get_finnhub_symbol(symbol, symbol_type)

        print(f"Fetching live price for {symbol} as {finnhub_symbol}...")
        quote = finnhub_client.quote(finnhub_symbol)

        if quote['c'] == 0 and quote['dp'] == 0:
            raise HTTPException(status_code=404, detail="Symbol not found or no data")

        return {
            "symbol": symbol,
            "price": quote['c'],
            "change": quote['d'],
            "percent_change": quote['dp']
        }

    except Exception as e:
        print(f"Error getting live price: {e}")
        raise HTTPException(status_code=500, detail="Error fetching live data")

# CHART DATA
@app.get("/api/v1/chart-data")
def get_chart_data(
    symbol: str = Query(..., min_length=1),
    timeframe: str = Query("1Y", min_length=2)
):
    """
    Fetches all historical data (prices, sentiment, trends)
    for the main dashboard chart, with a dynamic timeframe.
    """

    time_interval = {
        "1W": "7 days",
        "1M": "1 month",
        "1Y": "1 year",
        "ALL": "10 years"
    }.get(timeframe, "1 year")

    bucket_size = "1 hour" if timeframe in ["1W", "1M"] else "1 day"

    sql_query = f"""
    WITH time_series AS (
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
            AVG(sentiment_score) AS avg_sentiment
        FROM news_articles
        WHERE time > (NOW() - INTERVAL '{time_interval}')
        GROUP BY bucket
    ),
    trends_data AS (
        SELECT
            time_bucket('1 day', time) AS bucket,
            AVG(score) AS google_score
        FROM google_trends
        WHERE symbol = %s
          AND time > (NOW() - INTERVAL '{time_interval}')
        GROUP BY bucket
    ),
    price_data AS (
        SELECT
            time_bucket('{bucket_size}', time) AS bucket,
            LAST(price, time) AS close
        FROM prices
        WHERE symbol = %s
          AND time > (NOW() - INTERVAL '{time_interval}')
        GROUP BY bucket
    )
    SELECT
        t.bucket AS time,
        p.close AS price,
        g.google_score AS google_trends_score,
        s.avg_sentiment AS sentiment
    FROM time_series AS t
    LEFT JOIN price_data AS p ON t.bucket = p.bucket
    LEFT JOIN trends_data AS g ON time_bucket('1 day', t.bucket) = g.bucket
    LEFT JOIN sentiment_data AS s ON t.bucket = s.bucket
    ORDER BY t.bucket ASC;
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql_query, (symbol, symbol))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        data = [dict(zip(colnames, row)) for row in rows]
        cur.close()
        conn.close()
        return {"data": data}

    except Exception as e:
        print(f"Error fetching chart data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching chart data")

# POSITIVE NEWS
@app.get("/api/v1/positive-news")
def get_positive_news(symbol: str = Query(..., min_length=1)):
    """
    Fetches most recent positive news articles related to the chosen symbol.
    Symbol filtering is done by matching keywords in the headline or source.
    """
    sql = """
    SELECT headline, source_name, time
    FROM news_articles
    WHERE sentiment_score > 0.3
      AND (
          LOWER(headline) LIKE '%' || LOWER(%s) || '%'
          OR LOWER(source_name) LIKE '%' || LOWER(%s) || '%'
      )
    ORDER BY time DESC
    LIMIT 10;
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, (symbol, symbol))
        rows = cur.fetchall()
        news_list = [
            {"headline": r[0], "source_name": r[1], "time": r[2]} 
            for r in rows
        ]
        cur.close()
        conn.close()
        return {"data": news_list}
    except Exception as e:
        print(f"Error fetching positive news: {e}")
        raise HTTPException(status_code=500, detail="Error fetching news")

# NEGATIVE NEWS
@app.get("/api/v1/negative-news")
def get_negative_news(symbol: str = Query(..., min_length=1)):
    """
    Fetches most recent negative news articles related to the chosen symbol.
    """
    sql = """
    SELECT headline, source_name, time
    FROM news_articles
    WHERE sentiment_score < -0.3
      AND (
          LOWER(headline) LIKE '%' || LOWER(%s) || '%'
          OR LOWER(source_name) LIKE '%' || LOWER(%s) || '%'
      )
    ORDER BY time DESC
    LIMIT 10;
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, (symbol, symbol))
        rows = cur.fetchall()
        news_list = [
            {"headline": r[0], "source_name": r[1], "time": r[2]} 
            for r in rows
        ]
        cur.close()
        conn.close()
        return {"data": news_list}
    except Exception as e:
        print(f"Error fetching negative news: {e}")
        raise HTTPException(status_code=500, detail="Error fetching news")


# SEARCH
@app.get("/api/v1/search")
def search_symbols(query: str = Query(..., min_length=1)):
    sql = "SELECT symbol, type FROM tracked_symbols WHERE symbol ILIKE %s LIMIT 10;"
    search_query = f"%{query}%"
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(sql, (search_query,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {
        "data": [
            {"symbol": r[0], "name": f"{r[0]} ({r[1].capitalize()})", "exchange": "Tracked"}
            for r in rows
        ]
    }

# ✅ FIXED INGESTION ENDPOINTS — synchronous execution
@app.post("/api/v1/tasks/run-prices", dependencies=[Depends(verify_secret)])
async def trigger_price_ingest():
    print("Task received: run-prices STARTING NOW")
    fetch_price_data()
    print("Task received: run-prices FINISHED")
    return {"message": "Price ingest completed."}

@app.post("/api/v1/tasks/run-news", dependencies=[Depends(verify_secret)])
async def trigger_news_ingest():
    print("Task received: run-news STARTING NOW")
    fetch_news_data()
    print("Task received: run-news FINISHED")
    return {"message": "News ingest completed."}

@app.post("/api/v1/tasks/run-trends", dependencies=[Depends(verify_secret)])
async def trigger_trends_ingest():
    print("Task received: run-trends STARTING NOW")
    fetch_trends_data()
    print("Task received: run-trends FINISHED")
    return {"message": "Trends ingest completed."}

# ADD SYMBOL
@app.post("/api/v1/add-symbol", dependencies=[Depends(verify_admin_password)])
async def add_new_symbol(new_symbol: NewSymbol):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO tracked_symbols (symbol, type) VALUES (%s, %s) ON CONFLICT(symbol) DO NOTHING",
            (new_symbol.symbol, new_symbol.type)
        )
        added = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()

        print(f"New symbol {new_symbol.symbol} added. Running backfill...")

        fetch_price_data()
        fetch_news_data()
        fetch_trends_data()

        return {"message": f"Symbol {new_symbol.symbol} added and backfilled."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding symbol: {e}")
