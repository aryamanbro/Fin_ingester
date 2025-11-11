import os
import psycopg2
import psycopg2.extras
import finnhub
import time
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from ingest_prices import fetch_price_data
from ingest_news import fetch_news_data
from ingest_trends import fetch_trends_data

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")
TASK_SECRET_KEY = os.getenv("TASK_SECRET_KEY")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

app = FastAPI()
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

class NewSymbol(BaseModel):
    symbol: str
    type: str

# CORS config
allow_origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security checks
async def verify_secret(x_task_secret: str = Header(None)):
    if x_task_secret != TASK_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

async def verify_admin_password(x_admin_password: str = Header(None)):
    if x_admin_password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")

# DB connection helper
def get_db():
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as e:
        print("DB ERROR:", e)
        raise HTTPException(status_code=500, detail="Database connection error")

# Finnhub helper
def get_finnhub_symbol(symbol, type):
    if type == "crypto":
        return f"BINANCE:{symbol.upper()}USDT"
    return symbol.upper()

# Root
@app.get("/")
def root():
    return {"status": "API running"}

# Live price
@app.get("/api/v1/live-price")
def get_live_price(symbol: str):
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT type FROM tracked_symbols WHERE symbol = %s", (symbol.upper(),))
        res = cur.fetchone()
        typ = res["type"] if res else "stock"
        cur.close()
        conn.close()

        finnhub_symbol = get_finnhub_symbol(symbol, typ)
        print(f"Fetching live price for {symbol} as {finnhub_symbol}")

        quote = finnhub_client.quote(finnhub_symbol)

        if quote["c"] == 0:
            raise HTTPException(404, "No price found")

        return {
            "symbol": symbol,
            "price": quote["c"],
            "change": quote["d"],
            "percent_change": quote["dp"],
        }

    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail=str(e))

# Chart Data
@app.get("/api/v1/chart-data")
def chart(symbol: str, timeframe: str = "1Y"):
    time_interval = {
        "1W": "7 days",
        "1M": "1 month",
        "1Y": "1 year",
        "ALL": "10 years",
    }.get(timeframe, "1 year")

    bucket = "1 hour" if timeframe in ["1W", "1M"] else "1 day"

    sql = f"""
    WITH time_series AS (
        SELECT time_bucket('{bucket}', g.time) AS bucket
        FROM generate_series(NOW() - INTERVAL '{time_interval}', NOW(), INTERVAL '{bucket}') AS g(time)
        GROUP BY bucket
    ),
    price_data AS (
        SELECT time_bucket('{bucket}', time) AS bucket, LAST(price, time) AS close
        FROM prices
        WHERE symbol = %s
          AND time > NOW() - INTERVAL '{time_interval}'
        GROUP BY bucket
    ),
    sentiment_data AS (
        SELECT time_bucket('{bucket}', time) AS bucket, AVG(sentiment_score) AS sentiment
        FROM news_articles
        WHERE time > NOW() - INTERVAL '{time_interval}'
        GROUP BY bucket
    ),
    trends_data AS (
        SELECT time_bucket('1 day', time) AS bucket, AVG(score) AS google_score
        FROM google_trends
        WHERE symbol = %s
          AND time > NOW() - INTERVAL '{time_interval}'
        GROUP BY bucket
    )
    SELECT
        t.bucket AS time,
        p.close AS price,
        g.google_score AS google_trends_score,
        s.sentiment AS sentiment
    FROM time_series t
    LEFT JOIN price_data p ON p.bucket = t.bucket
    LEFT JOIN sentiment_data s ON s.bucket = t.bucket
    LEFT JOIN trends_data g ON g.bucket = time_bucket('1 day', t.bucket)
    ORDER BY t.bucket;
    """

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(sql, (symbol, symbol))
        data = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": data}
    except Exception as e:
        print("Chart error:", e)
        raise HTTPException(500, str(e))

# Positive news
@app.get("/api/v1/positive-news")
def positive(symbol: str):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT headline, source_name, time
            FROM news_articles
            WHERE symbol = %s AND sentiment_score > 0.1
            ORDER BY time DESC LIMIT 10;
        """, (symbol,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": rows}
    except Exception as e:
        print(e)
        raise HTTPException(500, str(e))

# Negative news
@app.get("/api/v1/negative-news")
def negative(symbol: str):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT headline, source_name, time
            FROM news_articles
            WHERE symbol = %s AND sentiment_score < -0.1
            ORDER BY time DESC LIMIT 10;
        """, (symbol,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": rows}
    except Exception as e:
        print(e)
        raise HTTPException(500, str(e))

# Ingestion endpoints
@app.post("/api/v1/tasks/run-prices", dependencies=[Depends(verify_secret)])
def run_prices():
    fetch_price_data()
    return {"status": "prices_ingested"}

@app.post("/api/v1/tasks/run-news", dependencies=[Depends(verify_secret)])
def run_news():
    fetch_news_data()
    return {"status": "news_ingested"}

@app.post("/api/v1/tasks/run-trends", dependencies=[Depends(verify_secret)])
def run_trends():
    fetch_trends_data()
    return {"status": "trends_ingested"}

# Add symbol
@app.post("/api/v1/add-symbol", dependencies=[Depends(verify_admin_password)])
def add_symbol(new: NewSymbol):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO tracked_symbols(symbol, type) VALUES(%s, %s) ON CONFLICT(symbol) DO NOTHING;",
            (new.symbol, new.type),
        )
        conn.commit()
        cur.close()
        conn.close()

        fetch_price_data()
        fetch_news_data()
        fetch_trends_data()

        return {"status": f"{new.symbol} added"}
    except Exception as e:
        raise HTTPException(500, str(e))
# ---------------------------
# Search tracked symbols only
# ---------------------------
@app.get("/api/v1/search")
def search_symbols(query: str = Query(..., min_length=1)):
    """
    Search symbols already tracked in your DB.
    Returns at most 10 matches from tracked_symbols.
    """
    try:
        like = f"%{query}%"
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, type
            FROM tracked_symbols
            WHERE symbol ILIKE %s
            ORDER BY symbol ASC
            LIMIT 10;
            """,
            (like,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return {
            "data": [
                {
                    "symbol": r["symbol"],
                    "name": f'{r["symbol"]} ({r["type"].capitalize()})',
                    "exchange": "Tracked"
                }
                for r in rows
            ]
        }
    except Exception as e:
        print("Search error:", e)
        raise HTTPException(status_code=500, detail="Error searching tracked symbols")



@app.get("/ping")
def ping():
    return {"status": "ok"}
