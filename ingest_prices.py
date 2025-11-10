import os
import psycopg2
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FMP_API_KEY = os.getenv("FMP_API_KEY")   # Stock data
CG_BASE = "https://api.coingecko.com/api/v3"   # Crypto data

# -------------------------------------------
# Helper: Database connection
# -------------------------------------------
def get_db():
    return psycopg2.connect(DATABASE_URL)

# -------------------------------------------
# Helper: Insert candle row
# -------------------------------------------
def insert_row(cur, symbol, ts, price, volume):
    sql = """
        INSERT INTO prices (time, symbol, price, volume)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (time, symbol) DO NOTHING;
    """
    cur.execute(sql, (ts, symbol, float(price), float(volume)))

# -------------------------------------------
# Fetch stock data from FMP
# -------------------------------------------
def fetch_stock(symbol):
    print(f"[FMP] Fetching daily candles for stock {symbol}...")

    url = f"https://financialmodelingprep.com/api/v3/historical-chart/1hour/{symbol}?apikey={FMP_API_KEY}"
    daily_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"

    results = {"hourly": [], "daily": []}

    try:
        r1 = requests.get(url, timeout=10)
        if r1.status_code == 200:
            results["hourly"] = r1.json()
        else:
            print(f"[FMP ERROR] Hourly data {symbol}: {r1.text}")
    except Exception as e:
        print(f"[FMP ERROR] Hourly fetch failed for {symbol}: {e}")

    try:
        r2 = requests.get(daily_url, timeout=10)
        if r2.status_code == 200:
            json_data = r2.json()
            if "historical" in json_data:
                results["daily"] = json_data["historical"]
        else:
            print(f"[FMP ERROR] Daily data {symbol}: {r2.text}")
    except Exception as e:
        print(f"[FMP ERROR] Daily fetch failed for {symbol}: {e}")

    return results

# -------------------------------------------
# Fetch crypto data from CoinGecko
# -------------------------------------------
def fetch_crypto(symbol):
    print(f"[CG] Fetching candles for crypto {symbol}...")

    # coingecko uses lowercase ids: bitcoin, ethereum
    id_map = {
        "BTC": "bitcoin",
        "ETH": "ethereum"
    }

    if symbol not in id_map:
        print(f"[CG ERROR] No mapping for {symbol}")
        return {"hourly": [], "daily": []}

    crypto_id = id_map[symbol]

    # Hourly last 30 days
    hourly_url = f"{CG_BASE}/coins/{crypto_id}/market_chart?vs_currency=usd&days=30&interval=hourly"

    # Daily 365 days
    daily_url = f"{CG_BASE}/coins/{crypto_id}/market_chart?vs_currency=usd&days=365"

    results = {"hourly": [], "daily": []}

    try:
        r1 = requests.get(hourly_url, timeout=10)
        if r1.status_code == 200:
            results["hourly"] = r1.json().get("prices", [])
        else:
            print(f"[CG ERROR] Hourly {symbol}: {r1.text}")
    except Exception as e:
        print(f"[CG ERROR] Hourly fetch failed for {symbol}: {e}")

    try:
        r2 = requests.get(daily_url, timeout=10)
        if r2.status_code == 200:
            results["daily"] = r2.json().get("prices", [])
        else:
            print(f"[CG ERROR] Daily {symbol}: {r2.text}")
    except Exception as e:
        print(f"[CG ERROR] Daily fetch failed for {symbol}: {e}")

    return results

# -------------------------------------------
# Main ingestion driver
# -------------------------------------------
def fetch_price_data():
    print("DEBUG: I am inside fetch_price_data() NOW!")  # <-- Confirm entry

    try:
        print("DEBUG: Starting DB connection...")
        conn = psycopg2.connect(DATABASE_URL)
        print("DEBUG: Database connected!")

        # continue existing code...

    except Exception as e:
        print("DEBUG ERROR: ", e)
        raise


if __name__ == "__main__":
    fetch_price_data()
