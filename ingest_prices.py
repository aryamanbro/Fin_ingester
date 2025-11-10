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
    print("Price Ingest: Starting...")

    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT symbol, type FROM tracked_symbols;")
        tracked = cur.fetchall()

        for symbol, type_ in tracked:
            total_inserted = 0

            if type_ == "stock":
                data = fetch_stock(symbol)
                # Daily
                for row in data["daily"]:
                    # FMP daily example row:
                    # {"date": "2024-11-01", "close": 260.21, "volume": 1000000}
                    try:
                        ts = datetime.strptime(row["date"], "%Y-%m-%d")
                        price = row["close"]
                        volume = row.get("volume", 0)
                        insert_row(cur, symbol, ts, price, volume)
                        total_inserted += 1
                    except:
                        pass

                # Hourly
                for row in data["hourly"]:
                    # Hourly row example:
                    # {"date": "2024-11-12 10:00:00", "close": 260.21, "volume": 100000}
                    try:
                        ts = datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S")
                        price = row["close"]
                        volume = row.get("volume", 0)
                        insert_row(cur, symbol, ts, price, volume)
                        total_inserted += 1
                    except:
                        pass

            elif type_ == "crypto":
                data = fetch_crypto(symbol)

                # DAILY
                for ts_ms, price in data["daily"]:
                    ts = datetime.utcfromtimestamp(ts_ms / 1000)
                    insert_row(cur, symbol, ts, price, 0)
                    total_inserted += 1

                # HOURLY
                for ts_ms, price in data["hourly"]:
                    ts = datetime.utcfromtimestamp(ts_ms / 1000)
                    insert_row(cur, symbol, ts, price, 0)
                    total_inserted += 1

            conn.commit()
            print(f"[OK] Inserted {total_inserted} points for {symbol}")

    except Exception as e:
        print("Price Ingest FAILED:", e)

    finally:
        cur.close()
        conn.close()
        print("Price Ingest: Complete ✅")

if __name__ == "__main__":
    fetch_price_data()
