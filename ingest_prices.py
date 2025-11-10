import os
import psycopg2
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FMP_API_KEY = os.getenv("FMP_API_KEY")
CG_API_KEY = os.getenv("CG_API_KEY")  # <-- NEW
CG_BASE = "https://api.coingecko.com/api/v3"


# -------------------------------------------
# DB connection helper
# -------------------------------------------
def get_db():
    return psycopg2.connect(DATABASE_URL)


# -------------------------------------------
# Insert row helper
# -------------------------------------------
def insert_row(cur, symbol, ts, price, volume):
    sql = """
        INSERT INTO prices (time, symbol, price, volume)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (time, symbol) DO NOTHING;
    """
    cur.execute(sql, (ts, symbol, float(price), float(volume)))


# -------------------------------------------
# Fetch from FMP
# -------------------------------------------
def fetch_stock(symbol):
    print(f"[FMP] Fetching data for stock {symbol}...")

    hourly_url = f"https://financialmodelingprep.com/api/v3/historical-chart/1hour/{symbol}?apikey={FMP_API_KEY}"
    daily_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"

    result = {"hourly": [], "daily": []}

    # Hourly
    try:
        r1 = requests.get(hourly_url, timeout=10)
        if r1.status_code == 200:
            result["hourly"] = r1.json()
        else:
            print(f"[FMP ERROR HOURLY {symbol}] {r1.text}")
    except Exception as e:
        print(f"[FMP EXCEPTION HOURLY {symbol}] {e}")

    # Daily
    try:
        r2 = requests.get(daily_url, timeout=10)
        if r2.status_code == 200:
            js = r2.json()
            if "historical" in js:
                result["daily"] = js["historical"]
        else:
            print(f"[FMP ERROR DAILY {symbol}] {r2.text}")
    except Exception as e:
        print(f"[FMP EXCEPTION DAILY {symbol}] {e}")

    return result


# -------------------------------------------
# Fetch from CoinGecko (with API key)
# -------------------------------------------
def fetch_crypto(symbol):
    print(f"[CG] Fetching data for crypto {symbol}...")

    id_map = {"BTC": "bitcoin", "ETH": "ethereum"}

    if symbol not in id_map:
        print(f"[CG ERROR] Mapping not found for {symbol}")
        return {"hourly": [], "daily": []}

    cid = id_map[symbol]

    headers = {
        "x-cg-demo-api-key": CG_API_KEY
    }

    # Hourly 30 days
    hourly_url = f"{CG_BASE}/coins/{cid}/market_chart?vs_currency=usd&days=30&interval=hourly"

    # Daily 365 days
    daily_url = f"{CG_BASE}/coins/{cid}/market_chart?vs_currency=usd&days=365"

    result = {"hourly": [], "daily": []}

    # Hourly
    try:
        r1 = requests.get(hourly_url, headers=headers, timeout=10)
        if r1.status_code == 200:
            result["hourly"] = r1.json().get("prices", [])
        else:
            print(f"[CG HOURLY ERROR {symbol}] {r1.status_code} {r1.text}")
    except Exception as e:
        print(f"[CG HOURLY EXCEPTION {symbol}] {e}")

    # Daily
    try:
        r2 = requests.get(daily_url, headers=headers, timeout=10)
        if r2.status_code == 200:
            result["daily"] = r2.json().get("prices", [])
        else:
            print(f"[CG DAILY ERROR {symbol}] {r2.status_code} {r2.text}")
    except Exception as e:
        print(f"[CG DAILY EXCEPTION {symbol}] {e}")

    return result


# -------------------------------------------
# Main ingestion driver
# -------------------------------------------
def fetch_price_data():
    print("=== DEBUG: ENTERED fetch_price_data() ===")

    try:
        print("DEBUG: Connecting to DB...")
        conn = get_db()
        cur = conn.cursor()
        print("DEBUG: DB connection OK")

        # Fetch tracked symbols
        cur.execute("SELECT symbol, type FROM tracked_symbols")
        symbols = cur.fetchall()
        print(f"[INFO] Tracking {len(symbols)} symbols...")

        for sym, typ in symbols:
            print(f"\n=== PROCESSING {sym} ({typ}) ===")

            inserted = 0

            if typ == "stock":
                data = fetch_stock(sym)

                # HOURLY
                for candle in data["hourly"]:
                    try:
                        ts = datetime.fromisoformat(candle["date"])
                        price = candle["close"]
                        volume = candle["volume"]
                        insert_row(cur, sym, ts, price, volume)
                        inserted += cur.rowcount
                    except Exception as e:
                        print(f"[FMP INSERT ERROR HOURLY {sym}] {e}")

                # DAILY
                for candle in data["daily"]:
                    try:
                        ts = datetime.fromisoformat(candle["date"])
                        price = candle["close"]
                        volume = candle["volume"]
                        insert_row(cur, sym, ts, price, volume)
                        inserted += cur.rowcount
                    except Exception as e:
                        print(f"[FMP INSERT ERROR DAILY {sym}] {e}")

            elif typ == "crypto":
                data = fetch_crypto(sym)

                # HOURLY → timestamp in ms
                for p in data["hourly"]:
                    try:
                        ts = datetime.utcfromtimestamp(p[0] / 1000)
                        price = p[1]
                        volume = 0
                        insert_row(cur, sym, ts, price, volume)
                        inserted += cur.rowcount
                    except Exception as e:
                        print(f"[CG INSERT ERROR HOURLY {sym}] {e}")

                # DAILY → timestamp in ms
                for p in data["daily"]:
                    try:
                        ts = datetime.utcfromtimestamp(p[0] / 1000)
                        price = p[1]
                        volume = 0
                        insert_row(cur, sym, ts, price, volume)
                        inserted += cur.rowcount
                    except Exception as e:
                        print(f"[CG INSERT ERROR DAILY {sym}] {e}")

            conn.commit()
            print(f"[OK] Inserted {inserted} points for {sym}")

        print("=== PRICE INGEST COMPLETE === ✅")

    except Exception as e:
        print("🔥 FATAL ERROR in price ingest:", e)

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    fetch_price_data()
