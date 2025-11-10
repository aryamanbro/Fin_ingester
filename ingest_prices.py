import yfinance as yf
import psycopg2
import os
from datetime import datetime, timedelta

DATABASE_URL = os.getenv("DATABASE_URL")

def fetch_price_data():
    print("Price Ingest: Starting...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("SELECT symbol, type FROM tracked_symbols")
    symbols = cur.fetchall()

    for symbol, type in symbols:
        print(f"[FETCH] Fetching OHLC for {symbol} using yfinance...")

        # yfinance expects crypto as BTC-USD
        if type == "crypto":
            yf_symbol = f"{symbol}-USD"
        else:
            yf_symbol = symbol

        try:
            # Historical daily data
            data_daily = yf.download(
                yf_symbol,
                period="1y",
                interval="1d",
                progress=False
            )

            inserts = 0
            for index, row in data_daily.iterrows():
                ts = index.to_pydatetime()
                close = float(row["Close"])
                volume = float(row["Volume"])

                cur.execute("""
                    INSERT INTO prices (time, symbol, price, volume)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (time, symbol) DO NOTHING;
                """, (ts, symbol, close, volume))
                inserts += cur.rowcount

            print(f"[OK] Inserted {inserts} points for {symbol}")

            conn.commit()
        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")

    cur.close()
    conn.close()
    print("Price Ingest: Complete ✅")
