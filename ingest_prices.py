import os
import psycopg2
import finnhub
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
FINNHUB_KEY = os.getenv('FINNHUB_API_KEY') # Use the same key from main.py

# Setup Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)

def get_finnhub_symbol(symbol, type):
    """Helper to format symbol for Finnhub crypto"""
    if type == 'crypto':
        # Assumes Binance and USDT. You can make this more robust later.
        if symbol == 'BTC':
            return "BINANCE:BTCUSDT"
        elif symbol == 'ETH':
            return "BINANCE:ETHUSDT"
        # Add other specific cryptos if needed
        return f"BINANCE:{symbol}USDT"
    return symbol # Return the plain symbol for stocks

def insert_data_to_db(cur, data, symbol):
    """Helper function to insert candle data into the DB"""
    if not data or data['s'] != 'ok':
        print(f"No data returned for {symbol} with this resolution.")
        return 0
        
    insert_count = 0
    # Finnhub returns data as a dictionary of lists
    # 'c' = close, 'v' = volume, 't' = timestamp
    for i in range(len(data['t'])):
        price_time = datetime.fromtimestamp(data['t'][i])
        price = data['c'][i]
        volume = data['v'][i]
        
        insert_query = """
        INSERT INTO prices (time, symbol, price, volume)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (time, symbol) DO NOTHING;
        """
        cur.execute(insert_query, (price_time, symbol, float(price), float(volume)))
        insert_count += cur.rowcount
    return insert_count

def fetch_price_data():
    print("Price Ingest Task: Connecting to database...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        print("Price Ingest Task: Fetching symbols to track from database...")
        cur.execute("SELECT symbol, type FROM tracked_symbols")
        symbols_to_track = cur.fetchall()
        print(f"Price Ingest Task: Found {len(symbols_to_track)} symbols.")

        # Timestamps for queries
        to_ts = int(time.time())
        daily_from_ts = to_ts - (365 * 24 * 60 * 60)   # 1 year ago
        hourly_from_ts = to_ts - (30 * 24 * 60 * 60)  # 30 days ago

        for symbol, type in symbols_to_track:
            print(f"Price Ingest Task: Fetching data for {symbol} ({type})...")
            try:
                total_inserted = 0
                fin_symbol = get_finnhub_symbol(symbol, type)

                # 1. Fetch DAILY data (for 1Y/All charts)
                print(f"  Fetching daily data for {symbol}...")
                daily_data = None
                if type == 'stock':
                    daily_data = finnhub_client.stock_candles(fin_symbol, 'D', daily_from_ts, to_ts)
                elif type == 'crypto':
                    daily_data = finnhub_client.crypto_candles(fin_symbol, 'D', daily_from_ts, to_ts)
                
                total_inserted += insert_data_to_db(cur, daily_data, symbol)
                
                # Be nice to the API
                time.sleep(1) 

                # 2. Fetch HOURLY data (for 1W/1M charts)
                print(f"  Fetching hourly data for {symbol}...")
                hourly_data = None
                if type == 'stock':
                    hourly_data = finnhub_client.stock_candles(fin_symbol, '60', hourly_from_ts, to_ts)
                elif type == 'crypto':
                    hourly_data = finnhub_client.crypto_candles(fin_symbol, '60', hourly_from_ts, to_ts)
                
                total_inserted += insert_data_to_db(cur, hourly_data, symbol)

                conn.commit()
                print(f"Price Ingest Task: Successfully inserted {total_inserted} new data points for {symbol}.")
            
            except Exception as e:
                print(f"Price Ingest Task: Error for {symbol}: {e}")
                conn.rollback()
            
            # Be nice to the API
            time.sleep(1)

    except Exception as e:
        print(f"Failed to connect to database or fetch symbols: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Price Ingest Task: Complete.")

if __name__ == "__main__":
    fetch_price_data()
