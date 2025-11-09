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

        # Calculate timeframe: from 1 year ago to now
        # Finnhub uses Unix timestamps
        to_ts = int(time.time())
        from_ts = to_ts - (365 * 24 * 60 * 60) # 1 year ago

        for symbol, type in symbols_to_track:
            print(f"Price Ingest Task: Fetching data for {symbol} ({type})...")
            try:
                data = None
                if type == 'stock':
                    # Fetch daily stock candles
                    data = finnhub_client.stock_candles(symbol, 'D', from_ts, to_ts)
                elif type == 'crypto':
                    # Fetch daily crypto candles
                    # Finnhub uses a different format for crypto symbols (e.g., 'BINANCE:BTCUSDT')
                    # We will assume a default exchange (BINANCE) and currency (USDT)
                    # TODO: You may need to make this logic smarter in the future
                    finnhub_symbol = f"BINANCE:{symbol}USDT"
                    if symbol == 'BTC':
                         finnhub_symbol = "BINANCE:BTCUSDT"
                    elif symbol == 'ETH':
                         finnhub_symbol = "BINANCE:ETHUSDT"
                         
                    data = finnhub_client.crypto_candles(finnhub_symbol, 'D', from_ts, to_ts)
                
                if not data or data['s'] != 'ok':
                    print(f"Price Ingest Task: No data returned for {symbol}")
                    continue
                
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
                
                conn.commit()
                print(f"Price Ingest Task: Successfully inserted {insert_count} new data points for {symbol}.")
            
            except Exception as e:
                print(f"Price Ingest Task: Error for {symbol}: {e}")
                conn.rollback()

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
