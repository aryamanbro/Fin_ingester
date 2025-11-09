import os
import psycopg2
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
ALPHA_VANTAGE_KEY = os.getenv('ALPHA_VANTAGE_KEY')

def fetch_price_data():
    print("Price Ingest Task: Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # --- THIS IS THE NEW DYNAMIC LOGIC ---
    print("Price Ingest Task: Fetching symbols to track from database...")
    cur.execute("SELECT symbol, type FROM tracked_symbols")
    symbols_to_track = cur.fetchall()  # Will be [('TSLA', 'stock'), ('BTC', 'crypto'), etc.]
    print(f"Price Ingest Task: Found {len(symbols_to_track)} symbols.")
    # --- END NEW LOGIC ---
    
    ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas')
    cc = CryptoCurrencies(key=ALPHA_VANTAGE_KEY, output_format='pandas')
    
    # Loop over the list from the database
    for symbol, type in symbols_to_track:
        print(f"Price Ingest Task: Fetching data for {symbol} ({type})...")
        try:
            data = None
            if type == 'stock':
                data, meta_data = ts.get_daily(symbol=symbol, outputsize='full')
            elif type == 'crypto':
                data, meta_data = cc.get_digital_currency_daily(symbol=symbol, market='USD')
            
            if data is None or data.empty:
                print(f"Price Ingest Task: No data returned for {symbol}")
                continue
                
            insert_count = 0
            for time, row in data.iterrows():
                price = row['4. close']
                volume = row['5. volume']
                
                insert_query = """
                INSERT INTO prices (time, symbol, price, volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (time, symbol) DO NOTHING;
                """
                cur.execute(insert_query, (time, symbol, float(price), float(volume)))
                insert_count += cur.rowcount
            
            conn.commit()
            print(f"Price Ingest Task: Successfully inserted {insert_count} new data points for {symbol}.")
            
        except Exception as e:
            print(f"Price Ingest Task: Error for {symbol}: {e}")
            conn.rollback()
    
    cur.close()
    conn.close()
    print("Price Ingest Task: Complete.")

if __name__ == "__main__":
    fetch_price_data()
