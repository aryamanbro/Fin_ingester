import os
import psycopg2
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies

# Load all environment variables from .env
load_dotenv()

# --- Config ---
DATABASE_URL = os.getenv('DATABASE_URL')
ALPHA_VANTAGE_KEY = os.getenv('ALPHA_VANTAGE_KEY')
SYMBOLS_TO_TRACK = {
    'TSLA': 'stock',
    'BTC': 'crypto'
}
# ----------------

def fetch_price_data():
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Initialize Alpha Vantage clients
    ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas')
    cc = CryptoCurrencies(key=ALPHA_VANTAGE_KEY, output_format='pandas')
    
    for symbol, type in SYMBOLS_TO_TRACK.items():
        print(f"Fetching data for {symbol} ({type})...")
        try:
            data = None
            if type == 'stock':
                # Get daily stock data
                data, meta_data = ts.get_daily(symbol=symbol, outputsize='full')
                
            elif type == 'crypto':
                # Get daily crypto data
                data, meta_data = cc.get_digital_currency_daily(symbol=symbol, market='USD')
            
            if data is None:
                print(f"No data returned for {symbol}")
                continue
                
            print(f"Got {len(data)} data points for {symbol}. Preparing for database...")
            
            # Format data for database insert
            insert_count = 0
            for time, row in data.iterrows():
                # --- FIX #2 IS HERE ---
                # Use '4. close' for both, as the library standardizes the output
                price = row['4. close']
                volume = row['5. volume']
                
                # SQL to insert data, ignoring conflicts
                insert_query = """
                INSERT INTO prices (time, symbol, price, volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (time, symbol) DO NOTHING;
                """
                
                # --- FIX #1 IS HERE ---
                # We must cast price and volume to standard float()
                cur.execute(insert_query, (time, symbol, float(price), float(volume)))
                insert_count += cur.rowcount
            
            conn.commit() # Commit changes to the database
            print(f"Successfully inserted {insert_count} new data points for {symbol}.")
            
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            conn.rollback() # Roll back any failed transactions
    
    # Close the connection
    cur.close()
    conn.close()
    print("Price ingest complete.")

# This makes the script runnable from the command line
if __name__ == "__main__":
    fetch_price_data()
