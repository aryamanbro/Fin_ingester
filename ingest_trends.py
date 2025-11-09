import os
import psycopg2
import time
from dotenv import load_dotenv
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

# Load all environment variables from .env
load_dotenv()

# --- Config ---
DATABASE_URL = os.getenv('DATABASE_URL')
SYMBOLS_TO_TRACK = ['TSLA', 'BTC']
# ----------------

def fetch_trends_data():
    print("Connecting to database...")
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Initialize Google Trends client
        # We add retries and a backoff_factor to be "polite"
        pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=0.5)
        
        for symbol in SYMBOLS_TO_TRACK:
            print(f"Fetching Google Trends data for {symbol}...")
            
            try:
                # Build payload for the last 90 days (which gives daily data)
                pytrends.build_payload([symbol], cat=0, timeframe='today 3-m')
                # Get the interest-over-time data as a pandas DataFrame
                data = pytrends.interest_over_time()
                
                if data.empty or data.iloc[:,0].empty:
                    print(f"No trend data returned for {symbol}")
                    continue
                
                print(f"Got {len(data)} data points for {symbol}. Preparing for database...")
                
                insert_count = 0
                for time, row in data.iterrows():
                    # The score is the first column, named after the symbol
                    score = row[symbol]
                    
                    if score == 0:
                        continue # Skip days with no interest
                    
                    # SQL to insert data, ignoring conflicts
                    insert_query = """
                    INSERT INTO google_trends (time, symbol, score)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (time, symbol) DO NOTHING;
                    """
                    cur.execute(insert_query, (time, symbol, int(score)))
                    insert_count += cur.rowcount
                
                conn.commit() # Commit changes for this symbol
                print(f"Successfully inserted {insert_count} new data points for {symbol}.")
                
            except TooManyRequestsError:
                print(f"HIT RATE LIMIT (429) for {symbol}. Stopping script.")
                print("Please wait 6-12 hours before running this script again.")
                conn.rollback()
                break # Stop the loop if we get rate limited
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
                conn.rollback()
        
        # Close the connection
        cur.close()
        conn.close()
        print("Google Trends ingest complete.")

    except Exception as e:
        print(f"Failed to connect to database: {e}")

# This makes the script runnable
if __name__ == "__main__":
    fetch_trends_data()
