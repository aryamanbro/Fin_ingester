import os
import psycopg2
from dotenv import load_dotenv
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

def fetch_trends_data():
    print("Trends Ingest Task: Connecting to database...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # --- THIS IS THE NEW DYNAMIC LOGIC ---
        print("Trends Ingest Task: Fetching symbols to track from database...")
        cur.execute("SELECT symbol FROM tracked_symbols")
        symbols_to_track = [row[0] for row in cur.fetchall()]
        print(f"Trends Ingest Task: Found {len(symbols_to_track)} symbols.")
        # --- END NEW LOGIC ---

        pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=0.5)
        
        # Loop over the list from the database
        for symbol in symbols_to_track:
            print(f"Trends Ingest Task: Fetching data for {symbol}...")
            try:
                pytrends.build_payload([symbol], cat=0, timeframe='today 3-m')
                data = pytrends.interest_over_time()
                
                if data.empty or data.iloc[:,0].empty:
                    print(f"Trends Ingest Task: No data for {symbol}")
                    continue
                
                insert_count = 0
                for time, row in data.iterrows():
                    score = row[symbol]
                    if score == 0:
                        continue
                    
                    insert_query = """
                    INSERT INTO google_trends (time, symbol, score)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (time, symbol) DO NOTHING;
                    """
                    cur.execute(insert_query, (time, symbol, int(score)))
                    insert_count += cur.rowcount
                
                conn.commit()
                print(f"Trends Ingest Task: Successfully inserted {insert_count} new data points for {symbol}.")
                
            except TooManyRequestsError:
                print(f"Trends Ingest Task: HIT RATE LIMIT for {symbol}. Stopping.")
                conn.rollback()
                break # Stop the loop if we get rate limited
            except Exception as e:
                print(f"Trends Ingest Task: Error for {symbol}: {e}")
                conn.rollback()
    
    except Exception as e:
        print(f"Failed to connect to database: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Google Trends Ingest Task: Complete.")

if __name__ == "__main__":
    fetch_trends_data()
