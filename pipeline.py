import requests
import sqlite3
import time
from datetime import datetime, timedelta, timezone

# --- Configuration ---
DB_NAME = "crypto_data.db"
PRODUCT_IDS = ["BTC-USD", "ETH-USD"]
# Analysis window: Nov 17 - Nov 24, 2025
START_TIME = datetime(2025, 11, 17, 0, 0)
END_TIME = datetime(2025, 11, 24, 23, 59, 59)

# Coinbase API Granularity: 60 seconds (1 minute).
# Note: Granularity dictates the maximum time window per request (max 300 data points).
GRANULARITY = 60  

# --- Database Setup ---
def init_db(reset=False):
    """
    Initializes the SQLite database. 
    Args:
        reset (bool): If True, drops existing table to start fresh. 
                      Useful for development/testing cycles.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    if reset:
        cursor.execute("DROP TABLE IF EXISTS candles")
    
    # Schema includes composite primary key (timestamp + product_id) to prevent duplicates
    cursor.execute('''
       CREATE TABLE IF NOT EXISTS candles (
                   timestamp TEXT,
                   product_id TEXT,
                   low REAL,
                   high REAL, 
                   open REAL, 
                   close REAL,
                   volume REAL,
                   PRIMARY KEY (timestamp, product_id))
    ''')
    conn.commit()
    return conn

def fetch_candles(product_id, start, end, granularity):
    """
    Fetches data from Coinbase Public API for the product like BTC-USD specified in the header
    Handles API pagination logic to bypass the 300-candle limit per request.
    """
    product_url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    all_candles = []
    
    # The Coinbase API limits us to 300 records per request.
    # Since we want the best granularity (60s / 1 min), we can ask 
    # for 300 minutes at a time without hitting API request limits
    delta = timedelta(minutes=300)
    
    current_start = start
    while current_start < end:
        current_end = current_start + delta
        if current_end > end:
            current_end = end

        # Coinbase expects ISO format timestamps
        params = {
            'start': current_start.isoformat(),
            'end': current_end.isoformat(),
            'granularity': granularity
        }

        response = requests.get(product_url, params=params)

        if response.status_code == 200:
            data = response.json()
            if data:
                all_candles.extend(data)
                print(f"Fetched {len(data)} candles for {product_id} from {current_start} to {current_end}")
            else:
                print(f"No data for {product_id} at {current_start}")
        else:
            print(f"Failed to fetch data for {id}: {response.status_code}")

        current_start = current_end

        # Sleep to respect API throughput limits
        time.sleep(0.5)
    
    return all_candles

# --- Store Data ---
def store_data(conn, product_id, candles):
    """
    Parses API response and inserts into SQLite.
    Converts Unix timestamps to ISO and handles deduplication.
    """
    cursor = conn.cursor()
    count = 0
    for candle in candles:
        # API response format: [time, low, high, open, close, volume]
        # Time is a unix timestamp, convert to ISO for readability
        ts = datetime.fromtimestamp(candle[0], timezone.utc).isoformat()
        low, high, open_p, close_p, volume = candle[1], candle[2], candle[3], candle[4], candle[5]
        
        # INSERT OR IGNORE manages risk of overlapping timestamps
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO candles (timestamp, product_id, low, high, open, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (ts, product_id, low, high, open_p, close_p, volume))
            count += 1
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            
    conn.commit()
    print(f"Successfully stored {count} rows for {product_id}")

def run_pipeline():
    # Initialize DB (reset=True cleans old data for this run)
    conn = init_db(reset=True)

    for id in PRODUCT_IDS:
        candles = fetch_candles(id, START_TIME, END_TIME, GRANULARITY)
        store_data(conn, id, candles)

    conn.close()
    print("Pipeline finished.")

if __name__ == "__main__":
    run_pipeline()

