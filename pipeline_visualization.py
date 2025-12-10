"""
pipeline_visualization.py

Description:
    This script connects to the local SQLite database created by pipeline.py.
    It aggregates minute-level candle data into hourly buckets to reduce noise 
    and generates a dual-axis visualization comparing Price trends vs. Volume spikes.

Output:
    Saves a high-resolution image 'btc_and_eth_analysis_chart.png' to the local directory.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# --- Configuration
DB_NAME = "crypto_data.db"
OUTPUT_IMAGE = "btc_and_eth_analysis_chart.png"

def fetch_aggregated_data():
    """
    Reads from SQLite and performs an hourly aggregation, as specified in project outline
    
    Raw minute-level data over 7 days (~10k points) is too noisy for a high-level trend analysis.
    Grouping by hour provides a clearer signal for volume anomalies.
    """
    conn = sqlite3.connect(DB_NAME)

    query = """
            SELECT 
                product_id,
                strftime('%Y-%m-%d %H:00:00', timestamp) as hour_bucket,
                AVG(close) as hourly_avg_price,
                SUM(volume) as hourly_total_volume
            FROM candles
            GROUP BY product_id, hour_bucket
            ORDER BY hour_bucket ASC
        """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Ensure datetime objects are strictly typed for Matplotlib
    df['hour_bucket'] = pd.to_datetime(df['hour_bucket'])
    return df

def generate_plot(df):
    """
    Generates a multi-subplot figure with dual y-axes.
    
    Visualization Strategy:
    - Primary Y-Axis (Bar): Volume. Uses bars to visually represent 'accumulation' or 'spikes'.
    - Secondary Y-Axis (Line): Price. Uses a line to overlay trend direction.
    - Dual Axis is crucial because Price ($90k) and Volume (Units) have vastly different scales.
    """
    
    # Create a figure with 2 subplots (one for BTC, one for ETH)
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 10), sharex=True)
    products = df['product_id'].unique()

    for ax, id in zip(axes, products):
        subset = df[df['product_id'] == id]
        
        # Plot 1: VOLUME (Bar Chart on Primary Y-Axis) 
        ax.bar(subset['hour_bucket'], subset['hourly_total_volume'], 
            color='cornflowerblue', width=0.04, label='Volume', alpha=0.5)
        
        ax.set_ylabel(f'Volume ({id})', color='cornflowerblue', fontweight='bold')
        ax.tick_params(axis='y', labelcolor='cornflowerblue')
        

        # PLOT 2: PRICE (Line Chart on Secondary Y-Axis) ---
        ax2 = ax.twinx()  # Creating a second y-axis that shares the same x-axis
        ax2.plot(subset['hour_bucket'], subset['hourly_avg_price'], 
                color='darkorange', linewidth=2, label='Avg Price')
        
        ax2.set_ylabel(f'Price USD ({id})', color='darkorange', fontweight='bold')
        ax2.tick_params(axis='y', labelcolor='darkorange')
        
        ax.set_title(f"{id}: Hourly Price & Volume Analysis (11/17 - 11/24)", fontsize=12)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %H:%M'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

    plt.xlabel("Date (UTC)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("btc_and_eth_analysis_chart.png", dpi=300)

    plt.show()

if __name__ == "__main__":
    df = fetch_aggregated_data()

    if not df.empty:
        print(f"Data loaded. Rows: {len(df)}")
        generate_plot(df)
    else:
        print("No data found. Run pipeline.py first.")