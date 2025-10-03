import yfinance as yf
import pandas as pd
import boto3
from datetime import datetime

"""
# AWS S3 configuration
S3_BUCKET = "your-s3-bucket-name"
s3 = boto3.client('s3')
"""

# List of 10 companies
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
           "META", "NVDA", "NFLX", "INTC", "PYPL"]

def fetch_stock_data(ticker):
    """Fetch last 30 days of historical data for a ticker"""
    # Download 30-day data
    data = yf.download(ticker, period="30d", interval="1d", progress=False)
    
    # Handle MultiIndex columns by flattening them
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    
    # Reset index to make Date a column
    data.reset_index(inplace=True)
    
    # Add ticker column
    data["ticker"] = ticker
    
    # Fetch all-time high and low data
    stock = yf.Ticker(ticker)
    all_time_data = stock.history(period="max")
    
    if not all_time_data.empty:
        # Find all-time high
        ath_idx = all_time_data["High"].idxmax()
        all_time_high = all_time_data.loc[ath_idx, "High"]
        all_time_high_date = ath_idx.date()
        
        # Find all-time low
        atl_idx = all_time_data["Low"].idxmin()
        all_time_low = all_time_data.loc[atl_idx, "Low"]
        all_time_low_date = atl_idx.date()
    else:
        all_time_high = None
        all_time_high_date = None
        all_time_low = None
        all_time_low_date = None
    
    # Add all-time high/low columns
    data["all_time_high"] = all_time_high
    data["all_time_high_date"] = all_time_high_date
    data["all_time_low"] = all_time_low
    data["all_time_low_date"] = all_time_low_date
    
    # Reorder and keep only important columns
    data = data[["ticker", "Date", "Open", "High", "Low", "Close", "Volume",
                 "all_time_high", "all_time_high_date", "all_time_low", "all_time_low_date"]]
    
    # Ensure proper data types
    data["Date"] = pd.to_datetime(data["Date"]).dt.date
    
    return data

def run_etl():
    """Run the ETL process for all tickers"""
    all_data = []
    today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    for ticker in TICKERS:
        print(f"Fetching data for {ticker}...")
        try:
            df = fetch_stock_data(ticker)
            all_data.append(df)
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            continue
    
    # Combine all dataframes
    if all_data:
        final_data = pd.concat(all_data, ignore_index=True)
        
        # Add timestamp column
        final_data["timestamp"] = today
        
        # Add timestamp column
        final_data["timestamp"] = today
        
        # Save locally
        filename = f"stocks_{today}.csv"
        final_data.to_csv(filename, index=False)
        print(f"\nSaved local CSV: {filename}")
        print(f"Total rows: {len(final_data)}")
        print(f"\nFirst few rows:\n{final_data.head()}")
        
        """
        # Upload to S3
        s3.upload_file(filename, S3_BUCKET, filename)
        print(f"Uploaded to S3 bucket {S3_BUCKET} as {filename}")
        """
    else:
        print("No data fetched successfully.")

if __name__ == "__main__":
    run_etl()