import yfinance as yf
import pandas as pd
import boto3
from datetime import datetime
import os

def run_yfinance_etl():
    """Main ETL function called by Airflow"""
    
    # AWS S3 configuration - BUCKET NAME ONLY (no s3:// prefix)
    S3_BUCKET = "yfinance-airflow-etl-data-01-10-2025"
    
    # Initialize S3 client (requires AWS credentials via IAM role or env vars)
    s3 = boto3.client('s3')
    
    # List of 10 companies
    TICKERS = ["ASML", "STNE", "MELI", "NTES", "BEKE",
               "GILD", "SERV", "NFLX", "INTC", "PYPL"]
    
    print("Starting yfinance ETL process...")
    all_data = []
    
    for ticker in TICKERS:
        print(f"Fetching data for {ticker}...")
        try:
            # Download data
            data = yf.download(ticker, period="30d", interval="1d", progress=False)
            
            # Check if data is empty
            if data.empty:
                print(f"Warning: No data returned for {ticker}, skipping...")
                continue
            
            # Handle MultiIndex columns by flattening them
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            # Reset index to make Date a column
            data.reset_index(inplace=True)
            
            # Add ticker column
            data["ticker"] = ticker
            
            # Verify required columns exist
            required_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                print(f"Warning: Missing columns {missing_cols} for {ticker}, skipping...")
                continue
            
            # Reorder and keep only important columns
            data = data[["ticker", "Date", "Open", "High", "Low", "Close", "Volume"]]
            
            # Ensure proper data types
            data["Date"] = pd.to_datetime(data["Date"]).dt.date
            
            all_data.append(data)
            print(f"Successfully fetched {len(data)} rows for {ticker}")
            
        except Exception as e:
            print(f"Error fetching {ticker}: {str(e)}")
            continue
    
    # Combine all dataframes
    if not all_data:
        error_msg = "ETL failed: No data was fetched for any ticker"
        print(error_msg)
        raise Exception(error_msg)
    
    final_data = pd.concat(all_data, ignore_index=True)
    
    # Save locally
    today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"stocks_{today}.csv"
    local_path = f"{filename}"  # Use /tmp for temporary storage on EC2
    
    final_data.to_csv(local_path, index=False)
    print(f"\nSaved local CSV: {local_path}")
    print(f"Total rows: {len(final_data)}")
    print(f"Tickers processed: {final_data['ticker'].nunique()}")
    print(f"\nFirst few rows:\n{final_data.head()}")
    
    try:
        # Upload to S3
        s3.upload_file(local_path, S3_BUCKET, filename)
        print(f"Successfully uploaded to S3 bucket '{S3_BUCKET}' as '{filename}'")
        
        # Clean up local file
        os.remove(local_path)
        print(f"Cleaned up local file: {local_path}")
        
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        print("Make sure:")
        print("  1. EC2 instance has IAM role with S3 write permissions")
        print("  2. S3 bucket exists and name is correct")
        print("  3. boto3 is properly configured")
        raise
    
    print(f"\nETL process completed successfully!")
    return filename  # Return filename for potential downstream tasks


if __name__ == "__main__":
    # This allows testing the script directly
    run_yfinance_etl()