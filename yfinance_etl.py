import yfinance as yf
import pandas as pd
import boto3
from datetime import datetime

def run_yfinance_etl():
    
    # AWS S3 configuration
    S3_BUCKET = "s3://yfinance-airflow-etl-data-01-10-2025"
    s3 = boto3.client('s3')
    

    # List of 10 companies
    TICKERS = ["ASML", "STNE", "MELI", "NTES", "BEKE",
            "GILD", "SERV", "NFLX", "INTC", "PYPL"]

    def fetch_stock_data(ticker):
        """Fetch last 30 days of historical data for a ticker"""
        # Download data and flatten MultiIndex columns
        data = yf.download(ticker, period="30d", interval="1d", progress=False)
        
        # Handle MultiIndex columns by flattening them
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        # Reset index to make Date a column
        data.reset_index(inplace=True)
        
        # Add ticker column
        data["ticker"] = ticker
        
        # Reorder and keep only important columns
        data = data[["ticker", "Date", "Open", "High", "Low", "Close", "Volume"]]
        
        # Ensure proper data types
        data["Date"] = pd.to_datetime(data["Date"]).dt.date
        
        return data

    def run_etl():
        """Run the ETL process for all tickers"""
        all_data = []
        
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
            
            # Save locally
            today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"stocks_{today}.csv"
            final_data.to_csv(filename, index=False)
            print(f"\nSaved local CSV: {filename}")
            print(f"Total rows: {len(final_data)}")
            print(f"\nFirst few rows:\n{final_data.head()}")
            
            
            # Upload to S3
            s3.upload_file(filename, S3_BUCKET, filename)
            print(f"Uploaded to S3 bucket {S3_BUCKET} as {filename}")
            
        else:
            print("No data fetched successfully.")

    if __name__ == "__main__":
        run_etl()