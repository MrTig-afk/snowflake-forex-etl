import os
import yfinance as yf
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv  

# Load secrets from .env file
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

def get_latest_prices():
    # FIXED: Changed 'INR=x' to 'INR=X' for consistency
    tickers = ["INR=X", "CL=F", "DX=F"]
    results = {}

    print("--- 1. Fetching Data from Yahoo Finance ---")
    for ticker in tickers:
        try:
            data = yf.download(ticker, period="5d", interval="15m", progress=False, auto_adjust=True)
            if not data.empty:
                price = data['Close'].iloc[-1].item()
                results[ticker] = round(price, 2)
                print(f"   > {ticker}: {results[ticker]}")
            else:
                print(f"   > {ticker}: No data found")
                results[ticker] = None        
        except Exception as e:
            print(f"   > {ticker}: Error ({e})")
            results[ticker] = None
    
    return results

def upload_to_snowflake(usd, oil, dxy):
    print("\n--- 2. Uploading to Snowflake ---")
    
    if not (SNOWFLAKE_USER and SNOWFLAKE_PASSWORD and SNOWFLAKE_ACCOUNT):
        print("   [!] CRITICAL: Snowflake credentials not found in Environment Variables.")
        return

    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse="COMPUTE_WH",
            database="RUPEE_INDEX_DB",
            schema="RAW_DATA"
        )
        cur = conn.cursor()
        
        current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        sql_query = f"""
        INSERT INTO MARKET_PRICES (SOURCE_TIMESTAMP, USD_INR, OIL_PRICE, DXY_INDEX)
        VALUES ('{current_time_str}', {usd}, {oil}, {dxy})
        """
        
        cur.execute(sql_query)
        print(f"   > Success! Data logged at {current_time_str}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"   [!] Snowflake Error: {e}")

def main():
    print(f"--- Pipeline Started: {datetime.now()} ---")
    
    # Step 1: Extract
    prices = get_latest_prices()
    
    # Step 2: Validate
    # Now this check will pass because the keys match ('INR=X')
    if prices and prices.get("INR=X") and prices.get("CL=F") and prices.get("DX=F"):
        # Step 3: Load
        upload_to_snowflake(
            usd=prices["INR=X"], 
            oil=prices["CL=F"], 
            dxy=prices["DX=F"]
        )
    else:
        print("\n[!] Data incomplete. Skipping upload.")

    print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()