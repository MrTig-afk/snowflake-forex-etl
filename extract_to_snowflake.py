import os
import yfinance as yf
import requests
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv  

# Load secrets from .env file
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

def get_latest_prices():
    # We use a Session with a custom User-Agent to bypass Rate Limits
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })

    tickers = ["INR=X", "CL=F", "DX=F"]
    results = {}

    print("--- 1. Fetching Data from Yahoo Finance ---")
    for ticker in tickers:
        try:
            # Pass the 'session' to fix the Rate Limit error
            data = yf.download(ticker, period="5d", interval="15m", progress=False, session=session)
            
            if not data.empty:
                # Get the last valid close price
                price = data['Close'].iloc[-1].item()
                results[ticker] = round(price, 2)
                print(f"   > {ticker}: {results[ticker]}")
            else:
                print(f"   > {ticker}: No data found (Yahoo returned empty)")
                results[ticker] = None        
        except Exception as e:
            print(f"   > {ticker}: Error ({e})")
            results[ticker] = None
    
    return results

def upload_to_snowflake(usd, oil, dxy):
    print("\n--- 2. Uploading to Snowflake ---")
    
    if not (SNOWFLAKE_USER and SNOWFLAKE_PASSWORD and SNOWFLAKE_ACCOUNT):
        print("   [!] CRITICAL: Snowflake credentials not found.")
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

        # Handle Missing Data: Convert None to SQL 'NULL'
        val_usd = str(usd) if usd is not None else "NULL"
        val_oil = str(oil) if oil is not None else "NULL"
        val_dxy = str(dxy) if dxy is not None else "NULL"

        # Safe SQL Query handling NULLs
        sql_query = f"""
        INSERT INTO MARKET_PRICES (SOURCE_TIMESTAMP, USD_INR, OIL_PRICE, DXY_INDEX)
        VALUES ('{current_time_str}', {val_usd}, {val_oil}, {val_dxy})
        """
        
        cur.execute(sql_query)
        print(f"   > Success! Data logged at {current_time_str}")
        print(f"     (Values -> USD: {val_usd}, Oil: {val_oil}, DXY: {val_dxy})")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"   [!] Snowflake Error: {e}")

def main():
    print(f"--- Pipeline Started: {datetime.now()} ---")
    
    prices = get_latest_prices()
    
    # NEW LOGIC: Upload if we have ANY data (even if just Oil)
    # This prevents one broken ticker from stopping the whole pipeline
    if any(value is not None for value in prices.values()):
        upload_to_snowflake(
            usd=prices.get("INR=X"), 
            oil=prices.get("CL=F"), 
            dxy=prices.get("DX=F")
        )
    else:
        print("\n[!] Data incomplete. All tickers failed. Skipping upload.")

    print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()