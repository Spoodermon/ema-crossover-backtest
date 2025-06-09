import os
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

#Load API key from .env
load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY in .env")

#Cache's the CSVs
CACHE_DIR = Path("data_cache")
CACHE_DIR.mkdir(exist_ok=True)

def fetch_symbol_data(symbol: str, output_size: str="compact") -> pd.DataFrame:
    #Fetches daily adjusted OHLCV for 'symbol' from Alpha Vantage.
    #Caches to data_cache/(symbol).csv to avoid repeated requests.
    
    csv_path = CACHE_DIR/ f"(symbol).csv"
    if csv_path.exists():
        # Load from cache
        df = pd.read_csv(csv_path, index_col="date", parse_dates=True)
        return df
    
    #otherwise pull fresh data
    url = "https://www.alphavantage.co/query"
    params = { "function": "TIME_SERIES_DAILY",
               "symbol": symbol,
               "outputsize": output_size,
               "apikey": API_KEY,
               "datatype": "json"}
    
    r = requests.get(url, params=params)
    r.raise_for_status()
    data=r.json()
    ts = data.get("Time Series (Daily)")
    if ts is None:
        raise RuntimeError(f"Error fetching {symbol}: {data.get('Note') or data}")
    
    #convert to DataFrame
    df = pd.DataFrame.from_dict(ts, orient="index")
    df.index.name = "date"
    #rename columns to more Pythonic names and convert to numeric
    df = df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    })
    df = df.astype(float)
    
    #Save cache
    df.to_csv(csv_path)
    #sleep to respect API rate limits
    time.sleep(12) #free tier allows 5 calls per minute
    
    return df

def load_data(symbols: list[str], output_size: str="compact") -> dict[str, pd.DataFrame]:
    #Fetches/loads cached data for a list of symbols.
    #Returns a dict mapping symbol -> Dataframe.
    
    data={}
    for sym in symbols:
        print(f"Loading data for {sym}...")
        data[sym]=fetch_symbol_data(sym, output_size=output_size)
    return data

#Example usage
    """if __name__ == "__main__":
    symbols = ["AAPL", "MSFT", "SPY"]
    dfs = load_data(symbols, output_size="full")
    for s, df in dfs.items():
        print(f"{s}:", df.shape)
    """