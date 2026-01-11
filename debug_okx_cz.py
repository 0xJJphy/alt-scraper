import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("COINALYZE_API_KEY")
headers = {"api-key": api_key}
url = "https://api.coinalyze.net/v1/ohlcv-history"

# OKX BTCUSDT.3
now_ts = int(time.time())
start_ts = now_ts - (5 * 86400)
    
params = {
    "symbols": "BTCUSDT.3",
    "interval": "daily",
    "from": start_ts,
    "to": now_ts
}
resp = requests.get(url, params=params, headers=headers)
print(f"Status: {resp.status_code}")
data = resp.json()
if data and data[0].get('history'):
    print("Columns:", data[0]['history'][0].keys())
    print("First row:", data[0]['history'][0])
else:
    print("No data found for BTCUSDT.3")
