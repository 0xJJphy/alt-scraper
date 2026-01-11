import os
import requests
import time
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("COINALYZE_API_KEY")
headers = {"api-key": api_key}
url = "https://api.coinalyze.net/v1/ohlcv-history"

test_symbols = ["ETHUSDT.A", "SOLUSDT.A", "BTCUSDC.A", "ETHUSDC.A", "BTCUSDT.3", "ETHUSDT.3"]
for sym in test_symbols:
    now_ts = int(time.time())
    start_ts = now_ts - (5 * 86400)
    params = {"symbols": sym, "interval": "daily", "from": start_ts, "to": now_ts}
    resp = requests.get(url, params=params, headers=headers)
    if resp.status_code == 200 and resp.json() and resp.json()[0].get('history'):
        print(f"SYMBOL {sym} WORKS!")
    else:
        print(f"SYMBOL {sym} FAILED")
