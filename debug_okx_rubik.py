import requests
import pandas as pd
from datetime import datetime, timezone

BASE_URL = "https://www.okx.com/api/v5"
symbol = "BTC"

params = {"ccy": symbol, "period": "1D", "instType": "SPOT"}
resp = requests.get(f"{BASE_URL}/rubik/stat/taker-volume", params=params).json()
print(f"Rubik Response Code: {resp.get('code')}")
data = resp.get("data", [])
print(f"Data length: {len(data)}")
if data:
    print("First row:", data[0])
    df = pd.DataFrame(data, columns=['timestamp', 'buy_volume_base', 'sell_volume_base'])
    print("DF head:\n", df.head())
else:
    print("No data in Rubik response")
