import requests
url = "https://api.binance.com/api/v3/ticker/24hr"
symbols = ["BTCUSDT", "BTCFDUSD", "BTCUSDC"]
r = requests.get(url, params={"symbols": '["BTCUSDT","BTCFDUSD","BTCUSDC"]'})
data = r.json()
print("-" * 50)
print(f"{'Symbol':<10} | {'Quote Volume (USD)':<20} | {'Trade Count':<10}")
print("-" * 50)
for item in data:
    sym = item['symbol']
    vol = float(item['quoteVolume'])
    count = int(item['count'])
    print(f"{sym:<10} | {vol:>20,.2f} | {count:>10,}")
print("-" * 50)
