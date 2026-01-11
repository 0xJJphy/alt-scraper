import requests
import json

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def test_cg_markets():
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": "false",
    }
    
    print("Fetching Top 50 from CoinGecko Markets...")
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code} - {resp.text}")
        return

    data = resp.json()
    symbols_to_check = ['btc', 'eth', 'usdt', 'xrp', 'bnb', 'shib', 'near']
    
    found = {}
    for coin in data:
        sym = coin.get('symbol', '').lower()
        if sym in symbols_to_check:
            found[sym] = {
                "id": coin.get("id"),
                "market_cap": coin.get("market_cap"),
                "market_cap_rank": coin.get("market_cap_rank")
            }
            
    print(json.dumps(found, indent=2))

if __name__ == "__main__":
    test_cg_markets()
