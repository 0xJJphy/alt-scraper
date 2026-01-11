import requests
import pandas as pd

def test_binance(symbol="BTCUSDT"):
    print(f"\n--- Testing Binance {symbol} ---")
    base = "https://fapi.binance.com"
    # Funding
    f_resp = requests.get(f"{base}/fapi/v1/premiumIndex", params={"symbol": symbol})
    # OI
    o_resp = requests.get(f"{base}/fapi/v1/openInterest", params={"symbol": symbol})
    
    if f_resp.status_code == 200:
        print("Funding:", f_resp.json().get("lastFundingRate"))
    if o_resp.status_code == 200:
        print("OI:", o_resp.json().get("openInterest"))

def test_bybit(symbol="BTCUSDT"):
    print(f"\n--- Testing Bybit {symbol} ---")
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "linear", "symbol": symbol}
    resp = requests.get(url, params=params).json()
    if resp.get("retCode") == 0:
        res = resp.get("result", {}).get("list", [])[0]
        print("Funding:", res.get("fundingRate"))
        print("Next Funding:", res.get("nextFundingRate"))
        print("OI:", res.get("openInterest"))

def test_okx(symbol="BTC-USDT-SWAP"):
    print(f"\n--- Testing OKX {symbol} ---")
    base = "https://www.okx.com/api/v5"
    # Funding
    f_resp = requests.get(f"{base}/public/funding-rate", params={"instId": symbol}).json()
    # OI
    o_resp = requests.get(f"{base}/public/open-interest", params={"instId": symbol}).json()
    
    if f_resp.get("code") == "0":
        print("Funding:", f_resp["data"][0].get("fundingRate"))
    if o_resp.get("code") == "0":
        print("OI:", o_resp["data"][0].get("oi"))

if __name__ == "__main__":
    test_binance()
    test_bybit()
    test_okx()
