import requests
import pandas as pd
import os

def test_binance():
    print("\n--- Testing Binance ---")
    # Try different potential paths
    paths = [
        "/futures/data/globalLongShortAccountRatio",
        "/fapi/v1/data/globalLongShortAccountRatio"
    ]
    base = "https://fapi.binance.com"
    for path in paths:
        url = base + path
        params = {"symbol": "BTCUSDT", "period": "1d", "limit": 5}
        try:
            resp = requests.get(url, params=params)
            print(f"Path {path} -> Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                print("First item keys:", data[0].keys() if data else "Empty")
                print("First item:", data[0] if data else "Empty")
        except Exception as e:
            print(f"Error {path}: {e}")

def test_okx():
    print("\n--- Testing OKX ---")
    base = "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio"
    # Try different params
    param_sets = [
        {"instId": "BTC-USDT-SWAP", "period": "1D"},
        {"instId": "BTC-USDT-SWAP", "period": "1d"},
        {"ccy": "BTC", "period": "1D"}
    ]
    for params in param_sets:
        try:
            resp = requests.get(base, params=params)
            print(f"Params {params} -> Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == "0":
                    print("Data length:", len(data.get("data", [])))
                    if data.get("data"):
                        print("First item:", data["data"][0])
                else:
                    print(f"Error Message: {data.get('msg')}")
        except Exception as e:
            print(f"Error {params}: {e}")

if __name__ == "__main__":
    test_binance()
    test_okx()
