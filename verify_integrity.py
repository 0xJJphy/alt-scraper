import pandas as pd
import glob
import os

files = glob.glob("data/spot/*/*.csv")
print(f"Found {len(files)} files.")

for f in files:
    df = pd.read_csv(f)
    print(f"\n--- File: {f} ---")
    print(f"Columns: {df.columns.tolist()}")
    
    # Check for metrics
    metrics = ['buy_volume_base', 'sell_volume_base', 'txn_count', 'buy_txn_count', 'sell_txn_count']
    present = [m for m in metrics if m in df.columns]
    print(f"Metrics present: {present}")
    
    if len(present) > 0:
        # Check non-null count for last 5 rows
        last_5 = df.tail(5)
        print("Last 5 rows non-null counts:")
        print(last_5[present].notna().sum())
        
        # Check calculation consistency for the last row
        last_row = df.iloc[-1]
        if 'txn_count' in last_row and 'buy_txn_count' in last_row and 'sell_txn_count' in last_row:
            if not pd.isna(last_row['txn_count']) and not pd.isna(last_row['buy_txn_count']):
                diff = last_row['txn_count'] - last_row['buy_txn_count']
                print(f"Consistency Check (txn): {last_row['txn_count']} - {last_row['buy_txn_count']} = {diff} (Stored: {last_row['sell_txn_count']})")
