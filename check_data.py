import os
import pandas as pd
import glob

def check_data(base_path="data/coinalyze"):
    print("="*80)
    print("FINAL DATA VALIDATION REPORT")
    print("="*80)
    
    csv_files = glob.glob(os.path.join(base_path, "**/*.csv"), recursive=True)
    
    if not csv_files:
        print(f"No CSV files found in {base_path}")
        return

    summary = []
    
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            file_name = os.path.basename(file_path)
            # Example filename: BTCUSDT_PERP.A_1d_metrics.csv
            symbol_part = file_name.replace("_1d_metrics.csv", "")
            
            # Extract exchange from path
            path_parts = os.path.normpath(file_path).split(os.sep)
            exchange = path_parts[-2] if len(path_parts) >= 2 else "unknown"
            
            rows = len(df)
            nan_counts = df.isnull().sum()
            total_cells = df.size
            total_nans = nan_counts.sum()
            nan_percentage = (total_nans / total_cells) * 100 if total_cells > 0 else 0
            
            # Key metrics to check for NaNs
            key_metrics = [
                'oi_usd_close', 'funding_close', 'ls_ratio', 
                'liq_total', 'price_close', 'volume_delta'
            ]
            
            missing_metrics = [m for m in key_metrics if m in df.columns and df[m].isnull().any()]
            
            summary.append({
                "Symbol": symbol_part,
                "Exchange": exchange,
                "Rows": rows,
                "NaN %": f"{nan_percentage:.2f}%",
                "Missing Metrics": ", ".join(missing_metrics) if missing_metrics else "None"
            })
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    summary_df = pd.DataFrame(summary)
    if not summary_df.empty:
        # Group by symbol to see exchange coverage
        coverage = summary_df.groupby("Symbol")["Exchange"].apply(list).reset_index()
        coverage = coverage.rename(columns={"Exchange": "Fetched Exchanges"})
        
        print("\nSYMBOL COVERAGE:")
        print(coverage.to_string(index=False))
        
        print("\nDATA INTEGRITY SUMMARY:")
        print(summary_df[["Symbol", "Exchange", "Rows", "NaN %", "Missing Metrics"]].to_string(index=False))
    else:
        print("No data available to summarize.")

    print("\n" + "="*80)

if __name__ == "__main__":
    check_data()
