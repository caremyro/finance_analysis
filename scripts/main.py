import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import extract as ex
import transform as tr
import pandas as pd
from db import database as db
import load as ld

tickers = ["^FCHI", "^GSPC", "TSLA", "AAPL", "BTC-USD"]

if __name__ == "__main__":
    for ticker in tickers:
        ex.extract_stock_data(ticker=ticker, output_dir="./data/raw")

        clean_name = ticker.replace('^','').replace('-','_')
        raw_p = f"./data/raw/{clean_name}_data.csv"
        proc_p = f"./data/processed/{clean_name}_clean.csv"
        
        tr.run_pipeline(ticker, raw_p, proc_p)

        print(f"Processing data for {ticker}...")
        ld.load_stocks_table(cursor=cursor, conn=conn, ticker=ticker, csv_path=raw_p)
        ld.load_indicators_table(cursor=cursor, conn=conn, ticker=ticker, csv_path=proc_p)
    conn.close()
    print("Process completed.")

