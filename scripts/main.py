import extract as ex
import transform as tr
import pandas as pd

tickers = ["^FCHI", "^GSPC", "TSLA", "AAPL", "BTC-USD"]

if __name__ == "__main__":
    for ticker in tickers:
        ex.extract_stock_data(ticker=ticker, output_dir="./data/raw")

        clean_name = ticker.replace('^','').replace('-','_')
        raw_p = f"./data/raw/{clean_name}_data.csv"
        proc_p = f"./data/processed/{clean_name}_clean.csv"
        
        tr.preprocess_data(ticker, raw_p)
        tr.daily_yield(ticker, raw_p, proc_p)
        tr.moving_average(ticker, raw_p, proc_p)
        tr.volatility(ticker, raw_p, proc_p)
        tr.drawdown(ticker, raw_p, proc_p)

