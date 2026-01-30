import yfinance as yf
import pandas as pd
import os
from pathlib import Path

def extract_stock_data(ticker, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    stock_data = yf.download(ticker, period="5y", group_by='ticker')
    clean_name = ticker.replace("^", "").replace("-", "_")
    if not Path(f"{clean_name}_data.csv").exists():
        output_path = os.path.join(output_dir, f"{clean_name}_data.csv")
        stock_data.to_csv(output_path)
    