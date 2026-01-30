import pandas as pd
import numpy as np
import os 

def preprocess_data(ticker, data):
    df_raw = pd.read_csv(data, header=[0, 1], index_col=0)
    df_raw.index = pd.to_datetime(df_raw.index)
    
    df_processed = pd.DataFrame(index=df_raw.index)

    output_dir = "./data/processed"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{ticker.replace('^','').replace('-','_')}_clean.csv")
    df_processed.to_csv(output_path)

def daily_yield(ticker, raw_path, processed_path):
    df_raw = pd.read_csv(raw_path, header=[0, 1], index_col=0)
    df_proc = pd.read_csv(processed_path, index_col=0)

    df_proc['Daily_Yield'] = df_raw[(ticker, 'Close')].pct_change()
    df_proc.to_csv(processed_path)

def moving_average(ticker, raw_path, processed_path, window=20):
    df_raw = pd.read_csv(raw_path, header=[0, 1], index_col=0)
    df_proc = pd.read_csv(processed_path, index_col=0)

    df_proc['Moving_Average'] = df_raw[(ticker, 'Close')].rolling(window).mean()
    df_proc.to_csv(processed_path)

def volatility(ticker, raw_path, processed_path, window=20):
    df_raw = pd.read_csv(raw_path, header=[0, 1], index_col=0)
    df_proc = pd.read_csv(processed_path, index_col=0)
    
    day = 365 if 'BTC' in ticker else 252
    df_proc['Volatility'] = df_raw[(ticker, 'Close')].pct_change().rolling(window).std()*(day**0.5)
    df_proc.to_csv(processed_path)

def drawdown(ticker, raw_path, processed_path):
    df_raw = pd.read_csv(raw_path, header=[0, 1], index_col=0)
    df_proc = pd.read_csv(processed_path, index_col=0)
    
    cummax = df_raw[(ticker, 'Close')].cummax()
    df_proc['Drawdown'] = (df_raw[(ticker, 'Close')] - cummax) / cummax
    df_proc.to_csv(processed_path)