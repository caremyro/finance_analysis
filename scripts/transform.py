import pandas as pd
import numpy as np
import os 

def preprocess_data(ticker, data):
    df_raw = pd.read_csv(data, header=[0, 1], index_col=0)
    df_raw.index = pd.to_datetime(df_raw.index)
    
    df_processed = pd.DataFrame(index=df_raw.index)
    return df_processed

def daily_yield(df_proc, df_raw, ticker):
    df_proc['Daily_Yield'] = df_raw[(ticker, 'Close')].pct_change()
    return df_proc

def moving_average(df_proc, df_raw, ticker, window=20):
    df_proc['Moving_Average'] = df_raw[(ticker, 'Close')].rolling(window).mean()
    return df_proc

def volatility(df_proc, df_raw, ticker, window=20):
    day = 365 if 'BTC' in ticker else 252
    returns = df_raw[(ticker, 'Close')].pct_change()
    df_proc['Volatility'] = returns.rolling(window).std() * (day**0.5)
    return df_proc

def drawdown(df_proc, df_raw, ticker):
    close = df_raw[(ticker, 'Close')]
    cummax = close.cummax()
    df_proc['Drawdown'] = (close - cummax) / cummax
    return df_proc

def run_pipeline(ticker, raw_path, processed_path):
    df_raw = pd.read_csv(raw_path, header=[0, 1], index_col=0)
    df_raw.index = pd.to_datetime(df_raw.index)
    df_proc = preprocess_data(ticker, raw_path)
    df_proc = daily_yield(df_proc, df_raw, ticker)
    df_proc = moving_average(df_proc, df_raw, ticker)
    df_proc = volatility(df_proc, df_raw, ticker)
    df_proc = drawdown(df_proc, df_raw, ticker)

    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    df_proc.to_csv(processed_path)