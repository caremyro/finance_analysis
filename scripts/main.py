import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import extract as ex
import transform as tr
import pandas as pd
from db import database as db
import load as ld

tickers = ["^FCHI", "^GSPC", "TSLA", "AAPL", "BTC-USD"]
tickers_metadata = [
    ('AAPL', 'Apple Inc.', 'Technology', 'USD', 'NASDAQ'),
    ('TSLA', 'Tesla, Inc.', 'Consumer Cyclical', 'USD', 'NASDAQ'),
    ('BTC-USD', 'Bitcoin USD', 'Crypto', 'USD', 'Coinbase'),
    ('^FCHI', 'CAC 40', 'Index', 'EUR', 'Euronext Paris'),
    ('^GSPC', 'S&P 500', 'Index', 'USD', 'NYSE')
    ]

if __name__ == "__main__":
    conn = db.get_db_connection()
    cursor = db.get_db_cursor(conn)

    print(f"Creating database and tables...")
    ld.create_database(cursor=cursor, db_name="finance_data")
    ld.create_tables(cursor=cursor, table_creation_queries=[
        """
        CREATE TABLE IF NOT EXISTS tickers (
            ticker_id VARCHAR(20) PRIMARY KEY,
            name VARCHAR(255),
            sector VARCHAR(100),
            currency VARCHAR(10),
            stock_exchange VARCHAR(50)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS stocks (
            date DATETIME,
            ticker_id VARCHAR(20),
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume BIGINT,
            PRIMARY KEY (date, ticker_id),
            FOREIGN KEY (ticker_id) REFERENCES tickers (ticker_id) ON DELETE CASCADE
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS indicators (
            date DATETIME,
            ticker_id VARCHAR(20),
            daily_yield REAL,
            moving_average REAL,
            volatility REAL,
            drawdown REAL,
            PRIMARY KEY (date, ticker_id),
            FOREIGN KEY (ticker_id) REFERENCES tickers (ticker_id) ON DELETE CASCADE
        ) ENGINE=InnoDB;
        """
    ])

    print(f"Loading data...")
    ld.load_tickers_table(cursor=cursor, tickers=tickers_metadata)
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

