import pandas as pd
import os

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.execute(f"USE {db_name}")

def drop_database(cursor, db_name):
    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")

def create_tables(cursor, table_creation_queries):
    for query in table_creation_queries:
        cursor.execute(query)

def drop_tables(cursor, table_names):
    for table_name in table_names:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def reset_database(cursor, db_name, table_creation_queries, table_names):
    drop_database(cursor, db_name)
    create_database(cursor, db_name)
    create_tables(cursor, table_creation_queries)

def clear_tables(cursor, table_names):
    for table_name in table_names:
        cursor.execute(f"DELETE FROM {table_name}")

def load_tickers_table(cursor, tickers):
    insert_query = "INSERT IGNORE INTO tickers (ticker_id, name, sector, currency, stock_exchange) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(insert_query, tickers)

def load_stocks_table(cursor, conn, ticker, csv_path):
    df = pd.read_csv(csv_path, header=[0, 1], index_col=0)
    
    for date, row in df.iterrows():
        sql = """
            INSERT IGNORE INTO stocks (date, ticker_id, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = (
            str(date), 
            ticker, 
            row[(ticker, 'Open')], 
            row[(ticker, 'High')], 
            row[(ticker, 'Low')], 
            row[(ticker, 'Close')], 
            int(row[(ticker, 'Volume')])
        )
        cursor.execute(sql, data)
    conn.commit()

def load_indicators_table(cursor, conn, ticker, csv_path):
    df = pd.read_csv(csv_path, index_col=0)
    df = df.replace({pd.NA: None, float('nan'): None})
    df = df.where(pd.notnull(df), None)
    
    for date, row in df.iterrows():
        sql = """
            INSERT IGNORE INTO indicators (date, ticker_id, daily_yield, moving_average, volatility, drawdown)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        def clean_val(val):
            try:
                if val is None or pd.isna(val): return None
                return float(val)
            except:
                return val

        data = (
            str(date),
            ticker,
            clean_val(row['Daily_Yield']),
            clean_val(row['Moving_Average']),
            clean_val(row['Volatility']),
            clean_val(row['Drawdown'])
        )
        cursor.execute(sql, data)
    conn.commit()