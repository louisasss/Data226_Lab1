from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import yfinance as yf
import pandas as pd
import os


def get_snowflake_cursor():
    """Helper: open Snowflake connection."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


# ------------------- EXTRACT -------------------
@task
def extract(symbol1: str, symbol2: str) -> str:
    """
    Download 180 days of stock data for 2 symbols.
    Save to CSV and return file path.
    """
    df = yf.download([symbol1, symbol2], period="180d")
    file_path = "/tmp/raw_stock_data.csv"
    df.to_csv(file_path)
    return file_path


# ------------------- TRANSFORM -------------------
@task
def transform(file_path: str) -> str:
    """
    Reshape and clean data so it matches Snowflake table schema.
    Save to CSV and return new file path.
    """
    df = pd.read_csv(file_path)

    # yfinance with multiple tickers → MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df = df.stack(level=1).reset_index()
        df.columns = [
            "date", "ticker", "open", "high_price",
            "low_price", "close", "adj_close", "volume"
        ]
    else:
        # single ticker case
        df.reset_index(inplace=True)
        df.rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high_price",
                "Low": "low_price",
                "Close": "close",
                "Volume": "volume",
            },
            inplace=True,
        )
        df["ticker"] = "UNKNOWN"

    # keep only required columns
    df = df[["ticker", "date", "open", "close", "low_price", "high_price", "volume"]]

    # ensure date is string (Snowflake DATE can parse it)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    out_path = "/tmp/clean_stock_data.csv"
    df.to_csv(out_path, index=False)
    return out_path


# ------------------- LOAD -------------------
@task
def load(file_path: str, target_table: str):
    """
    Full refresh load into Snowflake.
    """
    df = pd.read_csv(file_path)
    cur = get_snowflake_cursor()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR,
                date DATE,
                open FLOAT,
                close FLOAT,
                low_price FLOAT,
                high_price FLOAT,
                volume INT,
                PRIMARY KEY(symbol, date)
            )
        """)
        cur.execute(f"DELETE FROM {target_table}")

        for _, row in df.iterrows():
            cur.execute(
                f"""
                INSERT INTO {target_table} 
                (symbol, date, open, close, low_price, high_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["ticker"], row["date"], row["open"], row["close"],
                    row["low_price"], row["high_price"], row["volume"]
                ),
            )
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


# ------------------- DAG -------------------
with DAG(
    dag_id="Stock_ETL_Snowflake",
    start_date=datetime(2024, 10, 3),
    catchup=False,
    schedule_interval="0 0 * * *",  # daily at midnight
    tags=["ETL", "stocks", "snowflake"],
) as dag:

    target_table = "raw.lab1_market_data"
    symbol1 = "AVGO"
    symbol2 = "NVDA"

    raw_file = extract(symbol1, symbol2)
    clean_file = transform(raw_file)
    load(clean_file, target_table)
