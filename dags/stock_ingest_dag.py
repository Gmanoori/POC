from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json
import os


# ---------------- DAG DEFAULTS ---------------- #

default_args = {
    "owner": "yourname",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


# ---------------- INGESTION TASK ---------------- #

def fetch_stock_data(**context):
    import time

    api_key = "453D2VL695ZX6N5C"
    symbols = ["AAPL", "TSLA"]
    execution_date = context["ds"]

    output_dir = "/opt/airflow/data/landing"
    os.makedirs(output_dir, exist_ok=True)

    output_path = f"{output_dir}/{execution_date}_stocks.json"

    # -------- Idempotency guard -------- #
    if os.path.exists(output_path):
        print(f"RAW file already exists for {execution_date}. Skipping fetch.")
        return

    final_data = {}

    for symbol in symbols:
        url = (
            "https://www.alphavantage.co/query"
            f"?function=TIME_SERIES_DAILY"
            f"&symbol={symbol}"
            f"&apikey={api_key}"
        )

        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"API request failed for {symbol}: {e}")

        raw_data = resp.json()
        daily_data = raw_data.get("Time Series (Daily)", {})

        if not daily_data:
            print(
                f"No Time Series data for {symbol}. "
                f"Keys available: {raw_data.keys()}"
            )
            continue

        # Alpha Vantage returns most recent date first
        latest_date = list(daily_data.keys())[0]
        final_data[symbol] = daily_data[latest_date]

        # Respect free-tier rate limits
        time.sleep(1)

    if not final_data:
        raise ValueError("No valid stock data fetched. Aborting ingestion.")

    with open(output_path, "w") as f:
        json.dump(final_data, f)

    print(f"RAW stock data written to {output_path}")


# ---------------- DAG DEFINITION ---------------- #

with DAG(
    dag_id="stock_daily_ingest",
    description="EOD stock ingestion from Alpha Vantage (RAW layer)",
    default_args=default_args,
    start_date=datetime(2024, 1, 21),
    schedule="0 3 * * 1-5",  
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "ingestion", "raw"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_alpha_vantage",
        python_callable=fetch_stock_data,
        provide_context=True,
    )

    spark_etl = BashOperator(
        task_id="spark_process",
        bash_command="""
        docker exec spark-local /opt/spark/bin/spark-submit \
        --class MultiFormat \
        /opt/spark-jars/poc_2.12-0.1.0.jar {{ ds }}
        """,
        execution_timeout=timedelta(minutes=30),
    )

    fetch_task >> spark_etl
