from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
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
    
    # Use custom_date param if provided, otherwise use execution_date
    custom_date = context["params"].get("custom_date")
    execution_date = custom_date if custom_date else context["ds"]
    
    print(f"Processing data for date: {execution_date}")

    output_dir = f"/opt/airflow/data/landing/fact_date={execution_date}"
    os.makedirs(output_dir, exist_ok=True)

    output_path = f"{output_dir}/stocks.json"

    # -------- Idempotency guard -------- #
    if os.path.exists(output_path):
        print(f"RAW file already exists for {execution_date}. Skipping fetch.")
        return execution_date  # Return the date for downstream tasks

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
    return execution_date  # Return the date for downstream tasks


# ---------------- DAG DEFINITION ---------------- #

with DAG(
    dag_id="stock_daily_ingest",
    description="EOD stock ingestion from Alpha Vantage (RAW layer)",
    default_args=default_args,
    start_date=datetime(2024, 1, 21),
    schedule="0 3 * * 1-5",  # Runs Monday-Friday at 3 AM
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "ingestion", "raw"],
    params={
        "custom_date": Param(
            default=None,
            type=["null", "string"],
            description="Custom date for manual runs (format: YYYY-MM-DD). Leave empty for scheduled runs.",
            title="Custom Date (Optional)"
        )
    },
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_alpha_vantage",
        python_callable=fetch_stock_data,
        provide_context=True,
    )

    spark_etl = BashOperator(
        task_id="spark_process",
        bash_command="""
        CUSTOM_DATE="{{ params.custom_date }}"
        EXEC_DATE="{{ ds }}"
        
        # Handle None/null values from Airflow params
        if [ "$CUSTOM_DATE" = "None" ] || [ -z "$CUSTOM_DATE" ]; then
            DATE_TO_USE="$EXEC_DATE"
        else
            DATE_TO_USE="$CUSTOM_DATE"
        fi
        
        echo "Using date: $DATE_TO_USE"
        
        docker exec spark-local /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-avro_2.12:3.5.0 \
        --class MultiFormat \
        /opt/spark-jars/poc_2.12-0.1.0.jar $DATE_TO_USE
        """,
        execution_timeout=timedelta(minutes=30),
    )

    fetch_task >> spark_etl
