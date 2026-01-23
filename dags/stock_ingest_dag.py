from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json
# import pendulum  # For timezone-aware scheduling

default_args = {
    'owner': 'yourname',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def fetch_stock_data(**context):
    api_key = 'MDTU6XUYX06CRQKN'
    symbols = ['AAPL', 'TSLA']  # Configurable
    execution_date = context['ds']  # Airflow date
    data = {}
    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
        resp = requests.get(url).json()
        print(resp)  # Debugging line to inspect the response
        daily_data = resp.get('Time Series (Daily)', {})
        # Save full time series with dates intact
        first_date = daily_data.keys()[0]
        print(first_date)
        first_record = daily_data[first_date]
        print(first_record)
        data = data | {symbol: first_record}  # Append only the most recent day's data
        # data[symbol] = daily_data
    # Save to landing zone
    with open(f'/opt/airflow/data/landing/{execution_date}_stocks.json', 'w') as f:
        json.dump(data, f)

    # Multiple Format Changes. TODO: Implement later
    #format_type = 'avro'  # Or dynamic Variable.get('stock_format')
    # if format_type == 'json':
    #     with open(f'/landing/{execution_date}_stocks.json', 'w') as f: json.dump(data, f)
    # elif format_type == 'parquet':
    #     import pandas as pd; pd.DataFrame(data).to_parquet(f'/landing/{execution_date}_stocks.parquet')
    # Avro needs pyavro or fastavro lib


dag = DAG(
    'stock_daily_ingest',
    default_args=default_args,
    schedule='1 0 * * *',  # 4 PM ET weekdays (post-close)
    start_date=datetime(2026, 1, 21),
    catchup=False
)

fetch_task = PythonOperator(
    task_id='fetch_alpha_vantage',
    python_callable=fetch_stock_data,
    dag=dag
)

spark_etl = BashOperator(
    task_id='spark_process',
    bash_command='spark-submit --class MultiFormat /path/to/your-poc/target/scala-2.12/poc_2.12-0.1.0.jar {{ ds }}',
    dag=dag
)

fetch_task >> spark_etl
