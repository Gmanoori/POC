from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import json
import os
import time


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_PATH = "/opt/airflow/config/polygon_config.json"

def load_config():
    """Load configuration from external JSON file"""
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)

CONFIG = load_config()
POLYGON_API_KEY = CONFIG["api_key"]
SYMBOLS = CONFIG["symbols"]
RATE_LIMIT_DELAY = CONFIG["rate_limit_delay"]

RAW_BASE_PATH = "/opt/airflow/data/landing/polygon"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def api_call_with_retry(url: str, params: dict, max_retries: int = 3, timeout: int = 30) -> requests.Response:
    """
    Make API call with exponential backoff retry logic.
    
    Args:
        url: API endpoint URL
        params: Query parameters
        max_retries: Maximum number of retry attempts
        timeout: Request timeout in seconds
    
    Returns:
        Response object if successful
    
    Raises:
        Exception: If all retries fail
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            return response  # Success - return immediately
        
        except requests.exceptions.Timeout as e:
            if attempt < max_retries - 1:  # Not the last attempt
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"  Timeout on attempt {attempt + 1}/{max_retries}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:  # Last attempt failed
                raise Exception(f"Timeout after {max_retries} attempts: {str(e)}")
        
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"  Request failed on attempt {attempt + 1}/{max_retries}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Request failed after {max_retries} attempts: {str(e)}")


def infer_market_timezone(symbol: str) -> str:
    """Infer market timezone from symbol suffix"""
    if symbol.endswith(".NS") or symbol.endswith(".BO"):
        return "Asia/Kolkata"
    else:
        return "US/Eastern"


def check_raw_file_exists(symbol: str, endpoint_type: str, execution_date: str) -> bool:
    """Check if RAW file already exists (idempotency)"""
    path = f"{RAW_BASE_PATH}/{endpoint_type}/ingestion_date={execution_date}/{symbol}.json"
    return os.path.exists(path)


def write_raw_envelope(symbol: str, endpoint_type: str, execution_date: str, payload: dict):
    """Write API response to RAW layer with envelope pattern"""
    dir_path = f"{RAW_BASE_PATH}/{endpoint_type}/ingestion_date={execution_date}"
    os.makedirs(dir_path, exist_ok=True)
    
    envelope = {
        "symbol": symbol,
        "source": "polygon",
        "endpoint_type": endpoint_type,
        "ingestion_timestamp": datetime.utcnow().isoformat() + "Z",
        "execution_date": execution_date,
        "market_timezone": infer_market_timezone(symbol),
        "payload": payload
    }
    
    file_path = f"{dir_path}/{symbol}.json"
    with open(file_path, 'w') as f:
        json.dump(envelope, f, indent=2)
    
    print(f"✓ Written: {file_path}")


# ============================================================================
# TASK 1: POLYGON API INGESTION
# ============================================================================

def ingest_polygon_data(**context):
    """
    Ingest OHLCV and Dividend data from Polygon API
    
    Returns:
        dict: {
            "successful_symbols": [...],
            "failed_symbols": [{"symbol": ..., "endpoint": ..., "error": ...}],
            "execution_date": "YYYY-MM-DD"
        }
    """
    # Get execution date (custom or scheduled)
    custom_date = context["params"].get("custom_date")
    execution_date = custom_date if custom_date else context["ds"]
    
    print(f"\n{'='*70}")
    print(f"POLYGON INGESTION - Execution Date: {execution_date}")
    print(f"{'='*70}\n")
    
    successful_symbols = []
    failed_symbols = []
    
    for symbol in SYMBOLS:
        print(f"\n--- Processing: {symbol} ---")
        
        # ====================================================================
        # OHLCV INGESTION
        # ====================================================================
        
        ohlcv_exists = check_raw_file_exists(symbol, "daily_ohlcv", execution_date)
        
        if ohlcv_exists:
            print(f"  OHLCV: Already exists, skipping")
        else:
            print(f"  OHLCV: Fetching from Polygon...")
            
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{execution_date}/{execution_date}"
            params = {
                "adjusted": "true",
                "apiKey": POLYGON_API_KEY
            }
            
            try:
                response = api_call_with_retry(url, params, max_retries=3, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check if results exist
                    if data.get("resultsCount", 0) > 0:
                        write_raw_envelope(symbol, "daily_ohlcv", execution_date, data)
                        print(f"  OHLCV: ✓ Success")
                    else:
                        error_msg = f"No data returned (possibly non-trading day or future date)"
                        print(f"  OHLCV: ⚠ {error_msg}")
                        failed_symbols.append({
                            "symbol": symbol,
                            "endpoint": "ohlcv",
                            "error": error_msg
                        })
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                    print(f"  OHLCV: ✗ Failed - {error_msg}")
                    failed_symbols.append({
                        "symbol": symbol,
                        "endpoint": "ohlcv",
                        "error": error_msg
                    })
            
            except Exception as e:
                error_msg = f"Exception: {str(e)}"
                print(f"  OHLCV: ✗ Failed - {error_msg}")
                failed_symbols.append({
                    "symbol": symbol,
                    "endpoint": "ohlcv",
                    "error": error_msg
                })
            
            # Rate limiting
            print(f"  Waiting {RATE_LIMIT_DELAY}s (rate limit)...")
            time.sleep(RATE_LIMIT_DELAY)
        
        # ====================================================================
        # SPLITS INGESTION
        # ====================================================================
        
        splits_exists = check_raw_file_exists(symbol, "splits", execution_date)
        
        if splits_exists:
            print(f"  Splits: Already exists, skipping")
        else:
            print(f"  Splits: Fetching from Polygon...")
            
            url = "https://api.polygon.io/v3/reference/splits"
            params = {
                "ticker": symbol,
                "apiKey": POLYGON_API_KEY
            }
            
            try:
                response = api_call_with_retry(url, params, max_retries=3, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Always write (even if empty) for tracking
                    write_raw_envelope(symbol, "splits", execution_date, data)
                    
                    result_count = len(data.get("results", []))
                    print(f"  Splits: ✓ Success ({result_count} records)")
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                    print(f"  Splits: ✗ Failed - {error_msg}")
                    failed_symbols.append({
                        "symbol": symbol,
                        "endpoint": "splits",
                        "error": error_msg
                    })
            
            except Exception as e:
                error_msg = f"Exception: {str(e)}"
                print(f"  Splits: ✗ Failed - {error_msg}")
                failed_symbols.append({
                    "symbol": symbol,
                    "endpoint": "splits",
                    "error": error_msg
                })
            
            # Rate limiting
            print(f"  Waiting {RATE_LIMIT_DELAY}s (rate limit)...")
            time.sleep(RATE_LIMIT_DELAY)
        
        # ====================================================================
        # DIVIDEND INGESTION
        # ====================================================================
        
        dividend_exists = check_raw_file_exists(symbol, "dividends", execution_date)
        
        if dividend_exists:
            print(f"  Dividends: Already exists, skipping")
        else:
            print(f"  Dividends: Fetching from Polygon...")
            
            url = "https://api.polygon.io/v3/reference/dividends"
            params = {
                "ticker": symbol,
                "apiKey": POLYGON_API_KEY
            }
            
            try:
                response = api_call_with_retry(url, params, max_retries=3, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Always write (even if empty) for tracking
                    write_raw_envelope(symbol, "dividends", execution_date, data)
                    
                    result_count = len(data.get("results", []))
                    print(f"  Dividends: ✓ Success ({result_count} records)")
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                    print(f"  Dividends: ✗ Failed - {error_msg}")
                    failed_symbols.append({
                        "symbol": symbol,
                        "endpoint": "dividends",
                        "error": error_msg
                    })
            
            except Exception as e:
                error_msg = f"Exception: {str(e)}"
                print(f"  Dividends: ✗ Failed - {error_msg}")
                failed_symbols.append({
                    "symbol": symbol,
                    "endpoint": "dividends",
                    "error": error_msg
                })
            
            # Rate limiting
            print(f"  Waiting {RATE_LIMIT_DELAY}s (rate limit)...")
            time.sleep(RATE_LIMIT_DELAY)
        
        # Mark symbol as successful if no failures for this symbol
        symbol_failures = [f for f in failed_symbols if f["symbol"] == symbol]
        if not symbol_failures:
            successful_symbols.append(symbol)
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    
    print(f"\n{'='*70}")
    print(f"INGESTION SUMMARY")
    print(f"{'='*70}")
    print(f"Successful symbols: {len(successful_symbols)}/{len(SYMBOLS)}")
    print(f"Failed symbols: {len(set(f['symbol'] for f in failed_symbols))}")
    
    if failed_symbols:
        print(f"\nFailures:")
        for failure in failed_symbols:
            print(f"  - {failure['symbol']} ({failure['endpoint']}): {failure['error'][:100]}")
    
    print(f"{'='*70}\n")
    
    return {
        "successful_symbols": successful_symbols,
        "failed_symbols": failed_symbols,
        "execution_date": execution_date
    }


# ============================================================================
# TASK 2: VALIDATE COMPLETENESS
# ============================================================================

def validate_ingestion_completeness(**context):
    """
    Validate that all symbols were ingested successfully.
    Fails the DAG if any symbol failed, blocking Spark processing.
    """
    ti = context['ti']
    result = ti.xcom_pull(task_ids='ingest_polygon_data')
    
    failed_symbols = result.get('failed_symbols', [])
    execution_date = result.get('execution_date')
    
    print(f"\n{'='*70}")
    print(f"VALIDATION CHECK - {execution_date}")
    print(f"{'='*70}\n")
    
    if not failed_symbols:
        print("✓ All symbols ingested successfully")
        print("✓ Proceeding to Spark curation")
        return True
    else:
        unique_failed = set(f['symbol'] for f in failed_symbols)
        error_msg = f"\n✗ VALIDATION FAILED\n\n"
        error_msg += f"{len(unique_failed)} symbol(s) failed ingestion:\n\n"
        
        for failure in failed_symbols:
            error_msg += f"  Symbol: {failure['symbol']}\n"
            error_msg += f"  Endpoint: {failure['endpoint']}\n"
            error_msg += f"  Error: {failure['error']}\n\n"
        
        error_msg += "Action Required: Fix API issues and re-run DAG.\n"
        error_msg += "Spark processing blocked until all symbols succeed.\n"
        
        raise AirflowException(error_msg)


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    "owner": "intelligence-system",
    "retries": 0,  # No auto-retry - manual intervention required
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="polygon_intelligence_pipeline",
    description="Layered financial intelligence system - Polygon ingestion + curation",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * 1-5",  # 3 AM on weekdays (after market close)
    catchup=False,
    max_active_runs=1,
    tags=["intelligence", "polygon", "raw", "curated"],
    params={
        "custom_date": Param(
            default=None,
            type=["null", "string"],
            description="Custom date for backfill (YYYY-MM-DD). Leave empty for scheduled runs.",
            title="Custom Date (Optional)"
        )
    },
) as dag:

    # Task 1: Ingest from Polygon API
    ingest_task = PythonOperator(
        task_id="ingest_polygon_data",
        python_callable=ingest_polygon_data,
        provide_context=True,
    )

    # Task 2: Validate completeness
    validate_task = PythonOperator(
        task_id="validate_ingestion_completeness",
        python_callable=validate_ingestion_completeness,
        provide_context=True,
    )

    # Task 3: Spark curation job
    spark_curate_task = BashOperator(
        task_id="spark_curate_data",
        bash_command="""
        CUSTOM_DATE="{{ params.custom_date }}"
        EXEC_DATE="{{ ds }}"
        
        # Handle None/null values from Airflow params
        if [ "$CUSTOM_DATE" = "None" ] || [ -z "$CUSTOM_DATE" ]; then
            DATE_TO_USE="$EXEC_DATE"
        else
            DATE_TO_USE="$CUSTOM_DATE"
        fi
        
        echo "Curating data for date: $DATE_TO_USE"
        
        docker exec spark-local /opt/spark/bin/spark-submit \
        --class PolygonCurator \
        --packages org.apache.spark:spark-avro_2.12:3.5.0 \
        /opt/spark-jars/polygon_story_2.12-0.1.0.jar $DATE_TO_USE
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # Task 4: Calculate adjusted prices (corporate actions)
    spark_adjust_task = BashOperator(
        task_id="calculate_adjusted_prices",
        bash_command="""
        echo "Calculating adjusted prices (applying splits & dividends)..."
        
        docker exec spark-local /opt/spark/bin/spark-submit \
        --class CorporateActionsAdjuster \
        --packages org.apache.spark:spark-avro_2.12:3.5.0 \
        /opt/spark-jars/polygon_story_2.12-0.1.0.jar
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # Task 5: Detect market regimes (ADX, ATR, classification)
    spark_regime_task = BashOperator(
        task_id="detect_market_regimes",
        bash_command="""
        echo "Detecting market regimes (ADX, ATR, volatility)..."
        
        docker exec spark-local /opt/spark/bin/spark-submit \
        --class RegimeDetector \
        --packages org.apache.spark:spark-avro_2.12:3.5.0 \
        /opt/spark-jars/polygon_story_2.12-0.1.0.jar
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # Pipeline flow
    ingest_task >> validate_task >> spark_curate_task >> spark_adjust_task >> spark_regime_task
