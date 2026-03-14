# Financial Intelligence System

A **layered financial data pipeline** that structures market uncertainty through disciplined data engineering. Built with **Apache Spark, Apache Airflow, Scala, Python, and Docker**.

This is NOT a trading bot. This system quantifies risk, detects market regimes, and enforces institutional-style reasoning through clean data architecture.

---

## What This System Does

**Layer 0: Data Hygiene**
- Ingests OHLCV, splits, and dividends from Polygon API
- Calculates corporate action adjustments (splits/dividends)
- Ensures all price data is historically accurate

**Layer 1: Regime Detection**
- Calculates ADX (trend strength) and ATR (volatility)
- Classifies markets: Trending/Range × Stable/Volatile
- Provides confidence scores for each regime

**Future Layers:**
- Layer 2: Technical indicators (RSI, MACD, Bollinger Bands)
- Layer 3: Signal generation with regime awareness
- Layer 4: Risk management and position sizing

---

## Core Technologies

- **Apache Spark (Scala)** - Distributed data processing
- **Apache Airflow (Python)** - Pipeline orchestration
- **Docker** - Environment isolation
- **Polygon.io API** - Market data source

---

## Architecture

```
Polygon API → Landing (Raw JSON) → Curated (Parquet) → Adjusted (Corporate Actions) → Regime (Market State)
     ↓              ↓                    ↓                      ↓                           ↓
  Airflow      Immutable           Queryable            Split/Div Adjusted          ADX/ATR/Classification
```

**Pipeline Flow:**
1. **Ingest** - Python fetches OHLCV, splits, dividends (rate-limited)
2. **Validate** - Ensures all symbols succeeded before proceeding
3. **Curate** - Spark transforms raw JSON to typed Parquet
4. **Adjust** - Applies corporate actions for accurate historical prices
5. **Detect** - Classifies market regimes (trending/range, stable/volatile)

---

## Key Features

- **Corporate Action Adjustments** - Accurate historical prices accounting for splits/dividends
- **Regime Detection** - ADX/ATR-based market classification
- **Rate-Limited Ingestion** - Respects Polygon free tier (5 req/min)
- **Idempotent Pipeline** - Safe reruns, no duplicates
- **Partitioned Storage** - Efficient date-range queries

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 11+
- Scala & SBT
- Polygon.io API key (free tier works)

### Setup

1. **Clone and configure:**
```bash
git clone https://github.com/Gmanoori/POC.git
cd POC
```

2. **Add API key to `config/polygon_config.json`:**
```json
{
  "api_key": "your_polygon_api_key",
  "symbols": ["AAPL", "TSLA", "NVDA"],
  "rate_limit_delay": 12
}
```

3. **Build Spark jobs:**
```bash
cd polygon-story
sbt package
```

4. **Start services:**
```bash
docker-compose up -d
```

5. **Access Airflow UI:** http://localhost:8080
   - Trigger DAG: `polygon_intelligence_pipeline`
   - Set custom date or use scheduled run

### Verify Results

```bash
# Check regime classifications
docker exec -it spark-local /opt/spark/bin/spark-shell

scala> val regime = spark.read.parquet("/opt/airflow/data/curated/regime")
scala> regime.show()
```

---

## Project Structure

```
POC/
├── dags/                           # Airflow DAGs
│   └── polygon_intelligence_dag.py # Main pipeline (5 tasks)
├── polygon-story/                  # Spark jobs (Scala)
│   ├── src/main/scala/
│   │   ├── PolygonCurator.scala           # Raw → Curated
│   │   ├── CorporateActionsAdjuster.scala # Apply splits/dividends
│   │   ├── RegimeDetector.scala           # ADX/ATR/Classification
│   │   └── TimeUtils.scala                # Timezone helpers
│   └── build.sbt
├── spark-jars/                     # Compiled JARs
├── data/
│   ├── landing/polygon/            # Raw API responses (immutable)
│   │   ├── daily_ohlcv/
│   │   ├── splits/
│   │   └── dividends/
│   └── curated/                    # Processed data (Parquet)
│       ├── ohlcv/
│       ├── ohlcv_adjusted/
│       ├── splits/
│       ├── dividends/
│       └── regime/
├── config/
│   └── polygon_config.json         # API key, symbols, rate limits
├── doc/intelligence-system/        # Architecture docs
└── docker-compose.yml
```

---

## Key Design Principles

- **Idempotent** - Safe to rerun any date
- **Layered** - Each layer reads from previous, never modifies
- **Fail-fast** - Validation blocks pipeline if data incomplete
- **Auditable** - Raw layer preserves exact API responses
- **Regime-aware** - Signals filtered by market conditions

---

## Documentation

- `doc/intelligence-system/FLOW.md` - Complete system design
- `doc/intelligence-system/LAYER0-COMPLETE.md` - Data hygiene layer
- `doc/intelligence-system/LAYER1-REGIME.md` - Regime detection details
- `doc/intelligence-system/SETUP.md` - Deployment guide

---

## License

No license specified.
