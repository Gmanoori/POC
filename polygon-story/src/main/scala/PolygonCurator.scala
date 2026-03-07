import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import TimeUtils._

object PolygonCurator extends App {
  
  val spark = SparkSession.builder()
    .appName("PolygonCurator")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  val executionDate = args(0)
  
  println(s"\n${"="*70}")
  println(s"POLYGON CURATOR - Execution Date: $executionDate")
  println(s"${"="*70}\n")

  val rawBasePath = "/opt/airflow/data/landing/polygon"
  val curatedBasePath = "/opt/airflow/data/curated"

  // ==========================================================================
  // CURATE OHLCV
  // ==========================================================================
  
  println("--- Curating OHLCV ---")
  
  val ohlcvRawPath = s"$rawBasePath/daily_ohlcv/ingestion_date=$executionDate"
  
  try {
    val rawOHLCV = spark.read.option("multiline", "true").json(ohlcvRawPath).cache()
    
    println(s"Raw OHLCV records: ${rawOHLCV.count()}")
    rawOHLCV.printSchema()
    // rawOHLCV.show(false)

    // Explode nested results array
    val explodedOHLCV = rawOHLCV
      .select(
        col("symbol"),
        col("market_timezone"),
        explode(col("payload.results")).as("result")
      )
    
    // Transform to canonical schema
    val curatedOHLCV = explodedOHLCV.select(
      col("symbol"),

      epochMsToMarketDate(
        col("result.t"),
        col("market_timezone")
      ).as("trade_date_market"),

      epochMsToUTCDate(
        col("result.t")
      ).as("trade_date_utc"),

      col("result.o").as("open"),
      col("result.h").as("high"),
      col("result.l").as("low"),
      col("result.c").as("close"),
      col("result.v").as("volume"),
      col("result.vw").as("vwap"),
      col("result.n").cast("int").as("transactions"),

      col("result.t").as("raw_timestamp_ms"),
      lit("polygon").as("provider")
    )
    
    println("OHLCV Schema:")
    curatedOHLCV.printSchema()
    
    println("Sample OHLCV Data:")
    curatedOHLCV.select("trade_date_market").show(false)

//    curatedOHLCV.show(5, truncate = false)
    
    // Write to CURATED (partitioned by trade_date_market)
    val ohlcvOutputPath = s"$curatedBasePath/ohlcv"
    
    curatedOHLCV.write
      .mode("overwrite")
      .partitionBy("trade_date_market")
      .parquet(ohlcvOutputPath)
    
    println(s"✓ OHLCV written to: $ohlcvOutputPath")
    
  } catch {
    case e: Exception =>
      println(s"✗ OHLCV curation failed: ${e.getMessage}")
      throw e
  }

  // ==========================================================================
  // CURATE DIVIDENDS
  // ==========================================================================
  
  println("\n--- Curating Dividends ---")
  
  val dividendsRawPath = s"$rawBasePath/dividends/ingestion_date=$executionDate"
  
  try {
    val rawDividends = spark.read.json(dividendsRawPath).cache()  // Add .cache() to materialize
    
    println(s"Raw Dividend files: ${rawDividends.count()}")
    
    // Explode nested results array
    val explodedDividends = rawDividends
      .select(
        col("symbol"),
        explode(col("payload.results")).as("div")
      )
    
    // Transform to canonical schema
    val curatedDividends = explodedDividends.select(
      col("symbol"),
      col("div.id").as("dividend_id"),
      col("div.cash_amount").cast("double").as("cash_amount"),
      col("div.currency"),
      to_date(col("div.declaration_date")).as("declaration_date"),
      to_date(col("div.ex_dividend_date")).as("ex_dividend_date"),
      to_date(col("div.record_date")).as("record_date"),
      to_date(col("div.pay_date")).as("pay_date"),
      col("div.dividend_type").as("distribution_type"),
      col("div.frequency").cast("int"),
      current_timestamp().as("ingestion_timestamp")
    )
    
    val dividendCount = curatedDividends.count()
    
    if (dividendCount > 0) {
      println("Dividends Schema:")
      curatedDividends.printSchema()
      
      println("Sample Dividend Data:")
      curatedDividends.show(5, truncate = false)
      
      // Write to CURATED (partitioned by ex_dividend_date)
      val dividendsOutputPath = s"$curatedBasePath/dividends"
      
      curatedDividends.write
        .mode("append")  // Append mode for dividends (historical data)
        .partitionBy("ex_dividend_date")
        .parquet(dividendsOutputPath)
      
      println(s"✓ Dividends written to: $dividendsOutputPath ($dividendCount records)")
    } else {
      println("⚠ No dividends found (this is normal if symbols don't pay dividends)")
    }
    
  } catch {
    case e: Exception =>
      println(s"✗ Dividends curation failed: ${e.getMessage}")
      // Don't throw - dividends are optional
      println("Continuing despite dividend failure...")
  }

  // ==========================================================================
  // SUMMARY
  // ==========================================================================
  
  println(s"\n${"="*70}")
  println("CURATION COMPLETE")
  println(s"${"="*70}\n")

  spark.stop()
}
