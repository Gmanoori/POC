import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame

/**
 * CorporateActionsAdjuster - Calculates adjusted close prices
 * 
 * WHY THIS EXISTS:
 * ================
 * Stock splits and dividends create "fake" price movements that fool indicators.
 * 
 * Example without adjustment:
 * - Day 1: Close = $200
 * - Day 2: 2-for-1 split happens
 * - Day 3: Close = $100
 * 
 * Your moving average thinks price crashed 50%. WRONG.
 * You actually own 2 shares @ $100 = $200 total value.
 * 
 * This job fixes that by calculating what prices "would have been"
 * if corporate actions never happened.
 * 
 * HOW IT WORKS:
 * =============
 * 1. Load OHLCV (raw prices)
 * 2. Load splits (e.g., 2-for-1 means ratio = 2.0)
 * 3. Load dividends (e.g., $0.25 per share)
 * 4. Walk BACKWARD through time:
 *    - After split date: multiply all prior prices by split_ratio
 *    - After dividend date: add dividend_amount to all prior prices
 * 5. Result: "adjusted_close" column that shows true price movement
 * 
 * CRITICAL RULE:
 * ==============
 * ALL indicators (moving averages, RSI, etc.) MUST use adjusted_close.
 * Never use raw "close" for calculations.
 * 
 * Raw "close" is only for display (what you'd see on a broker screen).
 */
object CorporateActionsAdjuster extends App {
  
  val spark = SparkSession.builder()
    .appName("CorporateActionsAdjuster")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  println(s"\n${"="*70}")
  println("CORPORATE ACTIONS ADJUSTER - Calculating Adjusted Prices")
  println(s"${"="*70}\n")

  val curatedBasePath = "/opt/airflow/data/curated"

  // ==========================================================================
  // STEP 1: LOAD RAW OHLCV DATA
  // ==========================================================================
  
  println("--- Loading OHLCV Data ---")
  
  val ohlcv = spark.read.parquet(s"$curatedBasePath/ohlcv")
    .select(
      col("symbol"),
      col("trade_date_market"),
      col("open"),
      col("high"),
      col("low"),
      col("close"),
      col("volume")
    )
  
  println(s"OHLCV records loaded: ${ohlcv.count()}")
  
  // ==========================================================================
  // STEP 2: LOAD SPLITS DATA
  // ==========================================================================
  
  println("\n--- Loading Splits Data ---")
  
  val splits = try {
    spark.read.parquet(s"$curatedBasePath/splits")
      .select(
        col("symbol"),
        col("execution_date"),
        col("split_ratio")
      )
  } catch {
    case e: Exception =>
      println("⚠ No splits data found (creating empty DataFrame)")
      spark.emptyDataFrame
  }
  
  val splitCount = if (splits.columns.nonEmpty) splits.count() else 0
  println(s"Split events loaded: $splitCount")
  
  if (splitCount > 0) {
    println("Sample splits:")
    splits.show(5, truncate = false)
  }
  
  // ==========================================================================
  // STEP 3: LOAD DIVIDENDS DATA
  // ==========================================================================
  
  println("\n--- Loading Dividends Data ---")
  
  val dividends = try {
    spark.read.parquet(s"$curatedBasePath/dividends")
      .select(
        col("symbol"),
        col("ex_dividend_date"),
        col("cash_amount")
      )
      .filter(col("cash_amount").isNotNull && col("cash_amount") > 0)
  } catch {
    case e: Exception =>
      println("⚠ No dividends data found (creating empty DataFrame)")
      spark.emptyDataFrame
  }
  
  val dividendCount = if (dividends.columns.nonEmpty) dividends.count() else 0
  println(s"Dividend events loaded: $dividendCount")
  
  if (dividendCount > 0) {
    println("Sample dividends:")
    dividends.show(5, truncate = false)
  }
  
  // ==========================================================================
  // STEP 4: CALCULATE ADJUSTMENT FACTORS
  // ==========================================================================
  
  println("\n--- Calculating Adjustment Factors ---")
  
  /**
   * ADJUSTMENT LOGIC (walking backward through time):
   * 
   * For each symbol, for each date:
   * 1. Start with adjustment_factor = 1.0 (most recent date)
   * 2. Walk backward:
   *    - If we cross a split date: multiply factor by split_ratio
   *    - If we cross a dividend date: add (dividend / current_price) to factor
   * 3. adjusted_close = close * adjustment_factor
   * 
   * Example:
   * Date       Close   Event           Factor    Adjusted Close
   * Mar 10     $100    -               1.0       $100
   * Mar 9      $100    -               1.0       $100  
   * Mar 8      $200    2-for-1 split   2.0       $400  (multiply by 2)
   * Mar 7      $199    $1 dividend     2.01      $400  (2.0 + 0.01)
   */
  
  // Get all symbols
  val symbols = ohlcv.select("symbol").distinct().collect().map(_.getString(0))
  
  println(s"Processing ${symbols.length} symbols...")
  
  // Process each symbol separately (corporate actions are symbol-specific)
  val adjustedDataFrames = symbols.map { symbol =>
    println(s"\n  Processing: $symbol")
    
    // Get OHLCV for this symbol
    val symbolOHLCV = ohlcv
      .filter(col("symbol") === symbol)
      .orderBy(col("trade_date_market").desc)  // Most recent first
    
    // Get splits for this symbol
    val symbolSplits = if (splitCount > 0) {
      splits.filter(col("symbol") === symbol)
    } else {
      spark.emptyDataFrame
    }
    
    // Get dividends for this symbol
    val symbolDividends = if (dividendCount > 0) {
      dividends.filter(col("symbol") === symbol)
    } else {
      spark.emptyDataFrame
    }
    
    // Create cumulative adjustment factor using window function
    val windowSpec = Window
      .partitionBy("symbol")
      .orderBy(col("trade_date_market").desc)  // Descending (backward through time)
    
    // Join splits (left join - not all dates have splits)
    val withSplits = if (symbolSplits.columns.nonEmpty) {
      symbolOHLCV.join(
        symbolSplits,
        symbolOHLCV("trade_date_market") === symbolSplits("execution_date"),
        "left"
      ).select(
        symbolOHLCV("*"),
        coalesce(symbolSplits("split_ratio"), lit(1.0)).as("split_ratio")
      )
    } else {
      symbolOHLCV.withColumn("split_ratio", lit(1.0))
    }
    
    // Join dividends (left join - not all dates have dividends)
    val withDividends = if (symbolDividends.columns.nonEmpty) {
      withSplits.join(
        symbolDividends,
        withSplits("trade_date_market") === symbolDividends("ex_dividend_date"),
        "left"
      ).select(
        withSplits("*"),
        coalesce(symbolDividends("cash_amount"), lit(0.0)).as("dividend_amount")
      )
    } else {
      withSplits.withColumn("dividend_amount", lit(0.0))
    }
    
    // Calculate cumulative split factor (product of all splits going backward)
    val withCumulativeSplit = withDividends
      .withColumn(
        "cumulative_split_factor",
        exp(sum(log(col("split_ratio"))).over(windowSpec))
      )
    
    // Calculate cumulative dividend adjustment
    // For simplicity: dividend adjustment = sum of (dividend / close) going backward
    val withCumulativeDividend = withCumulativeSplit
      .withColumn(
        "dividend_adjustment",
        sum(when(col("close") > 0, col("dividend_amount") / col("close")).otherwise(0.0))
          .over(windowSpec)
      )
    
    // Final adjustment factor = split_factor * (1 + dividend_adjustment)
    val withAdjustmentFactor = withCumulativeDividend
      .withColumn(
        "adjustment_factor",
        col("cumulative_split_factor") * (lit(1.0) + col("dividend_adjustment"))
      )
    
    // Calculate adjusted OHLC
    val adjusted = withAdjustmentFactor.select(
      col("symbol"),
      col("trade_date_market"),
      col("open"),
      col("high"),
      col("low"),
      col("close"),
      (col("open") * col("adjustment_factor")).as("adjusted_open"),
      (col("high") * col("adjustment_factor")).as("adjusted_high"),
      (col("low") * col("adjustment_factor")).as("adjusted_low"),
      (col("close") * col("adjustment_factor")).as("adjusted_close"),
      col("volume"),
      col("adjustment_factor")
    )
    
    val recordCount = adjusted.count()
    println(s"    Adjusted $recordCount records")
    
    adjusted
  }
  
  // Union all symbols back together
  val allAdjusted = adjustedDataFrames.reduce(_ union _)
  
  println(s"\n✓ Total adjusted records: ${allAdjusted.count()}")
  
  // ==========================================================================
  // STEP 5: WRITE ADJUSTED OHLCV
  // ==========================================================================
  
  println("\n--- Writing Adjusted OHLCV ---")
  
  val outputPath = s"$curatedBasePath/ohlcv_adjusted"
  
  allAdjusted.write
    .mode("overwrite")
    .partitionBy("trade_date_market")
    .parquet(outputPath)
  
  println(s"✓ Adjusted OHLCV written to: $outputPath")
  
  // Show sample comparison
  println("\n--- Sample: Raw vs Adjusted Prices ---")
  allAdjusted
    .select("symbol", "trade_date_market", "close", "adjusted_close", "adjustment_factor")
    .orderBy(col("symbol"), col("trade_date_market").desc)
    .show(20, truncate = false)
  
  // ==========================================================================
  // SUMMARY
  // ==========================================================================
  
  println(s"\n${"="*70}")
  println("ADJUSTMENT COMPLETE")
  println(s"${"="*70}")
  println(s"Symbols processed: ${symbols.length}")
  println(s"Split events applied: $splitCount")
  println(s"Dividend events applied: $dividendCount")
  println(s"Output: $outputPath")
  println(s"\n⚠ IMPORTANT: All indicators MUST use 'adjusted_close', not 'close'")
  println(s"${"="*70}\n")

  spark.stop()
}
