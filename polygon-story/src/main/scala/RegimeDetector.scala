import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object RegimeDetector extends App {
  
  val spark = SparkSession.builder()
    .appName("RegimeDetector")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  println(s"\n${"="*70}")
  println(s"REGIME DETECTOR - Analyzing Market Conditions")
  println(s"${"="*70}\n")

  val curatedBasePath = "/opt/airflow/data/curated"
  val regimeOutputPath = s"$curatedBasePath/regime"

  // ==========================================================================
  // LOAD ADJUSTED OHLCV DATA
  // ==========================================================================
  
  println("--- Loading Adjusted OHLCV Data ---")
  
  val ohlcvAdjusted = spark.read
    .parquet(s"$curatedBasePath/ohlcv_adjusted")
    .select(
      col("symbol"),
      col("trade_date_market"),
      col("adjusted_open").as("open"),
      col("adjusted_high").as("high"),
      col("adjusted_low").as("low"),
      col("adjusted_close").as("close"),
      col("volume")
    )
  
  println(s"Loaded ${ohlcvAdjusted.count()} adjusted OHLCV records")
  
  // ==========================================================================
  // CALCULATE ATR (Average True Range) - 14 periods
  // ==========================================================================
  
  println("\n--- Calculating ATR (Volatility Measure) ---")
  
  val windowSpec = Window
    .partitionBy("symbol")
    .orderBy("trade_date_market")
    .rowsBetween(-13, 0)  // 14-day window (current + 13 previous)
  
  val withTR = ohlcvAdjusted
    .withColumn("prev_close", lag("close", 1).over(
      Window.partitionBy("symbol").orderBy("trade_date_market")
    ))
    .withColumn("tr", 
      greatest(
        col("high") - col("low"),                    // Today's range
        abs(col("high") - col("prev_close")),        // Gap up
        abs(col("low") - col("prev_close"))          // Gap down
      )
    )
  
  val withATR = withTR
    .withColumn("atr_14", avg("tr").over(windowSpec))
    .withColumn("atr_pct", (col("atr_14") / col("close")) * 100)  // ATR as % of price
  
  // ==========================================================================
  // CALCULATE ADX (Average Directional Index) - 14 periods
  // ==========================================================================
  
  println("--- Calculating ADX (Trend Strength) ---")
  
  // Step 1: Calculate +DM and -DM (Directional Movement)
  val withDM = withATR
    .withColumn("prev_high", lag("high", 1).over(
      Window.partitionBy("symbol").orderBy("trade_date_market")
    ))
    .withColumn("prev_low", lag("low", 1).over(
      Window.partitionBy("symbol").orderBy("trade_date_market")
    ))
    .withColumn("up_move", col("high") - col("prev_high"))
    .withColumn("down_move", col("prev_low") - col("low"))
    .withColumn("plus_dm",
      when(col("up_move") > col("down_move") && col("up_move") > 0, col("up_move"))
      .otherwise(0)
    )
    .withColumn("minus_dm",
      when(col("down_move") > col("up_move") && col("down_move") > 0, col("down_move"))
      .otherwise(0)
    )
  
  // Step 2: Smooth DM and TR (14-period average)
  val withSmoothedDM = withDM
    .withColumn("smoothed_plus_dm", avg("plus_dm").over(windowSpec))
    .withColumn("smoothed_minus_dm", avg("minus_dm").over(windowSpec))
    .withColumn("smoothed_tr", avg("tr").over(windowSpec))
  
  // Step 3: Calculate +DI and -DI (Directional Indicators)
  val withDI = withSmoothedDM
    .withColumn("plus_di", (col("smoothed_plus_dm") / col("smoothed_tr")) * 100)
    .withColumn("minus_di", (col("smoothed_minus_dm") / col("smoothed_tr")) * 100)
  
  // Step 4: Calculate DX (Directional Index)
  val withDX = withDI
    .withColumn("di_diff", abs(col("plus_di") - col("minus_di")))
    .withColumn("di_sum", col("plus_di") + col("minus_di"))
    .withColumn("dx", 
      when(col("di_sum") > 0, (col("di_diff") / col("di_sum")) * 100)
      .otherwise(0)
    )
  
  // Step 5: Calculate ADX (Average of DX)
  val withADX = withDX
    .withColumn("adx_14", avg("dx").over(windowSpec))
  
  // ==========================================================================
  // REGIME CLASSIFICATION
  // ==========================================================================
  
  println("--- Classifying Market Regimes ---")
  
  val withRegime = withADX
    .withColumn("regime",
      when(col("adx_14") > 25 && col("atr_pct") < 3, "trending_stable")
      .when(col("adx_14") > 25 && col("atr_pct") >= 3, "trending_volatile")
      .when(col("adx_14") <= 25 && col("atr_pct") >= 3, "range_volatile")
      .when(col("adx_14") <= 25 && col("atr_pct") < 3, "range_stable")
      .otherwise("unknown")
    )
    .withColumn("confidence",
      when(col("adx_14") > 30 || col("adx_14") < 20, 0.9)  // Strong signal
      .when(col("adx_14") > 27 || col("adx_14") < 23, 0.7)  // Moderate signal
      .otherwise(0.5)  // Weak signal (transition zone)
    )
  
  // ==========================================================================
  // SELECT FINAL COLUMNS
  // ==========================================================================
  
  val regimeData = withRegime
    .filter(col("adx_14").isNotNull && col("atr_14").isNotNull)  // Need 14 days of history
    .select(
      col("symbol"),
      col("trade_date_market"),
      col("regime"),
      col("confidence"),
      col("adx_14").cast("decimal(10,2)").as("adx"),
      col("atr_14").cast("decimal(10,2)").as("atr"),
      col("atr_pct").cast("decimal(10,2)").as("atr_percent"),
      col("plus_di").cast("decimal(10,2)"),
      col("minus_di").cast("decimal(10,2)"),
      col("close").cast("decimal(10,2)").as("close_price")
    )
  
  println("\nRegime Data Schema:")
  regimeData.printSchema()
  
  println("\nSample Regime Classifications:")
  regimeData
    .orderBy(col("symbol"), col("trade_date_market").desc)
    .show(20, truncate = false)
  
  // ==========================================================================
  // WRITE TO CURATED LAYER
  // ==========================================================================
  
  println(s"\n--- Writing Regime Data to: $regimeOutputPath ---")
  
  regimeData.write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("trade_date_market")
    .parquet(regimeOutputPath)
  
  println(s"✓ Regime data written successfully")
  
  // ==========================================================================
  // SUMMARY STATISTICS
  // ==========================================================================
  
  println("\n--- Regime Distribution ---")
  regimeData
    .groupBy("regime")
    .agg(
      count("*").as("count"),
      avg("confidence").cast("decimal(10,2)").as("avg_confidence"),
      avg("adx").cast("decimal(10,2)").as("avg_adx"),
      avg("atr_percent").cast("decimal(10,2)").as("avg_atr_pct")
    )
    .orderBy(desc("count"))
    .show(truncate = false)
  
  println(s"\n${"="*70}")
  println("REGIME DETECTION COMPLETE")
  println(s"${"="*70}\n")
  
  println("⚠ IMPORTANT: Only use signals that match the current regime!")
  println("  - Trending: Use momentum strategies")
  println("  - Range-bound: Use mean reversion")
  println("  - Volatile: Reduce position size or stay out")

  spark.stop()
}
