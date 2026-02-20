import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, lit}

object MultiFormat extends App {
 val spark = SparkSession.builder()
  .appName("StockETL")
  .config("spark.sql.avro.compression.codec", "snappy")
  .getOrCreate()

 val datePath = args(0)

// Read Airflow JSON (always) - now from fact_date folder structure
val rawDF = spark.read.option("multiLine", true).json(s"/opt/airflow/data/landing/fact_date=${datePath}/stocks.json")

println("=== Raw DataFrame Schema ===")
rawDF.printSchema()
// rawDF.show(false)

// Convert nested symbol structure to rows - each symbol becomes a row
val symbols = rawDF.columns
println(s"=== Found symbols: ${symbols.mkString(", ")} ===")

// Create individual DataFrames for each symbol and collect them
val symbolDataFrames = symbols.map { symbol =>
  rawDF.select(
    lit(symbol).as("symbol"),
    col(s"`$symbol`.`1. open`").cast("double").as("open"),
    col(s"`$symbol`.`2. high`").cast("double").as("high"),
    col(s"`$symbol`.`3. low`").cast("double").as("low"),
    col(s"`$symbol`.`4. close`").cast("double").as("close"),
    col(s"`$symbol`.`5. volume`").cast("long").as("volume"),
    lit(datePath).as("date")
  )
}

// Union all symbol DataFrames into one
val stocksDF = symbolDataFrames.reduce(_ union _)

println("=== Stocks DataFrame Schema ===")
stocksDF.printSchema()
// stocksDF.show(false)

val enrichedDF = stocksDF
  .withColumn("return_pct", (col("close") - col("open")) / col("open"))
  .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

// Write multiple formats (configurable) - using overwrite for idempotency
enrichedDF.write.mode("overwrite")
  .format("avro")
  .option("compression", "snappy")
  .partitionBy("date")  // Partition by date for idempotency
  .save(s"/opt/airflow/data/processed/avro/fact_date=${datePath}/")

enrichedDF.write.mode("overwrite")
  .format("parquet")
  .partitionBy("date", "symbol")  // Partition by date and symbol
  .option("compression", "snappy")
  .save(s"/opt/airflow/data/processed/parquet/fact_date=${datePath}/")


}
