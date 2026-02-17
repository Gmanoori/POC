import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, lit}

object MultiFormat extends App {
 val spark = SparkSession.builder()
  .appName("StockETL")
  .config("spark.sql.avro.compression.codec", "snappy")
  .getOrCreate()

 val datePath = args(0)

// Read Airflow JSON (always)
val rawDF = spark.read.option("multiLine", true).json(s"/opt/airflow/data/landing/${datePath}_stocks.json")

// Convert nested symbol structure to rows - each symbol becomes a row
val symbols = rawDF.columns
val stocksDF = symbols.foldLeft(spark.emptyDataFrame) { (acc, symbol) =>
  val symbolDF = rawDF.select(
    lit(symbol).as("symbol"),
    col(s"`$symbol`.`1. open`").cast("double").as("open"),
    col(s"`$symbol`.`2. high`").cast("double").as("high"),
    col(s"`$symbol`.`3. low`").cast("double").as("low"),
    col(s"`$symbol`.`4. close`").cast("double").as("close"),
    col(s"`$symbol`.`5. volume`").cast("long").as("volume"),
    lit(datePath).as("date")
  )
  if (acc.isEmpty) symbolDF else acc.union(symbolDF)
}

val enrichedDF = stocksDF
  .withColumn("return_pct", (col("close") - col("open")) / col("open"))
  .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

// Write multiple formats (configurable) - using overwrite for idempotency
enrichedDF.write.mode("overwrite")
  .format("avro")
  .option("compression", "snappy")
  .partitionBy("date")  // Partition by date for idempotency
  .save(s"/processed/avro/")

enrichedDF.write.mode("overwrite")
  .format("parquet")
  .partitionBy("date", "symbol")  // Partition by date and symbol
  .option("compression", "snappy")
  .save(s"/processed/parquet/")


}
