import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object MultiFormat extends App {
 val spark = SparkSession.builder()
  .appName("StockETL")
  .config("spark.sql.avro.compression.codec", "snappy")
  .getOrCreate()

 val datePath = args(0)

// Read Airflow JSON (always)
val rawDF = spark.read.json(s"/opt/airflow/data/landing/${datePath}_stocks.json")

val enrichedDF = rawDF.withColumn("return_pct", (col("close") - col("open")) / col("open"))
  .withColumn("date", to_date(col("timestamp")))

// Write multiple formats (configurable)
enrichedDF.write.mode("append")
  .format("avro")
  .option("compression", "snappy")
  .save(s"/processed/avro/$datePath")

enrichedDF.write.mode("append")
  .format("parquet")
  .partitionBy("symbol")  // Stock-wise partitioning
  .option("compression", "snappy")
  .save(s"/processed/parquet/$datePath")


}
