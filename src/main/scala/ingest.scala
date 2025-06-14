import org.apache.spark.sql.SparkSession

object ingest extends App {
  println("Hello World")
  val spark = SparkSession.builder().appName("POC").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val df1 = spark.read
    .option("header", value = true)
    .csv("C:\\data\\POC\\CodeYodha_1.csv")

  println("="*100)
  println("Original DF count = " + df1.count())
  df1.printSchema()
  df1.show()
  println("="*100)

  val after = df1.dropDuplicates(Seq("E-mail", "Contact Number"))

//  Duplicate mails & phone numbers dropped
  println("="*100)
  println("After DF count = " + after.count())
  after.show()
  println("="*100)
}