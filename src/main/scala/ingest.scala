import org.apache.spark.sql.{SparkSession}


object ingest extends App {
  println("Hello World")
  val spark = SparkSession
    .builder()
    .appName("POC")
//    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val jdbc_url = "jdbc:postgresql://localhost:5432/mydb"
  val properties = new java.util.Properties() {{
    put("user", "myuser")
    put("password", "mypass")
    put("driver", "org.postgresql.Driver")
  }}


  import spark.implicits._

  val df1 = spark.read
    .option("header", value = true)
    .csv("C:\\data\\POC\\CodeYodha_1.csv")

//  val query =
//    """
//  CREATE TABLE IF NOT EXISTS your_table_name (
//    Timestamp STRING,
//    Name STRING,
//    Email STRING,
//    Contact_Number STRING,
//    Based_Out_Of STRING,
//    Course STRING,
//    Specialization STRING,
//    Technical_Skills STRING,
//    Next_Steps STRING,
//    Interested_In_Internship STRING,
//    Interested_In_Webinars STRING,
//    Dream_Community_Features STRING,
//    Preferred_Problem_Types STRING
//  )
//  USING hive
//  """

  df1.write.jdbc(jdbc_url, "my_table", properties)

  println("Write Successful")
//  println("="*100)
//  println("Original DF count = " + df1.count())
//  df1.printSchema()
//  df1.show()
//  println("="*100)

  val after = df1.dropDuplicates(Seq("E-mail", "Contact Number"))

//  Duplicate mails & phone numbers dropped
//  println("="*100)
//  println("After DF count = " + after.count())
//  after.show()
//  println("="*100)
}